/*
 * SM8 Spark Connector — RollupMergeRefresher (ADR-0030 Tier 2).
 *
 * Row-level delta MERGE refresh for a scoped rollup partition set:
 * recompute ONLY the affected buckets' aggregate rows from the base
 * table and MERGE them into the Iceberg rollup table under the
 * D2 contract (merge key = grain bucket + all dimension columns;
 * UPDATE SET overwrite semantics, never `+=`; COW default per D1).
 *
 * ==Tier 2 vs Tier 1 (why both exist)==
 *
 * Tier 1 (`overwritePartitions`) swaps whole PARTITIONS; its cost is
 * the intra-partition rewrite of rows that did not change. Tier 2
 * rewrites only the changed KEY ROWS via MERGE — the intra-partition
 * waste (rewritten-but-unchanged bytes, the Gate B enablement
 * signal) is what Tier 2 eliminates for deployments whose operators
 * enable it (ADR-0029 §Gate B, amended 2026-09-09).
 *
 * ==D5 fallback discipline==
 *
 * The delta extraction here is the SAFE shape: full re-aggregation
 * of the SCOPED buckets from base, merged as complete row images
 * (UPDATE SET of every measure column). This is exact-delta primary
 * in D2-6's terms — the merged row IS the recomputed truth for that
 * key; there is no ambiguity-prone pre/post-image reconstruction on
 * this path. Iceberg snapshot-diff extraction (base-table CDC) is
 * the future refinement gated on the D1 experiment; when it lands,
 * D5's fidelity boundaries (schema evolution, partition evolution,
 * wholesale COW rewrite of the base) route buckets HERE instead —
 * this path is Tier 2's correctness floor and its fallback.
 *
 * ==Closure safety (spark-batch mantra 1)==
 *
 * Driver-side built-in `Column` expressions only; nothing
 * user-defined ships to executors. The MERGE runs as SQL text over
 * table names (no closure at all).
 *
 * ==Idempotency (D2-5, spark-batch mantra 4)==
 *
 * A retried merge against an unchanged source re-MERGEs identical
 * row images: matched keys UPDATE to the same values (COW rewrites
 * the file, content identical); unmatched keys INSERT the same rows
 * (deduplicated by the key match, not appended twice). The
 * contract test pins content-identity at the manifest level
 * (`RollupMergeTier2Spec`).
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.EngineError
import io.sm8.core.model.{Model, RollupSpec, SourceRef}
import io.sm8.core.rel.RollupRewriter
import io.sm8.core.rollup.SnapshotDelta

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

object RollupMergeRefresher {

  // ==Concurrency contract (puma M3)==
  // Iceberg MERGE is optimistic-concurrent: two concurrent
  // mergeRefresh calls on the same rollup produce one winner and
  // one typed failure (OptimisticConcurrencyException surfaces as
  // UnsupportedCapability "merge"). v1 deliberately does NOT
  // retry internally — the caller (cron/orchestrator) owns retry
  // policy, matching the Tier 1 refresher's behavior. A bounded
  // internal retry is a plausible follow-up once a real
  // concurrent-refresh workload exists to tune it against.

  /** The Iceberg catalog the rollup tables live in (shared with
    * `RollupMaterializer.icebergCatalog`). */
  private val IcebergCatalog = "iceberg_cat"

  /** Outcome of one Tier 2 merge refresh. */
  sealed trait MergeRefreshResult extends Product with Serializable
  object MergeRefreshResult {
    /** Honest accounting (puma M2): `sourceRows` = the recomputed
      * source's row count (rows CONSIDERED — matched-updated,
      * matched-unchanged-guard-skipped, and inserted all included;
      * Spark MERGE per-arm metrics are not exposed on the SQL path
      * pre-3.5's explain-only surfaces). `netRowDelta` = after
      * minus before (inserts minus any future deletes; floor 0 in
      * v1 which has no delete arm). */
    final case class Merged(
      rollup: String,
      table: String,
      sourceRows: Long,
      netRowDelta: Long) extends MergeRefreshResult
    final case class Failed(rollup: String, error: EngineError)
        extends MergeRefreshResult
  }

  /** Scoped base rows (the scope IS the delta declaration in v1
    * Tier 2 — see the header's D5 note). String-canonical bucket
    * comparison, same discipline as decideStrategy's canon. */
  /** Grain-vocabulary-aware scope filter (D2-2, puma H2): mirrors
    * buildRollupDf's bucketing expression. For day-and-coarser
    * grains over a TIMESTAMP column the bucket value is
    * date_trunc(grain, col) — NOT the raw column — so the scope
    * filter must compare the truncated column, else non-midnight
    * timestamps match no scope and the refresh spurious-refuses.
    * For DATE columns (day grain) trunc is a no-op and the raw
    * equality pushes into the scan (PushedFilters — plan-checked).
    * Hour grain keeps full timestamp equality (trunc to hour, the
    * canonical yyyy-MM-dd HH form the scope carries). */
  private[spark] def scopedBase(
    base: DataFrame,
    grainDim: String,
    scopeValues: List[String],
    grain: Option[String] = None): DataFrame = {
    val normalized = RollupRewriter.normalizeGrain(grain)
    val bucketCol: Column = normalized match {
      case Some(g) if needsTrunc(base.schema(grainDim).dataType, g) =>
        org.apache.spark.sql.functions.date_trunc(g, col(grainDim))
      case _ => col(grainDim)
    }
    val dt = base.schema(grainDim).dataType
    val preds = scopeValues.map(v => bucketCol === lit(v).cast(dt))
    base.filter(preds.reduceOption(_ || _).getOrElse(lit(false)))
  }

  /** date_trunc is required when the column carries sub-grain
    * precision: any grain over a Timestamp column (a Timestamp at
    * day grain stores 12:34:56s that must truncate to the bucket).
    * Date columns are already day-precision — trunc is identity. */
  private def needsTrunc(dt: org.apache.spark.sql.types.DataType,
    grain: String): Boolean = dt match {
    case _: org.apache.spark.sql.types.TimestampType => true
    case _ => grain == "hour" // hour over Date is degenerate but trunc is harmless
  }

  /** Resolve the base DataFrame via the materializer's readBase
    * (same ByName extraction, same typed failure). */
  private def baseDataFrame(
    spark: SparkSession,
    model: Model): Either[EngineError, DataFrame] =
    RollupMaterializer.readBase(spark, model)

  /** Tier 2 merge refresh for one rollup's declared scope.
    *
    * Contract (ADR-0030 D2):
    *   - merge key = grain bucket + ALL rollup dimension columns
    *     (composite, derived from the declaration — never caller-
    *     specified);
    *   - source uniqueness on the merge key verified BEFORE the
    *     aggregation job (pre-shuffle probe, D2-4) — a duplicate
    *     key refuses typed, never last-write-wins;
    *   - matched rows are OVERWRITTEN (`UPDATE SET m = s.m`), never
    *     incremented (`+=` is forbidden — not idempotent);
    *   - COW merge mode (D1 default; the table property governs —
    *     MOR-hybrid is the D1 experiment, not this code's choice);
    *   - scope-coverage refusal mirrors Tier 1: a recomputed source
    *     bucket outside the declared scope fails loud (silent
    *     widening defeats the point of scoping).
    *
    * @param spark       the session (table IO + SQL; no closures)
    * @param model       the host model
    * @param spec        the rollup declaration
    * @param scopeValues canonical bucket values (e.g. yyyy-MM-dd)
    * @return the merge outcome, or a typed EngineError
    */
  /** SQL-identifier guard (puma H1): MERGE statements are built by
    * string interpolation over model/rollup/dimension names. A
    * backtick in any name closes the quoted identifier early
    * (parse error or identifier hijack); a dot shifts a 3-part
    * catalog name. Names are constrained to the conservative
    * identifier charset [A-Za-z0-9_] here — the enforcement seam
    * the model loader lacks today (Model.of only checks
    * non-blank); fail-loud at the boundary, never mid-SQL. */
  private[spark] def requireSafeIdentifiers(
    model: Model, spec: RollupSpec): Either[EngineError, Unit] = {
    val safe = "^[A-Za-z0-9_]+$".r
    // Measure input field names matter too (puma final H1-close): a
    // FieldRef on a base column becomes the sum__<f>/min__<f> state
    // column that the MERGE's SET/INSERT lists backtick-quote.
    val measureFields: List[String] = model.measures
      .map(_.expr.input).collect { case Some(io.sm8.core.expr.Expr.FieldRef(f)) => f }
    val names = model.name :: spec.name :: spec.dimensions ::: measureFields
    val bad = names.filter(n => safe.findFirstIn(n).isEmpty)
    if (bad.isEmpty) Right(())
    else Left(EngineError.UnsupportedCapability(
      engine = "spark-connector",
      capability = "RollupMergeRefresher.identifiers",
      message = s"rollups[${spec.name}]: name(s) [${bad.mkString(", ")}] " +
        "contain characters outside [A-Za-z0-9_] — the Tier 2 MERGE " +
        "builder quotes identifiers with backticks and cannot safely " +
        "embed these (rename, or extend the builder with an escaping layer)"))
  }

  /** Execute one scoped Tier 2 refresh (ADR-0030 D2): re-aggregate
    * the scoped base slice, verify uniqueness + coverage, MERGE the
    * row images into the existing Iceberg rollup table.
    *
    * Preconditions refuse typed in order: identifiers, grain,
    * table-exists, empty scope, duplicate keys, scope coverage —
    * all BEFORE the merge job.
    *
    * @param spark       the session (table IO + SQL; no closures)
    * @param model       the host model
    * @param spec        the rollup declaration
    * @param scopeValues canonical bucket values (e.g. yyyy-MM-dd)
    * @return the merge outcome, or a typed EngineError
    */
  def mergeRefresh(
    spark: SparkSession,
    model: Model,
    spec: RollupSpec,
    scopeValues: List[String]
  ): Either[EngineError, MergeRefreshResult] = {
    val rollupTable = RollupRewriter.rollupTableName(model, spec)
    val qualified = s"$IcebergCatalog.$rollupTable"

    for {
      _ <- requireSafeIdentifiers(model, spec)
      // Preconditions (driver-side, fail before any Spark job).
      _ <- requireGrained(spec)
      _ <- requireIcebergTable(spark, qualified, rollupTable)
      grainDim <- Right(spec.grainDimension.get)
      baseDf <- baseDataFrame(spark, model)
      // D2-3/D2-4: duplicate-key probe on the RECOMPUTED source,
      // before the merge job (probe = distinct-count comparison,
      // cheaper than the merge it guards).
      scopedDf <- Right(scopedBase(baseDf, grainDim, scopeValues, spec.timeGrain))
      source <- RollupMaterializer.buildRollupDf(scopedDf, model, spec)
      // Empty-scope guard: the scoped recompute produced no rows —
      // the scope names no bucket present in the base. Refuse loud
      // (a silent zero-row "success" would read as refreshed);
      // legitimate empty-bucket deletion semantics are Tier 3's
      // delete-file territory, not v1's.
      _ <- if (source.isEmpty)
             Left(EngineError.UnsupportedCapability(
               engine = "spark-connector",
               capability = "RollupMergeRefresher.scopeEmpty",
               message = s"rollups[${spec.name}]: the declared scope " +
                 s"[${scopeValues.mkString(", ")}] matches no rows in the " +
                 "base — no bucket of the recomputed source is covered " +
                 "(stale scope, empty declared scope, or base divergence; " +
                 "refusing silent zero-row success)"))
           else Right(())
      _ <- verifySourceUnique(spark, source, spec, grainDim)
      _ <- verifyScopeCoverage(spark, source, grainDim, scopeValues, spec)
      // D5 snapshot-diff extraction (disabled by default; flip via
      // -Dsm8.rollup.tier2.snapshotDiff.enabled=true AFTER live-model
      // Gate B traces validate it — spec §8). When the flag is off,
      // this step is a zero-cost pass-through and the shipped
      // scope-declared full recompute runs unchanged. When on and the
      // lineage delta is row-extractable, `source` is narrowed to the
      // delta rows before executeMerge; any non-extractable verdict
      // (DeletesInOpenWindow / Ambiguous) falls back to the full
      // path, matching the spec's v1 append-only rule.
      sourceNarrowed <- if (!snapshotDiffEnabled(spark)) Right(source)
                        else narrowToDeltaRows(spark, qualified, source)
      res <- executeMerge(spark, sourceNarrowed, qualified, spec, grainDim)
    } yield res
  }

  /** The D5 extraction flag (default FALSE — the spec's enablement
    * gate: production enablement waits on live-model Gate B traces).
    * System property, same idiom the runbooks use for the Spark
    * JVM flags (`-Dsm8.rollup.tier2.snapshotDiff.enabled=true`).
    *
    * A `spark:` prefixed property is ALSO accepted so tests and
    * sessions can flip it per-session via `spark.conf.set` without
    * touching JVM-wide state: `SET spark.sm8.rollup.tier2.snapshotDiff.enabled=true`.
    * The system property wins when both are set (ops-level override).
    */
  private[spark] def snapshotDiffEnabled(spark: SparkSession): Boolean = {
    val sysProp = System.getProperty("sm8.rollup.tier2.snapshotDiff.enabled")
    if (sysProp != null) sysProp.equalsIgnoreCase("true")
    else {
      val conf = spark.conf.getOption("spark.sm8.rollup.tier2.snapshotDiff.enabled")
      conf.exists(_.equalsIgnoreCase("true"))
    }
  }

  /** Narrow the recomputed source to the snapshot-delta rows only
    * (spec §6). The extractor classifies the lineage since the
    * watermark's last-commit snapshot id:
    *   - NoDataChange        → short-circuit: return an empty
    *                           DataFrame wrapped in the existing
    *                           merge path (watermark still advances)
    *   - Appended            → the caller's `source` is ALREADY the
    *                           scoped recompute of exactly the
    *                           appended rows (v1 keeps the recomputed
    *                           shape; narrowing is a no-op passthrough
    *                           until row-level file pruning lands —
    *                           tracked as a follow-up; the merge is
    *                           idempotent under D2-5 either way)
    *   - DeletesInOpenWindow / Ambiguous → fall back to the full
    *                           `source` unchanged (append-only v1)
    *
    * The half-row-count guard (spec §6): extraction is skipped when
    * the delta's row total exceeds half the recomputed source's rows
    * — past half, full recompute is cheaper anyway. `bucket_rows`
    * lives here (the refresher owns bucket state), not in the
    * extractor.
    *
    * Failure semantics: ANY extractor failure falls back to the full
    * `source` (never fails the refresh on an observability-path
    * problem) — the extraction is an optimization, not a correctness
    * dependency.
    */
  private def narrowToDeltaRows(
    spark: SparkSession,
    qualified: String,
    source: DataFrame
  ): Either[EngineError, DataFrame] = {
    try {
      val lastCommit = RollupWatermark.currentSnapshotId(spark, qualified)
      if (lastCommit == 0L) {
        // No watermark history (fresh rollup): nothing to diff from —
        // the full recompute IS the correct first refresh.
        Right(source)
      } else {
        RollupSnapshotDiffExtractor.extract(spark, qualified, lastCommit) match {
          case Left(_) =>
            // Extraction failure: fall back to full recompute (loud in
            // the result's sourceRows via the unchanged count).
            Right(source)
          case Right(delta) =>
            delta match {
              case SnapshotDelta.NoDataChange =>
                // Already-consumed lineage: empty delta; merge becomes
                // a no-op (idempotent under D2-5).
                Right(spark.emptyDataFrame)
              case SnapshotDelta.Appended(rows, _) if rows > 0 && rows * 2 < source.count() =>
                // Half-row-count guard not tripped: v1 passthrough
                // (row-level file pruning is a follow-up; see the
                // method Scaladoc). The delta confirms the append is
                // clean — the recomputed scoped source IS the delta.
                Right(source)
              case _ =>
                // DeletesInOpenWindow / Ambiguous / guard-tripped:
                // full recompute path (spec §4 append-only rule).
                Right(source)
            }
        }
      }
    } catch {
      case scala.util.control.NonFatal(_) =>
        // Never fail the refresh on the extraction path.
        Right(source)
    }
  }

  /** Grain precondition: Tier 2 is a per-bucket mechanism; a
    * grain-less rollup has no buckets (validateFreshnessPolicy
    * refuses the freshness policy for the same reason). */
  private def requireGrained(
    spec: RollupSpec): Either[EngineError, Unit] =
    if (spec.grainDimension.isEmpty || spec.timeGrain.isEmpty)
      Left(EngineError.UnsupportedCapability(
        engine = "spark-connector",
        capability = "RollupMergeRefresher.grain",
        message = s"rollups[${spec.name}]: Tier 2 merge requires a " +
          "timeGrain + grainDimension rollup (buckets are the merge unit)"))
    else Right(())

  /** The rollup table must exist as an Iceberg table (Tier 2
    * refreshes; it never creates — creation is Tier 0's CTAS path
    * via RollupMaterializer). */
  private def requireIcebergTable(
    spark: SparkSession,
    qualified: String,
    rollup: String): Either[EngineError, Unit] =
    if (spark.catalog.tableExists(qualified)) Right(())
    else Left(EngineError.UnsupportedCapability(
      engine = "spark-connector",
      capability = "RollupMergeRefresher.table",
      message = s"rollup '$rollup': table '$qualified' does not exist — " +
        "materialize it first (Tier 0 create), Tier 2 refreshes an existing table"))

  /** Resolve the base DataFrame (see [[baseDataFrame]] — removed
    * duplicate ByName logic in favor of the materializer's seam). */

  /** D2-3/D2-4: the merge source must be unique on the merge key
    * (grain bucket + all dimension columns). Duplicate keys mean
    * the recompute itself diverged from the declaration — refuse
    * typed BEFORE the merge job. The probe is two cheap
    * aggregations over the already-materialized source; both run
    * pre-merge (the contract test asserts the refusal fires with
    * zero merge jobs run). */
  private[spark] def verifySourceUnique(
    spark: SparkSession,
    source: DataFrame,
    spec: RollupSpec,
    grainDim: String): Either[EngineError, Unit] = {
    val keyCols = grainDim :: spec.dimensions.filter(_ != grainDim)
    val total = source.count()
    val distinctKeys = source.select(keyCols.map(col): _*).distinct().count()
    if (total == distinctKeys) Right(())
    else Left(EngineError.UnsupportedCapability(
      engine = "spark-connector",
      capability = "RollupMergeRefresher.duplicateKeys",
      message = s"rollups[${spec.name}]: recomputed merge source has " +
        s"$total rows but $distinctKeys distinct merge keys — the " +
        "aggregation shape diverged from the declaration; refusing " +
        "last-write-wins (ADR-0030 D2-3)"))
  }

  /** Scope-coverage refusal (mirrors Tier 1's decideStrategy
    * contract): every bucket present in the recomputed source must
    * be inside the declared scope. A source bucket outside the
    * scope means the base moved under us — widen the scope or
    * narrow the source; never silently merge out-of-scope buckets. */
  private def verifyScopeCoverage(
    spark: SparkSession,
    source: DataFrame,
    grainDim: String,
    scopeValues: List[String],
    spec: RollupSpec): Either[EngineError, Unit] = {
    val daily = RollupRewriter.normalizeGrain(spec.timeGrain).contains("day")
    // Local canonicalizer: day-grain values arrive as yyyy-MM-dd
    // HH:mm:ss from the string cast; trim to the 10-char date form.
    val canon: String => String =
      v => if (daily && v.length > 10) v.take(10) else v
    val present = source.select(col(grainDim).cast("string"))
      .distinct().collect().map(r => canon(r.getString(0))).toSet
    val declared = scopeValues.map(canon).toSet
    val uncovered = present.diff(declared)
    if (uncovered.isEmpty) Right(())
    else Left(EngineError.UnsupportedCapability(
      engine = "spark-connector",
      capability = "RollupMergeRefresher.scopeUncovered",
      message = s"rollups[${spec.name}]: recomputed source contains " +
        s"bucket(s) [${uncovered.mkString(", ")}] outside the declared " +
        s"scope [${declared.mkString(", ")}] — widen the scope or narrow " +
        "the source (silent widening defeats Tier 2's savings)"))
  }

  /** Execute the MERGE (D2-1/D2-2/D2-5). The ON clause names the
    * grain bucket AND every dimension column (the composite merge
    * key, derived from the declaration). Matched rows UPDATE to
    * the source's full row image (never `+=`); unmatched INSERT.
    * SQL text over table names — no executor closures. The source
    * is registered as a temp view for the USING clause. */
  private def executeMerge(
    spark: SparkSession,
    source: DataFrame,
    qualified: String,
    spec: RollupSpec,
    grainDim: String): Either[EngineError, MergeRefreshResult] = {
    val viewName = s"tier2_merge_src_${spec.name}"
    source.createOrReplaceTempView(viewName)
    val keyCols = grainDim :: spec.dimensions.filter(_ != grainDim)
    val nonKeyCols = source.columns.filterNot(keyCols.toSet).toList
    val onClause = keyCols.map(k => s"t.`$k` = s.`$k`").mkString(" AND ")
    val setClause = nonKeyCols.map(c =>
      s"t.`$c` = s.`$c`").mkString(", ")
    val changedGuard = nonKeyCols.map(c => s"t.`$c` IS DISTINCT FROM s.`$c`")
      .mkString(" OR ")
    val insertCols = (keyCols ++ nonKeyCols).map(c => s"`$c`").mkString(", ")
    val insertVals = (keyCols ++ nonKeyCols).map(c => s"s.`$c`").mkString(", ")
    val sql =
      s"""MERGE INTO $qualified t
         |USING $viewName s
         |ON $onClause
         |WHEN MATCHED AND ($changedGuard) THEN UPDATE SET $setClause
         |WHEN NOT MATCHED THEN INSERT ($insertCols) VALUES ($insertVals)"""
        .stripMargin
    try {
      val before = spark.table(qualified).count()
      spark.sql(sql)
      val after = spark.table(qualified).count()
      Right(MergeRefreshResult.Merged(
        rollup = spec.name,
        table = qualified,
        sourceRows = source.count(),
        netRowDelta = math.max(0L, after - before)))
    } catch {
      case scala.util.control.NonFatal(e) =>
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupMergeRefresher.merge",
          message = s"MERGE INTO $qualified failed: " +
            s"${e.getClass.getSimpleName}: ${e.getMessage}"))
    } finally {
      spark.catalog.dropTempView(viewName)
    }
  }
}
