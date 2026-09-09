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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object RollupMergeRefresher {

  /** The Iceberg catalog the rollup tables live in (shared with
    * `RollupMaterializer.icebergCatalog`). */
  private val IcebergCatalog = "iceberg_cat"

  /** Outcome of one Tier 2 merge refresh. */
  sealed trait MergeRefreshResult extends Product with Serializable
  object MergeRefreshResult {
    final case class Merged(
      rollup: String,
      table: String,
      rowsMerged: Long,
      rowsInserted: Long) extends MergeRefreshResult
    final case class Failed(rollup: String, error: EngineError)
        extends MergeRefreshResult
  }

  /** Scoped base rows (the scope IS the delta declaration in v1
    * Tier 2 — see the header's D5 note). String-canonical bucket
    * comparison, same discipline as decideStrategy's canon. */
  private[spark] def scopedBase(
    base: DataFrame,
    grainDim: String,
    scopeValues: List[String]): DataFrame = {
    val grainCol = col(grainDim).cast("string")
    base.filter(grainCol.isInCollection(scopeValues))
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
  def mergeRefresh(
    spark: SparkSession,
    model: Model,
    spec: RollupSpec,
    scopeValues: List[String]
  ): Either[EngineError, MergeRefreshResult] = {
    val rollupTable = RollupRewriter.rollupTableName(model, spec)
    val qualified = s"$IcebergCatalog.$rollupTable"

    for {
      // Preconditions (driver-side, fail before any Spark job).
      _ <- requireGrained(spec)
      _ <- requireIcebergTable(spark, qualified, rollupTable)
      grainDim <- Right(spec.grainDimension.get)
      baseDf <- baseDataFrame(spark, model)
      // D2-3/D2-4: duplicate-key probe on the RECOMPUTED source,
      // before the merge job (probe = distinct-count comparison,
      // cheaper than the merge it guards).
      scopedDf <- Right(scopedBase(baseDf, grainDim, scopeValues))
      source <- RollupMaterializer.buildRollupDf(scopedDf, model, spec)
      _ <- verifySourceUnique(spark, source, spec, grainDim)
      _ <- verifyScopeCoverage(spark, source, grainDim, scopeValues, spec)
      res <- executeMerge(spark, source, qualified, spec, grainDim)
    } yield res
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
  private def verifySourceUnique(
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
      capability = "RollupMaterializer.merge.duplicateKeys",
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
    def canon(v: String): String =
      if (daily && v.length > 10) v.take(10) else v
    val present = source.select(col(grainDim).cast("string"))
      .distinct().collect().map(r => canon(r.getString(0))).toSet
    val declared = scopeValues.map(canon).toSet
    val uncovered = present.diff(declared)
    if (uncovered.isEmpty) Right(())
    else Left(EngineError.UnsupportedCapability(
      engine = "spark-connector",
      capability = "RollupMergeRefresher.scopeCoverage",
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
    val setClause = nonKeyCols.map(c => s"t.`$c` = s.`$c`").mkString(", ")
    val insertCols = (keyCols ++ nonKeyCols).map(c => s"`$c`").mkString(", ")
    val insertVals = (keyCols ++ nonKeyCols).map(c => s"s.`$c`").mkString(", ")
    val sql =
      s"""MERGE INTO $qualified t
         |USING $viewName s
         |ON $onClause
         |WHEN MATCHED THEN UPDATE SET $setClause
         |WHEN NOT MATCHED THEN INSERT ($insertCols) VALUES ($insertVals)"""
        .stripMargin
    try {
      val before = spark.table(qualified).count()
      spark.sql(sql)
      val after = spark.table(qualified).count()
      Right(MergeRefreshResult.Merged(
        rollup = spec.name,
        table = qualified,
        rowsMerged = source.count(),
        rowsInserted = math.max(0L, after - before)))
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
