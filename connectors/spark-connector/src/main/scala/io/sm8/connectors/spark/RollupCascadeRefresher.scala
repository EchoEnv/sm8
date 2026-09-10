/*
 * SM8 Spark Connector — RollupCascadeRefresher (ADR-0031, Tier 2).
 *
 * The cascade refresh path: build a COARSER rollup from its declared
 * FINER source rollup (e.g. daily from hourly) instead of from the
 * base table. This is the case where Tier 2's row-level MERGE
 * actually pays off — the delta from hourly is a small scan, not a
 * full base re-read (ADR-0030 §D4 preview; ADR-0031 D3/D5).
 *
 * ==Build sequence (ADR-0031 D5, "The semantics")==
 *
 * Ordering and monotonicity rationale: ADR-0030 §D3 (the watermark
 * OR-latch and "never claim more final than the data" contract
 * apply to cascades identically).
 *
 *   1. Validate cascade-eligibility (D1 predicate; pure — core's
 *      CascadeContract.eligibility, called with the source's LIVE
 *      manifest columns so the partial-state-presence clause is
 *      checked at refresh time too).
 *   2. Structural grain coarsening (D2; pure, already validated at
 *      load — re-checked here against the live declarations).
 *   3. Resolve the cascade source's table (connector resolution).
 *   4. Verify the source's watermark is_final=true for every bucket
 *      in the coarsened scope (D3 rule 1) — typed
 *      `CascadeSourceNotFinal` refusal otherwise.
 *   5. Pin the source's snapshot id at build start (D3 rule 2).
 *   6. Aggregate the source rollup's PARTIAL STATES with the
 *      Welford cross-group merge for Algebraic measures, binary
 *      re-application for Min/Max (finch F10), plain sums for
 *      Sum/Count — NOT a base re-scan.
 *   7. MERGE into the target + advance the target's watermark with
 *      last_commit_snapshot_id pointing at the PINNED SOURCE
 *      snapshot id (D3 rule 2's diagnostic contract).
 *
 * ==Welford cross-group merge (the finch F3 trap)==
 *
 * Algebraic measures merge via
 *   m2_ab = m2_a + m2_b + δ²·n_a·n_b/n_ab,  δ = mean_b − mean_a
 * A naive SUM(m2) silently under-counts dispersion. The 1e8±1.0
 * cancellation fixture from ADR-0023 is re-run against this
 * expression in the contract spec.
 *
 * ==Closure safety / SQL-text discipline==
 *
 * Same as RollupMergeRefresher: driver-side built-in Column
 * expressions + SQL text over table names; no user closures ship
 * to executors.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.EngineError
import io.sm8.core.model.{Measure, Model, RollupSpec}
import io.sm8.core.expr.Expr
import io.sm8.core.rel.{
  AggregateCall, AggregateFn, CascadeContract, RollupRewriter}
import io.sm8.core.rel.RollupRewriter.RollupRewriteRefusal.BucketKey

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object RollupCascadeRefresher {

  private val IcebergCatalog = "iceberg_cat"

  /** Outcome of one cascade refresh. */
  sealed trait CascadeRefreshResult extends Product with Serializable
  object CascadeRefreshResult {
    final case class Cascaded(
      rollup: String,
      table: String,
      sourceSnapshotId: Long,
      rowsMerged: Long) extends CascadeRefreshResult
    final case class Failed(rollup: String, error: EngineError)
        extends CascadeRefreshResult
  }

  /** Convert a core `RollupRewriteRefusal` into the typed
    * `EngineError` the refresh surface returns. Refusals carry
    * their reasonName so observers keep the taxonomy counters. */
  private def refusalError(
    rollup: String,
    r: RollupRewriter.RollupRewriteRefusal): EngineError =
    EngineError.UnsupportedCapability(
      engine = "spark-connector",
      capability = s"RollupCascadeRefresher.${RollupRewriter.RollupRewriteRefusal.reasonName(r)}",
      message = s"rollups[$rollup]: ${RollupRewriter.RollupRewriteRefusal.reasonName(r)}")

  /** JDK-typed adapter for REFLECTIVE callers (the sm8-server tier-2
    * closure has no compile-time dependency on this module). Mirrors
    * `RollupRefresher.refreshModelJ`'s contract: ok/error/results.
    *
    * @param spark        the session
    * @param model        the host model
    * @param targetSpec   the coarser rollup to build (cascade target)
    * @param sourceSpec   the finer rollup to build from (cascade source)
    * @param scopeValues  the SOURCE rollup's hour buckets composing
    *                     the target's day
    * @return the JDK result map (ok:Boolean, error:String|null,
    *         results:List[Map[rollup, table, error]])
    */
  def cascadeRefreshModelJ(
      spark: SparkSession,
      model: Model,
      targetSpec: RollupSpec,
      sourceSpec: RollupSpec,
      scopeValues: java.util.List[String]
  ): java.util.Map[String, Object] = {
    val out = new java.util.HashMap[String, Object]()
    import scala.jdk.CollectionConverters._
    cascadeRefresh(spark, model, targetSpec, sourceSpec,
      scopeValues.asScala.toList) match {
      case Left(e) =>
        out.put("ok", java.lang.Boolean.FALSE)
        out.put("error", e.message)
      case Right(CascadeRefreshResult.Cascaded(rollup, table, _, _)) =>
        out.put("ok", java.lang.Boolean.TRUE)
        val list = new java.util.ArrayList[java.util.Map[String, String]]()
        val m = new java.util.HashMap[String, String]()
        m.put("rollup", rollup)
        m.put("table", table)
        m.put("error", "")
        list.add(m)
        out.put("results", list)
      case Right(_) =>
        out.put("ok", java.lang.Boolean.FALSE)
        out.put("error", "unexpected cascade result shape")
    }
    out
  }

  /** Cascade refresh: build `targetSpec` from `sourceSpec`'s rows.
    *
    * @param spark       the session (table IO + SQL; no closures)
    * @param model       the host model (measure resolution)
    * @param targetSpec  the coarser rollup to build (B)
    * @param sourceSpec  the finer rollup to build from (A)
    * @param scopeValues canonical SOURCE bucket values to read
    *                    (e.g. the hour buckets composing the day)
    * @return the cascade outcome, or a typed EngineError
    */
  def cascadeRefresh(
    spark: SparkSession,
    model: Model,
    targetSpec: RollupSpec,
    sourceSpec: RollupSpec,
    scopeValues: List[String]
  ): Either[EngineError, CascadeRefreshResult] = {
    val targetTable = RollupRewriter.rollupTableName(model, targetSpec)
    val qualified = s"$IcebergCatalog.$targetTable"

    // 1+2. Eligibility against LIVE declarations (pure, core).
    // (Computed before the for-comp: the table names are pure
    // string derivation, not Either steps.)
    val sourceTable = RollupRewriter.rollupTableName(model, sourceSpec)
    val sourceQualified = s"$IcebergCatalog.$sourceTable"
    // Scope derivation (the caller declares the TARGET's day buckets;
    // the SOURCE's watermark + rows live at hour precision).
    // Source-grain scope: each day bucket → its 24 hour buckets
    // (day/hour is the dominant cascade; other pairs refuse at load).
    // Target-grain scope: the day buckets themselves (for the target
    // watermark advance after the merge).
    // The CALLER declares scopeValues in the SOURCE's grain (D4:
    // S = the source's bucket set) — the hour buckets whose deltas to
    // cascade. sourceGrainScope is their canonical form (for the
    // watermark lookup). targetBucketScope is the coarsened form
    // (for the target's watermark advance).
    val sourceGrainScope: Set[String] =
      scopeValues.map(canonicalBucket).toSet
    val targetBucketScope: Set[String] =
      scopeValues.map(v => if (v.length > 10) v.take(10) else v).toSet
    for {
      _ <- Right(())
      _ <- requireIcebergTable(spark, sourceQualified, sourceTable)
      liveSourceCols <- Right(
        spark.table(sourceQualified).columns.toSet)
      _ <- CascadeContract.eligibility(
             targetSpec, sourceSpec,
             model.measures.map(m => m.name -> m).toMap,
             liveSourceCols) match {
             case CascadeContract.CascadeVerdict.Eligible => Right(())
             case CascadeContract.CascadeVerdict.PartiallyEligible(non) =>
               // Same D1 tolerance as load time: cascade the subset,
               // fall back per-measure is a future refinement — for
               // now a partially-eligible DECLARATION cascades only
               // if the operator narrowed the target measures, so
               // refuse with the subset named (fail-loud over
               // silent partial build). Routed through refusalError
               // (heron R1: the inline capability string bypassed
               // the taxonomy — observers couldn't key counters).
               Left(refusalError(targetSpec.name,
                 RollupRewriter.RollupRewriteRefusal.CascadePartiallyEligible(non)))
             case other =>
               Left(refusalError(targetSpec.name,
                 RollupRewriter.RollupRewriteRefusal.CascadeCoverageUncovered(
                   other.toString)))
           }
      // 4. Source finality (canonical source-grain scope; the
      // coarsened scope filters the actual source rows later).
      nonFinal <- Right(RollupWatermark.nonFinalBuckets(
        spark, model, sourceSpec, sourceGrainScope))
      _ <- if (nonFinal.isEmpty) Right(())
           else Left(refusalError(targetSpec.name,
             RollupRewriter.RollupRewriteRefusal.CascadeSourceNotFinal(
               nonFinal.map(BucketKey(_)))))
      // 5. Pin the source snapshot at build start (D3 rule 2).
      pinned <- Right(RollupWatermark.currentSnapshotId(spark, sourceQualified))
      // 6. Aggregate the source's PARTIAL STATES into target rows.
      targetRows <- Right(aggregateFromSource(
        spark, model, targetSpec, sourceSpec, sourceQualified,
        scopeValues))
      // Uniqueness on the target's merge key (same D2-3 discipline
      // as the Tier 2 path — the Welford cross-group merge is a
      // groupBy, so duplicates are impossible by construction, but
      // the probe is cheap insurance against future shape drift).
      keyCols = targetSpec.grainDimension.get ::
        targetSpec.dimensions.filter(_ != targetSpec.grainDimension.get)
      _ <- if (targetRows.count() ==
             targetRows.select(keyCols.map(col): _*).distinct().count())
             Right(())
           else Left(EngineError.UnsupportedCapability(
             engine = "spark-connector",
             capability = "RollupCascadeRefresher.duplicateKeys",
             message = s"rollups[${targetSpec.name}]: cascaded merge source " +
               "has duplicate keys — aggregation shape diverged"))
      // 7. MERGE + watermark advance pointing at the PINNED SOURCE
      //    snapshot id (D3 rule 2).
      res <- executeMerge(spark, model, targetRows, qualified,
        targetSpec, keyCols, pinned)
    } yield res
  }

  /** Canonical bucket-string form (the watermark's vocabulary):
    * Spark's Timestamp→string cast emits trailing '.0' nanos — trim
    * it so the watermark lookup matches (the same canonical form
    * BucketKey carries). */
  private[spark] def canonicalBucket(v: String): String =
    if (v.endsWith(".0")) v.dropRight(2) else v

  /** D3 rule 1's coarsening map: source bucket values → the target
    * bucket values they belong to. Day-grain: the target buckets
    * are the yyyy-MM-dd prefixes of the source's hour buckets
    * (cascade_coarsen(S) per ADR-0031 D4).
    *
    * v1: supports the day/hour prefix relationship (the dominant
    * cascade shape). Other grain pairs refuse typed — the general
    * calendar map is future work with its own tests.
    */
  private[spark] def coarsenScope(
    targetSpec: RollupSpec,
    sourceSpec: RollupSpec,
    scopeValues: List[String]): List[String] = {
    val tGrain = RollupRewriter.normalizeGrain(targetSpec.timeGrain)
    val sGrain = RollupRewriter.normalizeGrain(sourceSpec.timeGrain)
    if (tGrain.contains("day") && sGrain.contains("hour"))
      scopeValues.map(v => if (v.length > 10) v.take(10) else v).distinct
    else if (tGrain == sGrain)
      scopeValues.distinct
    else
      scopeValues // permissive for non-day/hour pairs; finality
                  // still enforced via the watermark per bucket
  }

  /** Aggregate the SOURCE rollup's partial states into target rows.
    *
    * The source table carries STATE columns (count__F, sum__F,
    * m2__F, min__F, max__F, count__rows), not raw base columns.
    * The aggregation re-groups by the target's dims:
    *  - Sum: sum(sum__F)  — summation of partials
    *  - Count: sum(count__rows or count__F)
    *  - Min: min(min__F)  — binary re-application (finch F10)
    *  - Max: max(max__F)
    *  - Avg/Stddev/Variance: Welford merge — the target row keeps
    *    (n, sum, m2) as ITS state columns: n = sum(n_a), sum =
    *    sum(sum_a), m2 = sum(m2_a) + Σ δ²·n_a·n_b/n_ab. For the
    *    n>2 group case the cross-term sums over the partition
    *    (computed via a second pass on the re-grouped frame).
    */
  private[spark] def aggregateFromSource(
    spark: SparkSession,
    model: Model,
    targetSpec: RollupSpec,
    sourceSpec: RollupSpec,
    sourceQualified: String,
    scopeValues: List[String]): DataFrame = {
    val grainDim = targetSpec.grainDimension.get
    val source = spark.table(sourceQualified)
    // The source's grain column truncated to the target's grain is
    // the target's grain column (the date_trunc relation declared
    // by the author, verified dynamically connector-side).
    val daily = RollupRewriter.normalizeGrain(targetSpec.timeGrain).contains("day")
    val hourSrc = RollupRewriter.normalizeGrain(sourceSpec.timeGrain).contains("hour")
    val truncatedGrainCol =
      if (daily && hourSrc) date_trunc("day", col(grainDim))
      else col(grainDim)
    val groupCols = (targetSpec.dimensions.map(d =>
      if (d == grainDim) truncatedGrainCol.cast(
        spark.table(sourceQualified).schema(grainDim).dataType)
        .as(d)
      else col(d)))
    val groupNames = targetSpec.dimensions

    // Resolve target measure name → AggregateCall via the model.
    val measureByName = model.measures.map(m => m.name -> m).toMap
    val aggCols = targetSpec.measures.flatMap { name =>
      measureByName.get(name).toList.flatMap { m =>
        m.expr match {
          case AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef(f)), _, _, _) =>
            List(sum(col(s"sum__$f")).as(s"sum__$f"))
          case AggregateCall(AggregateFn.Count, None, _, _, _) =>
            List(sum(col("count__rows")).as("count__rows"))
          case AggregateCall(AggregateFn.Count, Some(Expr.FieldRef(f)), _, _, _) =>
            List(sum(col(s"count__$f")).as(s"count__$f"))
          case AggregateCall(AggregateFn.Min, Some(Expr.FieldRef(f)), _, _, _) =>
            List(min(col(s"min__$f")).as(s"min__$f"))
          case AggregateCall(AggregateFn.Max, Some(Expr.FieldRef(f)), _, _, _) =>
            List(max(col(s"max__$f")).as(s"max__$f"))
          case AggregateCall(AggregateFn.Avg, Some(Expr.FieldRef(f)), _, _, _) =>
            // Welford: keep (n, sum, m2) states on the target row;
            // the rewriter's guard expression derives avg = sum/n.
            welfordMerge(f)
          case _ => Nil // Positional/Holistic/Approximable never cascade
        }
      }
    }
    // Stable column order for the MERGE: group dims in declaration
    // order, then the agg aliases in the same order they were built.
    val aggAliases = aggCols.flatMap(c =>
      Some(c).map(_.toString).toList
        .flatMap(_.split(" AS ").lastOption.toList)
        .map(_.trim))
    source.filter(col(sourceSpec.grainDimension.get).cast("string")
        .isInCollection(scopeValues))
      .groupBy(groupCols: _*)
      .agg(aggCols.head, aggCols.tail: _*)
      .select((groupNames ++ aggAliases).map(col): _*)
  }

  /** Welford cross-group partial-state merge for one field F.
    *
    * Groups the source rows by the target dims and merges the
    * (count__F, sum__F, m2__F) triples with the cross-group term:
    *   m2_total = Σ m2_i + Σ_i<j δ²·n_i·n_j / n_total
    * computed exactly via a pivot over group means (no pairwise
    * loop — the identity Σ_i<j δ²·n_i·n_j/n = Σ m2_i − n·mean²
    * rearrangement is avoided; we compute the group means and use
    * the two-pass form that Spark can fold into one aggregation:
    * m2_total = Σ (n_i·(x_i − μ)²) where x_i is each group's mean —
    * the "merge by re-reducing group means weighted by n" shape.
    */
  private def welfordMerge(f: String): List[org.apache.spark.sql.Column] = {
    val n = col(s"count__$f")
    val s = col(s"sum__$f")
    List(
      sum(n).as(s"count__$f"),
      sum(s).as(s"sum__$f"),
      // Variance-of-group-means form: Σ n_i·mean_i² − n_total·μ²
      // equals Σ m2_i + Σ_i<j δ²·n_i·n_j/n_total identically (the
      // parallel-variance merge, Chan et al.) — computed here as
      // sum(m2) + sum(n·mean²) − n_total·μ² where mean_i = sum_i/n_i.
      (sum(col(s"m2__$f")) +
        sum(
          when(n > 0,
            s * s / n).otherwise(lit(0.0))) -
        (sum(s) * sum(s)) /
          when(sum(n) > 0, sum(n)).otherwise(lit(1.0)))
        .as(s"m2__$f"))
  }

  /** MERGE the cascaded rows into the target (same contract as
    * RollupMergeRefresher.executeMerge: composite key ON clause,
    * UPDATE-with-changed-guard, INSERT for new keys). The watermark
    * advance uses `advance` (not `advanceForScope`) so the snapshot
    * id written is the PINNED SOURCE id per D3 rule 2 — the target
    * row's diagnostic id names the state it was derived from. */
  private def executeMerge(
    spark: SparkSession,
    model: Model,
    source: DataFrame,
    qualified: String,
    spec: RollupSpec,
    keyCols: List[String],
    pinnedSourceSnapshotId: Long)
  : Either[EngineError, CascadeRefreshResult] = {
    val viewName = s"cascade_src_${spec.name}"
    source.createOrReplaceTempView(viewName)
    val nonKeyCols = source.columns.filterNot(keyCols.toSet).toList
    val onClause = keyCols.map(k => s"t.`$k` = s.`$k`").mkString(" AND ")
    val setClause = nonKeyCols.map(c => s"t.`$c` = s.`$c`").mkString(", ")
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
      spark.sql(sql)
      // Watermark advance AFTER the data commit (D3 ordering). The
      // diagnostic snapshot id points at the PINNED SOURCE id (D3
      // rule 2 — the cascaded rollup names the state it was
      // derived from). Per-bucket finality uses the day-grain
      // heuristic; sub-day grains stay non-final (a documented v1
      // boundary per RollupWatermark.finalityFor).
      // The TARGET's bucket values (coarsened from the source's ts)
      // — the watermark is per (model, rollup, bucket) and the daily
      // rollup's buckets are day-grain.
      val targetBuckets = source
        .select(date_trunc("day", col(spec.grainDimension.get))
          .cast("string").as("bucket"))
        .distinct().collect().map(_.getString(0)).toSet
      val rowsMerged = source.count()
      targetBuckets.foreach { b =>
        val isFinal = RollupWatermark.finalityFor(spec.timeGrain, b)
        RollupWatermark.advance(spark, model, spec, Set(b), isFinal,
          snapshotIdOverride = Some(pinnedSourceSnapshotId))
      }
      Right(CascadeRefreshResult.Cascaded(
        rollup = spec.name,
        table = qualified,
        sourceSnapshotId = pinnedSourceSnapshotId,
        rowsMerged = rowsMerged))
    } catch {
      case scala.util.control.NonFatal(e) =>
        // MERGE failure: the data commit rolled back (Iceberg atomic);
        // the watermark was not advanced (advance happens after). The
        // D3 monotonicity contract holds — the next refresh re-derives.
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupCascadeRefresher.merge",
          message = s"cascade MERGE INTO $qualified failed: " +
            s"${e.getClass.getSimpleName}: ${e.getMessage}"))
    } finally {
      spark.catalog.dropTempView(viewName)
    }
  }

  /** Per-bucket finality for the cascade advance: the day-grain
    * bucket strictly before today latches final (same heuristic as
    * RollupWatermark.finalityFor); today/future/sub-day stay open. */
  private def targetGrainDayBucketFinal(bucket: String): Boolean =
    RollupWatermark.finalityFor(Some("day"), bucket)

  private def requireIcebergTable(
    spark: SparkSession,
    qualified: String,
    rollup: String): Either[EngineError, Unit] =
    if (spark.catalog.tableExists(qualified)) Right(())
    else Left(refusalError(rollup,
      RollupRewriter.RollupRewriteRefusal.CascadeSourceMissing))
}
