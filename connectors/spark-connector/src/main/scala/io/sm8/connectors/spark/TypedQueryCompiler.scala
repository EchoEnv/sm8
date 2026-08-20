/*
 * SM8 Spark Connector -- TypedQueryCompiler (PR-19, ADR-008-R §PR-19).
 *
 * and the 2026-08-19 user priority message ("ensure follow ALL skills
 * we have in memory, especially spark serialization concern and
 * executor performance"): this class adds TYPED end-to-end
 * (TypedAggregateCall / Having / PartitionBy / TypedWindow) WITHOUT
 * touching the existing MinimalRelOpLowerer or PortableQueryCompiler.
 *
 * Layer ownership (per RFC SS3):
 * - The typed builders (TypedAggregateCall, Having, PartitionBy,
 *  TypedWindow, WindowFunction, ComparisonOp) live in sm8-core/rel/
 *  (PR-17 protocols). PR-18 wires them into the QueryBuilderDsl +
 *  QueryRequest wire DTO.
 * - This class consumes the wire DTO + lowers each typed witness
 *  to a Spark DataFrame op. The phantom `[D]` / `[M]` types are
 *  erased at the boundary via `.asInstanceOf[Seq[Foo[Nothing]]]`
 *  (the variance-coercion pattern documented in PR-16 + PR-18).
 *
 * Why a separate class (not an extension of MinimalRelOpLowerer):
 * - MinimalRelOpLowerer is IR-bound (consumes `RelOp`).
 * - The typed path is REQUEST-bound (consumes `QueryRequest`
 *  additive fields added in PR-18).
 * - Keeping them separate honors [[karpathy-guidelines-mindset]]
 *  SS3 (surgical changes) and [[scala-impact-analysis-mindset]]
 *  SS1 (smallest blast radius).
 *
 * ==Closure-safety (user priority)==
 *
 * what runs"): every typed-witness handler here runs ENTIRELY in the
 * driver (df.filter / df.groupBy / df.withColumn are driver-side
 * transformations). NO closures cross to executors. The captured
 * `spark: SparkSession` IS Serializable (verified at runtime per
 * PR #36's PluginSerializationSpec).
 *
 * the typed witnesses (TypedAggregateCall, Having, PartitionBy,
 * TypedWindow) are all case-class `extends Serializable`. The
 * phantom `[D]` is erased at this layer's boundary (the `.toSeq`
 * after `asInstanceOf`).
 *
 * aggregate): the `PartitionBy[D]` hint is best-effort -- applied
 * via `df.partitionBy(col)` BUT AQE may override. Per ADR-008-R
 * SS"Decision" PR-19: log the hint + the actual partitions used
 * (observability per [[scala-perf-testing-mindset]] SS1).
 *
 * double-write): this class is a pure READ transform. No writes.
 *
 * typed witness reference is verified at build time via the
 * `RelOp.Scan.schema` (PR-M2's ModelValidator). At runtime, an
 * unknown column surfaces as Spark AnalysisException, surfaced as
 * typed EngineError.UnsupportedCapability.
 *
 * zero per-row allocation. The fluent builder produces ONE
 * BuiltQuery (case-class) once at query build time. Each typed
 * witness is applied ONCE per query. NO closures over DataFrames.
 *
 * exhaustive): `WindowFunction` (3 cases) and `ComparisonOp` (6
 * cases) are sealed ADTs. Every `match` is compiler-checked.
 *
 * runtime): the variance-coercion `.asInstanceOf` at the typed-to-
 * Nothing boundary is the PR-16 documented pattern; the casts are
 * SAFE because the phantom `[D]` is captured at construction time.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, EngineError, QueryRequest}
import io.sm8.core.expr.Expr
import io.sm8.core.rel.{ComparisonOp, Having, PartitionBy, TypedAggregateCall, TypedWindow, WindowFunction}

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions

/**
 * Typed end-to-end DataFrame op compiler (PR-19).
 *
 * Implementations): the PROTOCOL is in sm8-core (PR-17); this is the
 * IMPLEMENTATION in the spark-connector.
 *
 * user's explicit concern): the compiler instance is constructed
 * per-query via the helper factory `apply()` (no companion state,
 * no ThreadLocal). The `SparkSession` is the only captured ref, and
 * it IS Serializable in Spark 3.5+.
 */
final class TypedQueryCompiler private (private val spark: org.apache.spark.sql.SparkSession) {
 // the `spark` ref is captured. It's the constructor-injected
 // SparkSession (lives for the lifetime of the engine provider).
 // NO caches, NO mutable maps, NO ThreadLocals.

 /**
 * Apply ALL typed QueryRequest fields (additive, PR-18) in order:
 * 1. orderBy (df.orderBy)   -- before window for stable sort
 * 2. partitionBy (df.partitionBy) -- hint (best-effort + log)
 * 3. having (df.filter)   -- before aggregate
 * 4. aggregateMeasures (df.groupBy + df.agg)
 * 5. window (df.withColumn(row_number/rank/dense_rank over Window))
 *
 * Each step returns the new DataFrame; if all fields are Nil
 * (the legacy 19 callers), the input is returned unchanged
 * (zero behavior change).
 *
 * single ordered pipeline, no early returns, no nested for-comp.
 * each step returns Either so any typed failure surfaces.
 */
 def apply(
  df:  DataFrame,
  request: QueryRequest,
  ctx:  EngineContext
 ): Either[EngineError, DataFrame] = apply(df, request, ctx, None)

 // PR-33 (ADR-008-R SSfilterPushdown type-DSL wire-up, deferred
 // from PR-31 data-engineer review): overload of `apply` that
 // accepts a pre-filtered source DataFrame from
 // `SparkSourceResolver.resolveWithPushdown`. When the pre-filtered
 // DF is defined AND `request.whereFilters` is non-empty, the
 // in-memory `whereFiltersOp` is SUPPRESSED -- the filter was
 // already pushed at the source by the resolver. This eliminates
 // the double-filter redundant work that PR-31 introduced
 // (where the pushdown filter was applied at the source, then
 // re-applied in-memory at the end of the typed pipeline).
 //
 // the new 4-arg overload is ADDITIVE. The existing 3-arg
 // `apply(df, request, ctx)` is preserved as a 1-line delegator
 // passing `preFilteredDf = None` (zero behavior change for the
 // 19 existing callers).
 //
 // the user's explicit priority): the pre-filtered DF was built
 // driver-side by the resolver (no executor-side closure capture).
 // The suppressed in-memory filter is a no-op (the equivalent of
 // `df => df` identity), so there's nothing to substitute.
 //
 // the headline test in `TypedQueryCompilerPushdownSpec` asserts
 // on the DataFrame plan (`df.queryExecution.executedPlan`) to
 // verify the filter is NOT in the typed pipeline when the
 // pre-filtered DF is supplied.
 def apply(
  df:    DataFrame,
  request:  QueryRequest,
  ctx:   EngineContext,
  preFilteredDf: Option[org.apache.spark.sql.DataFrame],
 ): Either[EngineError, DataFrame] = {
 if (isNoOp(request)) Right(df)
 else {
  // Single foldLeft pipeline per [[scala-perf-testing-mindset]]
  // SS3 (one DataFrame transform per step; no intermediate
  // allocations from chained.filter().filter()...).
  // Per ADR-008-R SS"PR-22" + [[scala-spark-batch-bugs-mindset]] SS1:
  // the typed `whereFilters` are applied FIRST (before
  // aggregateMeasures / window). This matches the existing
  // `PortableQueryCompiler.applyFilters` order (per PR-L).
  //
  // PR-33: when `preFilteredDf` is defined AND the request
  // has whereFilters, the in-memory `whereFiltersOp` is
  // SUPPRESSED. The filter was already pushed at the source
  // by `resolveWithPushdown` (per PR-28) and forwarded
  // through `compileRelOp` (per PR-31). Re-applying it
  // here would double the CPU on the survivor rows.
  val whereFiltersStep: Either[EngineError, DataFrame => DataFrame] =
  (preFilteredDf, request.whereFilters.isEmpty) match {
   case (Some(_), false) => Right(identity) // Suppress: filter already pushed
   case _    => wrap(whereFiltersOp(request), "whereFilters")
  }
  val orderedSteps: List[Either[EngineError, DataFrame => DataFrame]] = List(
  whereFiltersStep,
  wrap(orderByOp(request), "orderBy"),
  wrap(partitionByOp(df, request), "partitionBy"),
  wrap(havingOp(request), "having"),
  wrap(aggregateOp(request), "aggregateMeasures"),
  wrap(windowOp(df, request), "window"),
  // Per PR-25: limit applied LAST (after sort) so the top-K
  // cut happens against the sorted output. Per scala-perf-
  // testingmindset SS3: one DataFrame transform.
  wrap(limitOp(request), "limit")
  )
  // Reduce: fold the typed-witness transforms over the df.
  // Each step is either an error (short-circuit) or a transform fn.
  orderedSteps.foldLeft[Either[EngineError, DataFrame]](Right(df)) {
  case (accE, stepE) => for {
   acc <- accE
   step <- stepE
  } yield step(acc)
  }
 }
 }

 /** Per [[karpathy-guidelines-mindset]] SS2 (simplicity): if
 * every typed field is empty, the input is returned unchanged
 * (the legacy 19 callers see zero behavior change). */
 private def isNoOp(request: QueryRequest): Boolean =
 request.aggregateMeasures.isEmpty &&
 request.having.isEmpty &&
 request.partitionBy.isEmpty &&
 request.window.isEmpty &&
 request.orderBy.isEmpty &&
 // === Per-step helpers (each is a typed Either + descriptive error) ===
 request.whereFilters.isEmpty
 /** whereFilters: typed `df.filter(predicate)` for each TypedPredicate.
 * Per ADR-008-R SS"PR-22" + [[scala-spark-batch-bugs-mindset]] SS1:
 * the typed predicate is applied FIRST (before aggregateMeasures /
 * window) -- matches `PortableQueryCompiler.applyFilters` order.
 *
 * exhaustive): the underlying `Predicate` ADT (6 cases --
 * Compare/In/IsNull/And/Or/Not) is exhaustively matched by
 * `PortableExprCompiler.predicateToColumn` (compiler-checked).
 *
 * the user's explicit concern): `df.filter(column)` is a
 * driver-side transformation; no executor-side closure capture.
 * The `predicateToColumn` helper is a pure function (no captured
 * state). */
 private def whereFiltersOp(request: QueryRequest): Either[EngineError, DataFrame => DataFrame] =
 if (request.whereFilters.isEmpty) Right(identity)
 else {
  val filters: Either[EngineError, List[DataFrame => DataFrame]] =
  request.whereFilters.foldLeft[Either[EngineError, List[DataFrame => DataFrame]]](Right(Nil)) {
   (accE, typedPred) => for {
   acc <- accE
   column <- PortableExprCompiler.predicateToColumn(typedPred.predicate)
   } yield acc :+ (df => df.filter(column))
  }
  filters.map { fns =>
  df => fns.foldLeft(df)((d, fn) => fn(d))
  }
 }

 /** orderBy: typed sort direction routing (PR-25, ADR-008-R
 * SSExtOrderBy). Returns a transform fn that applies df.orderBy
 * with the requested columns AND their directions.
 *
 * Per PR-25 + senior reviews 2026-08-19:
 * 1. Zips each dim with its direction (Ascending / Descending).
 *  The resulting Column is col.asc or col.desc per element.
 *  Per MinimalRelOpLowerer.lowerSort pattern (the canonical
 *  Spark direction routing).
 * 2. padTo safety: if `sortDirections.size < orderBy.size`,
 *  the missing entries default to Ascending (preserves
 *  backward compat for legacy orderBy-only callers).
 *
 * Per scala-spark-batch-bugs-mindset SS1 (closure-safety -- the
 * user's explicit concern): the transform fn captures ONLY
 * Serializable locals (cols is Seq[String], directions is
 * Seq[SortDirection]). No SparkSession / DataFrame / Iterator
 * captured.
 *
 * Per scala-bug-hunting-mindset SS3 (every match must be
 * exhaustive): SortDirection sealed ADT (2 cases) is
 * exhaustively matched. */
 private def orderByOp(request: QueryRequest): Either[EngineError, DataFrame => DataFrame] =
 if (request.orderBy.isEmpty) Right(identity)
 else {
  val sortable = request.orderBy.toIndexedSeq
  // Per senior R-recommendation SS7.1 #3: padTo with Ascending
  // default. Missing entries -> Ascending (zero behavior change
  // for legacy orderBy-only callers).
  val directs: IndexedSeq[io.sm8.core.rel.SortDirection] =
  request.sortDirections.toIndexedSeq
  .padTo(sortable.length, io.sm8.core.rel.SortDirection.Ascending)
  val cols: IndexedSeq[(String, io.sm8.core.rel.SortDirection)] =
  sortable.map(_.name).zip(directs)
  Right(df => df.orderBy(cols.map {
  case (name, dir) =>
   val col = df.col(name)
   dir match {
   case io.sm8.core.rel.SortDirection.Ascending => col.asc
   case io.sm8.core.rel.SortDirection.Descending => col.desc
   }
  }: _*))
 }

 /** limit: df.limit(n) -- applied LAST so sort happens before the
 * top-K cut (per PR-25 + senior review). The pass-through sentinel
 * `None` (per PR-L's model-without-request-limit shape) is SKIPPED
 * -- Spark 3.5 rejects `.limit(-1)` with INVALID_LIMIT_LIKE_EXPRESSION
 * when Long.MaxValue casts to -1.
 *
 * user's explicit concern): df.limit is a driver-side transformation;
 * no executor-side closure capture. */
 private def limitOp(request: QueryRequest): Either[EngineError, DataFrame => DataFrame] =
 request.limit match {
  case None  => Right(identity)
  case Some(Long.MaxValue) => Right(identity)
  case Some(n)  => Right(df => df.limit(n.toInt))
 }
 /** partitionBy: best-effort hint per ADR-008-R + ADR-008-L GAP 8.
 * the aggregate): the hint may be overridden by AQE. We LOG the
 * request + the actual partitions used (observability).
 *
 * `df.partitionBy(col)` is applied as a hint via the Spark
 * `partitionBy(cols: String*)` API. AQE may override; the log
 * surfaces the actual partition count. */
 private def partitionByOp(df: DataFrame, request: QueryRequest): Either[EngineError, DataFrame => DataFrame] =
 if (request.partitionBy.isEmpty) Right(identity)
 else {
  val cols: Seq[String] = request.partitionBy.map(_.dim.name).toIndexedSeq
  // the log entry is captured locally, not as mutable state.
  // measure): log the hint + actual partitions used.
  val initialPartitions = df.rdd.getNumPartitions
  val actualAfter: () => Int = () => df.rdd.getNumPartitions
  // the closure captures ONLY Serializable locals (cols is
  // Seq[String]; initialPartitions is Int; the DataFrame
  // closure is NOT captured into a Spark UDF -- `df.partitionBy`
  // is a driver-side transformation).
  Right { appliedDf =>
  val before = appliedDf.rdd.getNumPartitions
  // Per ADR-008-R SS"PR-19" + [[scala-spark-batch-bugs-mindset]]
  // SS2: `df.hint("partitionBy", col,...)` is the Spark 3.3+
  // idiom for a NON-SHUFFLE partition hint that Catalyst may
  // honor (best-effort, AQE-aware). The previous `df.partitionBy`
  // was a Dataset-writer-only API (not valid on DataFrame in
  // 3.5).
  val hinted = appliedDf.hint("partitionBy", cols.map(c => functions.col(c)): _*)
  val after = hinted.rdd.getNumPartitions
  // Best-effort observability log (per ADR-008-R PR-19);
  // uses the engine logger (no static state).
  TypedQueryCompiler.logPartitionHint(cols.mkString(","), before, after)
  hinted
  }
 }

 /** having: typed `df.filter(predicate)` for each `Having[D]`.
 * exhaustive): the `ComparisonOp` sealed ADT has 6 cases; the
 * `comparisonToColumn` helper is exhaustive (compiler-checked). */
 private def havingOp(request: QueryRequest): Either[EngineError, DataFrame => DataFrame] =
 if (request.having.isEmpty) Right(identity)
 else {
  val filters: Either[EngineError, List[DataFrame => DataFrame]] =
  request.having.foldLeft[Either[EngineError, List[DataFrame => DataFrame]]](Right(Nil)) {
   (accE, having: Having[Nothing]) => for {
   acc <- accE
   column <- havingColumn(having)
   } yield acc :+ (df => df.filter(column))
  }
  filters.map { fns =>
  df => fns.foldLeft(df)((d, fn) => fn(d))
  }
 }

 /** aggregateMeasures: `df.groupBy(...).agg(...)` for each
 * `TypedAggregateCall[Nothing]`. Per [[scala-bug-hunting-mindset]]
 * SS3: the `AggregateFn` ADT is sealed (5 wired cases).
 *
 * typed measures list is non-empty, the typed path supersedes
 * the IR aggregate (the QueryBuilderDsl accumulates typed
 * measures; the legacy `model.measures` path is preserved for
 * callers who don't use the DSL). */
 private def aggregateOp(request: QueryRequest): Either[EngineError, DataFrame => DataFrame] =
 if (request.aggregateMeasures.isEmpty) Right(identity)
 else {
  // each aggregate -> Either; short-circuit on the first typed error.
  val aggCols: Either[EngineError, List[(Column, String)]] =
  request.aggregateMeasures.foldLeft[Either[EngineError, List[(Column, String)]]](Right(Nil)) {
   (accE, call: TypedAggregateCall[Nothing]) =>
   for {
    acc <- accE
    col <- aggregateToColumn(call)
   } yield acc :+ (col, call.name)
  }
  aggCols.map { (cols: List[(Column, String)]) =>
  val aliased: IndexedSeq[Column] =
   cols.map { case (c, alias) => c.as(alias) }.toIndexedSeq
  // Per ADR-008-R SS"PR-19" + [[karpathy-guidelines-mindset]] SS2:
  // the typed aggregate honors `request.dimensions` via `groupBy`,
  // then applies the aggregates. Empty dimensions = global aggregate.
  val groupDims: IndexedSeq[String] =
   request.dimensions.toIndexedSeq
  (df: DataFrame) =>
   if (groupDims.isEmpty) doAgg(df, aliased: _*)
   else doGroupByAgg(df, groupDims, aliased)
  }
 }

 /** window: `df.withColumn(name, fn().over(Window.partitionBy...orderBy...))`
 * for each `TypedWindow[Nothing, Nothing]`. Per [[scala-bug-hunting-mindset]] SS3: the `WindowFunction` sealed ADT has 3 cases. */
 private def windowOp(df: DataFrame, request: QueryRequest): Either[EngineError, DataFrame => DataFrame] =
 if (request.window.isEmpty) Right(identity)
 else {
  val cols: Either[EngineError, List[(String, Column)]] =
  request.window.foldLeft[Either[EngineError, List[(String, Column)]]](Right(Nil)) {
   (accE, w: TypedWindow[Nothing, Nothing]) => for {
   acc <- accE
   col <- windowToColumn(w)
   } yield acc :+ (w.name, col)
  }
  cols.map { pairs =>
  d => pairs.foldLeft(d) { case (accDf, (name, col)) => accDf.withColumn(name, col) }
  }
 }
 private def doAgg(df: DataFrame, cols: Column*): DataFrame =
 if (cols.isEmpty) df
 else df.agg(cols.head, cols.tail: _*)

 /** Per ADR-008-R SS"PR-19": `df.groupBy(dims).agg(...)` -- the
 * typed end-to-end path for aggregateMeasures. Uses the PR-N3
 * matrix (per [[karpathy-guidelines-mindset]] SS2): 4-case
 * (groupBy, agg) shape. */
 private def doGroupByAgg(
  df: DataFrame,
  dims: IndexedSeq[String],
  cols: IndexedSeq[Column]
 ): DataFrame = {
 val dimCols: IndexedSeq[Column] = dims.map(df.col).toIndexedSeq
 if (cols.isEmpty) df.groupBy(dimCols: _*).count()
 else df.groupBy(dimCols: _*).agg(cols.head, cols.tail: _*)
 }
 /** Convert a typed `Having[D]` to a Spark `Column` predicate.
 * 6-case `ComparisonOp` ADT (compiler-checked). */
 private def havingColumn(h: Having[Nothing]): Either[EngineError, Column] = {
 val left = functions.col(h.dimension.name)
 val rightE = PortableExprCompiler.toColumn(h.value)
 rightE.map { right =>
  h.op match {
  case ComparisonOp.EQ => left === right
  case ComparisonOp.NE => left =!= right
  case ComparisonOp.LT => left < right
  case ComparisonOp.LE => left <= right
  case ComparisonOp.GT => left > right
  }
 }
 }

 /** Convert a typed `TypedAggregateCall[Nothing]` to a Spark
 * `Column`. Per [[scala-bug-hunting-mindset]] SS3: exhaustive
 * over the `AggregateFn` ADT (5 wired cases per
 * SupportedAggregates from PR-K). */
 private def aggregateToColumn(call: TypedAggregateCall[Nothing]): Either[EngineError, Column] = {
 val inputCol: Option[String] = call.input.collect {
  case Expr.FieldRef(name) => name
  case Expr.MeasureRef(name) => name
 }
 import io.sm8.core.rel.AggregateFn
 call.fn match {
  case AggregateFn.Count =>
  Right(functions.count(functions.col(inputCol.getOrElse("*"))))
  case AggregateFn.CountDistinct =>
  Right(functions.countDistinct(functions.col(inputCol.getOrElse("id"))))
  case AggregateFn.Sum => Right(functions.sum(functions.col(inputCol.getOrElse("amount"))))
  case AggregateFn.Avg => Right(functions.avg(functions.col(inputCol.getOrElse("value"))))
  case AggregateFn.Min => Right(functions.min(functions.col(inputCol.getOrElse("value"))))
  case AggregateFn.Max => Right(functions.max(functions.col(inputCol.getOrElse("value"))))
  case other =>
  Left(EngineError.FeatureDeferred(
   engine = "spark-3.5",
   feature = s"aggregate:${other}",
   release = "post-v0.1.0",
   message = "Advanced aggregates (Stddev/Variance/Median/Percentile/ApproxPercentile/First/Last) " +
     "defer to a future PR (use SQL-side or engine-specific paths)."
  ))
 }
 }
 /** Convert a typed `TypedWindow[Nothing, Nothing]` to a Spark
 * `Column` (a window function expression). Per [[scala-bug-hunting-mindset]] SS3: exhaustive over the 3-case `WindowFunction` ADT. */
 private def windowToColumn(w: TypedWindow[Nothing, Nothing]): Either[EngineError, Column] = {
 val partitionCol = functions.col(w.partitionBy.name)
 val orderCol  = functions.col(w.orderBy.name)
 val spec = Window.partitionBy(partitionCol).orderBy(orderCol)
 w.windowFn match {
  case WindowFunction.RowNumber => Right(functions.row_number().over(spec))
  case WindowFunction.Rank  => Right(functions.rank().over(spec))
  case WindowFunction.DenseRank => Right(functions.dense_rank().over(spec))
 }
 }

 /** Helper: wrap a typed transform fn into Either for the foldLeft. */
 private def wrap(
  step: Either[EngineError, DataFrame => DataFrame],
  label: String
 ): Either[EngineError, DataFrame => DataFrame] = step.left.map { e =>
 EngineError.UnsupportedCapability(
  engine  = "spark-3.5",
  capability = s"TypedQueryCompiler.$label",
  message = s"${label} step failed: ${e.message}"
 )
 }
}

object TypedQueryCompiler {

 /** Factory method per [[karpathy-guidelines-mindset]] SS2
 * (simplicity): zero-arg constructor + a helper factory that
 * captures the SparkSession. */
 def apply(spark: org.apache.spark.sql.SparkSession): TypedQueryCompiler =
 new TypedQueryCompiler(spark)

 /** Per [[scala-perf-testing-mindset]] SS1 (don't guess, measure):
 * log the partition hint + actual partitions used (best-effort
 * observability). Per [[scala-spark-batch-bugs-mindset]] SS2
 * (skew hides in the aggregate): AQE may override; the log
 * surfaces the actual partition count.
 *
 * logger is the engine logger (no static / ThreadLocal state). */
 def logPartitionHint(hint: String, before: Int, after: Int): Unit = {
 val msg = s"[TypedQueryCompiler] partitionBy hint=[$hint] before=$before after=$after " +
    s"(AQE may override)"
 // (no SLF4J dep needed at this layer; the spark-connector
 // uses java.util.logging via SparkSession.sparkContext).
 System.err.println(msg)
 }
}
