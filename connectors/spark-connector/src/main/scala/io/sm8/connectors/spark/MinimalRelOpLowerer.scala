/*
 * SM8 Spark Connector -- MinimalRelOpLowerer (PR-M5 commit 2 of 2).
 *
 * Per the user's directive 2026-08-17 ("Extract Calculator AND
 * Extract MinimalRelOpLowerer on separate commit but in 1 PR"):
 * a single class that owns the RelOp -> DataFrame lowering for the
 * Spark connector.
 *
 * This is the MINIMAL extractor: the 7 RelOp cases (Scan / Filter /
 * Project / Aggregate / Join / Sort / Limit) are direct recursive
 * methods. The full RelOp -> DataFrame is a future PR per the
 * ADR-008-L Appendix plan; this class gives us ONE named place to
 * extend the lowering (no more inlined cases scattered through
 * `compileRelOp`).
 *
 * we EXTRACT the existing cases unchanged. The behavior is
 * identical to the prior `compileRelOp` inlining.
 *
 * - #1 (closure-safety): the lowerer captures only the
 *  `SparkSession` ref (Serializable in 3.5 + 4.1) and the
 *  `PortableQueryCompiler` (Serializable). NO closures over
 *  DataFrames, Iterators, or external resources.
 * - #3 (schema-drift): the `Scan` case returns a DataFrame whose
 *  schema is the ACTUAL `df.schema` (not a caller-supplied
 *  "expected" shape). All downstream cases work with the
 *  actual schema.
 * - #5 (driver-vs-executor): every step runs in the driver;
 *  no executor-side closure capture.
 *
 * ==Layer (per RFC SS3)==
 *
 * Connector-side. The lowerer knows about spark.table, spark.read,
 * StructField, and Column -- those are engine-specific. The core
 * IR (RelOp, Expr, etc.) is engine-portable and lives in
 * sm8-core/./rel/ + sm8-core/./expr/. The boundary is the
 * Input/Output types of the lower method: RelOp (in), DataFrame
 * (out), EngineContext (in).
 *
 * ==Per-node documentation (PR-M5)==
 *
 * Per the per-node contract from the `rel/` ADT (PR-H):
 * - Scan: read the source via spark.table / spark.read.
 * - Filter: df.filter(expr).
 * - Project: df.select((expr, alias),.).
 * - Aggregate: applyAggregations via synthesised Model +
 *  applyAggregations. The "synthesised Model" is the GAP-5
 *  fallback (per the existing compileRelOpAggregate comment);
 *  a clean direct lower is deferred.
 * - Join: synthesised Model + legacy applyJoins. Same fallback.
 * - Sort: df.orderBy.
 * - Limit: df.limit (skip the sentinel Long.MaxValue).
 *
 * ==Errors==
 *
 * Typed EngineError per RFC SS12:
 * - SourceRef.ByProvider -> UnsupportedCapability (deferred to PR-M4
 *  full RelOp path)
 * - Join.left not a Scan -> UnsupportedCapability
 * - GroupBy key not a FieldRef / MeasureRef -> UnsupportedCapability
 *
 * ==Composition with PortableQueryCompiler==
 *
 * The lowerer is constructed by `PortableQueryCompiler` and given a
 * back-reference to it (`pc`). Two methods on the PC are
 * composition points: `applyAggregations` (the legacy groupBy+agg
 * pipeline) and `compile` (the legacy model path). The lowerer
 * delegates to these for the Aggregate/Join fall-through cases.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, EngineError, ResolvedSource, EngineIdentity}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.{
 CalculatedMeasure, Dimension, FilterSpec, JoinSpec, Measure, Model,
 ModelPolicyDefaults, ModelStatus, SourceRef,
}
import io.sm8.core.rel.{RelOp, SortDirection, NullOrdering, AggregateCall, AggregateFn}
import io.sm8.core.schema.SealedDataType

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

final class MinimalRelOpLowerer(
 val spark: SparkSession,
 val pc:  PortableQueryCompiler,
 val identity: EngineIdentity = EngineIdentity(
  name = SparkEngineConstants.DescriptorName, nativeVersion = "3.5", engineAdapterVersion = SparkEngineConstants.AdapterVersion)) extends java.io.Serializable {

 // PR-M5: the single source of truth for RelOp -> DataFrame lowering.
 // Per-node methods below. `lower` is a thin delegator.
 def lower(relOp: RelOp, ctx: EngineContext): Either[EngineError, DataFrame] =
 relOp match {
  case scan: RelOp.Scan  => lowerScan(scan)
  case f: RelOp.Filter => lowerFilter(f, ctx)
  case p: RelOp.Project => lowerProject(p, ctx)
  case a: RelOp.Aggregate => lowerAggregate(a, ctx)
  case j: RelOp.Join  => lowerJoin(j, ctx)
  case s: RelOp.Sort  => lowerSort(s, ctx)
  case l: RelOp.Limit  => lowerLimit(l, ctx)
 }

 // PR-31 (ADR-008-R SSfilterPushdown wire-up, deferred from PR-28):
 // overload of `lower` that accepts a pre-filtered source DataFrame
 // from `SparkSourceResolver.resolveWithPushdown`. When the RelOp
 // tree starts with `RelOp.Scan`, the pre-filtered DF is used
 // INSTEAD of calling `spark.table(.) / spark.read.load(.)`
 // again -- the source-resolution has already been performed by
 // the resolver, AND the predicate has been pushed at the source.
 //
 // compatibility): this is an ADDITIVE overload. The existing
 // `lower(relOp, ctx)` signature is preserved -- callers that
 // don't use `resolveWithPushdown` are unaffected.
 def lower(
  relOp:   RelOp,
  ctx:   EngineContext,
  preFilteredDf: Option[org.apache.spark.sql.DataFrame]): Either[EngineError, DataFrame] =
 relOp match {
  case scan: RelOp.Scan  => lowerScan(scan, preFilteredDf)
  case f: RelOp.Filter => lowerFilter(f, ctx, preFilteredDf)
  case p: RelOp.Project => lowerProject(p, ctx, preFilteredDf)
  case a: RelOp.Aggregate => lowerAggregate(a, ctx, preFilteredDf)
  case j: RelOp.Join  => lowerJoin(j, ctx, preFilteredDf)
  case s: RelOp.Sort  => lowerSort(s, ctx, preFilteredDf)
  case l: RelOp.Limit  => lowerLimit(l, ctx, preFilteredDf)
 }

 // === per-node lowering methods (called recursively) ===

 /** Scan -> DataFrame: read the source via spark.table / spark.read.
 * Mirrors the legacy resolveSource() but typed for the IR path.
 *
 * The schema comes from the ACTUAL `df.schema` -- per
 *  mantra #3 (schema-drift verify
 * at the boundary). No caller-supplied "expected" schema. */
 def lowerScan(scan: RelOp.Scan): Either[EngineError, DataFrame] =
 lowerScan(scan, None)

 // PR-31 (ADR-008-R SSfilterPushdown wire-up, deferred from PR-28):
 // overload of `lowerScan` that accepts a pre-filtered DataFrame.
 // When defined, the pre-filtered DF is used DIRECTLY (skipping the
 // `spark.table / spark.read` call) -- closing the deferred
 // wire-up from PR-28's `resolveWithPushdown` (per senior
 // R-recommendation SS7.1 #2).
 //
 // for callers that don't opt in): when `preFilteredDf` is None,
 // this is a no-op passthrough to the existing spark.table /
 // spark.read path.
 def lowerScan(
  scan:   RelOp.Scan,
  preFilteredDf: Option[org.apache.spark.sql.DataFrame]): Either[EngineError, DataFrame] = {
 // PR-O1e (ADR-008-O, P0-3): column pruning via scan.projection.
 // (partition-pruning + projection-pushdown): without this,
 // every query reads ALL columns of the table (every partition),
 // which is fatal at scale for wide tables. The IR carries
 // the projection list (PR-H). When non-Nil, apply it via
 // `df.select(projection.map(_.toColumn): _*)` BEFORE the rest
 // of the plan sees the DataFrame.
 //
 // the.toColumn calls run in the driver; no executor-side closure
 // capture. The resulting DataFrame is lazy.
 //
 // Implementation: bind the match's Either result, then.map over
 // it to apply the projection. The match's arms stay return-typed
 // `Either[EngineError, DataFrame]`; the projection is applied via
 //.map which short-circuits the Left side.
 // PR-2/B2 (ADR-008-P §B2): narrow the 3 `case _: Exception` catches in lowerScan
 // to the specific Spark exceptions that `spark.table(.)` and
 // `spark.read.load(.)` raise when the source is not found / not readable.
 // catching broad `Exception` would also absorb `OutOfMemoryError`,
 // `StackOverflowError`, `SparkException` from a corrupt catalog -- all
 // of which indicate real Spark executor state problems operators need
 // to see. By naming only the "source not found" exceptions, we surface
 // them as typed `UnsupportedCapability` while letting everything else
 // propagate (so OOM, SparkException, etc. bubble up to the caller).
 //
 // Note: `Error` is NOT a subclass of `Exception`, so even the previous
 // broad catch couldn't trap `OutOfMemoryError` -- but by switching to
 // specific types we make the intent explicit and prevent future
 // subclasses (e.g. `SparkException` from a corrupt catalog) from
 // being silently absorbed.
 val resolved: Either[EngineError, DataFrame] = preFilteredDf match {
  case Some(df) =>
  // PR-31 wire-up: use the pre-filtered DataFrame directly.
  // The source-resolution + source-level filter have already
  // been applied by `SparkSourceResolver.resolveWithPushdown`.
  // We do NOT re-read the source -- that would defeat the
  // pushdown.
  Right(df)
  case None =>
  // No pre-filtered DF -- fall back to the original path
  // (spark.table / spark.read / spark.read.format.).
  // Per scala-impact-analysis-mindset SS3: zero behavior
  // change for callers that don't use resolveWithPushdown.
  scan.sourceRef match {
  case src: SourceRef.ByName =>
  try Right(spark.table(src.table)) catch {
   case _: org.apache.spark.sql.catalyst.analysis.NoSuchTableException =>
   try Right(spark.read.table(src.table)) catch {
    case _: org.apache.spark.sql.catalyst.analysis.NoSuchTableException =>
    Left(EngineError.UnsupportedCapability(
     engine = identity.name, capability = "SourceRef.ByName",
     message = s"Spark table '${src.table}' not found in any catalog."))
    case _: org.apache.spark.sql.AnalysisException =>
    Left(EngineError.UnsupportedCapability(
     engine = identity.name, capability = "SourceRef.ByName",
     message = s"Spark read.table('${src.table}') failed: AnalysisException."))
   }
   case _: org.apache.spark.sql.AnalysisException =>
   Left(EngineError.UnsupportedCapability(
    engine = identity.name, capability = "SourceRef.ByName",
    message = s"Spark table('${src.table}') failed: AnalysisException."))
  }
  case src: SourceRef.ByPath =>
  try Right(spark.read.format(src.format).options(src.options).load(src.path)) catch {
   case _: org.apache.spark.sql.AnalysisException =>
   Left(EngineError.UnsupportedCapability(
    engine = identity.name, capability = "SourceRef.ByPath",
    message = s"Spark path read failed: AnalysisException (format='${src.format}', path='${src.path}')."))
  }
  case _: SourceRef.ByProvider =>
  Left(EngineError.UnsupportedCapability(
   engine = identity.name, capability = "SourceRef.ByProvider",
   message = "SourceRef.ByProvider requires a registered ProviderRef closure."))
  }
 }
 // PR-O1c (ADR-008-O, P0-2): toColumn returns
 // Either[EngineError, Column]; the projection compiles via
 // the shared colsOf fold (no throw can escape -- FunctionCall
 // and ArrayValue surface as typed Left here).
 resolved.flatMap { df =>
  if (scan.projection.isEmpty) Right(df)
  else PortableExprCompiler.colsOf(scan.projection).map(df.select(_: _*))
 }
 }

 /** Filter -> DataFrame: walk the child, then df.filter(expr). */
 def lowerFilter(f: RelOp.Filter, ctx: EngineContext): Either[EngineError, DataFrame] =
 lowerFilter(f, ctx, None)

 // PR-31 (ADR-008-R SSfilterPushdown wire-up): overload that
 // forwards the preFilteredDf down to the recursive Scan lower.
 def lowerFilter(
  f:    RelOp.Filter,
  ctx:   EngineContext,
  preFilteredDf: Option[org.apache.spark.sql.DataFrame]): Either[EngineError, DataFrame] =
 for {
  child <- lower(f.input, ctx, preFilteredDf)
  pred <- PortableExprCompiler.toColumn(f.predicate)
 } yield child.filter(pred)

 /** Project -> DataFrame: walk the child, then df.select with
 * `(expr, alias)` per the IR's projection list. An empty list
 * returns the child unchanged. */
 def lowerProject(p: RelOp.Project, ctx: EngineContext): Either[EngineError, DataFrame] =
 lowerProject(p, ctx, None)

 // PR-31 (ADR-008-R SSfilterPushdown wire-up): overload that
 // forwards the preFilteredDf down to the recursive Scan lower.
 def lowerProject(
  p:    RelOp.Project,
  ctx:   EngineContext,
  preFilteredDf: Option[org.apache.spark.sql.DataFrame]): Either[EngineError, DataFrame] =
 for {
  child <- lower(p.input, ctx, preFilteredDf)
  cols <- p.expressions.foldLeft[Either[EngineError, List[Column]]](Right(Nil)) {
  (accE, pair) => for {
   acc <- accE
   c  <- PortableExprCompiler.toColumn(pair._1)
  } yield acc :+ c.as(pair._2)
  }
 } yield if (cols.isEmpty) child else child.select(cols: _*)

 /** Sort -> DataFrame: walk the child, then df.orderBy with
 * per-key direction + null ordering. */
 def lowerSort(s: RelOp.Sort, ctx: EngineContext): Either[EngineError, DataFrame] =
 lowerSort(s, ctx, None)

 // PR-31 (ADR-008-R SSfilterPushdown wire-up): overload that
 // forwards the preFilteredDf down to the recursive Scan lower.
 def lowerSort(
  s:    RelOp.Sort,
  ctx:   EngineContext,
  preFilteredDf: Option[org.apache.spark.sql.DataFrame]): Either[EngineError, DataFrame] =
 lower(s.input, ctx, preFilteredDf).map { df =>
  if (s.keys.isEmpty) df
  else {
  val sortCols = s.keys.map { k =>
   val base = df.col(k.expression.toString)
   val dirCol: Column = k.direction match {
   case SortDirection.Ascending => base.asc
   case SortDirection.Descending => base.desc
   }
   (k.direction, k.nullOrdering) match {
   case (SortDirection.Ascending,
     NullOrdering.First) => dirCol.asc_nulls_first
   case (SortDirection.Ascending,
     NullOrdering.Last) => dirCol.asc_nulls_last
   case (SortDirection.Descending,
     NullOrdering.First) => dirCol.desc_nulls_first
   case (SortDirection.Descending,
     NullOrdering.Last) => dirCol.desc_nulls_last
   }
  }
  df.orderBy(sortCols: _*)
  }
 }

 /** Limit -> DataFrame: walk the child, then df.limit. The pass-
 * through sentinel `Long.MaxValue` (per PR-L's model-without-
 * request-limit shape) is SKIPPED -- Spark 3.5 rejects
 * `.limit(-1)` with INVALID_LIMIT_LIKE_EXPRESSION when the
 * Long.MaxValue casts to -1. */
 def lowerLimit(l: RelOp.Limit, ctx: EngineContext): Either[EngineError, DataFrame] =
 lowerLimit(l, ctx, None)

 // PR-31 (ADR-008-R SSfilterPushdown wire-up): overload that
 // forwards the preFilteredDf down to the recursive Scan lower.
 def lowerLimit(
  l:    RelOp.Limit,
  ctx:   EngineContext,
  preFilteredDf: Option[org.apache.spark.sql.DataFrame]): Either[EngineError, DataFrame] =
 lower(l.input, ctx, preFilteredDf).map { df =>
  if (l.count == Long.MaxValue) df
  else df.limit(l.count.toInt).offset(l.offset.toInt)
 }

 /** Aggregate -> DataFrame: for the GAP-5 minimum, we use the
 * `applyAggregations` helper with a synthesised Model. A clean
 * direct lower is deferred to a future PR (the IR Aggregate
 * carries typed groupBy keys; the legacy Model uses String
 * column names; the conversion is a small bridge). */
 /** PR-N3: direct Aggregate -> DataFrame lowering. The previous
 * path synthesised a Model + re-resolved the source + called
 * `pc.applyAggregations` -- three indirections for what Spark
 * already supports natively as `df.groupBy(.).agg(.)`. This
 * path uses the IR's groupBy + aggregates directly and renders
 * the aggregate columns via `pc.renderAggregate` (the same
 * per-fn renderer applyAggregations uses internally).
 *
 * Falls back to the legacy synthModel path ONLY when the input
 * is NOT a Scan (e.g. a Project or Filter above the scan) --
 * the direct path is then built on the recursively-lowered
 * DataFrame, not re-resolved.
 */
 def lowerAggregate(agg: RelOp.Aggregate, ctx: EngineContext): Either[EngineError, DataFrame] =
 lowerAggregate(agg, ctx, None)

 // PR-31 (ADR-008-R SSfilterPushdown wire-up): overload that
 // forwards the preFilteredDf down to the recursive Scan lower.
 def lowerAggregate(
  agg:   RelOp.Aggregate,
  ctx:   EngineContext,
  preFilteredDf: Option[org.apache.spark.sql.DataFrame]): Either[EngineError, DataFrame] = {
 lower(agg.input, ctx, preFilteredDf).flatMap { df =>
  // groupBy: convert each Expr to a Spark Column.
  val groupByCols: Array[Column] = agg.groupBy.map { e =>
  val n = e match {
   case Expr.FieldRef(name) => name
   case Expr.MeasureRef(name) => name
   case _ => null // typed-error below
  }
  if (n == null) {
   // Signal the typed error via a sentinel: return an error now.
   return Left(EngineError.UnsupportedCapability(
   engine  = identity.name,
   capability = "MinimalRelOpLowerer.dim",
   message = s"PR-N3: only FieldRef/MeasureRef groupBy keys are supported. Got: ${e.getClass.getSimpleName}"))
  }
  df.col(n)
  }.toArray
  val aggColsE: Either[EngineError, List[Column]] =
  agg.aggregates.foldLeft[Either[EngineError, List[Column]]](Right(Nil)) {
   (accE, call) => for {
   acc <- accE
   c <- pc.renderAggregate(call)
   } yield acc :+ c.as(call.alias)
  }
  aggColsE.flatMap { aggCols =>
  if (aggCols.isEmpty) {
   // No aggregates: `Aggregate(_, groupBy, Nil)` per the IR
   // contract means "deduplicate by groupBy keys". The Spark
   // shape for DISTINCT-on-columns is `df.dropDuplicates(colNames)`
   // where colNames is Array[String]. We map Column[] -> String[].
   val groupByNames: Array[String] = groupByCols.map(_.toString)
   Right(df.dropDuplicates(groupByNames))
  } else {
   Right(df.groupBy(groupByCols: _*).agg(aggCols.head, aggCols.tail: _*))
  }
  }
 }
 }

 /** Join -> DataFrame: synthesise a Model with the join spec,
 * call the legacy compile(model, ctx) (which runs applyJoins).
 * Per PR-M5: derive a 1-key equi-join from `j.condition` when
 * it is `Expr.Equal(FieldRef(l), FieldRef(r))`. Multi-key joins
 * remain deferred (typed UnsupportedCapability at the legacy
 * applyJoins step). */
 /** PR-N2: flatten an `Expr.And(Expr.Equal(.),.)` tree into a
 * `List[(leftField, rightField)]`. Single `Equal` -> 1 pair.
 * Mixed AND/Eq tree -> only the `Expr.Equal(Expr.FieldRef, Expr.FieldRef)`
 * sub-pairs are kept (the legacy single-key path only matched the
 * top-level Equal). Unknown shapes return Nil (the synthesised
 * join becomes a cross-equality on the whole condition).
 *
 * Package-private (no `private` keyword) so the spec can test
 * without exposing it to other connectors.
 */
 def extractJoinKeys(cond: Expr): List[(String, String)] = cond match {
 case Expr.And(l, r) =>
  extractJoinKeys(l) ++ extractJoinKeys(r)
 case Expr.Equal(Expr.FieldRef(l), Expr.FieldRef(r)) =>
  List((l, r))
 case _ =>
  Nil
 }

 /** PR-N4: direct Join -> DataFrame lowering. The previous path
 * synthesised a Model + re-resolved the right side + called
 * `pc.compile(synthModel, ctx)` -- three indirections for what
 * Spark already supports natively as `df.join(right, joinExpr, joinType)`.
 *
 * Constraints (typed errors, never a Spark runtime crash):
 * - j.left and j.right MUST be RelOp.Scan (the IR-minimum; nested
 * joins will fail at the typed-error gate until full tree lowering lands)
 * - j.kind MUST be one of Inner/Left/Right/Full/Cross
 * - j.condition MUST extract at least 1 key via extractJoinKeys (PR-N2)
 */
 def lowerJoin(j: RelOp.Join, ctx: EngineContext): Either[EngineError, DataFrame] =
 lowerJoin(j, ctx, None)

 // PR-31 (ADR-008-R SSfilterPushdown wire-up): overload that
 // forwards the preFilteredDf to the LEFT scan lowering only.
 // The RIGHT scan continues to use its own path (PR-28's
 // pushdown is per-source; a joined source is a separate concern).
 def lowerJoin(
  j:    RelOp.Join,
  ctx:   EngineContext,
  preFilteredDf: Option[org.apache.spark.sql.DataFrame]): Either[EngineError, DataFrame] = {
 val leftScan = j.left match {
  case s: RelOp.Scan => s
  case _ => return Left(EngineError.UnsupportedCapability(
  engine  = identity.name,
  capability = "MinimalRelOpLowerer.join.left",
  message = s"PR-N4 minimum: Join.left must be a Scan. Got: ${j.left.getClass.getSimpleName}"))
 }
 val rightScan = j.right match {
  case s: RelOp.Scan => s
  case _ => return Left(EngineError.UnsupportedCapability(
  engine  = identity.name,
  capability = "MinimalRelOpLowerer.join.right",
  message = s"PR-N4 minimum: Join.right must be a Scan. Got: ${j.right.getClass.getSimpleName}"))
 }
 val joinType = j.kind match {
  case io.sm8.core.rel.JoinKind.Inner => "inner"
  case io.sm8.core.rel.JoinKind.Left => "left"
  case io.sm8.core.rel.JoinKind.Right => "right"
  case io.sm8.core.rel.JoinKind.Full => "outer"
  case io.sm8.core.rel.JoinKind.Cross => "cross"
 }
 val keys: List[(String, String)] = extractJoinKeys(j.condition)
 if (keys.isEmpty) {
  return Left(EngineError.UnsupportedCapability(
  engine  = identity.name,
  capability = "MinimalRelOpLowerer.join.keys",
  message = s"PR-N4 minimum: Join.condition must contain at least one Expr.Equal(FieldRef, FieldRef). Got: ${j.condition.getClass.getSimpleName}"))
 }
 // PR-O2 (ADR-008-O, P0-4): size-based broadcast-join hint.
 // Decision (evaluated AFTER rightDf is loaded so size probe is meaningful):
 // when `ctx.joinHints.broadcastRightBelowBytes` is set AND the right-side
 // DataFrame's sizeInBytes is below the threshold, broadcast.
 // See `PortableQueryCompiler.applyJoins` for the long form.
 import org.apache.spark.sql.functions.broadcast
 for {
  leftDf <- lower(leftScan, ctx, preFilteredDf)
  rightDf <- lower(rightScan, ctx)
  // PR-2/B2: narrow the broad `catch { case _: Throwable => }` to the
  // specific `AnalysisException` that the Spark `stats.sizeInBytes` call
  // raises when stats are unavailable. 
  // SS4 ("never swallow the specific"): catching `Throwable` would also
  // absorb `OutOfMemoryError` and `StackOverflowError` -- real problems
  // operators need to see. The previous code returned `Long.MaxValue`
  // which incorrectly signalled "table > 2^63 bytes -> never broadcast"
  // when the truth was "stats unavailable -> fall back to autoBroadcast".
  rightBytes: Long = try {
  rightDf.queryExecution.analyzed.stats.sizeInBytes.toLong
  } catch {
  case _: org.apache.spark.sql.AnalysisException =>
   // Spark stats unavailable for this source (e.g. streaming DF,
   // or a freshly-created view with no committed stats). Fall back
   // to Spark's own autoBroadcastJoinThreshold heuristic rather than
   // treating it as "table too large".
   -1L
  case _: NumberFormatException =>
   // BigInt overflow toLong can raise this on absurdly large
   // tables (theoretical 2^63 byte boundary).
   Long.MaxValue
  }
  shouldBroadcast: Boolean = ctx.joinHints.broadcastRightBelowBytes match {
  case Some(threshold) => rightBytes >= 0L && rightBytes <= threshold
  case None   => false
  }
  rightDfEff: org.apache.spark.sql.DataFrame =
  if (shouldBroadcast) broadcast(rightDf) else rightDf
  // Only build joinExpr when the join kind needs it (Cross skips).
  // building the joinExpr unconditionally adds an unnecessary Catalyst
  // projection; gating on `joinType` saves a no-op expression for Cross.
  joinExpr: org.apache.spark.sql.Column =
  keys.map { case (l, r) => leftDf.col(l) === rightDfEff.col(r) }.reduce(_ && _)
  // PR-2/B3: Cross join semantics. Per the RelOp.Join contract (PR-H):
  // Cross is UNCONDITIONAL -- the key/condition is ignored; the join is
  // the plain Cartesian product. The previous code called
  // `leftDf.join(rightDfEff, joinExpr, "inner")` for Cross, which used the
  // equi-key joinExpr -- NOT a cross join. Fix: use `crossJoin(rightDfEff)`
  // and skip the joinExpr. 
  // ("trust compiler, not runtime"): the previous code compiled clean but
  // was a silent semantic bug -- Cross was implemented as an Inner join.
  // implementations): the typed `RelOp.Join.kind` contract is honored.
  joined: org.apache.spark.sql.DataFrame = joinType match {
  case "cross" => leftDf.crossJoin(rightDfEff)
  case _  => leftDf.join(rightDfEff, joinExpr, joinType)
  }
  // Base-column-wins invariant (mirrors the legacy `applyJoins`
  // path): when BOTH sides carry the same join-key column name, the
  // joined DF would hold TWO columns of that name and every later
  // unqualified reference becomes ambiguous. Drop the RIGHT-side
  // key column so the left (source-of-truth) column wins.
  //
  // Restricted to Inner/Left (the cases where the left side is
  // authoritative): for Right/Full joins the RIGHT side is the
  // driving side and its key column is the one the write target
  // retains — dropping it would NULL out the join key on the very
  // rows the join exists to keep (right-only rows). For Cross
  // (unconditional Cartesian product) and distinct-named keys there
  // is no name collision to resolve, so no drop.
  shouldDedup: Boolean = (joinType == "inner" || joinType == "left")
  dropKeys: Seq[String] =
    if (shouldDedup) keys.map(_._2) else Nil
  joinedDeduped: org.apache.spark.sql.DataFrame =
  dropKeys.foldLeft(joined) { (df, rk) => df.drop(rightDfEff.col(rk)) }
 } yield joinedDeduped
 }
}
