/*
 * SM8 Spark Query Compiler - the engine-specific DataFrame builder
 * that walks a portable `io.sm8.core.model.Model` and emits a Spark
 * `DataFrame`.
 *
 * PR-K (per ADR-008-H section PR-K + the user's 2026-08-16 directive
 * "join one to one, one to many, many to many op cross-join" +
 * "aggregate"): adds the three compile stages the legacy shipped:
 *
 * 1. `applyJoins`  -- model.joins (JoinSpec from PR-J) folded
 *       onto the source DataFrame. 5 kinds
 *       (Inner/Left/Right/Full/Cross), single-key
 *       equi-join (multi-key deferred, typed
 *       UnsupportedCapability).
 * 2. `applyAggregations` -- model.measures (typed AggregateCall
 *       from PR-J) + model.calculatedMeasures
 *       (Expr from PR-J). Two paths: groupBy+agg
 *       (default) or window functions (when any
 *       calculated measure references Expr.All).
 * 3. `renderAggregate` -- AggregateCall → Spark Column for the 6
 *       wired fns (Sum/Count/CountDistinct/Avg/
 *       Min/Max). The other 10 surface as typed
 *       EngineError.FeatureDeferred at the
 *       compile boundary (pre-validated -- never
 *       a silent no-op, per ADR-008-H).
 *
 * Per  section 1 (behavior in adapters,
 * data in core): the `Model` is pure data in sm8-core; this
 * compiler is the Spark-specific behavior that converts it to a
 * `DataFrame`. Other engines (Trino, DuckDB) have analogous
 * compilers that emit SQL strings instead.
 *
 * Per  mantra #1 (closures captured
 * by Spark UDFs / lambdas must avoid non-serializable refs):
 * - This class `extends java.io.Serializable`.
 * - Captures a SparkSession (which Spark 3.5 + 4.1 guarantee is
 *  Serializable). NO static / ThreadLocal state (the legacy's
 *  `object PortableQueryCompiler { @volatile var _spark }` +
 *  setSparkSession/clearSparkSession companion state is NOT
 *  ported -- constructor injection only).
 * - The SparkTypeBridge companion is a pure object (Serializable).
 * - No DataFrame / Iterator / Connection is closed over.
 *
 * Per  mantra #3 (long-lived state):
 * - No `@volatile var`, no `clear()` method. The SparkSession ref
 *  is constructor-frozen.
 *
 * Per  mantra #3 (count allocations):
 * - The compile path is iterative over the flat Model fields.
 * - applyFilters/applyJoins: single foldLeft each (no double-walk).
 * - collectAllReferences: single mutable-set accumulator walk.
 * - The groupBy+agg path allocates one Column per measure; the
 *  window path one withColumn per measure + one per calc.
 *
 * Per [[scala-error-handling-mindset]]: unsupported shapes surface at
 * the compile boundary as typed `EngineError` (FeatureDeferred for
 * the 10 unwired aggregates; UnsupportedCapability for multi-key
 * joins + unresolvable right-side models). Internals are total.
 *
 * Per scala-impact-analysis-mindset: the compile path DOES NOT
 * cross the executor boundary. The output `DataFrame` is lazy;
 * only `collect()` triggers execution. Per
 *  mantra #5 (driver vs executor
 * asymmetry): the engine driver calls compile() + collect() in
 * the driver process.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, EngineError, JoinStrategy}
import io.sm8.core.expr.Expr
import io.sm8.core.model.{CalculatedMeasure, FilterSpec, JoinSpec, Model, SourceRef}
import io.sm8.core.rel.{AggregateCall, AggregateFn, JoinKind}

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
 avg, count, countDistinct, lit,
 max => sparkMax, min => sparkMin, sum => sparkSum,
 sumDistinct => sparkSumDistinct,
 row_number => sparkRowNumber, rank => sparkRank, dense_rank => sparkDenseRank}

// Not `final`: the P2 cluster regression tests (site 4) subclass this
 // compiler to override the `readTableByName` / `readTableByPath` /
 // `readTableByPathFallback` seams and inject controlled failures
 // (e.g. `InterruptedException`), verifying the PR-176 NonFatal
 // discipline narrowing at the `resolveSource` IO boundaries. The
 // compiler remains a public API entry point; the seams are
 // package-private.
class PortableQueryCompiler(val spark: SparkSession)
 extends java.io.Serializable {

 /**
 * The aggregate functions wired to Spark Columns in PR-K (the
 * cases (Stddev, Variance, Median, Percentile, ApproxPercentile,
 * First, Last) surface as typed `EngineError.FeatureDeferred` at
 * the compile boundary -- per ADR-008-H: never a silent no-op.
 *
 * right shape -- membership is the only question; order is
 * irrelevant; the closed set lives beside the only function
 * that consumes it.
 */
 private val SupportedAggregates: Set[AggregateFn] = Set(
 AggregateFn.Sum, AggregateFn.Count, AggregateFn.CountDistinct,
 AggregateFn.Avg, AggregateFn.Min, AggregateFn.Max)

 /** Compile a portable 
 *
 * The path is (PR-K order, matching the legacy):
 * 1. resolveSource(model.source) -> Either[EngineError, DataFrame]
 * 2. applyFilters(df, model.filters) -> DataFrame (foldLeft)
 * 3. applyJoins(df, model.joins)  -> Either (foldLeft; 5 kinds,
 *  single-key equi-join; multi-key + missing right-side are
 *  typed UnsupportedCapability)
 * 4. if model.measures.nonEmpty: applyAggregations (groupBy+agg,
 *  or window functions when a calculated measure references
 *  Expr.All) -- dimensions become the groupBy keys.
 *  else: selectDimensions (the pre-PR-K projection path --
 *  a measure-less model is a plain filtered projection).
 *
 * @return `Right(DataFrame)` on success; `Left(EngineError)` if
 *   the source can't be resolved, a join can't be applied,
 *   or a measure's aggregate is not wired.
 */
 def compile(
  model: Model,
  ctx: EngineContext): Either[EngineError, DataFrame] = {
 // Seed the broadcast byte-gate from the model's join estimates
 // BEFORE the join step. Without this seed, callers that bypass
 // the request-layer seed sites (`SparkEngineProvider.query`,
 // `compileModelToDataFrame`) read an un-seeded `ctx`, and
 // `applyJoins` falls through to Spark's `autoBroadcastJoinThreshold`
 // heuristic — the gate is silently OFF. The null-spark provider
 // path skips the seed (the config-less smoke); the helper itself
 // is null-safe.
 val seededCtx = PortableQueryCompiler.seedBroadcastThreshold(spark, ctx, model)
 for {
  sourceDf <- resolveSource(model.source)
  filtered <- applyFilters(sourceDf, model.filters)
  joined  <- applyJoins(filtered, model.joins, seededCtx)
  aggregated <- applyAggregations(joined, model)
 } yield aggregated
 }

 /** PR-M4 (GAP 5): the IR path. `QueryBuilder.build` lowers a
 * `Model` to a portable `RelOp` tree; this method compiles a
 * `RelOp` to a `DataFrame` via the same join+agg+projection
 * pipeline. Symmetric to `compile(model, ctx)` but operates on
 * the IR rather than the model directly.
 *
 * The current implementation is intentionally MINIMAL: it
 * dispatches the top-level node (Scan / Filter / Project /
 * Aggregate / Join / Sort / Limit) and falls through to the
 * legacy path for nested cases. Full RelOp->DataFrame lowering
 * is the next major PR; the minimum that GAP 5 demands is a
 * public entry point + the existing pipeline accepting the
 * produced tree. */
 /** PR-M5 commit 2: delegate to `MinimalRelOpLowerer`. The
 * per-node lowering logic now lives in its own class (one place
 * to extend the full RelOp -> DataFrame lowering in future PRs).
 * behavior is identical to the prior inlining. */
 private val minimalRelOpLowerer: MinimalRelOpLowerer =
 new MinimalRelOpLowerer(spark, this)

 /** PR-M4 (GAP 5) + PR-M5 commit 2: IR -> DataFrame lowering.
 * Thin delegator to `MinimalRelOpLowerer`. The 7 RelOp cases
 * (Scan / Filter / Project / Sort / Limit / Aggregate / Join)
 * are owned by the lowerer. */
 def compileRelOp(
  relOp: io.sm8.core.rel.RelOp,
  ctx: EngineContext): Either[EngineError, DataFrame] = minimalRelOpLowerer.lower(relOp, ctx)

 // PR-31 (ADR-008-R SSfilterPushdown wire-up, deferred from PR-28):
 // overload of `compileRelOp` that accepts a pre-filtered source
 // DataFrame from `SparkSourceResolver.resolveWithPushdown`. The
 // pre-filtered DF flows down through the RelOp tree (forwarded
 // to the Scan lower) so the source-level filter is preserved
 // across the compile step.
 //
 // compatibility): this is an ADDITIVE overload. The existing
 // `compileRelOp(relOp, ctx)` signature is preserved.
 def compileRelOp(
  relOp:   io.sm8.core.rel.RelOp,
  ctx:   EngineContext,
  preFilteredDf: Option[org.apache.spark.sql.DataFrame]): Either[EngineError, DataFrame] =
 minimalRelOpLowerer.lower(relOp, ctx, preFilteredDf)

 // PR-32 (ADR-008-R SSR3 broader fix; the PR-27 / PR-31 work-around
 // is lifted into the canonical entry point): overload of
 // `compileRelOp` that accepts a `Model` + the resolved source
 // schema. The validator (`ModelValidator.validateAgainstSchema`)
 // runs FIRST, surfacing any schema-mismatch as a typed `Left`
 // before the lowerer sees the relOp. This means ANY direct caller
 // of `compileRelOp` benefits from the validation -- not just
 // callers that go through `SparkEngineProvider.compileModelToDataFrame`.
 //
 // compatibility): this is an ADDITIVE overload. The existing
 // `compileRelOp(relOp, ctx)` and `compileRelOp(relOp, ctx,
 // preFilteredDf)` signatures are preserved.
 //
 // constructor for validity-at-boundary): the validator is the
 // single source of truth for schema-mismatch detection. By
 // moving it into the canonical entry point, we eliminate the
 // earlier work-around in `compileModelToDataFrame` (the
 // PR-27 helper that called the validator BEFORE compileRelOp).
 //
 // the `Either[ModelValidationError, Unit]` return type forces the
 // caller to handle the validation result at compile time (no
 // silent null / no try-catch / no runtime `UNRESOLVED_COLUMN`).
 // The 5-arg overload has the Model in scope, so it can apply the
 // broadcast seed. The 2-arg / 3-arg overloads only see a `RelOp`
 // (no Model, no `joins[].estimatedRows`); direct callers of those
 // overloads must seed `ctx.joinHints.broadcastRightBelowBytes`
 // themselves — request-layer paths (`SparkEngineProvider.query` and
 // `compileModelToDataFrame`) and `SparkBroadcastSeedSpec` fixtures
 // already do so before delegating.
 def compileRelOp(
  model:   io.sm8.core.model.Model,
  relOp:   io.sm8.core.rel.RelOp,
  ctx:   EngineContext,
  scan:   io.sm8.core.engine.ResolvedSource.Scan,
  preFilteredDf: Option[org.apache.spark.sql.DataFrame]): Either[EngineError, DataFrame] = {
 val seededCtx = PortableQueryCompiler.seedBroadcastThreshold(spark, ctx, model)
 io.sm8.core.model.ModelValidator.validateAgainstSchema(model, scan).left.map(e => EngineError.UnsupportedCapability(
  engine  = "spark-connector",
  capability = "ModelValidator.validateAgainstSchema",
  message = e.message)).flatMap(_ => compileRelOp(relOp, seededCtx, preFilteredDf))
 }

/** Aggregate -> DataFrame: for the GAP-5 minimum, we use the
 * `compileRelOpAggregateSubtree` helper that recursively walks
 * the relOp's child and uses the child's resulting DataFrame
 * as the base for the aggregate application. The existing
 */
private def resolveSource(
 source: SourceRef): Either[EngineError, DataFrame] = source match {
 case src: SourceRef.ByName =>
 // Resolution strategy: try spark.table(.) first (handles
 // both catalog tables AND session-scoped temp views); fall
 // back to spark.read.table(src.table) for catalog tables.
 //
 // P2 cluster (PR-176 NonFatal discipline by topic): both
 // catches are narrowed to `AnalysisException` (the specific
 // Spark exception raised when a table is not in the active
 // catalog). The narrow discipline mirrors the canonical
 // `MinimalRelOpLowerer.scala:194-211` pattern for spark-connector
 // IO boundaries. Other `NonFatal` failures propagate to
 // `EngineService.executeEngine:258-264`'s `NonFatal -> ProviderInvocationFailed`
 // typed conversion; `Error` subclasses propagate to the caller
 // per the PR-176 discipline.
 try {
 Right(readTableByName(src.table))
 } catch {
 case _: org.apache.spark.sql.AnalysisException =>
  try {
  Right(readTableByNameFallback(src.table))
  } catch {
  case _: org.apache.spark.sql.AnalysisException =>
   Left(EngineError.UnsupportedCapability(
   engine = "spark-3.5",
   capability = "SourceRef.ByName",
   message = s"Spark table '${src.table}' not found."))
  }
 }

 case src: SourceRef.ByPath =>
 // P2 cluster (PR-176 NonFatal discipline by topic): the
 // catch is narrowed to `AnalysisException`, mirroring
 // `MinimalRelOpLowerer.scala:213-218`. The single-convention
 // rule (karpathy-app-design) requires all spark-connector
 // IO-boundary catches to follow this pattern.
 try {
 Right(
 readPathByPath(src.format, src.options, src.path)
)
 } catch {
 case _: org.apache.spark.sql.AnalysisException =>
 Left(EngineError.UnsupportedCapability(
 engine = "spark-3.5",
 capability = "SourceRef.ByPath",
 message = s"Spark path read failed: AnalysisException (format='${src.format}', path='${src.path}')."))
 }

 case _: SourceRef.ByProvider =>
 Left(EngineError.UnsupportedCapability(
 engine = "spark-3.5",
 capability = "SourceRef.ByProvider",
 message = "SourceRef.ByProvider requires a registered ProviderRef closure (deferred to future PR)."))
}

// P2 cluster (PR-176 NonFatal discipline by topic): test seams.
// Exposed `protected[spark]` so the regression tests in this package
// can inject controlled failures (e.g. `InterruptedException`) without
// depending on Spark's runtime catalog / filesystem behavior.
// Production behavior is unchanged: `spark.table(name)` /
// `spark.read.table(name)` / `spark.read.format(fmt).options(opts).load(path)`.
protected[spark] def readTableByName(name: String): org.apache.spark.sql.DataFrame =
 spark.table(name)
protected[spark] def readTableByNameFallback(name: String): org.apache.spark.sql.DataFrame =
 spark.read.table(name)
protected[spark] def readPathByPath(
 format: String,
 options: Map[String, String],
 path: String): org.apache.spark.sql.DataFrame = {
 val reader = spark.read.format(format)
 options.foldLeft(reader)((acc, kv) => acc.option(kv._1, kv._2)).load(path)
}


 private def applyFilters(
  df:  DataFrame,
  filters: List[FilterSpec]): Either[EngineError, DataFrame] = filters.foldLeft[Either[EngineError, DataFrame]](Right(df)) {
 (accE, f) => for {
  acc <- accE
  col <- PortableExprCompiler.toColumn(f.predicate)
 } yield acc.filter(col)
 }

 // -- join application (PR-K) --

 /** Fold the model's 
 *
 * v0.1.0 scope (matching the legacy's v0.3.1): single-key
 * equi-joins over the 5 kinds. The right side resolves via
 * `spark.table(js.rightModel)` -- a catalog table or temp view
 * registered under the joined model's name. Multi-key joins
 * and unresolvable right-side models surface as typed
 * `EngineError.UnsupportedCapability` (never a silent no-op).
 *
 * runs in the driver; `df.join` builds the logical plan (lazy);
 * no executor-side closure capture.
 */
 private def applyJoins(
  df: DataFrame,
  joins: List[JoinSpec],
  ctx: io.sm8.core.engine.EngineContext): Either[EngineError, DataFrame] =
 joins.foldLeft[Either[EngineError, DataFrame]](Right(df)) { (accE, js) =>
  accE.flatMap { accDf =>
  // Multi-key is deferred (typed error, per the legacy scope).
  if (js.keys.size != 1) {
   Left(EngineError.UnsupportedCapability(
   engine  = "spark-3.5",
   capability = "JoinSpec.keys",
   message = s"Multi-key joins (${js.keys.size} keys) deferred to a future PR."))
  } else {
   // Resolve the right-side model by name in the active catalog.
   // PR-2/B2 (ADR-008-P §B2): narrow the catch from broad `case _: Exception`
   // to the specific Spark exceptions that `spark.table(.)` raises when the
   // table is not found. 
   // swallow the specific"): catching `Exception` would also absorb
   // `OutOfMemoryError` / `StackOverflowError` / `SparkException` /
   // `AnalysisException` -- all of which indicate real Spark executor
   // state problems operators need to see. By naming only the "table
   // not found" exceptions, we surface `NoSuchTableException` as
   // `UnsupportedCapability(capability = "JoinSpec.rightModel")` while
   // letting everything else propagate. `OutOfMemoryError` is NOT
   // a subclass of `Exception` (it's an `Error`), so even the previous
   // broad catch couldn't trap it -- but by switching to specific
   // types we make the intent explicit and prevent future
   // subclasses (e.g. `SparkException` from a corrupt catalog)
   // from being silently absorbed.
   val rightDf = try Right(spark.table(js.rightModel)) catch {
   case _: org.apache.spark.sql.catalyst.analysis.NoSuchTableException =>
    Left(EngineError.UnsupportedCapability(
    engine  = "spark-3.5",
    capability = "JoinSpec.rightModel",
    message = s"Right-side model '${js.rightModel}' not found."))
   case _: org.apache.spark.sql.AnalysisException =>
    Left(EngineError.UnsupportedCapability(
    engine  = "spark-3.5",
    capability = "JoinSpec.rightModel",
    message = s"Right-side model '${js.rightModel}' not resolvable: AnalysisException."))
   }
   // PR-2/B3 (ADR-008-P §B3): reject Cross + non-None preferredStrategy BEFORE
   // building the join. Per the RelOp.Join contract (PR-H): Cross is
   // UNCONDITIONAL (key/condition ignored; the join is the plain Cartesian
   // product). Spark's `hint()` API is a no-op on `crossJoin()` -- the
   // Broadcast/ShuffleHash/SortMerge hints cannot influence a Cartesian
   // product's execution. So if a caller asks for Cross + a non-None
   // preferredStrategy, that's a request that's impossible to honor.
   // runtime): fail loud with a typed error rather than silently
   // dropping the hint. 
   // implementations): the typed-error contract is honored.
   rightDf.flatMap { rDf =>
   (js.kind, ctx.joinHints.preferredStrategy) match {
    case (JoinKind.Cross, Some(strategy)) =>
    Left(EngineError.UnsupportedCapability(
     engine  = "spark-3.5",
     capability = "JoinSpec.kind + preferredStrategy",
     message = s"Cross join (PR-H) is unconditional (Cartesian product); " +
        s"the preferredStrategy hint '$strategy' cannot be honored. " +
        s"Either drop the preferredStrategy hint, or change JoinKind to " +
        s"Inner/Left/Right/Full."))
    case _ => Right(rDf)
   }
   }.flatMap { rDf =>
   val (leftKey, rightKey) = js.keys.head
   // Per the legacy DESIGN SS6.3 (4): when both sides carry the
   // join key, Spark keeps both columns and every later
   // unqualified reference is ambiguous. Dedup by dropping the
   // RIGHT-side key column (left-authoritative -- the legacy's
   // "base-column-wins-on-collision" invariant).
   //
   // PR-M4 (GAP 8): honor `ctx.joinHints.preferredStrategy` via
   // the Spark `hint()` API. Broadcast is the most consequential
   // hint (turns a shuffle+shuffle-exchange into a local
   // map-side broadcast). 
   // mantra #1 (closure-safety): the hint is a string
   // descriptor -- no captured lambdas.
   // PR-O2 (ADR-008-O, P0-4): honor
   // `ctx.joinHints.broadcastRightBelowBytes`. Per
   // (broadcast joins for small right-side tables): a
   // 100MB-left x 1MB-right join WITHOUT broadcast
   // becomes a shuffle-hash join -- slow at scale.
   //
   // Decision: when the hint is set AND `rDf.sizeInBytes`
   // is below the threshold, broadcast. Otherwise fall
   // through to the default (no broadcast). When the hint
   // is unset, we trust Spark's own
   // `autoBroadcastJoinThreshold` (typically 10MB).
   //
   // is a single `queryExecution.analyzed.stats.sizeInBytes`
   // call -- O(1), no row materialization.
   import org.apache.spark.sql.functions.broadcast
   // Spark 3.5 returns sizeInBytes as BigInt; convert to Long.
   val rightBytes: Long = try {
    rDf.queryExecution.analyzed.stats.sizeInBytes.toLong
   } catch { case _: org.apache.spark.sql.AnalysisException => Long.MaxValue }
   val shouldBroadcast: Boolean = ctx.joinHints.broadcastRightBelowBytes match {
    case Some(threshold) => rightBytes >= 0L && rightBytes <= threshold
    case None   => false // trust Spark's default heuristic
   }
   val rDfEff: DataFrame = if (shouldBroadcast) broadcast(rDf) else rDf

   val baseJoin: DataFrame = js.kind match {
    case JoinKind.Inner => accDf.join(rDfEff, accDf(leftKey) === rDfEff(rightKey), "inner")
    case JoinKind.Left => accDf.join(rDfEff, accDf(leftKey) === rDfEff(rightKey), "left")
    case JoinKind.Right => accDf.join(rDfEff, accDf(leftKey) === rDfEff(rightKey), "right")
    case JoinKind.Full => accDf.join(rDfEff, accDf(leftKey) === rDfEff(rightKey), "outer")
    // Per the RelOp.Join contract (PR-H): Cross is UNCONDITIONAL
    // -- the key/condition is ignored; the join is the plain
    // Cartesian product.
    case JoinKind.Cross => accDf.crossJoin(rDfEff)
   }
   val hinted: DataFrame = ctx.joinHints.preferredStrategy match {
    case Some(JoinStrategy.Broadcast) => baseJoin.hint("broadcast")
    case Some(JoinStrategy.ShuffleHash) => baseJoin.hint("shuffle_hash")
    case Some(JoinStrategy.SortMerge) => baseJoin.hint("merge")
    case None => baseJoin
   }
   Right(hinted.drop(rDfEff(rightKey)))
   }
  }
  }
 }

 // -- calculated-measure All-reference walker (PR-K) --

 /** Collect every distinct [[Expr.All]] reference in the
 * calculated-measure expressions. Used by `applyAggregations`
 * to decide whether to use the window-function path (preserves
 * per-row data so `amount / All(total_amount)` makes sense)
 * or the groupBy+agg path (per-group totals; loses per-row
 * data).
 *
 * Covers the full 24-case Expr family incl. PR-I's CaseWhen
 * (walks every branch condition + result + otherwise) and
 * Alias (walks the inner expression).
 *
 * walker, single mutable-set accumulator, returns `Set[String]`.
 */
 private def collectAllReferences(
  calcMeasures: List[CalculatedMeasure]): Set[String] = {
 val out = scala.collection.mutable.Set.empty[String]
 def go(e: Expr): Unit = e match {
  case Expr.All(name)    => out += name
  case Expr.FieldRef(_)   => ()
  case Expr.MeasureRef(_)   => ()
  case Expr.Literal(_, _)   => ()
  case Expr.Not(inner)   => go(inner)
  case Expr.IsNull(inner)   => go(inner)
  case Expr.IsNotNull(inner)  => go(inner)
  case Expr.Cast(inner, _)  => go(inner)
  case Expr.Alias(_, inner)  => go(inner)
  case Expr.Add(l, r)    => go(l); go(r)
  case Expr.Subtract(l, r)  => go(l); go(r)
  case Expr.Multiply(l, r)  => go(l); go(r)
  case Expr.Divide(l, r)   => go(l); go(r)
  case Expr.Modulo(l, r)   => go(l); go(r)
  case Expr.Equal(l, r)   => go(l); go(r)
  case Expr.NotEqual(l, r)  => go(l); go(r)
  case Expr.LessThan(l, r)  => go(l); go(r)
  case Expr.LessOrEqual(l, r)  => go(l); go(r)
  case Expr.GreaterThan(l, r)  => go(l); go(r)
  case Expr.GreaterOrEqual(l, r) => go(l); go(r)
  case Expr.And(l, r)    => go(l); go(r)
  case Expr.Or(l, r)    => go(l); go(r)
  case Expr.CaseWhen(branches, otherwise) =>
  branches.foreach { case (c, v) => go(c); go(v) }
  go(otherwise)
  case Expr.FunctionCall(_, args) => args.foreach(go)
 }
 calcMeasures.foreach { cm => go(cm.expr) }
 out.toSet
 }

 // -- aggregation application (PR-K) --

 /** Apply the model's measures + calculated measures.
 *
 * Two code paths (per [[scala-spark-batch-bugs-mindset]] section 1 + the legacy):
 * - window path when any calculated measure references
 *  Expr.All (preserves per-row data for percent-of-total)
 * - groupBy+agg path otherwise (per-group totals)
 *
 * A measure-less model returns the DataFrame unchanged (the
 * caller keeps the plain filtered/joined projection).
 *
 * Pre-validates every measure's fn against
 * `SupportedAggregates` BEFORE rendering -- the 10 unwired fns
 * surface as typed `EngineError.FeatureDeferred` at this
 * boundary (never a silent no-op).
 */
 def applyAggregations(
  df: DataFrame,
  model: Model): Either[EngineError, DataFrame] = {
 if (model.measures.isEmpty) {
  Right(selectDimensions(df, model))
 } else {
  // Pre-validate: every measure's aggregate must be wired.
  val unwired = model.measures.map(_.expr.fn).filterNot(SupportedAggregates.contains).distinct
  if (unwired.nonEmpty) {
  Left(EngineError.FeatureDeferred(
   engine = "spark-3.5",
   feature = s"aggregate:${unwired.mkString(",")}",
   release = "post-v0.1.0",
   message = "Advanced aggregates (Stddev/Variance/Median/Percentile/ApproxPercentile/First/Last) " +
      "defer to a future PR (use SQL-side or engine-specific paths)."))
  } else {
  val dimColsE: Either[EngineError, Array[Column]] = PortableExprCompiler.colsOf(
   model.dimensions.map(_.expr).toList
  )
  val resultE: Either[EngineError, DataFrame] = dimColsE.flatMap { dimCols =>
   if (collectAllReferences(model.calculatedMeasures).nonEmpty)
   applyWithWindows(df, model, dimCols)
   else
   applyGroupByAgg(df, model, dimCols)
  }
  // PR-N5: apply MaterializePolicy.Persist(level) at the
  // aggregate boundary -- one df.persist() per model query.
  // (cache-the-stable-shape): the aggregated result is the
  // shape most likely to be reused across multiple downstream
  // queries (dashboard refreshes, drill-downs, etc.), so this
  // is the right boundary. None / Cache dispatch is a no-op
  // (Cache is owned by the cache-plugin, not the connector).
  resultE.flatMap { result =>
   model.defaultPolicies.materialize match {
   case io.sm8.core.model.MaterializePolicy.Persist(level) =>
    try {
    Right(result.persist(org.apache.spark.storage.StorageLevel.fromString(level)))
    } catch {
    case e: java.lang.IllegalArgumentException =>
     Left(EngineError.UnsupportedCapability(
     engine  = "spark-3.5",
     capability = "MaterializePolicy.Persist",
     message = s"Unknown Spark StorageLevel: '$level'. Expected one of: DISK_ONLY, DISK_ONLY_2, MEMORY_ONLY, MEMORY_ONLY_2, MEMORY_AND_DISK, MEMORY_AND_DISK_2, MEMORY_AND_DISK_SER, MEMORY_AND_DISK_SER_2, OFF_HEAP."))
    }
   case io.sm8.core.model.MaterializePolicy.Cache =>
    // ADR-009-f v3.2 Fix 3: Cache is NOT a silent no-op. The model
    // declares `materialize = Cache` expecting "cache the aggregate",
    // but the materialize path in this connector only knows how to
    // `.persist(level)`. For result-caching at the query boundary,
    // the model should declare a CachePolicy (ReadThrough/WriteThrough)
    // routed through the cache-plugin — that is the actual cache
    // mechanism and is a separate concern from materialize. The
    // materialize-side Cache handoff (telling the cache-plugin "I just
    // materialized this aggregate; please cache it") is a separate ADR
    // and is not yet wired. Surface this as a typed Left so the
    // contributor's expectation isn't silently ignored.
    Left(EngineError.UnsupportedCapability(
      engine  = "spark-3.5",
      capability = "MaterializePolicy.Cache",
      message = "MaterializePolicy.Cache is not yet wired to the cache-plugin persist handoff. " +
                "For connector-side materialization, use MaterializePolicy.Persist(<storage-level>) " +
                "(e.g. Persist(\"MEMORY_ONLY\")). For result caching, set ModelPolicyDefaults.cache = " +
                "CachePolicy.ReadThrough(<cache-name>) — this routes through the cache-plugin, not " +
                "the materialize path. The cache-handoff for materialize-side Cache is a separate ADR."))
   case _ =>
    Right(result)
   }
  }
 }
 }
}

 /** groupBy+agg path. Per-group totals; loses per-row data.
 * Used when no calculated measure references Expr.All.
 *
 * PR-M4 (GAP 7): also applies `calculatedMeasures` via
 * `withColumn` BEFORE the agg -- so `share = amount / total`
 * is materialised end-to-end. The measure column `total` exists
 * in scope after the agg; the calc references it via Expr.FieldRef.
 *
 * Ordering: dim cols first (projeted — addSelect here for
 * clarity), then agg, then calc withColumns. Spark allows
 * withColumn after agg. */
 private def applyGroupByAgg(
  df:  DataFrame,
  model: Model,
  dimCols: Array[Column]): Either[EngineError, DataFrame] = {
 val aggColsE: Either[EngineError, List[Column]] =
  model.measures.foldLeft[Either[EngineError, List[Column]]](Right(Nil)) {
  (accE, m) => for {
   acc <- accE
   c <- renderAggregate(m.expr)
  } yield acc :+ c.as(m.name)
  }
 for {
  aggCols <- aggColsE
  aggregated = (dimCols.isEmpty, aggCols.isEmpty) match {
     case (true, true) => df              // SELECT * with no aggregations
     case (true, false) => df.agg(aggCols.head, aggCols.tail: _*)     // agg only
     case (false, true) => df.groupBy(dimCols: _*).count()       // groupBy only, no measures — Spark requires agg() with >=1 arg; count() is the safest no-op aggregate
     case (false, false) => df.groupBy(dimCols: _*).agg(aggCols.head, aggCols.tail: _*)
     }
  result  <- applyCalculatedMeasures(aggregated, model)
 } yield result
 }

 /** PR-M4 (GAP 7): apply all calculated measures as withColumn.
 * Order-agnostic w.r.t. each other (a single pass; PR-M2's
 * cycle detection guarantees no calc references another calc
 * unbound at this point). The expressions are typed Exprs,
 * compiled via `PortableExprCompiler.toColumn` (the same
 * compiler that handles CASE WHEN / Alias / All from PR-I). */
 private def applyCalculatedMeasures(
  df: DataFrame,
  model: Model): Either[EngineError, DataFrame] = model.calculatedMeasures.foldLeft[Either[EngineError, DataFrame]](Right(df)) {
 (accE, calc) => for {
  acc <- accE
  c <- PortableExprCompiler.toColumn(calc.expr)
 } yield acc.withColumn(calc.name, c)
 }

 /** Window-function path. Computes each measure via a window
 * aggregation (sum / avg / etc. over partitionBy dimensions)
 * so the per-row input columns remain in scope for calculated
 * measures that reference `Expr.All(name)`.
 *
 * `All(name)` lowers to `col(name)` (PortableExprCompiler's
 * existing case) because the measure column now exists in
 * scope after the `withColumn`. */
 private def applyWithWindows(
  df:  DataFrame,
  model: Model,
  dimCols: Array[Column]): Either[EngineError, DataFrame] = {
  // SM-08 (ADR-009-e): a zero-dimension window is a single-window
  // whole-scan — one executor touches EVERY partition during
  // execution, BEFORE any driver-side limit can run. Truncation
  // cannot protect the executor, and the AQE skew factor is
  // irrelevant with no partition to balance. Return early (compile-
  // time typed rejection): there is no valid "truncated global
  // percent-of-total". NO Spark job runs — the window plan is
  // never built. (We only reach this method when a calculated
  // measure references Expr.All — the entry condition in
  // applyAggregations — so dimCols.isEmpty is the discriminating
  // trigger; the explicit Expr.All check keeps the gate
  // self-documenting.)
  if (dimCols.isEmpty && collectAllReferences(model.calculatedMeasures).nonEmpty) {
   return Left(EngineError.UnsupportedCapability(
    engine = "spark-3.5",
    capability = "Window.UnpartitionedPercentOfTotal",
    message = "A window aggregation over an unpartitioned frame " +
     "(zero dimensions + a calculated measure referencing Expr.All) scans every " +
     "partition in a single executor before any limit can bound it; drive-side " +
     "truncation cannot protect it. Add a dimension or drop the Expr.All reference."))
  }
 val windowSpec =
  if (dimCols.isEmpty) Window.partitionBy()
  else Window.partitionBy(dimCols: _*)
 val withMeasuresE: Either[EngineError, DataFrame] =
  model.measures.foldLeft[Either[EngineError, DataFrame]](Right(df)) {
  (accE, m) => for {
   acc <- accE
   c <- renderAggregate(m.expr)
  } yield acc.withColumn(m.name, c.over(windowSpec))
  }
 withMeasuresE.flatMap(applyCalculatedMeasures(_, model))
 }

 /** Render a portable 
 *
 * Total for the 6 SupportedAggregates (pre-validated by
 * applyAggregations -- reaching the fallback here is an internal
 * invariant violation, hence the loud throw with the fn name).
 *
 * Package-private (no `private` keyword) so `MinimalRelOpLowerer`
 * can compose the direct `df.groupBy().agg()` path without
 * duplicating the per-fn rendering logic.
 */
def renderAggregate(call: AggregateCall): Either[EngineError, Column] = {
 // Mirror the validator's allowlist at the lowering boundary:
 // Count is exempt (lowered as `count(lit(1))` for the COUNT(*) shape);
 // every other AggregateFn requires a real input expression and fails loud
 // here if the validator was bypassed. The non-empty path preserves
 // the existing for-comprehension body verbatim.
 import io.sm8.core.rel.AggregateFn
 call.fn match {
  case AggregateFn.Count if call.input.isEmpty =>
   Right(count(lit(1)))
  case fn if call.input.isEmpty =>
   Left(EngineError.UnsupportedCapability(
    engine    = "spark-3.5",
    capability = s"renderAggregate:${call.alias}:${fn}",
    message   = s"measures[${call.alias}].input is required for aggregate function $fn"))
  case fn =>
   for {
    inputCol <- PortableExprCompiler.toColumn(call.input.get)
    out <- fn match {
     case AggregateFn.Sum   => Right(sparkSum(inputCol))
     case AggregateFn.Count   => Right(count(inputCol))
     case AggregateFn.CountDistinct => Right(countDistinct(inputCol))
     case AggregateFn.Avg   => Right(avg(inputCol))
     case AggregateFn.Min   => Right(sparkMin(inputCol))
     case AggregateFn.Max   => Right(sparkMax(inputCol))
     case other =>
      // Invariant-violation guard: pre-validation in applyAggregations
      // rejects anything outside SupportedAggregates. Reaching here
      // is an internal invariant violation.
      Left(EngineError.ProviderInvocationFailed(
       engine = "spark-3.5",
       name = "PortableQueryCompiler.renderAggregate",
       reason = "InvariantViolation",
       message = s"PortableQueryCompiler.renderAggregate: $other reached the renderer " +
        s"without FeatureDeferred pre-validation -- internal invariant violation."))
    }
   } yield out
 }
}

 // -- dimension projection (measure-less models) --

 /** Project the dimensions onto the DataFrame.
 *
 * Used when the model has no measures (plain filtered/joined
 * projection). An empty `dimensions` list returns the
 * DataFrame unchanged.
 */
 private def selectDimensions(
  df: DataFrame,
  model: Model): DataFrame = {
 // PR-O4b (ADR-008-O): dimension expr is now a typed Expr. For the
 // common FieldRef case we extract the name; other Expr shapes are
 // flattened to their first FieldRef here (the column-projection
 // contract is "select these column names").
 val dimNames: Array[String] = model.dimensions.map(d =>
  io.sm8.core.expr.Calculator.fieldNamesOf(d.expr).headOption.getOrElse(d.name)
 ).toArray
 if (dimNames.isEmpty) df
 else df.select(dimNames.map(name => df.col(name)): _*)
 }
}

object PortableQueryCompiler extends java.io.Serializable {

 /** Default broadcast seed threshold (10 MiB), matching Spark's
  * `spark.sql.autoBroadcastJoinThreshold` default. Used when no
  * session-supplied threshold is available and no plugin oracle
  * provides one.
  */
 val BroadcastSeedDefaultBytes: Long = 10L * 1024L * 1024L

 /**
  * Arms the adapter's broadcast byte-gate when the model declares
  * any join `estimatedRows` (inline presence rule) OR when a
  * plugin's PreExecute hook armed the broadcast oracle (typed
  * transport via `EngineContext.decisionHints`).
  *
  * The estimate is an ARM (presence), not a value: the runtime
  * `sizeInBytes` check stays authoritative, so a large side is
  * never physically broadcast. Caller/hook-set hints always win
  * over the seed; `preferredStrategy` is untouched (the Cross
  * + strategy rejection guard is never triggered).
  *
  * Oracle precedence: when the broadcast oracle is present
  * (`Some(b)`), the oracle's arm Boolean wins over the inline
  * presence rule; when present, the oracle's threshold bytes
  * win over the session `autoBroadcastJoinThreshold` default.
  * `None` on either field preserves today's behavior (inline arm +
  * session default).
  *
  * The null-spark path (the supported config-less smoke used by
  * tests like `PortableQueryCompilerSpec`) returns `eCtx` unchanged —
  * no seed is meaningful when there is no live SparkSession to
  * consult for the operator-disabled sentinel or the session default.
  *
  * @param spark the connected Spark session whose configured
  *              auto-broadcast threshold seeds the gate when no
  *              oracle threshold is provided; `null` is allowed
  *              (the smoke path returns `eCtx` unchanged)
  * @param eCtx the possibly-hint-bearing engine context
  * @param model the query model (join estimates consulted by the
  *              inline presence rule)
  * @return the context with a seeded broadcast threshold if armed
  */
 def seedBroadcastThreshold(
  spark: org.apache.spark.sql.SparkSession,
  eCtx:  io.sm8.core.engine.EngineContext,
  model: io.sm8.core.model.Model
 ): io.sm8.core.engine.EngineContext = {
  if (spark == null) eCtx
  else {
   val broadcastOracle = eCtx.decisionHints
   val oracleArm: Option[Boolean]    = broadcastOracle.flatMap(_.broadcastArmed)
   val oracleThreshold: Option[Long] = broadcastOracle.flatMap(_.broadcastThresholdBytes)
   val sessionThreshold: Long = oracleThreshold.getOrElse(
    try {
     val raw = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
     val v = SparkConfBytes.parseBytes(raw.trim)
     // PR-197 (Round 1 audit MED F-03): Spark's `-1` is the
     // "disable the auto-broadcast heuristic" sentinel; the
     // pre-PR-197 shape fell through to `BroadcastSeedDefaultBytes`
     // (10 MiB), which silently re-enabled broadcast on operators
     // who explicitly disabled it. Honor the operator intent: when
     // the session threshold is `-1` (or any value <= 0), the
     // inline arm is also disarmed (see `operatorDisabledBroadcast`
     // below). The branch here returns the default for non-`-1`
     // parse failures only.
     if (v > 0L) v else BroadcastSeedDefaultBytes
    } catch {
     case _: NoSuchElementException       => BroadcastSeedDefaultBytes
     case _: NumberFormatException        => BroadcastSeedDefaultBytes
     case _: IllegalArgumentException     => BroadcastSeedDefaultBytes
    })
   // PR-197 (Round 1 audit MED F-03): detect the disable sentinel
   // so an operator who disabled Spark's auto-broadcast isn't
   // silently re-armed by the inline rule. PR-209 (tigress MED-1
   // residual): Spark's `JoinSelection.canBroadcastBySize` (in
   // `org.apache.spark.sql.catalyst.optimizer.joins`) is the
   // authoritative broadcast-arm rule — it consults
   // `autoBroadcastJoinThreshold` and arms broadcast only when
   // `sizeInBytes >= 0 && sizeInBytes <= threshold`. ANY negative
   // threshold (`-2b`, `-100b`) short-circuits to "no broadcast"
   // — not just the literal `-1`. PR-209 mirrors that contract:
   // the sm8 seed is disarmed for every negative threshold,
   // matching Spark's own `< 0` semantics. The `0b` case (Spark's
   // "always broadcast" sentinel) is correctly left as enabled
   // — `0L < 0L` is false, so the seed remains free to arm.
   // PR-210: both session reads now go through
   // [[io.sm8.connectors.spark.SparkConfBytes.parseBytes]], a
   // connector-local mirror of Spark's own
   // `ConfigHelpers.byteFromString` (the routine
   // `SQLConf.bytesConf(ByteUnit.BYTE)` wires for this key) — the
   // original is `private[spark]`, and the public
   // `JavaUtils.byteStringAsBytes` rejects a leading `-`, so `-1`
   // (Spark's documented disable sentinel) would throw instead of
   // parsing. The replaced hand `stripSuffix("b").stripSuffix("B")
   // .toLong` parser only accepted plain integers (and Spark's
   // `10MB` default only worked by accident — the
   // NumberFormatException fell through to `BroadcastSeedDefaultBytes`,
   // which happens to equal Spark's default). Any suffixed operator
   // value (`1g`, `-2m`, `512kb`) threw NumberFormatException and
   // silently reverted to the defaults — a false positive on both
   // the threshold and the disable-detection paths. Malformed
   // values (`abc`) still throw NumberFormatException, so the
   // existing catch arms (and their documented fallback
   // semantics) are unchanged.
   val operatorDisabledBroadcast: Boolean =
    try {
     val raw = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
     SparkConfBytes.parseBytes(raw.trim) < 0L
    } catch {
     case _: NumberFormatException        => false
     case _: NoSuchElementException       => false
     case _: IllegalArgumentException     => false
    }
   val armed: Boolean = oracleArm.getOrElse(
    if (operatorDisabledBroadcast) false
    else model.joins.exists(_.estimatedRows.isDefined)
   )
   val seeded: Option[Long] = eCtx.joinHints.broadcastRightBelowBytes.orElse(
    if (armed) Some(sessionThreshold) else None
   )
   if (seeded == eCtx.joinHints.broadcastRightBelowBytes) eCtx
   else eCtx.copy(joinHints = eCtx.joinHints.copy(broadcastRightBelowBytes = seeded))
  }
 }
}
