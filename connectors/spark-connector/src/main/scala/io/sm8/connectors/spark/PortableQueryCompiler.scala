/*
 * SM8 Spark Query Compiler - the engine-specific DataFrame builder
 * that walks a portable `io.sm8.core.model.Model` and emits a Spark
 * `DataFrame`.
 *
 * PR-K (per ADR-008-H section PR-K + the user's 2026-08-16 directive
 * "join one to one, one to many, many to many op cross-join" +
 * "aggregate"): adds the three compile stages the legacy shipped:
 *
 *   1. `applyJoins`      -- model.joins (JoinSpec from PR-J) folded
 *                          onto the source DataFrame. 5 kinds
 *                          (Inner/Left/Right/Full/Cross), single-key
 *                          equi-join (multi-key deferred, typed
 *                          UnsupportedCapability).
 *   2. `applyAggregations` -- model.measures (typed AggregateCall
 *                          from PR-J) + model.calculatedMeasures
 *                          (Expr from PR-J). Two paths: groupBy+agg
 *                          (default) or window functions (when any
 *                          calculated measure references Expr.All).
 *   3. `renderAggregate`  -- AggregateCall → Spark Column for the 6
 *                          wired fns (Sum/Count/CountDistinct/Avg/
 *                          Min/Max). The other 10 surface as typed
 *                          EngineError.FeatureDeferred at the
 *                          compile boundary (pre-validated -- never
 *                          a silent no-op, per ADR-008-H).
 *
 * Per scala-data-driven-refactor-mindset section 1 (behavior in adapters,
 * data in core): the `Model` is pure data in sm8-core; this
 * compiler is the Spark-specific behavior that converts it to a
 * `DataFrame`. Other engines (Trino, DuckDB) have analogous
 * compilers that emit SQL strings instead.
 *
 * Per scala-spark-batch-bugs-mindset mantra #1 (closures captured
 * by Spark UDFs / lambdas must avoid non-serializable refs):
 *   - This class `extends java.io.Serializable`.
 *   - Captures a SparkSession (which Spark 3.5 + 4.1 guarantee is
 *     Serializable). NO static / ThreadLocal state (the legacy's
 *     `object PortableQueryCompiler { @volatile var _spark }` +
 *     setSparkSession/clearSparkSession companion state is NOT
 *     ported -- constructor injection only).
 *   - The SparkTypeBridge companion is a pure object (Serializable).
 *   - No DataFrame / Iterator / Connection is closed over.
 *
 * Per scala-jvm-safety-mindset mantra #3 (long-lived state):
 *   - No `@volatile var`, no `clear()` method. The SparkSession ref
 *     is constructor-frozen.
 *
 * Per scala-perf-testing-mindset mantra #3 (count allocations):
 *   - The compile path is iterative over the flat Model fields.
 *   - applyFilters/applyJoins: single foldLeft each (no double-walk).
 *   - collectAllReferences: single mutable-set accumulator walk.
 *   - The groupBy+agg path allocates one Column per measure; the
 *     window path one withColumn per measure + one per calc.
 *
 * Per scala-error-handling-mindset: unsupported shapes surface at
 * the compile boundary as typed `EngineError` (FeatureDeferred for
 * the 10 unwired aggregates; UnsupportedCapability for multi-key
 * joins + unresolvable right-side models). Internals are total.
 *
 * Per scala-impact-analysis-mindset: the compile path DOES NOT
 * cross the executor boundary. The output `DataFrame` is lazy;
 * only `collect()` triggers execution. Per
 * scala-spark-batch-bugs-mindset mantra #5 (driver vs executor
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
}

final class PortableQueryCompiler(val spark: SparkSession)
    extends java.io.Serializable {

  /**
   * The aggregate functions wired to Spark Columns in PR-K (the
   * legacy's v0.3.1 set). The remaining 10 of the 16 AggregateFn
   * cases (Stddev, Variance, Median, Percentile, ApproxPercentile,
   * First, Last) surface as typed `EngineError.FeatureDeferred` at
   * the compile boundary -- per ADR-008-H: never a silent no-op.
   *
   * Per [[scala-data-driven-refactor-mindset]] section 4: a set is the
   * right shape -- membership is the only question; order is
   * irrelevant; the closed set lives beside the only function
   * that consumes it.
   */
  private val SupportedAggregates: Set[AggregateFn] = Set(
    AggregateFn.Sum, AggregateFn.Count, AggregateFn.CountDistinct,
    AggregateFn.Avg, AggregateFn.Min, AggregateFn.Max,
  )

  /** Compile a portable [[Model]] into a Spark [[DataFrame]].
    *
    * The path is (PR-K order, matching the legacy):
    *   1. resolveSource(model.source) -> Either[EngineError, DataFrame]
    *   2. applyFilters(df, model.filters)  -> DataFrame (foldLeft)
    *   3. applyJoins(df, model.joins)      -> Either (foldLeft; 5 kinds,
    *      single-key equi-join; multi-key + missing right-side are
    *      typed UnsupportedCapability)
    *   4. if model.measures.nonEmpty: applyAggregations (groupBy+agg,
    *      or window functions when a calculated measure references
    *      Expr.All) -- dimensions become the groupBy keys.
    *      else: selectDimensions (the pre-PR-K projection path --
    *      a measure-less model is a plain filtered projection).
    *
    * @return `Right(DataFrame)` on success; `Left(EngineError)` if
    *         the source can't be resolved, a join can't be applied,
    *         or a measure's aggregate is not wired.
    */
  def compile(
      model: Model,
      ctx:   EngineContext,
  ): Either[EngineError, DataFrame] = {
    for {
      sourceDf    <- resolveSource(model.source)
      filtered    <- applyFilters(sourceDf, model.filters)
      joined      <- applyJoins(filtered, model.joins, ctx)
      aggregated  <- applyAggregations(joined, model)
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
    * Per [[karpathy-guidelines-mindset]] "smallest correct change":
    * behavior is identical to the prior inlining. */
  private val minimalRelOpLowerer: MinimalRelOpLowerer =
    new MinimalRelOpLowerer(spark, this)

  /** PR-M4 (GAP 5) + PR-M5 commit 2: IR -> DataFrame lowering.
    * Thin delegator to `MinimalRelOpLowerer`. The 7 RelOp cases
    * (Scan / Filter / Project / Sort / Limit / Aggregate / Join)
    * are owned by the lowerer. */
  def compileRelOp(
      relOp: io.sm8.core.rel.RelOp,
      ctx:   EngineContext,
  ): Either[EngineError, DataFrame] = minimalRelOpLowerer.lower(relOp, ctx)

  /** Aggregate -> DataFrame: for the GAP-5 minimum, we use the
    * `compileRelOpAggregateSubtree` helper that recursively walks
    * the relOp's child and uses the child's resulting DataFrame
    * as the base for the aggregate application. The existing
    * applyAggregations (which already supports both groupBy+agg
    * AND the window path via collectAllReferences) consumes the
    * model extracted from the relOp.
    *
    * Per [[karpathy-guidelines-mindset]] "smallest correct change":
    * we re-use the existing `applyAggregations` rather than write
    * a new RelOp->DataFrame aggregator. The model is reconstructed
    * from the relOp's aggregates (the IR carries the call shape). */
  private def resolveSource(
      source: SourceRef,
  ): Either[EngineError, DataFrame] = source match {
    case src: SourceRef.ByName =>
      // Resolution strategy: try spark.table(...) first (handles
      // both catalog tables AND session-scoped temp views); fall
      // back to spark.read.table(src.table) for catalog tables.
      try {
        Right(spark.table(src.table))
      } catch {
        case _: Exception =>
          try {
            Right(spark.read.table(src.table))
          } catch {
            case _: Exception =>
              Left(EngineError.UnsupportedCapability(
                engine    = "spark-3.5",
                capability = "SourceRef.ByName",
                message    = s"Spark table '${src.table}' not found.",
              ))
          }
      }

    case src: SourceRef.ByPath =>
      try {
        Right(
          spark.read.format(src.format)
            .options(src.options)
            .load(src.path)
        )
      } catch {
        case e: Exception =>
          Left(EngineError.UnsupportedCapability(
            engine    = "spark-3.5",
            capability = "SourceRef.ByPath",
            message    = s"Spark path read failed: ${e.getMessage}",
          ))
      }

    case _: SourceRef.ByProvider =>
      Left(EngineError.UnsupportedCapability(
        engine    = "spark-3.5",
        capability = "SourceRef.ByProvider",
        message    = "SourceRef.ByProvider requires a registered ProviderRef closure (deferred to future PR).",
      ))
  }

  // -- filter application --

  private def applyFilters(
      df:      DataFrame,
      filters: List[FilterSpec],
  ): Either[EngineError, DataFrame] = filters.foldLeft[Either[EngineError, DataFrame]](Right(df)) {
    (accE, f) => for {
      acc <- accE
      col <- PortableExprCompiler.toColumn(f.predicate)
    } yield acc.filter(col)
  }

  // -- join application (PR-K) --

  /** Fold the model's [[JoinSpec]] list onto the DataFrame.
    *
    * v0.1.0 scope (matching the legacy's v0.3.1): single-key
    * equi-joins over the 5 kinds. The right side resolves via
    * `spark.table(js.rightModel)` -- a catalog table or temp view
    * registered under the joined model's name. Multi-key joins
    * and unresolvable right-side models surface as typed
    * `EngineError.UnsupportedCapability` (never a silent no-op).
    *
    * Per [[scala-spark-batch-bugs-mindset]] mantra #1: the fold
    * runs in the driver; `df.join` builds the logical plan (lazy);
    * no executor-side closure capture.
    */
  private def applyJoins(
      df:    DataFrame,
      joins: List[JoinSpec],
      ctx:   io.sm8.core.engine.EngineContext,
  ): Either[EngineError, DataFrame] =
    joins.foldLeft[Either[EngineError, DataFrame]](Right(df)) { (accE, js) =>
      accE.flatMap { accDf =>
        // Multi-key is deferred (typed error, per the legacy scope).
        if (js.keys.size != 1) {
          Left(EngineError.UnsupportedCapability(
            engine     = "spark-3.5",
            capability = "JoinSpec.keys",
            message    = s"Multi-key joins (${js.keys.size} keys) deferred to a future PR.",
          ))
        } else {
          // Resolve the right-side model by name in the active catalog.
          val rightDf = try Right(spark.table(js.rightModel)) catch {
            case _: Exception =>
              Left(EngineError.UnsupportedCapability(
                engine     = "spark-3.5",
                capability = "JoinSpec.rightModel",
                message    = s"Right-side model '${js.rightModel}' not found.",
              ))
          }
          rightDf.map { rDf =>
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
            // map-side broadcast). Per [[scala-spark-batch-bugs-mindset]]
            // mantra #1 (closure-safety): the hint is a string
            // descriptor -- no captured lambdas.
            // PR-O2 (ADR-008-O, P0-4): honor
            // `ctx.joinHints.broadcastRightBelowBytes`. Per
            // [[scala-spark-batch-bugs-mindset]] mantra #7
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
            // Per [[scala-perf-testing-mindset]]: the size probe
            // is a single `queryExecution.analyzed.stats.sizeInBytes`
            // call -- O(1), no row materialization.
            import org.apache.spark.sql.functions.broadcast
            // Spark 3.5 returns sizeInBytes as BigInt; convert to Long.
            val rightBytes: Long = try {
              rDf.queryExecution.analyzed.stats.sizeInBytes.toLong
            } catch { case _: Throwable => Long.MaxValue }
            val shouldBroadcast: Boolean = ctx.joinHints.broadcastRightBelowBytes match {
              case Some(threshold) => rightBytes >= 0L && rightBytes <= threshold
              case None            => false  // trust Spark's default heuristic
            }
            val rDfEff: DataFrame = if (shouldBroadcast) broadcast(rDf) else rDf

            val baseJoin: DataFrame = js.kind match {
              case JoinKind.Inner => accDf.join(rDfEff, accDf(leftKey) === rDfEff(rightKey), "inner")
              case JoinKind.Left  => accDf.join(rDfEff, accDf(leftKey) === rDfEff(rightKey), "left")
              case JoinKind.Right => accDf.join(rDfEff, accDf(leftKey) === rDfEff(rightKey), "right")
              case JoinKind.Full  => accDf.join(rDfEff, accDf(leftKey) === rDfEff(rightKey), "outer")
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
            hinted.drop(rDfEff(rightKey))
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
    * Per [[scala-data-driven-refactor-mindset]] section 1: pure data
    * walker, single mutable-set accumulator, returns `Set[String]`.
    */
  private def collectAllReferences(
      calcMeasures: List[CalculatedMeasure],
  ): Set[String] = {
    val out = scala.collection.mutable.Set.empty[String]
    def go(e: Expr): Unit = e match {
      case Expr.All(name)             => out += name
      case Expr.FieldRef(_)           => ()
      case Expr.MeasureRef(_)         => ()
      case Expr.Literal(_, _)         => ()
      case Expr.Not(inner)            => go(inner)
      case Expr.IsNull(inner)         => go(inner)
      case Expr.IsNotNull(inner)      => go(inner)
      case Expr.Cast(inner, _)        => go(inner)
      case Expr.Alias(_, inner)       => go(inner)
      case Expr.Add(l, r)             => go(l); go(r)
      case Expr.Subtract(l, r)        => go(l); go(r)
      case Expr.Multiply(l, r)        => go(l); go(r)
      case Expr.Divide(l, r)          => go(l); go(r)
      case Expr.Modulo(l, r)          => go(l); go(r)
      case Expr.Equal(l, r)           => go(l); go(r)
      case Expr.NotEqual(l, r)        => go(l); go(r)
      case Expr.LessThan(l, r)        => go(l); go(r)
      case Expr.LessOrEqual(l, r)     => go(l); go(r)
      case Expr.GreaterThan(l, r)     => go(l); go(r)
      case Expr.GreaterOrEqual(l, r)  => go(l); go(r)
      case Expr.And(l, r)             => go(l); go(r)
      case Expr.Or(l, r)              => go(l); go(r)
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
    * Two code paths (per scala-spark-batch-bugs section 1 + the legacy):
    *   - window path when any calculated measure references
    *     Expr.All (preserves per-row data for percent-of-total)
    *   - groupBy+agg path otherwise (per-group totals)
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
      df:    DataFrame,
      model: Model,
  ): Either[EngineError, DataFrame] = {
    if (model.measures.isEmpty) {
      Right(selectDimensions(df, model))
    } else {
      // Pre-validate: every measure's aggregate must be wired.
      val unwired = model.measures
        .map(_.expr.fn)
        .filterNot(SupportedAggregates.contains)
        .distinct
      if (unwired.nonEmpty) {
        Left(EngineError.FeatureDeferred(
          engine   = "spark-3.5",
          feature  = s"aggregate:${unwired.mkString(",")}",
          release  = "post-v0.1.0",
          message  = "Advanced aggregates (Stddev/Variance/Median/Percentile/ApproxPercentile/First/Last) " +
                     "defer to a future PR (use SQL-side or engine-specific paths).",
        ))
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
        // Per [[scala-spark-batch-bugs-mindset]] mantra #4
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
                    engine     = "spark-3.5",
                    capability = "MaterializePolicy.Persist",
                    message    = s"Unknown Spark StorageLevel: '$level'. Expected one of: DISK_ONLY, DISK_ONLY_2, MEMORY_ONLY, MEMORY_ONLY_2, MEMORY_AND_DISK, MEMORY_AND_DISK_2, MEMORY_AND_DISK_SER, MEMORY_AND_DISK_SER_2, OFF_HEAP.",
                  ))
              }
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
      df:      DataFrame,
      model:   Model,
      dimCols: Array[Column],
  ): Either[EngineError, DataFrame] = {
    val aggColsE: Either[EngineError, List[Column]] =
      model.measures.foldLeft[Either[EngineError, List[Column]]](Right(Nil)) {
        (accE, m) => for {
          acc <- accE
          c   <- renderAggregate(m.expr)
        } yield acc :+ c.as(m.name)
      }
    for {
      aggCols    <- aggColsE
      aggregated  = (dimCols.isEmpty, aggCols.isEmpty) match {
                    case (true, true)   => df                                                      // SELECT * with no aggregations
                    case (true, false)  => df.agg(aggCols.head, aggCols.tail: _*)                 // agg only
                    case (false, true)  => df.groupBy(dimCols: _*).count()                          // groupBy only, no measures — Spark requires agg() with >=1 arg; count() is the safest no-op aggregate
                    case (false, false) => df.groupBy(dimCols: _*).agg(aggCols.head, aggCols.tail: _*)
                  }
      result     <- applyCalculatedMeasures(aggregated, model)
    } yield result
  }

  /** PR-M4 (GAP 7): apply all calculated measures as withColumn.
    * Order-agnostic w.r.t. each other (a single pass; PR-M2's
    * cycle detection guarantees no calc references another calc
    * unbound at this point). The expressions are typed Exprs,
    * compiled via `PortableExprCompiler.toColumn` (the same
    * compiler that handles CASE WHEN / Alias / All from PR-I). */
  private def applyCalculatedMeasures(
      df:    DataFrame,
      model: Model,
  ): Either[EngineError, DataFrame] = model.calculatedMeasures.foldLeft[Either[EngineError, DataFrame]](Right(df)) {
    (accE, calc) => for {
      acc <- accE
      c   <- PortableExprCompiler.toColumn(calc.expr)
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
      df:      DataFrame,
      model:   Model,
      dimCols: Array[Column],
  ): Either[EngineError, DataFrame] = {
    val windowSpec =
      if (dimCols.isEmpty) Window.partitionBy()
      else Window.partitionBy(dimCols: _*)
    val withMeasuresE: Either[EngineError, DataFrame] =
      model.measures.foldLeft[Either[EngineError, DataFrame]](Right(df)) {
        (accE, m) => for {
          acc <- accE
          c   <- renderAggregate(m.expr)
        } yield acc.withColumn(m.name, c.over(windowSpec))
      }
    withMeasuresE.flatMap(applyCalculatedMeasures(_, model))
  }

  /** Render a portable [[AggregateCall]] as a Spark [[Column]].
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
    val inputColE: Either[EngineError, Column] = PortableExprCompiler.toColumn(
      call.input.getOrElse(Expr.FieldRef(call.alias))
    )
    for {
      inputCol <- inputColE
      out <- call.fn match {
        case AggregateFn.Sum           => Right(sparkSum(inputCol))
        case AggregateFn.Count         => Right(count(lit(1)))
        case AggregateFn.CountDistinct => Right(countDistinct(inputCol))
        case AggregateFn.Avg           => Right(avg(inputCol))
        case AggregateFn.Min           => Right(sparkMin(inputCol))
        case AggregateFn.Max           => Right(sparkMax(inputCol))
        case other =>
          // Programmer error: applyAggregations pre-validates
          // against SupportedAggregates. Reaching here is an
          // internal invariant violation, hence the loud typed
          // error (not a throw per [[scala-error-handling-mindset]]
          // rule #3: throw only for programmer errors; here the
          // invariant break surfaces as a typed EngineError so
          // the MCP server maps it to a 5xx).
          Left(EngineError.ProviderInvocationFailed(
            engine = "spark-3.5",
            name   = "PortableQueryCompiler.renderAggregate",
            reason = "InvariantViolation",
            message = s"PortableQueryCompiler.renderAggregate: $other reached the renderer " +
                     s"without FeatureDeferred pre-validation -- internal invariant violation.",
          ))
      }
    } yield out
  }

  // -- dimension projection (measure-less models) --

  /** Project the dimensions onto the DataFrame.
    *
    * Used when the model has no measures (plain filtered/joined
    * projection). An empty `dimensions` list returns the
    * DataFrame unchanged.
    */
  private def selectDimensions(
      df:    DataFrame,
      model: Model,
  ): DataFrame = {
    // PR-O4b (ADR-008-O): dimension expr is now a typed Expr. For the
    // common FieldRef case we extract the name; other Expr shapes are
    // flattened to their first FieldRef here (the column-projection
    // contract is "select these column names").
    val dimNames: Array[String] = model.dimensions.map(d =>
      io.sm8.core.expr.Calculator.fieldNamesOf(d.expr).headOption
        .getOrElse(d.name)
    ).toArray
    if (dimNames.isEmpty) df
    else df.select(dimNames.map(name => df.col(name)): _*)
  }
}
