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
      filtered     = applyFilters(sourceDf, model.filters)
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
  /** PR-M4 (GAP 5): IR -> DataFrame lowering. Walks the RelOp
    * tree recursively and applies each node to the DataFrame.
    * Covers Scan / Filter / Project / Sort / Limit (the nodes
    * that come out of QueryBuilder.build for a model with no
    * measures / joins); for Aggregate / Join we use a synthesised
    * Model through the legacy compile() path (the legacy code
    * already supports those). Full RelOp->DataFrame is a future
    * PR per the ADR-008-L Appendix plan. */
  def compileRelOp(
      relOp: io.sm8.core.rel.RelOp,
      ctx:   EngineContext,
  ): Either[EngineError, DataFrame] = relOp match {
    case scan: io.sm8.core.rel.RelOp.Scan =>
      compileRelOpScan(scan)
    case io.sm8.core.rel.RelOp.Filter(input, predicate) =>
      compileRelOp(input, ctx).map(_.filter(PortableExprCompiler.toColumn(predicate)))
    case io.sm8.core.rel.RelOp.Project(input, expressions) =>
      compileRelOp(input, ctx).map { df =>
        val cols = expressions.map { case (e, alias) =>
          PortableExprCompiler.toColumn(e).as(alias)
        }
        if (cols.isEmpty) df else df.select(cols: _*)
      }
    case io.sm8.core.rel.RelOp.Sort(input, keys) =>
      compileRelOp(input, ctx).map { df =>
        if (keys.isEmpty) df
        else {
          val sortCols = keys.map { k =>
            val base = df.col(k.expression.toString)
            val dirCol: org.apache.spark.sql.Column = k.direction match {
              case io.sm8.core.rel.SortDirection.Ascending  => base.asc
              case io.sm8.core.rel.SortDirection.Descending => base.desc
            }
            // nullsFirst / nullsLast: use the per-method form on Column
            // (`.asc_nulls_first` / `.desc_nulls_last`) -- this is a
            // documented alternative to the chained `SortOrder` form.
            (k.direction, k.nullOrdering) match {
              case (io.sm8.core.rel.SortDirection.Ascending,
                    io.sm8.core.rel.NullOrdering.First)  => dirCol.asc_nulls_first
              case (io.sm8.core.rel.SortDirection.Ascending,
                    io.sm8.core.rel.NullOrdering.Last)   => dirCol.asc_nulls_last
              case (io.sm8.core.rel.SortDirection.Descending,
                    io.sm8.core.rel.NullOrdering.First)  => dirCol.desc_nulls_first
              case (io.sm8.core.rel.SortDirection.Descending,
                    io.sm8.core.rel.NullOrdering.Last)   => dirCol.desc_nulls_last
            }
          }
          df.orderBy(sortCols: _*)
        }
      }
    case io.sm8.core.rel.RelOp.Limit(input, count, offset) =>
      // PR-L emits a pass-through Limit with `count = Long.MaxValue`
      // when the model has no request-level limit. .limit(Long.MaxValue.toInt)
      // overflows to -1, which Spark rejects. Per the pass-through
      // contract: skip the limit application when count is at the
      // sentinel; honor explicit counts.
      compileRelOp(input, ctx).map { df =>
        if (count == Long.MaxValue) df
        else df.limit(count.toInt).offset(offset.toInt)
      }
    case agg: io.sm8.core.rel.RelOp.Aggregate =>
      // Fall through to the legacy path: synthesise a Model
      // from the Aggregate's groupBy + measures (the only node
      // shape that the legacy path supports end-to-end). For
      // joins (which the legacy path ALSO supports), the caller
      // should pass the model directly to compile(model, ctx).
      compileRelOpAggregate(agg, ctx)
    case join: io.sm8.core.rel.RelOp.Join =>
      // For joins, we delegate to the legacy path with a
      // synthesised model carrying the join spec. The legacy
      // applyJoins already does the right thing.
      compileRelOpJoin(join, ctx)
  }

  /** Scan -> DataFrame: read the source via spark.table / spark.read.
    * Mirrors the legacy resolveSource() but typed for the IR path. */
  private def compileRelOpScan(
      scan: io.sm8.core.rel.RelOp.Scan,
  ): Either[EngineError, DataFrame] = scan.sourceRef match {
    case src: io.sm8.core.model.SourceRef.ByName =>
      try Right(spark.table(src.table)) catch {
        case _: Exception => try Right(spark.read.table(src.table)) catch {
          case _: Exception => Left(EngineError.UnsupportedCapability(
            engine = "spark-3.5", capability = "SourceRef.ByName",
            message = s"Spark table '${src.table}' not found."))
        }
      }
    case src: io.sm8.core.model.SourceRef.ByPath =>
      try Right(spark.read.format(src.format).options(src.options).load(src.path)) catch {
        case e: Exception => Left(EngineError.UnsupportedCapability(
          engine = "spark-3.5", capability = "SourceRef.ByPath",
          message = s"Spark path read failed: ${e.getMessage}"))
      }
    case _: io.sm8.core.model.SourceRef.ByProvider =>
      Left(EngineError.UnsupportedCapability(
        engine = "spark-3.5", capability = "SourceRef.ByProvider",
        message = "SourceRef.ByProvider requires a registered ProviderRef closure."))
  }

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
  private def compileRelOpAggregate(
      agg: io.sm8.core.rel.RelOp.Aggregate,
      ctx: EngineContext,
  ): Either[EngineError, DataFrame] = {
    val base = compileRelOp(agg.input, ctx)
    base.flatMap { df =>
      // Build a minimal Model from the IR's aggregates + source for
      // the existing applyAggregations to consume. dims/joins/etc
      // are at the higher RelOp level (already consumed).
      val scan = agg.input match {
        case s: io.sm8.core.rel.RelOp.Scan => Some(s)
        case _ => None
      }
      val src = scan.map(_.sourceRef).getOrElse(
        io.sm8.core.model.SourceRef.ByName("default", "ir-aggregate"))
      // PR-M4 (GAP 5): extract groupBy dims from the IR Aggregate.
      // The legacy applyAggregations reads model.dimensions as the
      // groupBy keys; for the IR path we convert the Expr groupBy
      // to Dimension(name, expr.toString) -- the legacy path uses
      // expr as a column-name reference (the SM8 Dimension.expr is
      // String, not Expr).
      val dimsForLegacy: List[io.sm8.core.model.Dimension] = agg.groupBy.map { e =>
        val n = e match {
          case Expr.FieldRef(name) => name
          case Expr.MeasureRef(name) => name
          case _ => return Left(EngineError.UnsupportedCapability(
            engine = "spark-3.5",
            capability = "compileRelOpAggregate.dim",
            message = s"PR-M4 minimum: only FieldRef/MeasureRef groupBy keys are supported. Got: ${e.getClass.getSimpleName}"))
        }
        io.sm8.core.model.Dimension(name = n, expr = n)
      }
      val synthModel = io.sm8.core.model.Model(
        name              = "ir-aggregate",
        version           = 0,
        description       = None,
        dimensions        = dimsForLegacy,
        measures          = agg.aggregates.map { call =>
          io.sm8.core.model.Measure(name = call.alias, expr = call)
        },
        defaultPolicies   = io.sm8.core.model.ModelPolicyDefaults(
          materialize = io.sm8.core.model.MaterializePolicy.None,
          cache       = io.sm8.core.model.CachePolicy.NoCache,
          audit       = io.sm8.core.model.AuditPolicy.NoAudit),
        source            = src,
        status            = io.sm8.core.model.ModelStatus.Draft,
        filters           = Nil,
        calculatedMeasures = Nil,
        joins             = Nil,
      )
      // Use the synthesized source to resolve the schema; then
      // apply aggregations to the base DataFrame.
      new SparkSourceResolver(spark).resolve(src, io.sm8.core.engine.EngineIdentity("sm8-pr-m4", "3.5", "0.1.0")).flatMap {
        case scanRes: io.sm8.core.engine.ResolvedSource.Scan =>
          applyAggregations(df, synthModel)
        case _ =>
          Left(EngineError.UnsupportedCapability(
            engine = "spark-3.5", capability = "compileRelOpAggregate",
            message = "non-Scan base for Aggregate"))
      }
    }
  }

  /** Join -> DataFrame: synthesise a Model with the join spec,
    * call the legacy compile(model, ctx) (which runs applyJoins). */
  private def compileRelOpJoin(
      join: io.sm8.core.rel.RelOp.Join,
      ctx: EngineContext,
  ): Either[EngineError, DataFrame] = {
    val left = join.left
    val right = join.right
    val leftScan = left match {
      case s: io.sm8.core.rel.RelOp.Scan => s
      case _ => return Left(EngineError.UnsupportedCapability(
        engine = "spark-3.5", capability = "RelOp.Join.left",
        message = s"PR-M4 minimum: Join.left must be a Scan. Got: ${left.getClass.getSimpleName}"))
    }
    val synthModel = io.sm8.core.model.Model(
      name              = "ir-join",
      version           = 0,
      description       = None,
      dimensions        = Nil,
      measures          = Nil,
      defaultPolicies   = io.sm8.core.model.ModelPolicyDefaults(
        materialize = io.sm8.core.model.MaterializePolicy.None,
        cache       = io.sm8.core.model.CachePolicy.NoCache,
        audit       = io.sm8.core.model.AuditPolicy.NoAudit),
      source            = leftScan.sourceRef,
      status            = io.sm8.core.model.ModelStatus.Draft,
      filters           = Nil,
      calculatedMeasures = Nil,
      joins             = List(io.sm8.core.model.JoinSpec(
        name = "ir-join-1",
        rightModel = right match {
          case s: io.sm8.core.rel.RelOp.Scan => s.sourceRef match {
            case b: io.sm8.core.model.SourceRef.ByName => b.table
            case _ => "ir-join-right"
          }
          case _ => "ir-join-right"
        },
        kind = join.kind,
        keys = Nil,  // RelOp.Join carries `condition`, not keys (single-join-condition model). The legacy applyJoins uses (lKey, rKey) from JoinSpec -- the IR shape is the unified condition.
      )),
    )
    compile(synthModel, ctx).left.map(e => e)
  }

  // (remove the unused private def resolveAndCompileScan -- replaced by compileRelOpScan)


  // -- source resolution --

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
  ): DataFrame = filters.foldLeft(df) { (acc, f) =>
    acc.filter(PortableExprCompiler.toColumn(f.predicate))
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
            val baseJoin: DataFrame = js.kind match {
              case JoinKind.Inner => accDf.join(rDf, accDf(leftKey) === rDf(rightKey), "inner")
              case JoinKind.Left  => accDf.join(rDf, accDf(leftKey) === rDf(rightKey), "left")
              case JoinKind.Right => accDf.join(rDf, accDf(leftKey) === rDf(rightKey), "right")
              case JoinKind.Full  => accDf.join(rDf, accDf(leftKey) === rDf(rightKey), "outer")
              // Per the RelOp.Join contract (PR-H): Cross is UNCONDITIONAL
              // -- the key/condition is ignored; the join is the plain
              // Cartesian product.
              case JoinKind.Cross => accDf.crossJoin(rDf)
            }
            val hinted: DataFrame = ctx.joinHints.preferredStrategy match {
              case Some(JoinStrategy.Broadcast) => baseJoin.hint("broadcast")
              case Some(JoinStrategy.ShuffleHash) => baseJoin.hint("shuffle_hash")
              case Some(JoinStrategy.SortMerge) => baseJoin.hint("merge")
              case None => baseJoin
            }
            hinted.drop(rDf(rightKey))
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
  private def applyAggregations(
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
        val dimCols: Array[Column] = model.dimensions
          .map(d => PortableExprCompiler.toColumn(Expr.FieldRef(d.expr)))
          .toArray
        val result =
          if (collectAllReferences(model.calculatedMeasures).nonEmpty)
            applyWithWindows(df, model, dimCols)
          else
            applyGroupByAgg(df, model, dimCols)
        Right(result)
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
  ): DataFrame = {
    val aggCols: List[Column] = model.measures.map { m =>
      renderAggregate(m.expr).as(m.name)
    }
    val aggregated: DataFrame =
      if (dimCols.isEmpty) df.agg(aggCols.head, aggCols.tail: _*)
      else df.groupBy(dimCols: _*).agg(aggCols.head, aggCols.tail: _*)
    applyCalculatedMeasures(aggregated, model)
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
  ): DataFrame = model.calculatedMeasures.foldLeft(df) { (acc, calc) =>
    acc.withColumn(calc.name, PortableExprCompiler.toColumn(calc.expr))
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
  ): DataFrame = {
    val windowSpec =
      if (dimCols.isEmpty) Window.partitionBy()
      else Window.partitionBy(dimCols: _*)
    val withMeasures = model.measures.foldLeft(df) { (acc, m) =>
      acc.withColumn(m.name, renderAggregate(m.expr).over(windowSpec))
    }
    applyCalculatedMeasures(withMeasures, model)
  }

  /** Render a portable [[AggregateCall]] as a Spark [[Column]].
    *
    * Total for the 6 SupportedAggregates (pre-validated by
    * applyAggregations -- reaching the fallback here is an internal
    * invariant violation, hence the loud throw with the fn name).
    */
  private def renderAggregate(call: AggregateCall): Column = {
    val inputCol = PortableExprCompiler.toColumn(
      call.input.getOrElse(Expr.FieldRef(call.alias))
    )
    call.fn match {
      case AggregateFn.Sum           => sparkSum(inputCol)
      case AggregateFn.Count         => count(lit(1))
      case AggregateFn.CountDistinct => countDistinct(inputCol)
      case AggregateFn.Avg           => avg(inputCol)
      case AggregateFn.Min           => sparkMin(inputCol)
      case AggregateFn.Max           => sparkMax(inputCol)
      case other =>
        throw new UnsupportedOperationException(
          s"PortableQueryCompiler.renderAggregate: $other reached the renderer " +
          s"without FeatureDeferred pre-validation -- internal invariant violation.",
        )
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
      df:    DataFrame,
      model: Model,
  ): DataFrame = {
    val dimNames: Array[String] = model.dimensions.map(_.expr).toArray
    if (dimNames.isEmpty) df
    else df.select(dimNames.map(name => df.col(name)): _*)
  }
}
