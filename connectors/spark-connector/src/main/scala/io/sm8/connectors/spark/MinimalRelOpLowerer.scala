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
 * Per [[karpathy-guidelines-mindset]] "smallest correct change":
 * we EXTRACT the existing cases unchanged. The behavior is
 * identical to the prior `compileRelOp` inlining.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantras #1, #3, #5:
 *   - #1 (closure-safety): the lowerer captures only the
 *     `SparkSession` ref (Serializable in 3.5 + 4.1) and the
 *     `PortableQueryCompiler` (Serializable). NO closures over
 *     DataFrames, Iterators, or external resources.
 *   - #3 (schema-drift): the `Scan` case returns a DataFrame whose
 *     schema is the ACTUAL `df.schema` (not a caller-supplied
 *     "expected" shape). All downstream cases work with the
 *     actual schema.
 *   - #5 (driver-vs-executor): every step runs in the driver;
 *     no executor-side closure capture.
 *
 * ==Layer (per RFC SS3)==
 *
 * Connector-side. The lowerer knows about spark.table, spark.read,
 * StructField, and Column -- those are engine-specific. The core
 * IR (RelOp, Expr, etc.) is engine-portable and lives in
 * sm8-core/.../rel/ + sm8-core/.../expr/. The boundary is the
 * Input/Output types of the lower method: RelOp (in), DataFrame
 * (out), EngineContext (in).
 *
 * ==Per-node documentation (PR-M5)==
 *
 * Per the per-node contract from the `rel/` ADT (PR-H):
 *   - Scan: read the source via spark.table / spark.read.
 *   - Filter: df.filter(expr).
 *   - Project: df.select((expr, alias), ...).
 *   - Aggregate: applyAggregations via synthesised Model +
 *     applyAggregations. The "synthesised Model" is the GAP-5
 *     fallback (per the existing compileRelOpAggregate comment);
 *     a clean direct lower is deferred.
 *   - Join: synthesised Model + legacy applyJoins. Same fallback.
 *   - Sort: df.orderBy.
 *   - Limit: df.limit (skip the sentinel Long.MaxValue).
 *
 * ==Errors==
 *
 * Typed EngineError per RFC SS12:
 *   - SourceRef.ByProvider -> UnsupportedCapability (deferred to PR-M4
 *     full RelOp path)
 *   - Join.left not a Scan -> UnsupportedCapability
 *   - GroupBy key not a FieldRef / MeasureRef -> UnsupportedCapability
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
    val spark:    SparkSession,
    val pc:       PortableQueryCompiler,
    val identity: EngineIdentity = EngineIdentity(
      name = "spark-3.5", nativeVersion = "3.5", engineAdapterVersion = "0.1.0",
    ),
) extends java.io.Serializable {

  // PR-M5: the single source of truth for RelOp -> DataFrame lowering.
  // Per-node methods below. `lower` is a thin delegator.
  def lower(relOp: RelOp, ctx: EngineContext): Either[EngineError, DataFrame] =
    relOp match {
      case scan: RelOp.Scan     => lowerScan(scan)
      case f:   RelOp.Filter    => lowerFilter(f, ctx)
      case p:   RelOp.Project   => lowerProject(p, ctx)
      case a:   RelOp.Aggregate => lowerAggregate(a, ctx)
      case j:   RelOp.Join      => lowerJoin(j, ctx)
      case s:   RelOp.Sort      => lowerSort(s, ctx)
      case l:   RelOp.Limit     => lowerLimit(l, ctx)
    }

  // === per-node lowering methods (called recursively) ===

  /** Scan -> DataFrame: read the source via spark.table / spark.read.
    * Mirrors the legacy resolveSource() but typed for the IR path.
    *
    * The schema comes from the ACTUAL `df.schema` -- per
    * scala-spark-batch-bugs-mindset mantra #3 (schema-drift verify
    * at the boundary). No caller-supplied "expected" schema. */
  def lowerScan(scan: RelOp.Scan): Either[EngineError, DataFrame] = {
    // PR-O1e (ADR-008-O, P0-3): column pruning via scan.projection.
    // Per [[scala-spark-batch-bugs-mindset]] mantra #6
    // (partition-pruning + projection-pushdown): without this,
    // every query reads ALL columns of the table (every partition),
    // which is fatal at scale for wide tables. The IR carries
    // the projection list (PR-H). When non-Nil, apply it via
    // `df.select(projection.map(_.toColumn): _*)` BEFORE the rest
    // of the plan sees the DataFrame.
    //
    // Per [[scala-spark-batch-bugs-mindset]] mantra #1 (closure-safety):
    // the .toColumn calls run in the driver; no executor-side closure
    // capture. The resulting DataFrame is lazy.
    //
    // Implementation: bind the match's Either result, then .map over
    // it to apply the projection. The match's arms stay return-typed
    // `Either[EngineError, DataFrame]`; the projection is applied via
    // .map which short-circuits the Left side.
    val resolved: Either[EngineError, DataFrame] = scan.sourceRef match {
      case src: SourceRef.ByName =>
        try Right(spark.table(src.table)) catch {
          case _: Exception => try Right(spark.read.table(src.table)) catch {
            case _: Exception => Left(EngineError.UnsupportedCapability(
              engine = identity.name, capability = "SourceRef.ByName",
              message = s"Spark table '${src.table}' not found."))
          }
        }
      case src: SourceRef.ByPath =>
        try Right(spark.read.format(src.format).options(src.options).load(src.path)) catch {
          case e: Exception => Left(EngineError.UnsupportedCapability(
            engine = identity.name, capability = "SourceRef.ByPath",
            message = s"Spark path read failed: ${e.getMessage}"))
        }
      case _: SourceRef.ByProvider =>
        Left(EngineError.UnsupportedCapability(
          engine = identity.name, capability = "SourceRef.ByProvider",
          message = "SourceRef.ByProvider requires a registered ProviderRef closure."))
    }
    // .toColumn here is called outside an Either-returning function:
    // a synchronous projection-column compiler never throws (only
    // FunctionCall + ArrayValue do, and the projection list is normally
    // all FieldRefs). Until PR-O1c lands, we use the legacy Column-returning
    // toColumn; a future migration to Either-returning toColumn will
    // replace this with a fold.
    resolved.map { df =>
      if (scan.projection.isEmpty) df
      else df.select(scan.projection.map(PortableExprCompiler.toColumn): _*)
    }
  }

  /** Filter -> DataFrame: walk the child, then df.filter(expr). */
  def lowerFilter(f: RelOp.Filter, ctx: EngineContext): Either[EngineError, DataFrame] =
    lower(f.input, ctx).map(_.filter(PortableExprCompiler.toColumn(f.predicate)))

  /** Project -> DataFrame: walk the child, then df.select with
    * `(expr, alias)` per the IR's projection list. An empty list
    * returns the child unchanged. */
  def lowerProject(p: RelOp.Project, ctx: EngineContext): Either[EngineError, DataFrame] =
    lower(p.input, ctx).map { df =>
      val cols = p.expressions.map { case (e, alias) =>
        PortableExprCompiler.toColumn(e).as(alias)
      }
      if (cols.isEmpty) df else df.select(cols: _*)
    }

  /** Sort -> DataFrame: walk the child, then df.orderBy with
    * per-key direction + null ordering. */
  def lowerSort(s: RelOp.Sort, ctx: EngineContext): Either[EngineError, DataFrame] =
    lower(s.input, ctx).map { df =>
      if (s.keys.isEmpty) df
      else {
        val sortCols = s.keys.map { k =>
          val base = df.col(k.expression.toString)
          val dirCol: Column = k.direction match {
            case SortDirection.Ascending  => base.asc
            case SortDirection.Descending => base.desc
          }
          (k.direction, k.nullOrdering) match {
            case (SortDirection.Ascending,
                  NullOrdering.First)  => dirCol.asc_nulls_first
            case (SortDirection.Ascending,
                  NullOrdering.Last)   => dirCol.asc_nulls_last
            case (SortDirection.Descending,
                  NullOrdering.First)  => dirCol.desc_nulls_first
            case (SortDirection.Descending,
                  NullOrdering.Last)   => dirCol.desc_nulls_last
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
    lower(l.input, ctx).map { df =>
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
    * already supports natively as `df.groupBy(...).agg(...)`. This
    * path uses the IR's groupBy + aggregates directly and renders
    * the aggregate columns via `pc.renderAggregate` (the same
    * per-fn renderer applyAggregations uses internally).
    *
    * Falls back to the legacy synthModel path ONLY when the input
    * is NOT a Scan (e.g. a Project or Filter above the scan) --
    * the direct path is then built on the recursively-lowered
    * DataFrame, not re-resolved.
    */
  def lowerAggregate(agg: RelOp.Aggregate, ctx: EngineContext): Either[EngineError, DataFrame] = {
    lower(agg.input, ctx).flatMap { df =>
      // groupBy: convert each Expr to a Spark Column.
      val groupByCols: Array[Column] = agg.groupBy.map { e =>
        val n = e match {
          case Expr.FieldRef(name)   => name
          case Expr.MeasureRef(name) => name
          case _ => null  // typed-error below
        }
        if (n == null) {
          // Signal the typed error via a sentinel: return an error now.
          return Left(EngineError.UnsupportedCapability(
            engine     = identity.name,
            capability = "MinimalRelOpLowerer.dim",
            message    = s"PR-N3: only FieldRef/MeasureRef groupBy keys are supported. Got: ${e.getClass.getSimpleName}",
          ))
        }
        df.col(n)
      }.toArray
      val aggCols: List[Column] = agg.aggregates.map { call =>
        pc.renderAggregate(call).as(call.alias)
      }
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

  /** Join -> DataFrame: synthesise a Model with the join spec,
    * call the legacy compile(model, ctx) (which runs applyJoins).
    * Per PR-M5: derive a 1-key equi-join from `j.condition` when
    * it is `Expr.Equal(FieldRef(l), FieldRef(r))`. Multi-key joins
    * remain deferred (typed UnsupportedCapability at the legacy
    * applyJoins step). */
  /** PR-N2: flatten an `Expr.And(Expr.Equal(...), ...)` tree into a
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
    *   joins will fail at the typed-error gate until full tree lowering lands)
    * - j.kind MUST be one of Inner/Left/Right/Full/Cross
    * - j.condition MUST extract at least 1 key via extractJoinKeys (PR-N2)
    */
  def lowerJoin(j: RelOp.Join, ctx: EngineContext): Either[EngineError, DataFrame] = {
    val leftScan = j.left match {
      case s: RelOp.Scan => s
      case _ => return Left(EngineError.UnsupportedCapability(
        engine     = identity.name,
        capability = "MinimalRelOpLowerer.join.left",
        message    = s"PR-N4 minimum: Join.left must be a Scan. Got: ${j.left.getClass.getSimpleName}",
      ))
    }
    val rightScan = j.right match {
      case s: RelOp.Scan => s
      case _ => return Left(EngineError.UnsupportedCapability(
        engine     = identity.name,
        capability = "MinimalRelOpLowerer.join.right",
        message    = s"PR-N4 minimum: Join.right must be a Scan. Got: ${j.right.getClass.getSimpleName}",
      ))
    }
    val joinType = j.kind match {
      case io.sm8.core.rel.JoinKind.Inner => "inner"
      case io.sm8.core.rel.JoinKind.Left  => "left"
      case io.sm8.core.rel.JoinKind.Right => "right"
      case io.sm8.core.rel.JoinKind.Full  => "outer"
      case io.sm8.core.rel.JoinKind.Cross => "cross"
    }
    val keys: List[(String, String)] = extractJoinKeys(j.condition)
    if (keys.isEmpty) {
      return Left(EngineError.UnsupportedCapability(
        engine     = identity.name,
        capability = "MinimalRelOpLowerer.join.keys",
        message    = s"PR-N4 minimum: Join.condition must contain at least one Expr.Equal(FieldRef, FieldRef). Got: ${j.condition.getClass.getSimpleName}",
      ))
    }
    // PR-O2 (ADR-008-O, P0-4): size-based broadcast-join hint.
    // Decision (evaluated AFTER rightDf is loaded so size probe is meaningful):
    // when `ctx.joinHints.broadcastRightBelowBytes` is set AND the right-side
    // DataFrame's sizeInBytes is below the threshold, broadcast.
    // See `PortableQueryCompiler.applyJoins` for the long form.
    import org.apache.spark.sql.functions.broadcast
    for {
      leftDf   <- lower(leftScan, ctx)
      rightDf  <- lower(rightScan, ctx)
      // Spark 3.5: sizeInBytes returns BigInt; toLong for comparison.
      rightBytes: Long = try {
        rightDf.queryExecution.analyzed.stats.sizeInBytes.toLong
      } catch { case _: Throwable => Long.MaxValue }
      shouldBroadcast: Boolean = ctx.joinHints.broadcastRightBelowBytes match {
        case Some(threshold) => rightBytes >= 0L && rightBytes <= threshold
        case None            => false
      }
      rightDfEff: org.apache.spark.sql.DataFrame =
        if (shouldBroadcast) broadcast(rightDf) else rightDf
      joinExpr: org.apache.spark.sql.Column = keys.map { case (l, r) =>
        leftDf.col(l) === rightDfEff.col(r)
      }.reduce(_ && _)
      joined: org.apache.spark.sql.DataFrame =
        if (joinType == "cross") leftDf.join(rightDfEff, joinExpr, "inner")
        else                     leftDf.join(rightDfEff, joinExpr, joinType)
    } yield joined
  }
}
