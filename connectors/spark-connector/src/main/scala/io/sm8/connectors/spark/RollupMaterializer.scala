/*
 * SM8 Spark Connector — RollupMaterializer (Ticket 5 of
 * docs/wayfinder/2026-09-06-pre-aggregation.md; design in
 * docs/adr/0022-pre-aggregation-sm8-native.md).
 *
 * Physically builds one declared rollup table (the pre-aggregation
 * map's "write job"). Writes the rollup under the canonical table
 * name `RollupRewriter.rollupTableName(model, spec)` =
 * `<model>__<rollup>`, with the state-column contract Ticket 4's
 * rewriter consumes:
 *
 *   - count state: `count__rows` — non-nullable Long (BigInt)
 *   - Sum(x):  `sum__x`  — nullable Double (engines skip NULL
 *     partials; an all-NULL group yields NULL, matching base)
 *   - Min(x):  `min__x`  — nullable Double
 *   - Max(x):  `max__x`  — nullable Double
 *
 * ==Spark closure serialization (audit contract, user directive
 * 2026-09-06: "ensure it serializable to spark closure and executor
 * not overhead")==
 *
 * The ENTIRE write path builds driver-side Spark `Column`
 * expressions using ONLY built-in aggregate functions
 * (`functions.sum/count/min/max`). There are NO UDFs, NO Scala
 * lambdas applied to rows, NO closure capture of the SparkSession
 * or any caller scope — the lazy DataFrame is described on the
 * driver and executed entirely by Spark's own aggregation machinery
 * (Tungsten UnsafeRows; no user code ships to executors at all).
 * `RollupMaterializer` is a stateless `object`: nothing to
 * serialize, no per-call allocation state.
 *
 * ==Executor overhead==
 *
 * One shuffle (groupBy dims) + built-in partial/merge aggregation —
 * Spark's native code path, no interpreter overhead, no
 * per-row Scala dispatch.
 *
 * ==Refusals (typed, never silent)==
 *
 *   - rollup with timeGrain set: grain BUCKETING is not defined in
 *     v1 (the Ticket 4 review carry-item: finer-than-grain dim
 *     value-domains must be pinned first) — refuse loudly.
 *   - Algebraic measures (Avg/Stddev/Variance): v1 state contract
 *     does not carry partial states; routing refuses them too.
 *     When they land, prefer Welford-merge columns (n, mean, M2)
 *     over raw (n, sum, sumSq) for large-mean variance data to
 *     avoid catastrophic cancellation (wayfinder AC).
 *   - COUNT(expr) (non-null count): the state contract stores row
 *     counts only (Ticket 4 F1).
 *
 * ==Persistence seam==
 *
 * v1 default persists the rollup as a temp view (`createOrReplace
 * TempView`) — resolvable by `SparkSourceResolver.resolveByName`
 * via `spark.table(name)` and sufficient for the end-to-end
 * regression; production catalogs wire `saveAsTable` at the
 * refresh-trigger surface (Ticket 6) where the target catalog is
 * known.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.EngineError
import io.sm8.core.model.{Model, RollupSpec, SourceRef}
import io.sm8.core.rel.{AggregateCall, AggregateFn, Decomposability, RollupRewriter}

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object RollupMaterializer {

  /** Materialize ONE declared rollup for the model.
    *
    * Driver-side only until `write` (all Column construction is
    * lazy); the write triggers one Spark job.
    *
    * @param spark the session (used for table IO only — never
    *              captured in any closure shipped to executors)
    * @param model the host model (declares dims + measures)
    * @param spec  the rollup declaration to materialize
    * @return the written table name, or a typed EngineError
    */
  def materialize(
      spark: SparkSession,
      model: Model,
      spec: RollupSpec
  ): Either[EngineError, String] = {
    val tableName = RollupRewriter.rollupTableName(model, spec)
    for {
      _ <- validateSpec(model, spec)
      baseDf <- readBase(spark, model)
      rollupDf = buildRollupDf(baseDf, model, spec)
      _ <- persist(rollupDf, tableName)
    } yield tableName
  }

  /** Typed refusals BEFORE any Spark work (driver-side, pure). */
  private[spark] def validateSpec(model: Model, spec: RollupSpec): Either[EngineError, Unit] = {
    if (spec.timeGrain.isDefined)
      Left(EngineError.UnsupportedCapability(
        engine = "spark-connector",
        capability = "RollupMaterializer.timeGrain",
        message = s"rollups[${spec.name}]: grain bucketing is not defined in v1 (finer-than-grain " +
          "dim value-domains must be pinned first — Ticket 4 review carry-item). " +
          "Declare the rollup without time_grain, or extend the materializer with bucketing."))
    else {
      val declared = model.measures.filter(m => spec.measures.contains(m.name))
      val unsupported = declared.filter { m =>
        val d = AggregateFn.decomposability(m.expr.fn)
        d != Decomposability.Additive ||
          m.expr.distinct ||
          (m.expr.fn == AggregateFn.Count && m.expr.input.isDefined)
      }
      if (unsupported.nonEmpty)
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupMaterializer.measureState",
          message = s"rollups[${spec.name}]: measures ${unsupported.map(_.name).mkString(", ")} need " +
            "state this materializer does not store (Algebraic partials / DISTINCT / COUNT(expr)). " +
            "v1 stores Additive row-count + sum/min/max states only."))
      else Right(())
    }
  }

  /** Read the base table. ByName only (matches the Ticket 4 v1
    * routing contract: SourceKindUnsupported otherwise). */
  private[spark] def readBase(spark: SparkSession, model: Model): Either[EngineError, DataFrame] =
    model.source match {
      case SourceRef.ByName(_, _, table) =>
        try Right(spark.table(table))
        catch {
          case e: org.apache.spark.sql.AnalysisException =>
            Left(EngineError.UnsupportedCapability(
              engine = "spark-connector",
              capability = "RollupMaterializer.baseTable",
              message = s"base table '$table' not readable: ${e.getMessage}"))
        }
      case other =>
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupMaterializer.sourceKind",
          message = s"v1 materializes ByName sources only (got ${other.getClass.getSimpleName}); " +
            "matches the rewriter's SourceKindUnsupported routing contract."))
    }

  /** Build the rollup DataFrame: groupBy(dims).agg(state columns).
    *
    * CLOSURE SAFETY: every expression below is a built-in Spark
    * `Column` combinator (`col`, `sum`, `count`, `min`, `max`,
    * `lit`) — no user function objects are created, so the physical
    * plan ships NO Scala closure to executors.
    */
  private[spark] def buildRollupDf(baseDf: DataFrame, model: Model, spec: RollupSpec): DataFrame = {
    val dimCols: List[Column] = spec.dimensions.map(baseDf.col)
    val declared = model.measures.filter(m => spec.measures.contains(m.name))
    val stateCols: List[Column] = declared.flatMap { m =>
      stateColumns(m.expr)
    }
    baseDf.groupBy(dimCols: _*).agg(stateCols.head, stateCols.tail: _*)
  }

  /** The state columns for ONE declared measure (Ticket 4 contract:
    * `count__rows` / `sum__<f>` / `min__<f>` / `max__<f>`).
    * Built-in functions only — see the closure-audit note in the
    * file header. */
  private[spark] def stateColumns(call: AggregateCall): List[Column] =
    call.input.collectFirst { case io.sm8.core.expr.Expr.FieldRef(f) => f }.toList.flatMap { f =>
      call.fn match {
        case AggregateFn.Sum => List(sum(col(f)).as(s"sum__$f"))
        case AggregateFn.Min => List(min(col(f)).as(s"min__$f"))
        case AggregateFn.Max => List(max(col(f)).as(s"max__$f"))
        case _               => Nil
      }
    } match {
      case Nil if call.fn == AggregateFn.Count && call.input.isEmpty =>
        List(count(lit(1)).as("count__rows"))
      case other => other
    }

  /** Persistence seam. v1 default: temp view (resolvable by
    * `spark.table`; sufficient for the regression and local runs).
    * Production wiring (Ticket 6 refresh triggers) passes a
    * `saveAsTable` persist against the target catalog. */
  private def persist(df: DataFrame, tableName: String): Either[EngineError, Unit] =
    try {
      df.createOrReplaceTempView(tableName)
      Right(())
    } catch {
      case e: org.apache.spark.sql.AnalysisException =>
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupMaterializer.persist",
          message = s"persisting rollup view '$tableName' failed: ${e.getMessage}"))
    }
}
