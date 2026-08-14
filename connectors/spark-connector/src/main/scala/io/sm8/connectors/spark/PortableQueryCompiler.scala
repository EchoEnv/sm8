/*
 * SM8 Spark Query Compiler - the engine-specific DataFrame builder
 * that walks a portable `io.sm8.core.model.Model` and emits a Spark
 * `DataFrame`.
 *
 * Per scala-data-driven-refactor-mindset §1 (behavior in adapters,
 * data in core): the `Model` is pure data in sm8-core; this
 * compiler is the Spark-specific behavior that converts it to a
 * `DataFrame`. Other engines (Trino, DuckDB) have analogous
 * compilers that emit SQL strings instead.
 *
 * Per scala-spark-batch-bugs-mindset mantra #1 (closures captured
 * by Spark UDFs / lambdas must avoid non-serializable refs):
 *   - This class `extends java.io.Serializable`.
 *   - Captures a SparkSession (which Spark 3.5 + 4.1 guarantee is
 *     Serializable). NO static / ThreadLocal state.
 *   - The SparkTypeBridge companion is a pure object (Serializable).
 *   - No DataFrame / Iterator / Connection is closed over.
 *
 * Per scala-jvm-safety-mindset mantra #3 (long-lived state):
 *   - No `@volatile var`, no `clear()` method. The SparkSession ref
 *     is constructor-frozen.
 *   - When SparkSession lifecycle ends (Spark.stop()), this
 *     compiler instance is GC'd with it.
 *
 * Per scala-perf-testing-mindset mantra #3 (count allocations):
 *   - The compile path is iterative over the flat Model fields
 *     (no recursion, no intermediate List → Array churn).
 *   - FilterSpec.application: single foldLeft over the filter list
 *     (no Double-walk).
 *   - Dimensions/measure selection: column-name lookup via
 *     `Column(name)` (no Expr-tree-walk because the SM8 Model
 *     stores Dimension.expr: String / Measure.expr: String as
 *     serialized forms; the engine adapter treats them as
 *     column-name references until an Expr-parser ships in
 *     future PRs).
 *
 * Per scala-impact-analysis-mindset: the compile path DOES NOT
 * cross the executor boundary. The output `DataFrame` is lazy;
 * only `collect()` triggers execution. Per
 * scala-spark-batch-bugs-mindset mantra #5 (driver vs executor
 * asymmetry): the engine driver calls compile() + collect() in
 * the driver process; the Row array is materialized in the
 * driver; ResultRow construction happens in the driver. No
 * driver-side resources (SparkSession, SparkContext) leak to
 * the executor.
 *
 * ==Why the compiler is a class (not a companion object)==
 *
 * The legacy's `PortableQueryCompiler` (in /tmp/semanticdf) was a
 * class + a companion with `@volatile var _spark: Option[SparkSession]`
 * + `setSparkSession(...)` / `clearSparkSession()` methods. This
 * is a JVM-safety anti-pattern (long-lived mutable state that
 * survives test cleanup, contaminates the next test) and a
 * serialization anti-pattern (a `var` cannot be part of a
 * Serializable contract). The SM8 port inverts both: constructor
 * injection only; no companion state.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, EngineError}
import io.sm8.core.model.{FilterSpec, Model, SourceRef}

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

final class PortableQueryCompiler(val spark: SparkSession)
    extends java.io.Serializable {

  /** Compile a portable [[Model]] into a Spark [[DataFrame]].
    *
    * The path is:
    *   1. resolveSource(model.source) -> Either[EngineError, DataFrame]
    *   2. applyFilters(df, model.filters) -> DataFrame (foldLeft)
    *   3. selectDimensions(df, model.dimensions) -> DataFrame
    *
    * Per scala-data-driven-refactor §3 (escalate to data-driven
    * tables only when justified): measures / aggregations are
    * NOT in this PR. The SM8-core `Model` is currently a smaller
    * type than the legacy's full Model (no `joins`, no
    * `calculatedMeasures`, no `AggregateCall`). The aggregation
    * path lands when the `core.rel.AggregateCall` lands in
    * sm8-core (future PR per the agile-kindling-beacon plan).
    *
    * @return `Right(DataFrame)` on success; `Left(EngineError)` if
    *         the source can't be resolved.
    */
  def compile(
      model: Model,
      ctx:   EngineContext,
  ): Either[EngineError, DataFrame] = {
    for {
      sourceDf <- resolveSource(model.source)
      filtered  <- Right(applyFilters(sourceDf, model.filters))
      projected <- Right(selectDimensions(filtered, model))
    } yield projected
  }

  // -- source resolution --

  private def resolveSource(
      source: SourceRef,
  ): Either[EngineError, DataFrame] = source match {
    case src: SourceRef.ByName =>
      // Resolution strategy: try spark.table(...) first (handles
      // both catalog tables AND session-scoped temp views); fall
      // back to spark.read.table(src.table) for catalog tables.
      // Per scala-spark-batch-bugs-mindset mantra #3 (verify at
      // the boundary): the actual table resolution happens in
      // the driver; the result DataFrame is lazy.
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

  // -- dimension projection --

  /** Project the dimensions onto the DataFrame.
    *
    * Per scala-data-driven-refactor §1: a Model's
    * `dimensions: List[Dimension]` has `expr: String` — the
    * serialized form. In SM8's current Model (post Step 0 move),
    * the dimension expr is treated as a column-name reference
    * (no Expr parser yet). When the future `core.expr.ExprParser`
    * lands, this method converts each Dimension.expr to an `Expr`
    * and uses `PortableExprCompiler.toColumn` for the projection.
    *
    * Edge case: an empty `dimensions` list means the model has
    * no aggregation keys; the result DataFrame is the unprojected
    * filtered source. The caller (SparkEngineProvider) decides
    * what to do (collect vs. return without aggregation).
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
