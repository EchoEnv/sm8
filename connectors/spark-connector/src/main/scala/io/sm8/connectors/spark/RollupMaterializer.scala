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
    * NOTE: with the v1 temp-view persistence the view creation is
    * LAZY — `Right(tableName)` means the view is registered, not
    * that the aggregation job has run; the job fires on first read
    * (the caller's query, or an explicit `.collect()`/`count()`).
    * The Ticket 6 saveAsTable path is eager.
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
  ): Either[EngineError, String] = materialize(spark, model, spec, eager = false)

  /** Eager (catalog) variant: used by the Ticket 6 refresh trigger.
    * Writes a REAL table via `saveAsTable` (whole-table
    * overwrite (the desired refresh semantics)) — the job RUNS before Right.
    * Refuses to overwrite a non-rollup table (name convention
    * guard: only `<model>__<rollup>` names are ever written).
    *
    * @param eager true = saveAsTable (job runs, durable); false =
    *              temp view (lazy, session-scoped — v1 default)
    * @param spark the session (table IO only — never captured in
    *              any closure shipped to executors)
    * @param model the host model (declares dims + measures)
    * @param spec  the rollup declaration to materialize
    * @return the written table name, or a typed EngineError
    */
  def materialize(
      spark: SparkSession,
      model: Model,
      spec: RollupSpec,
      eager: Boolean
  ): Either[EngineError, String] = {
    val tableName = RollupRewriter.rollupTableName(model, spec)
    for {
      _ <- validateSpec(model, spec)
      baseDf <- readBase(spark, model)
      rollupDf <- buildRollupDf(baseDf, model, spec)
      _ <- if (eager) persistCatalog(spark, rollupDf, tableName) else persist(spark, rollupDf, tableName)
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
          (m.expr.fn == AggregateFn.Count && m.expr.input.isDefined) ||
          !m.expr.input.forall(_.isInstanceOf[io.sm8.core.expr.Expr.FieldRef])
      }
      if (unsupported.nonEmpty)
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupMaterializer.measureState",
          message = s"rollups[${spec.name}]: measures ${unsupported.map(_.name).mkString(", ")} need " +
            "state this materializer does not store (Algebraic partials / DISTINCT / COUNT(expr) / " +
            "composite inputs). v1 stores Additive row-count + sum/min/max over a single field only."))
      else if (declared.isEmpty && spec.measures.nonEmpty)
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupMaterializer.unknownMeasures",
          message = s"rollups[${spec.name}]: measures ${spec.measures.mkString(", ")} are not declared " +
            "on the model — a rollup with no resolvable state columns cannot be materialized."))
      else if (declared.isEmpty && spec.measures.isEmpty && spec.dimensions.isEmpty)
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupMaterializer.emptyRollup",
          message = s"rollups[${spec.name}]: nothing to group by and nothing to aggregate — " +
            "degenerate rollup (matches the loader's Ticket 3 both-empty guard)."))
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
  private[spark] def buildRollupDf(
      baseDf: DataFrame,
      model: Model,
      spec: RollupSpec
  ): Either[EngineError, DataFrame] = {
    // Dim resolution is typed: a declared dim missing from the base
    // table is a loud EngineError, not a raw AnalysisException.
    val missingDims = spec.dimensions.filterNot(baseDf.columns.contains)
    if (missingDims.nonEmpty)
      Left(EngineError.UnsupportedCapability(
        engine = "spark-connector",
        capability = "RollupMaterializer.dimColumns",
        message = s"rollups[${spec.name}]: dimension(s) ${missingDims.mkString(", ")} not present " +
          s"in base table (columns: ${baseDf.columns.mkString(", ")})"))
    else {
      val dimCols: List[Column] = spec.dimensions.map(baseDf.col)
      val declared = model.measures.filter(m => spec.measures.contains(m.name))
      val stateCols: List[Column] = declared.flatMap { m =>
        stateColumns(m.expr)
      }
      // A dims-only rollup (no measures) is legal: distinct group
      // rows. agg() with zero columns is invalid Spark, so lower to
      // dropDuplicates (the IR's Aggregate(Nil-aggregates) contract).
      if (stateCols.isEmpty)
        Right(baseDf.select(dimCols: _*).dropDuplicates(spec.dimensions))
      else
        Right(baseDf.groupBy(dimCols: _*).agg(stateCols.head, stateCols.tail: _*))
    }
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
    * `saveAsTable` persist against the target catalog.
    *
    * MASKING GUARD: a temp view silently shadows a real catalog
    * table of the same name. Re-materializing over OUR OWN temp
    * view is allowed (idempotent refresh); shadowing a persisted
    * TABLE/VIEW is refused typed.
    *
    * Note: temp views are LAZY — returning Right does not imply a
    * job has run; the first query against the view triggers it. */
  /** Eager catalog persist (Ticket 6 refresh surface): the
    * aggregation job RUNS here (saveAsTable is eager) — `Right`
    * means the rollup table is durable in the session catalog.
    * Defense-in-depth: only `<model>__<rollup>` convention names
    * are ever written (the caller passes tableName built by
    * `RollupRewriter.rollupTableName`; this re-checks the shape).
    */
  private[spark] def persistCatalog(
      spark: SparkSession,
      df: DataFrame,
      tableName: String
  ): Either[EngineError, Unit] = {
    // Name-convention guard: `^<name>__<name>$` (exactly one __).
    val convention = raw"^[^_][^_]*__[^_][^_]*$$".r
    if (!convention.matches(tableName))
      Left(EngineError.UnsupportedCapability(
        engine = "spark-connector",
        capability = "RollupMaterializer.persistCatalog.name",
        message = s"refusing to write '$tableName': not a <model>__<rollup> convention name"))
    else if (spark.catalog.tableExists(tableName))
      // Ticket 4/5 carry-item resolved: tableExists (O(1) lookup)
      // instead of listTables().collect(); re-materializing our OWN
      // rollup table is the refresh path — overwrite is intended.
      // Whole-table overwrite (no partitionOverwriteMode configured).
      // NonFatal catch: an aggregation-job SparkException on a big
      // base is the LIKELY failure — it must become a typed
      // per-rollup EngineError, not escape and abort the whole
      // refresh (per-rollup isolation contract).
      try {
        df.write.mode("overwrite").saveAsTable(tableName)
        Right(())
      } catch {
        case scala.util.control.NonFatal(e) =>
          Left(EngineError.UnsupportedCapability(
            engine = "spark-connector",
            capability = "RollupMaterializer.persistCatalog",
            message = s"saveAsTable('$tableName') failed: ${e.getClass.getSimpleName}: ${e.getMessage}"))
      }
    else
      try {
        df.write.saveAsTable(tableName)
        Right(())
      } catch {
        case scala.util.control.NonFatal(e) =>
          Left(EngineError.UnsupportedCapability(
            engine = "spark-connector",
            capability = "RollupMaterializer.persistCatalog",
            message = s"saveAsTable('$tableName') failed: ${e.getClass.getSimpleName}: ${e.getMessage}"))
      }
  }

  private def persist(spark: SparkSession, df: DataFrame, tableName: String): Either[EngineError, Unit] = {
    val existing = spark.catalog.listTables().collect().find(_.name == tableName)
    val shadowsRealTable = existing.exists(_.tableType != "TEMPORARY")
    if (shadowsRealTable)
      Left(EngineError.UnsupportedCapability(
        engine = "spark-connector",
        capability = "RollupMaterializer.persist.shadowing",
        message = s"a real catalog table named '$tableName' already exists — materializing the " +
          "rollup view would shadow it. Rename the rollup or drop the table explicitly."))
    else
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
}
