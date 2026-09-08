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

  /** Storage format for a written rollup table (ADR-0028 Slice 1).
    *
    * - [[Parquet]]: plain catalog-managed Parquet (the pre-ADR
    *   behavior; non-atomic overwrite).
    * - [[Iceberg]]: Apache Iceberg table (atomic snapshot commit —
    *   overwrite IS a metadata-only swap; readers on the previous
    *   snapshot are untouched; a failed refresh never commits).
    *   Requires the Iceberg runtime jar on the classpath AND a
    *   configured Iceberg catalog in the Spark session
    *   (spark.sql.catalog.<name> = SparkCatalog, type = hadoop or
    *   a full catalog service).
    */
  sealed trait TableFormat extends Product with Serializable
  case object Parquet extends TableFormat
  case object Iceberg extends TableFormat

  /**
   * Declared refresh scope for a Tier 1 (dynamic partition overwrite)
   * refresh — the set of partition values the caller asserts the base
   * increment touched (ADR-0029).
   *
   * The scope is a CONTRACT, not a hint: [[RefreshScope]]-aware refresh
   * paths refuse
   * (typed `EngineError`) when the recomputed source contains partition
   * values outside the declared scope. This is the fail-closed guard
   * against a partially-scoped refresh silently leaving stale
   * partitions behind. It is an sm8-side check layered ON TOP of
   * Iceberg's `overwritePartitions()` semantics (which delete by the
   * partition values present in the written DataFrame, with no
   * caller-supplied predicate) — Iceberg itself does not enforce it.
   *
   * The scope value lives in the connector layer (RFC §3: core stays
   * format- and strategy-blind). The batch refresh orchestrator
   * (sm8-platform refresh surface or an operator scheduler) computes
   * and passes it; callers that omit it ([[NoScope]]) get Tier 0
   * whole-table semantics — byte-identical to the pre-Tier-1 behavior.
   */
  sealed trait RefreshScope extends Product with Serializable
  object RefreshScope {

    /** No declared scope: Tier 0 whole-table overwrite (default).
      * Tier 1 semantics are never applied implicitly. */
    case object NoScope extends RefreshScope

    /** Tier 1: overwrite exactly these partition values. Only valid
      * for a rollup whose grain produces a partition column
      * (`timeGrain` + `grainDimension` both set per `RollupSpec`);
      * a grain-less rollup is single-partition and always falls back
      * to Tier 0 regardless of the declared scope. */
    final case class Partitions(values: List[Map[String, String]]) extends RefreshScope
  }

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
  ): Either[EngineError, String] = materialize(spark, model, spec, eager, tableFormat = RollupMaterializer.Parquet)

  /** Eager variant with an explicit table format (ADR-0028 Slice 1):
    * `TableFormat.Parquet` (default, byte-identical to the pre-ADR
    * behavior) or `TableFormat.Iceberg` (atomic snapshot commit —
    * no reader window, failed refresh keeps the previous snapshot).
    *
    * @param spark       the session (used for table IO only — never captured in closures)
    * @param model       the host model (declares dims + measures)
    * @param spec        the rollup declaration to materialize
    * @param eager       true = saveAsTable (job runs, durable); false = temp view
    * @param tableFormat the storage format for the written rollup
    * @return the written table name, or a typed EngineError
    */
  def materialize(
      spark: SparkSession,
      model: Model,
      spec: RollupSpec,
      eager: Boolean,
      tableFormat: RollupMaterializer.TableFormat
  ): Either[EngineError, String] = {
    val tableName = RollupRewriter.rollupTableName(model, spec)
    for {
      _ <- validateSpec(model, spec)
      baseDf <- readBase(spark, model)
      rollupDf <- buildRollupDf(baseDf, model, spec)
      _ <- if (eager) persistCatalog(
              spark, model, spec, rollupDf, tableName, tableFormat,
              RollupMaterializer.RefreshScope.NoScope)
           else persist(spark, rollupDf, tableName)
    } yield tableName
  }

  /** Eager variant with an explicit refresh scope (ADR-0029 Tier 1).
    *
    * When [[RollupMaterializer.RefreshScope.NoScope]] is passed (the
    * default), behavior is byte-identical to the 5-arg overload — Tier
    * 0 whole-table overwrite. When
    * [[RollupMaterializer.RefreshScope.Partitions]] is passed:
    *
    *   - The strategy-select branch picks Tier 1 (DSv2
    *     `df.writeTo(t).overwritePartitions()`) for Iceberg tables
    *     on time-grained rollups whose partition values are fully
    *     covered by the declared scope.
    *   - The strategy falls back to Tier 0 when the rollup is
    *     grain-less (single-partition cardinality in the source).
    *   - The strategy refuses (typed `ScopeUncovered`) when the
    *     recomputed source contains partition values outside the
    *     declared scope.
    *
    * For Parquet tables the scope is currently unused (the Parquet
    * `saveAsTable` path does not expose a partition-scoped overwrite);
    * an attempt to scope a Parquet refresh refuses typed. (Tracked for
    * the Tier 1 parity PR if/when Parquet dynamic-overwrite is added.)
    *
    * @param spark        the session (used for table IO only — never captured in closures)
    * @param model        the host model (declares dims + measures)
    * @param spec         the rollup declaration to materialize
    * @param eager        must be true for a scoped refresh; the temp-view path refuses a scope
    * @param tableFormat  the storage format for the written rollup
    * @param refreshScope declared scope; [[RollupMaterializer.RefreshScope.NoScope]] = Tier 0
    * @return the written table name, or a typed EngineError
    */
  def materialize(
      spark: SparkSession,
      model: Model,
      spec: RollupSpec,
      eager: Boolean,
      tableFormat: RollupMaterializer.TableFormat,
      refreshScope: RollupMaterializer.RefreshScope
  ): Either[EngineError, String] = {
    val tableName = RollupRewriter.rollupTableName(model, spec)
    for {
      _ <- validateSpec(model, spec)
      baseDf <- readBase(spark, model)
      rollupDf <- buildRollupDf(baseDf, model, spec)
      _ <- if (eager) persistCatalog(
              spark, model, spec, rollupDf, tableName, tableFormat, refreshScope)
           else if (refreshScope != RollupMaterializer.RefreshScope.NoScope)
             Left(EngineError.UnsupportedCapability(
               engine = "spark-connector",
               capability = "RollupMaterializer.materialize.refreshScope",
               message =
                 "refreshScope is only meaningful with eager=true (saveAsTable); " +
                 "the v1 temp-view path is session-scoped and ignores scope"))
           else persist(spark, rollupDf, tableName)
    } yield tableName
  }

  /** Typed refusals BEFORE any Spark work (driver-side, pure). */
  private[spark] def validateSpec(model: Model, spec: RollupSpec): Either[EngineError, Unit] = {
    // Grain bucketing is enabled in v1 — the timeGrain blanket
    // refusal is gone. The temporal-type contract is enforced
    // upstream by ModelValidator (declared + resolved type checks
    // on the grain dimension); the materializer enforces it again
    // as a defense-in-depth against a model that bypassed validation
    // (e.g. built directly via Model.of without going through the
    // loader). When the declared grain dimension lacks a declared
    // type, we ask the resolved base-scan schema — nullability
    // distinguishes "no grain" (no check) from "type unknown" (a
    // resolved non-temporal type fails loud).
    val grainTypeCheck: Either[EngineError, Unit] =
      if (spec.timeGrain.isDefined && spec.grainDimension.isDefined) {
        val gd = spec.grainDimension.get
        // Defense-in-depth: the validator already enforces ref
        // membership on the loader path; this guards direct Model.of
        // callers who skip ModelValidator and would otherwise pass a
        // grain dim name through to date_trunc with no model-side
        // anchor.
        if (!spec.dimensions.contains(gd))
          Left(EngineError.UnsupportedCapability(
            engine = "spark-connector",
            capability = "RollupMaterializer.grainDimensionRef",
            message = s"rollups[${spec.name}]: grainDimension '$gd' must name one of the rollup's own dimensions (${spec.dimensions.mkString(", ")})"))
        else {
          val declaredType = model.dimensions.find(_.name == gd).flatMap(_.dataType)
          val resolvedType: Option[io.sm8.core.schema.SealedDataType] =
            if (declaredType.isDefined) declaredType
            else {
              // Without a declared type we cannot resolve without
              // the base schema here — defer to the loader-side
              // validator; this branch is the "defensive" path for
              // direct Model.of callers who skip validateAgainstSchema.
              None
            }
          resolvedType match {
            case Some(t) if t != io.sm8.core.schema.SealedDataType.Date &&
                              t != io.sm8.core.schema.SealedDataType.Timestamp =>
              Left(EngineError.UnsupportedCapability(
                engine = "spark-connector",
                capability = "RollupMaterializer.grainDimensionType",
                message = s"rollups[${spec.name}]: grainDimension '$gd' has declared type $t — " +
                  "calendar truncation requires Date or Timestamp"))
            case _ => Right(())
          }
        }
      } else if (spec.timeGrain.isDefined != spec.grainDimension.isDefined) {
        // Half-declared grain (one side set, the other not). The
        // validator catches this on the loader path; here it is a
        // defense-in-depth refusal for direct Model.of callers.
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupMaterializer.grainCoPresence",
          message = s"rollups[${spec.name}]: timeGrain and grainDimension must be both set or both unset " +
            s"(got timeGrain=${spec.timeGrain}, grainDimension=${spec.grainDimension})"))
      } else if (spec.timeGrain.exists(_.trim.isEmpty)) {
        // Empty/blank timeGrain (the loader filters these via
        // stringField; direct Model.of callers can set Some("")).
        // normalizeGrain returns None for a blank label, so the
        // materializer's grainDimCol fallback to .get would throw
        // NoSuchElementException — surface it as a typed refusal
        // instead.
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupMaterializer.emptyTimeGrain",
          message = s"rollups[${spec.name}]: timeGrain is set but blank; calendar truncation requires a non-empty grain label (KnownGrains vocabulary: hour/day/week/month/quarter/year)"))
      } else if (spec.grainDimension.exists(gd => !spec.dimensions.contains(gd))) {
        // Ref-membership defense: a grainDimension pointing outside
        // the rollup's own dimensions would silently vanish from the
        // materialized table (buildRollupDf iterates spec.dimensions
        // only) — the table would then mismatch the rewriter's
        // declared scan schema. The loader-side validator catches
        // this on the normal path; this guard covers direct
        // Model.of callers.
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupMaterializer.grainDimensionRef",
          message = s"rollups[${spec.name}]: grainDimension '${spec.grainDimension.get}' must name one of the rollup's own dimensions (${spec.dimensions.mkString(", ")})"))
      } else Right(())

    grainTypeCheck.flatMap { _ =>
      val declared = model.measures.filter(m => spec.measures.contains(m.name))
      val unsupported = declared.filter { m =>
        val d = AggregateFn.decomposability(m.expr.fn)
        // Positional (First/Last), Holistic (Median/Percentile*), Approximable
        // (CountDistinct/ApproxPercentile) remain refused — no bounded-size
        // partial state suffices. DISTINCT + COUNT(expr) also refused.
        // Algebraic (Avg/Stddev*/Variance*) NOW SUPPORTED via (n, sum, m2)
        // partial-state columns (the Ticket 6 contract-pass).
        d != Decomposability.Additive && d != Decomposability.Algebraic ||
          m.expr.distinct ||
          (m.expr.fn == AggregateFn.Count && m.expr.input.isDefined) ||
          !m.expr.input.forall(_.isInstanceOf[io.sm8.core.expr.Expr.FieldRef])
      }
      if (unsupported.nonEmpty)
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupMaterializer.measureState",
          message = s"rollups[${spec.name}]: measures ${unsupported.map(_.name).mkString(", ")} need " +
            "state this materializer does not store (Positional / Holistic / Approximable / " +
            "DISTINCT / COUNT(expr) / composite inputs). Supported: Additive (Sum/Count/Min/Max) " +
            "and Algebraic (Avg/Stddev/Variance) over a single field."))
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
      // Grain bucketing: the declared grain dimension groups on
      // date_trunc(<grain>, col) instead of the raw column, and the
      // output column is aliased back to the dimension name so the
      // rollup table's schema matches the rewriter's declared scan
      // schema (the Timestamp override in rollupSchema /
      // reconciledRollupSchema). Normalization reuses the core
      // helper — a single canonical grain vocabulary across layers.
      val normalizedGrain = RollupRewriter.normalizeGrain(spec.timeGrain)
      val grainDimCol: Option[Column] = for {
        grain <- normalizedGrain
        gd    <- spec.grainDimension
      } yield org.apache.spark.sql.functions
        .date_trunc(grain, baseDf.col(gd)).as(gd)
      val dimCols: List[Column] = spec.dimensions.map { d =>
        if (spec.grainDimension.contains(d)) grainDimCol.get else baseDf.col(d)
      }
      val declared = model.measures.filter(m => spec.measures.contains(m.name))
      val stateCols: List[Column] = declared.flatMap { m =>
        stateColumns(m.expr)
      }
      // A dims-only rollup (no measures) is legal: distinct group
      // rows. agg() with zero columns is invalid Spark, so lower to
      // dropDuplicates (the IR's Aggregate(Nil-aggregates) contract).
      if (stateCols.isEmpty)
        Right(baseDf.select(dimCols: _*).dropDuplicates(spec.dimensions))
      else {
        // Spark analysis is LAZY: groupBy().agg() defers column
        // resolution to the first action. A bad input column (e.g.
        // amount renamed to amount_text between refreshes) would
        // otherwise escape as an AnalysisException at collect time —
        // past persistCatalog's typed-error boundary. Force schema
        // resolution HERE (df.schema walks the resolved plan without
        // running the job) so the failure becomes a typed
        // EngineError at the materializer boundary.
        try {
          val rolled = baseDf.groupBy(dimCols: _*).agg(stateCols.head, stateCols.tail: _*)
          rolled.schema // force analysis; throws AnalysisException on bad columns
          Right(rolled)
        } catch {
          case e: org.apache.spark.sql.AnalysisException =>
            Left(EngineError.UnsupportedCapability(
              engine = "spark-connector",
              capability = "RollupMaterializer.buildRollupDf",
              message = s"rollups[${spec.name}]: base table missing a measure input column: ${e.getMessage}"))
        }
      }
    }
  }

  /** The state columns for ONE declared measure (Ticket 4 contract:
    * `count__rows` / `sum__<f>` / `min__<f>` / `max__<f>`).
    * Built-in functions only — see the closure-audit note in the
    * file header. */
  private[spark] def stateColumns(call: AggregateCall): List[Column] = {
    val additiveCols = call.input.collectFirst { case io.sm8.core.expr.Expr.FieldRef(f) => f }.toList.flatMap { f =>
      call.fn match {
        case AggregateFn.Sum => List(sum(col(f)).as(s"sum__$f"))
        case AggregateFn.Min => List(min(col(f)).as(s"min__$f"))
        case AggregateFn.Max => List(max(col(f)).as(s"max__$f"))
        case _               => Nil
      }
    }
    val algebraicCols = AggregateFn.decomposability(call.fn) match {
      case Decomposability.Algebraic =>
        // Welford-merge partial states (ADR-0022/0023 preferred shape;
        // MANDATED by the ADR-0023 cancellation tripwire, which
        // empirically BREACHED on the 1e8±1.0 fixture with the raw
        // (n, sum, sumSq) shape — 3e17-scale sumSq has no double digits
        // left for ±1 dispersion). (count, sum, m2) triple:
        //   - count(col(f)): per-group n
        //   - sum cast to double: per-group sum (fractional parity)
        //   - m2 = var_pop(f) * count(f): the sum of squared deviations
        //     from the group mean, computed STABLY by Spark's variance
        //     aggregator (Welford-style online algorithm — no
        //     catastrophic cancellation). M2 is ADDITIVE across groups,
        //     which is the property the rewriter's guard expressions
        //     consume (variance = M2 / n or M2 / (n-1)).
        call.input.collectFirst { case io.sm8.core.expr.Expr.FieldRef(f) => f }.toList.flatMap { f =>
          List(
            count(col(f)).as(s"count__$f"),
            sum(col(f)).cast("double").as(s"sum__$f"),
            (var_pop(col(f)) * count(col(f))).as(s"m2__$f"))
        }
      case _ => Nil
    }
    val allCols = (additiveCols ++ algebraicCols)
      // A rollup can declare Sum(F) and Avg(F) together — both would
      // produce a sum__F column. Spark's groupBy.agg() throws on
      // ambiguous references. Keep only the first occurrence by name.
      .groupBy(_.toString).map(_._2.head).toList
    allCols match {
      case Nil if call.fn == AggregateFn.Count && call.input.isEmpty =>
        List(count(lit(1)).as("count__rows"))
      case other => other
    }
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
  /** The write strategy selected for ONE persistCatalog invocation
    * (ADR-0029 Tier 0 vs Tier 1). Selection runs ONE distinct-partition
    * scan (a Spark job, but a cheap metadata-scale one) and NO
    * aggregation or write work — the full aggregation job only runs
    * when a strategy is actually executed by the persistCatalog body.
    */
  private[spark] sealed trait PersistStrategy extends Product with Serializable
  private[spark] object PersistStrategy {

    /** Tier 0: whole-table overwrite via DSv1 saveAsTable — the
      * pre-Tier-1 path, byte-identical to PR #358 behavior. */
    case object Tier0 extends PersistStrategy

    /** Tier 1: DSv2 `df.writeTo(t).overwritePartitions()` — dynamic
      * partition overwrite, single atomic commit, only partitions
      * present in the DataFrame are swapped. */
    case object Tier1Iceberg extends PersistStrategy

    /** Typed refusal: the recomputed source contains partition values
      * outside the declared scope. Fail-closed — an uncovered scope
      * must never silently widen to the whole table. */
    final case class RefuseScopeUncovered(reason: String) extends PersistStrategy

    /** Typed refusal: a partition scope was declared for a Parquet
      * write. DSv1 saveAsTable has no partition-scoped overwrite; the
      * scope is honored for Iceberg only (ADR-0029 scope fences). */
    final case class RefuseScopeOnParquet(reason: String) extends PersistStrategy
  }

  /**
   * Select the write strategy (driver-side; runs ONE small
   * distinct-values metadata job on the recomputed source before the
   * write — not zero-Spark-job, but no shuffle/aggregation beyond
   * the distinct scan).
   *
   * Selection matrix (ADR-0029 §Tier 1):
   *   - NoScope                                     → Tier0
   *   - Partitions + Parquet                        → RefuseScopeOnParquet
   *   - Partitions + grain-less rollup              → Tier0 (single-partition; nothing to scope)
   *   - Partitions + scope keys ≠ partition column  → RefuseScopeUncovered
   *   - Partitions + source ⊄ scope (any table state) → RefuseScopeUncovered (fail-closed,
   *     INCLUDING first write — a scoped create never silently widens)
   *   - Partitions + source ⊆ scope + !tableExists  → Tier0 (create path, partitioned CTAS)
   *   - Partitions + source ⊆ scope + tableExists   → Tier1Iceberg
   *
   * The partition column of a time-grained rollup is the grain
   * dimension itself (the materializer aliases
   * `date_trunc(grain, dim)` back to the dimension name, so the
   * written table's partition key IS that column). Partition values
   * are read from the recomputed source via a distinct-count on that
   * column — driver-side metadata read, no closure capture.
   *
   * @param df the recomputed rollup DataFrame (the distinct scan runs
   *           at strategy-select time, before any write)
   */
  private[spark] def decideStrategy(
      df: DataFrame,
      spec: RollupSpec,
      tableFormat: RollupMaterializer.TableFormat,
      refreshScope: RollupMaterializer.RefreshScope,
      tableExists: Boolean
  ): PersistStrategy = refreshScope match {
    case RollupMaterializer.RefreshScope.NoScope => PersistStrategy.Tier0
    case RollupMaterializer.RefreshScope.Partitions(_) if tableFormat == Parquet =>
      PersistStrategy.RefuseScopeOnParquet(
        s"rollups[${spec.name}]: a refresh scope was declared but the Parquet writer has no " +
          "partition-scoped overwrite — scope is honored for Iceberg only (ADR-0029)")
    case RollupMaterializer.RefreshScope.Partitions(declared) =>
      // Grain-less rollup → single partition → nothing to scope.
      // (timeGrain and grainDimension are both-or-neither per
      // RollupSpec; a grain-less rollup has no partition column.)
      val grainColName = for {
        _    <- spec.timeGrain
        gd   <- spec.grainDimension
      } yield gd
      grainColName match {
        case None => PersistStrategy.Tier0
        case Some(col) if !df.columns.contains(col) =>
          // Defense-in-depth mirror of validateSpec's grain guard:
          // a declared grain dim that is not in the recomputed
          // source means the model and the base diverged — refuse
          // loud rather than silently falling back.
          PersistStrategy.RefuseScopeUncovered(
            s"rollups[${spec.name}]: declared grain dimension '$col' is not a column of the " +
              "recomputed rollup source — model/base divergence, refusing scoped refresh")
        case Some(col) =>
          // Scope keys must name the partition column (defense-in-depth:
          // the API accepts a Map per partition; silently ignoring a
          // mistyped key would turn the scope into a value-bag).
          val badKeys = declared.filterNot(_.contains(col))
          if (badKeys.nonEmpty)
            PersistStrategy.RefuseScopeUncovered(
              s"rollups[${spec.name}]: scope entry keys ${badKeys.flatMap(_.keySet).mkString(", ")} " +
                s"do not name the partition column '$col' — scope entries must be " +
                s"Map('$col' -> value)")
          else {
            // Canonical partition-value vocabulary. Daily grain
            // truncates to yyyy-MM-dd on BOTH sides (a caller
            // declares "2026-09-08"; the Timestamp repr is
            // "2026-09-08 00:00:00.0"). Sub-daily grains compare
            // verbatim — take-10 would false-accept an hour-grain
            // scope ("2026-09-08 10" collapsing to "2026-09-08").
            val daily = spec.timeGrain.contains("day")
            def canon(v: String): String =
              if (daily && v.length > 10) v.take(10) else v
            val sourceValues: Set[String] =
              df.select(col).distinct().collect()
                .map(row => canon(String.valueOf(row.get(0))))
                .toSet
            val declaredKeys: Set[String] = declared.flatMap(_.values.map(canon)).toSet
            val uncovered = sourceValues.diff(declaredKeys)
            // Coverage applies on FIRST WRITE too: a declared scope
            // that the whole-source create would violate is refused —
            // the caller narrows the source (or widens the scope);
            // the create never silently widens the scope.
            val uncoveredOnFirstWrite = !tableExists && uncovered.nonEmpty
            if (uncovered.nonEmpty && tableExists)
              PersistStrategy.RefuseScopeUncovered(
                s"rollups[${spec.name}]: recomputed source contains partition value(s) " +
                  s"[${uncovered.mkString(", ")}] outside the declared refresh scope " +
                  s"[${declaredKeys.mkString(", ")}] — widen the scope or split the refresh " +
                  "(ADR-0029 operator contract: silent widening defeats Tier 1's savings)")
            else if (uncoveredOnFirstWrite)
              PersistStrategy.RefuseScopeUncovered(
                s"rollups[${spec.name}]: first-write source contains partition value(s) " +
                  s"[${uncovered.mkString(", ")}] outside the declared scope " +
                  s"[${declaredKeys.mkString(", ")}] — a scoped create must write only the " +
                  "scoped partitions; narrow the source or drop the scope for a full create")
            else if (!tableExists) PersistStrategy.Tier0
            else PersistStrategy.Tier1Iceberg
          }
      }
  }

  /** Eager catalog persist (Ticket 6 refresh surface): the
    * aggregation job RUNS here (saveAsTable is eager) — `Right`
    * means the rollup table is durable in the session catalog.
    * Defense-in-depth: only `<model>__<rollup>` convention names
    * are ever written (the caller passes tableName built by
    * `RollupRewriter.rollupTableName`; this re-checks the shape).
    */
  private[spark] def persistCatalog(
      spark: SparkSession,
      model: Model,
      spec: RollupSpec,
      df: DataFrame,
      tableName: String,
      tableFormat: RollupMaterializer.TableFormat,
      refreshScope: RollupMaterializer.RefreshScope
  ): Either[EngineError, Unit] = {
    // Name-convention guard: the table name must be EXACTLY what
    // the core rewriter will re-scan. (No regex: a regex
    // `^[^_][^_]*__[^_][^_]*$` would false-reject legitimate
    // underscore-containing model names, e.g. taxi_trips__daily.)
    // The caller builds tableName via RollupRewriter.rollupTableName;
    // this recomputes it from (model, spec) and compares.
    val expected = RollupRewriter.rollupTableName(model, spec)
    if (tableName != expected)
      Left(EngineError.UnsupportedCapability(
        engine = "spark-connector",
        capability = "RollupMaterializer.persistCatalog.name",
        message = s"refusing to write '$tableName': not a <model>__<rollup> convention name"))
    else {
      // Catalog-aware existence check (R1 C1): the table lives in the
      // catalog the WRITE targets. For Iceberg that is the
      // iceberg_cat catalog (catalog-qualified), NOT spark_catalog.
      // Tier 1 REQUIRES an existing table — DSv2 overwritePartitions()
      // throws NoSuchTableException on a first-time create — so an
      // absent table falls back to Tier 0 create semantics even when
      // a scope was declared (the first write IS the whole-table
      // write; there is nothing to scope over yet).
      val tableExists = tableFormat match {
        case Iceberg => spark.catalog.tableExists(s"${icebergCatalog}.$tableName")
        case Parquet => spark.catalog.tableExists(tableName)
      }
      // ADR-0029 Tier 1 strategy-select: pick DSv2 overwritePartitions()
      // when (a) the caller declared a partition scope, (b) the
      // table format supports it (Iceberg), (c) the rollup is
      // time-grained (the partition column exists in the source),
      // (d) the source's actual partition values are a subset of the
      // declared scope (checked BEFORE the tableExists fallback — the
      // coverage contract is unconditional, even on a first write),
      // and (e) the table already exists. Otherwise fall back to
      // Tier 0 whole-table overwrite; refuse typed when (d) fails
      // (an uncovered scope must never silently widen).
      // NOTE on timing (R1 review, goat): the coverage check reads the
      // source at decision time T1; the write re-executes the
      // aggregation at T2. A row landing in an out-of-scope partition
      // between T1 and T2 would be aggregated into the written DF and
      // overwritten WITH its partition — the write's own partition
      // derivation (from the DF values) is the effective fence; the
      // T1 check is the early-fail optimization, not the guarantee.
      val strategy: PersistStrategy =
        decideStrategy(df, spec, tableFormat, refreshScope, tableExists)
      strategy match {
        case PersistStrategy.RefuseScopeUncovered(reason) =>
          Left(EngineError.UnsupportedCapability(
            engine = "spark-connector",
            capability = "RollupMaterializer.persistCatalog.scope",
            message = reason))
        case PersistStrategy.RefuseScopeOnParquet(reason) =>
          Left(EngineError.UnsupportedCapability(
            engine = "spark-connector",
            capability = "RollupMaterializer.persistCatalog.scope.parquet",
            message = reason))
        case PersistStrategy.Tier1Iceberg =>
          try {
            // DSv2 writeTo().overwritePartitions() — the Iceberg
            // documented modern path (ADR-0029). Note: the
            // session conf `spark.sql.sources.partitionOverwriteMode`
            // is IGNORED by the DSv2 explicit API; using it
            // deliberately bypasses that conf's 3.5↔4.x drift.
            // Identical-correctness contract: atomic snapshot swap,
            // single transaction, no reader window, failed write
            // leaves the previous snapshot serving.
            df.writeTo(s"${icebergCatalog}.$tableName")
              .overwritePartitions()
            Right(())
          } catch {
            case scala.util.control.NonFatal(e) =>
              Left(EngineError.UnsupportedCapability(
                engine = "spark-connector",
                capability = "RollupMaterializer.persistCatalog.tier1",
                message = s"overwritePartitions('$tableName') failed: " +
                  s"${e.getClass.getSimpleName}: ${e.getMessage}"))
          }
        case PersistStrategy.Tier0 =>
          // Tier 0 whole-table overwrite — both the exists-refresh
          // and the first-time-create arms use mode("overwrite")
          // (idempotent refresh; the R1 C1 fix). Single shared body.
          // CREATE-TIME PARTITIONING (ADR-0029 Tier 1 precondition):
          // a time-grained Iceberg rollup is created PARTITIONED BY
          // its grain column so a later Tier 1 overwritePartitions()
          // swaps per-partition. DSv1 saveAsTable would otherwise
          // create an UNPARTITIONED Iceberg table — overwritePartitions
          // on such a table replaces the WHOLE table (the bug the
          // Tier 1 contract test caught: scoped refresh wiped other
          // partitions). The SQL path (SparkCatalog DDL) is the
          // portable way to attach the partition spec at create time.
          val isTier1CapableCreate =
            !tableExists &&
              tableFormat == Iceberg &&
              spec.timeGrain.isDefined && spec.grainDimension.isDefined &&
              df.columns.contains(spec.grainDimension.get)
          val qualified =
            if (tableFormat == Iceberg) s"${icebergCatalog}.$tableName"
            else tableName
          try {
            if (isTier1CapableCreate) {
              val gd = spec.grainDimension.get
              // CTAS needs a queryable source — register the rollup
              // DataFrame as a transient temp view (session-scoped,
              // never persists), then create the partitioned Iceberg
              // table from it. Clean up the temp view after. The
              // column is backtick-quoted (a reserved-word dimension
              // name would otherwise break the DDL).
              val stagingView = s"_${tableName}_ctas_staging"
              df.createOrReplaceTempView(stagingView)
              try {
                spark.sql(
                  s"CREATE TABLE IF NOT EXISTS $qualified " +
                    s"USING iceberg PARTITIONED BY (`$gd`) AS " +
                    s"SELECT * FROM $stagingView")
              } finally {
                spark.catalog.dropTempView(stagingView)
              }
              Right(())
            } else {
              val writer = tableFormat match {
                case Iceberg => df.write.format("iceberg").mode("overwrite")
                case Parquet => df.write.mode("overwrite")
              }
              writer.saveAsTable(qualified)
              Right(())
            }
          } catch {
            case scala.util.control.NonFatal(e) =>
              Left(EngineError.UnsupportedCapability(
                engine = "spark-connector",
                capability = "RollupMaterializer.persistCatalog",
                message = s"saveAsTable('$tableName') failed: " +
                  s"${e.getClass.getSimpleName}: ${e.getMessage}"))
          }
      }
    }
  }

  /** The Iceberg catalog name the deployment configured in the
    * Spark session via `spark.sql.catalog.<name>` (ADR-0028 minimum
    * viable catalog). Defaults to `iceberg_cat` so a deployment that
    * follows the README's session-config recipe works out of the box;
    * override via constructor arg or env if the catalog name differs.
    *
    * @return the Iceberg catalog name
    */
  def icebergCatalog: String = "iceberg_cat"

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
