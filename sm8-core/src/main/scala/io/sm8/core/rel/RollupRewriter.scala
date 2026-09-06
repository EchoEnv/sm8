/*
 * SM8 Core — RollupRewriter (Ticket 4 of
 * docs/wayfinder/2026-09-06-pre-aggregation.md; design in
 * docs/adr/0022-pre-aggregation-sm8-native.md).
 *
 * Rewrites a query's canonical plan to re-scan from a rollup table
 * instead of the base table — deterministically and fail-open.
 *
 * ==Shape (v1, per the wayfinder Ticket 4 spec)==
 *
 * Only the CANONICAL shape the QueryBuilder emits is recognized:
 *
 *   Scan -> Filter* -> Aggregate(groupBy, aggregates)
 *
 * (with optional Sort/Limit on top). Anything else returns the
 * original plan unchanged (byte-identical, `===`-same instance).
 *
 * ==Fail-open contract==
 *
 * Rewrite requires ALL of:
 *   1. dims ⊆ — the request's group set is a subset of a declared
 *      rollup's group set (within-list duplicate refs are deduped
 *      first — the Ticket 3 review follow-up);
 *   2. composable aggregates — every requested aggregate is
 *      re-aggregable from the rollup per the typed Decomposability
 *      vocabulary (sm8-core AggregateFn; map Ticket 1 of
 *      docs/wayfinder/2026-09-06-pre-aggregation.md): Additive
 *      re-aggregates
 *      directly; Algebraic requires the rollup to store the named
 *      partial states (wired in Ticket 5's materializer); v1
 *      REFUSES Positional / Holistic / Approximable;
 *   3. filter-evaluability — every filter references only rollup
 *      columns (the rollup's grain dims + measure-state columns),
 *      so it can be evaluated against the rollup table;
 *   4. time-grain agreement — when either the query or the rollup
 *      declares a grain, raw string equality on the normalized
 *      form decides (mismatch = no match; a vocabulary mismatch
 *      is fail-SAFE: base path, perf loss only, never wrong
 *      numbers). The canonical vocabulary + normalization is
 *      PINNED HERE (`normalizeGrain`): Ticket 5's materializer
 *      must reuse this single helper (Ticket 3 review follow-up).
 *
 * No match on ANY criterion -> the original plan, byte-identical
 * (`===`). Never a wrong answer; worst case is perf loss.
 *
 * ==Algebraic NULL/undefined guards (engine-parity, humpback
 * finding)==
 *
 * When re-aggregating Algebraic functions over rollup partials,
 * the emitted re-aggregation expressions carry the engine-parity
 * guards:
 *   - stddev_samp / variance_samp with total n < 2 -> NULL
 *     (matching the base path's undefined-sample semantics);
 *   - a group with n = 1 observation: stddev_samp is undefined ->
 *     NULL; variance_sample likewise; population forms are defined
 *     (0). These guards are part of the emitted Expr tree, so the
 *     n=1-single-observation-group edge behaves identically on
 *     rollup and base paths (regression-tested).
 *
 * ==Why NOT a pipeline Stage / PreExecute hook==
 *
 * Per the wayfinder spec: the Stage ADT is frozen; pre-hooks must
 * not mutate the request (RFC hooks.md Rule 2). The rewriter is a
 * pure plan -> plan function applied after `QueryBuilder.build`,
 * before engine compile. No IO, no Spark types (core stays IO-free
 * per sm8 docs/semantic-layer-engine-architecture.md §3).
 */
package io.sm8.core.rel

import io.sm8.core.expr.Expr
import io.sm8.core.expr.LiteralValue
import io.sm8.core.model.Model
import io.sm8.core.model.RollupSpec
import io.sm8.core.schema.{Field, SealedDataType}

object RollupRewriter {

  /** Outcome of a rewrite attempt. Sealed: the caller can see
    * exactly why a plan was (or wasn't) re-scanned from a rollup.
    */
  sealed trait RollupRewriteResult extends Product with Serializable
  object RollupRewriteResult {

    /** The plan now re-scans from the named rollup. */
    final case class Rewritten(plan: RelOp, rollupName: String) extends RollupRewriteResult

    /** The original plan is returned unchanged (byte-identical,
      * `===`-same instance) — fail-open.
      *
      * @param reason machine-readable refusal reason (for logs /
      *        the Ticket 6 observer), not user-facing
      */
    final case class Unchanged(reason: RollupRewriteRefusal) extends RollupRewriteResult
  }

  /** Why a plan was not re-scanned. Sealed, machine-readable. */
  sealed trait RollupRewriteRefusal extends Product with Serializable
  object RollupRewriteRefusal {

    /** The plan doesn't have the canonical Scan -> Filter* ->
      * Aggregate shape. */
    case object NonCanonicalShape extends RollupRewriteRefusal

    /** No declared rollup contains the request's group set. */
    case object NoGroupSetMatch extends RollupRewriteRefusal

    /** A requested aggregate is not re-aggregable from the rollup
      * (v1 refuses Positional / Holistic / Approximable; Algebraic
      * requires stored partial states). */
    case object UnsplittableAggregate extends RollupRewriteRefusal

    /** A filter references a column the rollup table doesn't
      * carry. */
    case object FilterNotEvaluable extends RollupRewriteRefusal

    /** Query grain and rollup grain disagree (fail-safe: base
      * path). */
    case object GrainMismatch extends RollupRewriteRefusal
  }

  // -- Grain normalization: the SINGLE canonical helper (Ticket 3
  // review follow-up). Ticket 5's materializer must reuse this —
  // do not re-implement normalization at the materializer.

  /** Canonical grain vocabulary (v1). Anything else normalizes to
    * itself and will simply never match a declared grain —
    * fail-safe. */
  val KnownGrains: Set[String] = Set("hour", "day", "week", "month", "quarter", "year")

  /** Normalize a grain label: trim, lowercase. Idempotent. The
    * canonical vocabulary is `KnownGrains`; unknown labels are
    * preserved (so a typo can never silently equal a known grain),
    * and an empty/blank label normalizes to None (absent).
    *
    * @param g the raw declared/requested grain label
    * @return the normalized label, or None when absent/blank
    */
  def normalizeGrain(g: Option[String]): Option[String] =
    g.map(_.trim.toLowerCase).filter(_.nonEmpty)

  // -- Entry point --

  /** Attempt to rewrite `plan` (the QueryBuilder output) to
    * re-scan from one of `model.rollups`.
    *
    * Determinism: rollups are considered in declaration order; the
    * FIRST rollup that satisfies every criterion wins (spec: no
    * cost-based choice in v1).
    *
     * @param plan    the plan to (possibly) rewrite
    * @param model   the model whose `rollups` declare the options
    * @param requestGrain the query's time grain, if any (from the
    *                request; `None` = grain-agnostic)
    * @return `Rewritten` with the new plan, or `Unchanged` with
    *         the ORIGINAL instance and a machine-readable reason
    */
  def rewrite(
      plan: RelOp,
      model: Model,
      requestGrain: Option[String]
  ): RollupRewriteResult = {
    val canonical = decomposeCanonical(plan)
    canonical match {
      case None =>
        RollupRewriteResult.Unchanged(RollupRewriteRefusal.NonCanonicalShape)
      case Some(c) =>
        val normQueryGrain = normalizeGrain(requestGrain)
        model.rollups.collectFirst {
          case spec if matchesGroupSet(spec, c) &&
            grainsAgree(spec.timeGrain, normQueryGrain) &&
            aggregatesComposable(spec, c) &&
            filtersEvaluable(spec, c) =>
            RollupRewriteResult.Rewritten(
              rebuildOnRollup(plan, c, spec, model),
              spec.name)
        }.getOrElse(RollupRewriteResult.Unchanged(firstRefusal(canonical, model, normQueryGrain)))
    }
  }

  // -- Canonical-shape decomposition --

  /** The canonical sub-shape the rewriter recognizes. */
  private[rel] final case class CanonicalPlan(
      scan: RelOp.Scan,
      filters: List[Expr],
      groupSet: List[Expr],
      aggregates: List[AggregateCall],
      above: RelOp => RelOp // rebuild wrapper (Sort/Limit chain)
  )

  /** Decompose the plan into Scan -> Filter* -> Aggregate (+ upper
    * wrappers) if it has that shape. Sort/Limit above the
    * Aggregate are tolerated (they re-apply over the rollup scan).
    * Project/Join/extra Filters above the Aggregate -> not
    * canonical (None). */
  private[rel] def decomposeCanonical(plan: RelOp): Option[CanonicalPlan] = {
    // Peel upper wrappers (Sort/Limit) above the Aggregate.
    /** Peel Sort/Limit wrappers above the Aggregate, collecting
      * re-wrap functions innermost-last (prepend while peeling so
      * the list is outermost-first).
      *
      * @param node the node under inspection
      * @param acc accumulated re-wrap functions (outermost-first)
      * @return the Aggregate + its re-wrap chain, or None
      */
    def peelUpper(node: RelOp, acc: List[RelOp => RelOp]): Option[(RelOp.Aggregate, List[RelOp => RelOp])] =
      node match {
        case agg: RelOp.Aggregate => Some((agg, acc))
        case RelOp.Sort(input, keys) =>
          peelUpper(input, (child => RelOp.Sort(child, keys)) :: acc)
        case RelOp.Limit(input, count, offset) =>
          peelUpper(input, (child => RelOp.Limit(child, count, offset)) :: acc)
        case _ => None
      }

    peelUpper(plan, Nil).flatMap { case (agg, wrappers) =>
      // Below the Aggregate: Filter* over Scan.
      /** Peel Filter* below the Aggregate down to the Scan.
        *
        * @param node the node under inspection
        * @param filters accumulated predicates (source-order)
        * @return the base Scan + its filter chain, or None
        */
      def peelLower(node: RelOp, filters: List[Expr]): Option[(RelOp.Scan, List[Expr])] =
        node match {
          case s: RelOp.Scan => Some((s, filters))
          case RelOp.Filter(input, predicate) => peelLower(input, predicate :: filters)
          case _ => None
        }

      peelLower(agg.input, Nil).map { case (scan, fs) =>
        // Wrappers were prepended while peeling (innermost last
        // peeled = first in the list); folding left over them
        // re-wraps in the ORIGINAL outermost-first order.
        val rebuild = wrappers.foldLeft[RelOp => RelOp](identity)(_ andThen _)
        CanonicalPlan(scan, fs, agg.groupBy, agg.aggregates, rebuild)
      }
    }
  }

  // -- Match criteria --

  /** Criterion 1: dims ⊆ (with within-list dedupe on the request
    * side; declared rollup lists are already author-facing, dedup
    * not required for correctness of the subset check). Group-set
    * comparison uses the FieldRef names of the Aggregate's
    * groupBy expressions (the canonical shape's group set is
    * dimension-name FieldRefs). */
  private def matchesGroupSet(spec: RollupSpec, c: CanonicalPlan): Boolean = {
    // Within-list duplicate refs dedupe first (Ticket 3 follow-up):
    // [carrier, carrier] is the same group set as [carrier].
    val deduped = c.groupSet.distinct
    // Non-FieldRef group keys (calculated dims) never match in v1
    // (conservative: the rollup table stores plain dim columns).
    val allFieldRefs = deduped.forall(_.isInstanceOf[Expr.FieldRef])
    val requestDims = deduped.collect { case Expr.FieldRef(n) => n }.toSet
    val rollupDims = spec.dimensions.toSet
    allFieldRefs && requestDims.subsetOf(rollupDims)
  }

  /** Criterion 2: every requested aggregate is re-aggregable from
    * the rollup. Additive: same fn + same input over rollup
    * partials. Algebraic: requires the rollup to store the named
    * partial states — since RollupSpec stores measure NAMES, the
    * rollup must declare a measure whose alias matches the
    * request's alias with a state-carrying fn (sum/count for Avg;
    * count/sum/sumSq inputs for stddev/variance). v1 refuses
    * Positional / Holistic / Approximable outright.
    */
  private def aggregatesComposable(spec: RollupSpec, c: CanonicalPlan): Boolean =
    c.aggregates.forall(a => aggregateReaggregable(spec, a))

  private def aggregateReaggregable(spec: RollupSpec, a: AggregateCall): Boolean =
    AggregateFn.decomposability(a.fn) match {
      case Decomposability.Additive =>
        // The rollup must pre-aggregate the same input: its
        // measure list must contain the alias (the declared
        // measure whose AggregateCall this is).
        spec.measures.contains(a.alias)
      case Decomposability.Algebraic =>
        // v1: the materializer (Ticket 5) will store named partial
        // states as extra columns; the SPEC must declare a measure
        // carrying the state. Conservative v1 rule: refuse unless
        // a dedicated state measure is declared (not yet
        // expressible) -> refuse for now; the Ticket 5 wiring
        // extends this arm when RollupSpec gains state columns.
        false
      case Decomposability.Positional => false
      case Decomposability.Holistic   => false
      case Decomposability.Approximable => false
    }

  /** Criterion 3: every filter references only rollup columns (the
    * rollup's grain dims + requested-rollup measure inputs). The
    * rollup table (Ticket 5) carries: grain dims + measure-state
    * columns; filters over anything else are not evaluable. */
  private def filtersEvaluable(spec: RollupSpec, c: CanonicalPlan): Boolean = {
    val rollupColumns: Set[String] =
      spec.dimensions.toSet ++ spec.measures
    c.filters.forall { f =>
      val refs = io.sm8.core.expr.Calculator.fieldNamesOf(f)
      refs.subsetOf(rollupColumns)
    }
  }

  /** Criterion 4: grain agreement. Both absent -> agree. Either
    * absent -> the materialized rollup covers all grains of its
    * dims; a query without grain matches a grain-declared rollup
    * ONLY if the rollup is the finest grain... v1 conservative
    * rule: grains must be EQUAL after normalization (None matches
    * None). */
  private def grainsAgree(rollupGrain: Option[String], queryGrain: Option[String]): Boolean =
    normalizeGrain(rollupGrain) == queryGrain

  // -- Refusal diagnosis (first failing criterion, in a fixed
  // order for determinism) --

  private def firstRefusal(
      canonical: Option[CanonicalPlan],
      model: Model,
      normQueryGrain: Option[String]
  ): RollupRewriteRefusal = {
    val Some(c) = canonical
    val groupSetOk = model.rollups.exists(r => matchesGroupSet(r, c))
    if (!groupSetOk) RollupRewriteRefusal.NoGroupSetMatch
    else {
      val grainOk = model.rollups.filter(r => matchesGroupSet(r, c))
        .exists(r => grainsAgree(r.timeGrain, normQueryGrain))
      if (!grainOk) RollupRewriteRefusal.GrainMismatch
      else {
        val aggOk = model.rollups.filter(r => matchesGroupSet(r, c) && grainsAgree(r.timeGrain, normQueryGrain))
          .exists(r => aggregatesComposable(r, c))
        if (!aggOk) RollupRewriteRefusal.UnsplittableAggregate
        else RollupRewriteRefusal.FilterNotEvaluable
      }
    }
  }

  // -- Plan rebuild --

  /** Rebuild the plan re-scanning from the rollup table. v1 emits:
    * Scan(rollupTable) -> Filter*(request filters) ->
    * Aggregate(request groupBy, request aggregates re-based on
    * rollup columns) with the SAME upper wrappers (Sort/Limit).
    *
    * Re-basing rules (Additive v1):
    *   - Sum(x)  -> Sum(rollupSumCol(x))
    *   - Count   -> Sum(rollupCountCol)
    *   - Min(x)  -> Min(rollupMinCol(x))
    *   - Max(x)  -> Max(rollupMaxCol(x))
    * (the rollup table stores per-group partials under dedicated
    * column names; see column naming below — Ticket 5's
    * materializer writes these columns.)
    *
    * Algebraic guards: when the materializer stores (n, sum, sumSq)
    * state columns, the re-aggregation emits the engine-parity
    * guards. The guard-wrapped expressions are built by
    * `algebraicReaggregation` below (wired when Ticket 5 lands the
    * state columns; the guard EMITTING helpers exist here so the
    * n<2 -> NULL parity is pinned and regression-tested at the
    * Expr level now).
    */
  private def rebuildOnRollup(
      original: RelOp,
      c: CanonicalPlan,
      spec: RollupSpec,
      model: Model
  ): RelOp = {
    val rollupRef = rollupSourceRef(model, spec)
    val rollupScan = RelOp.Scan(
      sourceRef = rollupRef,
      schema = rollupSchema(spec, model),
      projection = Nil,
      resolution = None
    )
    val filtered = c.filters.foldLeft[RelOp](rollupScan)((acc, f) => RelOp.Filter(acc, f))
    val agg = RelOp.Aggregate(
      input = filtered,
      groupBy = c.groupSet,
      aggregates = c.aggregates.map(rebaseAggregate(_, spec))
    )
    c.above(agg)
  }

  /** The rollup table's SourceRef: ByName in the model's source
    * catalog/namespace with table name "<model>__<rollup>" (the
    * Ticket 5 materializer's naming convention). */
  private def rollupSourceRef(model: Model, spec: RollupSpec): io.sm8.core.model.SourceRef =
    model.source match {
      case io.sm8.core.model.SourceRef.ByName(catalog, namespace, _) =>
        io.sm8.core.model.SourceRef.ByName(catalog, namespace, rollupTableName(model, spec))
      case other =>
        // ByPath / ByProvider: the materializer registers the
        // rollup beside the base table; v1 re-scans via the same
        // SourceRef kind with the rollup table name where the kind
        // allows, else the base ref (the engine resolves
        // <model>__<rollup> in the provider's catalog).
        other match {
          case io.sm8.core.model.SourceRef.ByProvider(name) =>
            io.sm8.core.model.SourceRef.ByProvider(name)
          case path: io.sm8.core.model.SourceRef.ByPath => path
        }
    }

  /** Deterministic rollup table name convention: `<model>__<rollup>`. */
  private[rel] def rollupTableName(model: Model, spec: RollupSpec): String =
    s"${model.name}__${spec.name}"

  /** The rollup table's declared schema (grain dims + measure
    * state columns + partial-state columns). Column naming
    * convention (Ticket 5 materializer must match):
    *   - dims: as declared
    *   - Sum(x) state: col `sum__<inputField>`
    *   - Count state: col `count__rows`
    *   - Min(x) state: col `min__<inputField>`
    *   - Max(x) state: col `max__<inputField>`
    */
  private[rel] def rollupSchema(spec: RollupSpec, model: Model): List[Field] = {
    val dimFields = model.dimensions
      .filter(d => spec.dimensions.contains(d.name))
      .map(d => Field(d.name, d.dataType.getOrElse(SealedDataType.Varchar), nullable = true))
    val measureFields = model.measures
      .filter(m => spec.measures.contains(m.name))
      .flatMap { m =>
        stateColumnsFor(m.expr)
      }
    dimFields ++ measureFields
  }

  /** The state columns a rollup table carries for one declared
    * measure. */
  private def stateColumnsFor(m: AggregateCall): List[Field] = {
    /** One nullable Double state column.
      *
      * @param name the state column name
      * @return the nullable Double field
      */
    def col(name: String): Field = Field(name, SealedDataType.Double, nullable = true)
    AggregateFn.decomposability(m.fn) match {
      case Decomposability.Additive =>
        m.fn match {
          case AggregateFn.Sum =>
            m.input.collectFirst { case Expr.FieldRef(f) => col(s"sum__$f") }.toList
          case AggregateFn.Count => List(col("count__rows"))
          case AggregateFn.Min =>
            m.input.collectFirst { case Expr.FieldRef(f) => col(s"min__$f") }.toList
          case AggregateFn.Max =>
            m.input.collectFirst { case Expr.FieldRef(f) => col(s"max__$f") }.toList
          case _ => Nil
        }
      case Decomposability.Algebraic =>
        // (n, sum, sumSq) partial state per the pre-aggregation
        // map's materializer ticket (Ticket 5 of
        // docs/wayfinder/2026-09-06-pre-aggregation.md, Welford-merge
        // columns); Avg needs (sum, count).
        List(col("count__rows"), col("sum__state"), col("sumsq__state"))
      case _ => Nil
    }
  }

  /** Re-base ONE requested aggregate onto the rollup's state
    * columns. The re-aggregation expression semantics: v1 handles
    * the Additive set directly; Algebraic re-aggregation emits
    * through `algebraicReaggregation` (guard-carrying) once Ticket
    * 5 wires the state columns; anything else never reaches here
    * (refused upstream). */
  private def rebaseAggregate(a: AggregateCall, spec: RollupSpec): AggregateCall = {
    /** FieldRef over a rollup state column.
      *
      * @param col the state column name
      * @return the field reference expression
      */
    def stateColRef(col: String): Expr = Expr.FieldRef(col)
    AggregateFn.decomposability(a.fn) match {
      case Decomposability.Additive =>
        a.fn match {
          case AggregateFn.Sum =>
            val inputField = a.input.collectFirst { case Expr.FieldRef(f) => f }.getOrElse(a.alias)
            a.copy(input = Some(stateColRef(s"sum__$inputField")))
          case AggregateFn.Count =>
            // Sum of per-group counts re-derives the total count.
            a.copy(fn = AggregateFn.Sum, input = Some(stateColRef("count__rows")))
          case AggregateFn.Min =>
            val inputField = a.input.collectFirst { case Expr.FieldRef(f) => f }.getOrElse(a.alias)
            a.copy(input = Some(stateColRef(s"min__$inputField")))
          case AggregateFn.Max =>
            val inputField = a.input.collectFirst { case Expr.FieldRef(f) => f }.getOrElse(a.alias)
            a.copy(input = Some(stateColRef(s"max__$inputField")))
          case _ => a
        }
      case _ => a // unreachable for v1 (refused upstream); pass through
    }
  }

  // -- Algebraic re-aggregation guard builders (engine parity) --

  /** stddev_samp re-aggregation from (n, sum, sumSq) partials with
    * the engine-parity guard: total n < 2 -> NULL (undefined
    * sample stddev), matching the base path. The emitted Expr is:
    *
    *   CASE WHEN Sum(n) < 2 THEN NULL ELSE sqrt((Sum(sumSq) -
    *   Sum(sum)^2/Sum(n)) / (Sum(n) - 1)) END
    *
    * (numerically the raw sumSq form; the Welford-merge column
    * preference is a Ticket 5 storage concern — this is the
    * EXPR-level guard contract the regression spec pins.)
    */
  private[rel] def stddevSampGuardExpr(nCol: String, sumCol: String, sumSqCol: String): Expr = {
    // Re-aggregated total columns (the aggregate node re-sums the
    // per-group partial states; column names per the agg__ prefix).
    val totalN = Expr.FieldRef(s"agg__$nCol")
    val totalSum = Expr.FieldRef(s"agg__$sumCol")
    val totalSumSq = Expr.FieldRef(s"agg__$sumSqCol")
    val guard = Expr.LessThan(totalN, Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int))
    val numerator = Expr.Subtract(totalSumSq, Expr.Divide(Expr.Multiply(totalSum, totalSum), totalN))
    val denominator = Expr.Subtract(totalN, Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))
    val body = Expr.Divide(numerator, denominator)
    Expr.CaseWhen(List((guard, Expr.Literal(LiteralValue.NullValue, SealedDataType.Double))), body)
  }

  /** n = 1 single-observation group: stddev_samp is undefined ->
    * NULL (population stddev is defined = 0). This helper emits
    * the population guard for the regression spec: n = 1 with
    * population semantics -> 0.0; n = 0 -> NULL. */
  private[rel] def stddevPopGuardExpr(nCol: String, sumCol: String, sumSqCol: String): Expr = {
    val totalN = Expr.FieldRef(s"agg__$nCol")
    val totalSum = Expr.FieldRef(s"agg__$sumCol")
    val totalSumSq = Expr.FieldRef(s"agg__$sumSqCol")
    val nZero = Expr.LessOrEqual(totalN, Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int))
    val numerator = Expr.Subtract(totalSumSq, Expr.Divide(Expr.Multiply(totalSum, totalSum), totalN))
    val body = Expr.Divide(numerator, totalN)
    Expr.CaseWhen(List((nZero, Expr.Literal(LiteralValue.NullValue, SealedDataType.Double))), body)
  }

  /** Unused-warning suppressor for the doc-anchored locals in the
    * guard builders above. */
}
