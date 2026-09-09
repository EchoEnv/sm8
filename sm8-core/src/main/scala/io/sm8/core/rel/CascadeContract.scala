/*
 * SM8 Core — CascadeContract (ADR-0031, Tier 2 cascade).
 *
 * The cascade-eligibility predicate (D1), the structural grain
 * coarsening check (D2), and the cascade-specific refusal vocabulary
 * for the validation surface (D4's DAG seam is in
 * `ModelValidator.validateCascadeDag`; this object is the *pure*
 * per-pair eligibility predicate the validator and connector both
 * consume).
 *
 * ==Why a separate object==
 *
 * The cascade predicate is a *pure function of two declarations*
 * (B = cascade TARGET/coarser; A = cascade SOURCE/finer). No IO, no
 * Spark, no engine concepts — same RFC §3 discipline as the rest
 * of core. Keeping it standalone means the validator (deployment-
 * time check) and the connector (refresh-time check) share ONE
 * implementation; ADR-0030's freshness-policy split (declaration in
 * core, resolution in connector) is mirrored here: declaration-time
 * shape checks live in core, dynamic completeness (does the source
 * actually have its partitions populated for this scope?) lives in
 * the connector.
 *
 * ==Convention (R1 reviews, both reviewers caught the original
 * inversion in the ADR draft):==
 *   A = cascade SOURCE (the finer, e.g. hourly)
 *   B = cascade TARGET (the coarser, e.g. daily)
 * "Coarser" = FEWER dimension keys, NOT more. The check
 *   `B.dimensions ⊆ A.dimensions` is the correct direction.
 *
 * ==Min/Max caveat (R1 finch F10):==
 * Min and Max are `Additive` per `AggregateFn.decomposability` but
 * merge by *binary re-application* across partials (`min(min_a,
 * min_b)`), NOT plain summation. The cascade MERGE expression
 * (lives in connector, not here) must use the binary re-application
 * form for these — a naive `SUM(min)` over partials would be wrong.
 * The structural partial-state-presence check below is identical for
 * both subclasses (they all carry one `min__F` or `max__F` column);
 * the merge expression distinction is connector-side per the ADR.
 */
package io.sm8.core.rel

import io.sm8.core.expr.Expr
import io.sm8.core.model.{Measure, RollupSpec}

/** Pure cascade-eligibility + coarsening checks (ADR-0031 D1, D2).
  *
  * Used by:
  *  - `ModelValidator.validateCascadeDag` for the per-pair + global
  *    validity surface (deployment-time).
  *  - The connector's resolution layer at refresh time to verify the
  *    declared cascade source is still eligible under the live
  *    source rollup shape (the declaration and the live shape can
  *    diverge — a schema drift on the source invalidates the
  *    cascade; refuse typed, do not silently re-route).
  */
object CascadeContract {

  /** The verdict of the cascade-eligibility predicate (D1).
    *
    * NOT a refusal itself — the validator's *job* is to turn a
    * negative verdict into a typed `RollupRewriteRefusal` sibling
    * (`CascadeCoverageUncovered` for the partial-coarsening case,
    * etc.). The contract returns the predicate result; the call
    * site picks the refusal.
    */
  sealed trait CascadeVerdict extends Product with Serializable
  object CascadeVerdict {
    /** B is cascade-eligible from A — every D1 clause holds. */
    case object Eligible extends CascadeVerdict
    /** B has a measure whose `Decomposability` is not in
      * `{Additive, Algebraic}` (i.e. Positional/Holistic/Approximable).
      * Per ADR-0031 D1: the cascade-eligible measures still
      * cascade; the rest fall back to base-derived build. The
      * verdict is therefore NOT "ineligible" — it's "partially
      * eligible" — and the caller decides whether to proceed
      * (cascade the eligible subset) or refuse (e.g., a strict
      * operator policy that requires every measure to cascade).
      */
    final case class PartiallyEligible(nonCascadingMeasures: Set[String])
        extends CascadeVerdict
    /** A dimension B references is missing from A — the
      * `B.dimensions ⊆ A.dimensions` clause failed. */
    final case class DimensionsNotContained(missing: Set[String])
        extends CascadeVerdict
    /** B's grain does NOT coarsen A's grain structurally (D2).
      * `reason` is a human-readable detail of which axis fails
      * (time-grain mismatch, non-time-grain coarsen impossible,
      * etc.). */
    final case class GrainCoarseningFailed(reason: String)
        extends CascadeVerdict
    /** B needs an Algebraic partial state on A that A does not
      * carry. The required state column names are reported so the
      * refusal can show the operator exactly what's missing. */
    final case class PartialStateMissing(
      measure: String,
      required: Set[String],
      found: Set[String]) extends CascadeVerdict
  }

  /** Compute the cascade-eligibility verdict of B (target) from
    * A (source). Pure; no IO.
    *
    * @param target the coarser rollup (cascade target / B)
    * @param source the finer rollup (cascade source / A)
    * @param measuresOf the HOST MODEL's measure-name → Measure map
    *        (rollup `measures` are NAME-STRINGS; resolving them to
    *        `AggregateCall`s needs the host model — the validator
    *        has it, the connector resolves the model by name first)
    * @param sourceStateColumns the state-column names actually
    *        present in the source's physical table (the connector's
    *        resolution layer fills this from the source rollup's
    *        manifest at refresh time). At validator time
    *        (declaration-only) this is EMPTY and partial-state
    *        checks skip; at refresh time it's the source's live
    *        column names
    * @return the D1 verdict — Eligible, PartiallyEligible (the
    *         non-cascading measure subset), or a specific failure
    *         (DimensionsNotContained / GrainCoarseningFailed /
    *         PartialStateMissing) naming exactly what blocks the
    *         cascade
    */
  def eligibility(
    target: RollupSpec,
    source: RollupSpec,
    measuresOf: Map[String, Measure] = Map.empty,
    sourceStateColumns: Set[String] = Set.empty
  ): CascadeVerdict = {
    // Dimensions containment (D1 + D2): B.dimensions ⊆ A.dimensions,
    // with the documented exception for the grain coarsening case
    // (ADR-0031 D2: target's grain dim is the source's grain dim
    // truncated via date_trunc — e.g. source `order_date_hour` →
    // target `order_date_day`. The two are NOT identical names but
    // ARE the same axis at different grains; structural
    // coarsening has already passed above, so this is the only
    // place that knows the truncation is allowed).
    val sourceDims = source.dimensions.toSet
    val grainCarveOut: Set[String] = (source.grainDimension, target.grainDimension) match {
      case (Some(sgd), Some(tgd)) if sgd != tgd && target.dimensions.contains(tgd) =>
        // The target's grain dim is the date_trunc'd form of the
        // source's grain dim — not in A.dimensions literally, but
        // derivable. Carve it out of the missing set; the rest is
        // still a hard missing-dim refusal.
        Set(tgd)
      case _ => Set.empty
    }
    val missingDims = target.dimensions
      .filterNot(sourceDims.contains)
      .filterNot(grainCarveOut.contains)
      .toSet
    if (missingDims.nonEmpty)
      return CascadeVerdict.DimensionsNotContained(missingDims)

    // Grain coarsening (D2): structural check — B's grain must
    // coarsen A's grain. Pure declaration-shape; the DYNAMIC
    // completeness check ("does A actually have all its partitions
    // for the scope?") lives in the connector.
    grainCoarsens(target, source) match {
      case Some(reason) =>
        return CascadeVerdict.GrainCoarseningFailed(reason)
      case None => ()
    }

    // Measures: split into cascade-eligible (Additive/Algebraic)
    // and non-cascading (Positional/Holistic/Approximable). The
    // latter are tolerated but downgrade the verdict to
    // PartiallyEligible (D1 — the cascade subset proceeds, the
    // rest fall back to base-derived).
    // Resolve B's measure NAME-STRINGS against the host model's
    // measures (RollupSpec.measures are name-strings). Unknown names
    // degrade to non-cascading (fail-safe: cannot cascade what we
    // cannot classify; the unknown-ref rule in ModelValidator is
    // the loud path).
    val resolved = target.measures.map(n => n -> measuresOf.get(n)).toMap
    val nonCascading = resolved.collect {
      case (n, None) => n
      case (n, Some(m)) if !eligibleForCascade(m) => n
    }.toSet
    if (nonCascading.size == target.measures.size)
      return CascadeVerdict.PartiallyEligible(nonCascading)
    val partially = nonCascading.nonEmpty

    // Partial-state presence: for every cascade-eligible measure on
    // B that requires partial states on A, those columns must
    // exist on A's physical table (the connector's sourceMeasures
    // set). This is the partial-state-presence clause from D1:
    //   e.g. B declares Avg(F) ⇒ A must have count__F + sum__F +
    //   m2__F columns, not just sum__F
    // At validator time (sourceMeasures empty) the check is skipped
    // — the declaration cannot know the source's physical column
    // names; the connector is responsible.
    if (sourceStateColumns.nonEmpty) {
      val missing = resolved.collectFirst {
        case (n, Some(m)) if eligibleForCascade(m) =>
          val req = requiredStateColumns(m)
          val found = req.intersect(sourceStateColumns)
          if (found != req) Some((n, req, found)) else None
      }.flatten
      missing.foreach { case (n, req, found) =>
        return CascadeVerdict.PartialStateMissing(
          measure = n, required = req, found = found)
      }
    }

    if (partially) CascadeVerdict.PartiallyEligible(nonCascading)
    else CascadeVerdict.Eligible
  }

  /** The state columns a measure requires on its source rollup
    * (D1 partial-state clause). Pure — derived from the measure's
    * `AggregateCall`. For Additive measures: one column named per
    * the convention (sum__F, count__rows, min__F, max__F). For
    * Algebraic measures (Avg, Stddev×2, Variance×2): the Welford
    * triple (count__F, sum__F, m2__F).
    *
    * The naming convention must match the connector's
    * `RollupMaterializer` state-column emission; this is the
    * single source of truth for that contract on the core side.
    *
    * @param m the measure whose state columns to enumerate
    * @return the required state-column names
    */
  def requiredStateColumns(m: Measure): Set[String] =
    m.expr match {
      case AggregateCall(fn, Some(Expr.FieldRef(f)), _, _, _) =>
        fn match {
          case AggregateFn.Sum | AggregateFn.Min | AggregateFn.Max =>
            Set(prefixFor(fn) + "__" + f)
          case AggregateFn.Avg | AggregateFn.StddevSample |
               AggregateFn.StddevPopulation |
               AggregateFn.VarianceSample | AggregateFn.VariancePopulation =>
            // Welford triple. The m2 column's presence (NOT
            // sumSq) is what makes this mergeable across groups per
            // the cross-group term in ADR-0031 D1.
            Set(s"count__$f", s"sum__$f", s"m2__$f")
          case _ => Set.empty
        }
      case AggregateCall(AggregateFn.Count, None, _, _, _) =>
        Set("count__rows")
      case _ => Set.empty
    }

  /** Whether a measure's `Decomposability` makes it cascade-eligible
    * (D1: Additive + Algebraic only).
    *
    * @param m the measure to classify
    * @return true when the measure's aggregate function is Additive
    *         (incl. binary-reducible Min/Max) or Algebraic (Welford)
    */
  def eligibleForCascade(m: Measure): Boolean = {
    val d = AggregateFn.decomposability(m.expr.fn)
    d == Decomposability.Additive || d == Decomposability.Algebraic
  }

  /** The state-column name prefix for an Additive aggregate.
    * Sum/Count emit `sum__F`; Min/Max emit `min__F`/`max__F`. */
  private def prefixFor(fn: AggregateFn): String = fn match {
    case AggregateFn.Sum  => "sum"
    case AggregateFn.Count => "count"
    case AggregateFn.Min  => "min"
    case AggregateFn.Max  => "max"
    case _ => ""
  }

  /** Structural grain coarsening (D2). Pure; no IO.
    *
    * @param target the coarser rollup (cascade target / B)
    * @param source the finer rollup (cascade source / A)
    * @return None when the structural relation holds (the dynamic
    *         completeness check is connector-side, per ADR-0031);
    *         Some(reason) naming the failing axis/label otherwise
    *
    * Cases considered:
    *  1. Both grained: B's grain must be coarser than A's on the
    *     SAME grain-dimension axis (day > hour, week > day, etc. —
    *     the grain vocabulary is `RollupRewriter.KnownGrains`).
    *     Different axes = fail (you can't coarsen day-grain over
    *     hour-of-week-grain — those are independent partitions).
    *  2. B grained, A ungrained: A's whole-table view is the
    *     coarsest; B grouping a single axis over it is NOT a
    *     coarsening of A — fail (the cascaded rollup would lose
    *     the base's "no-grouping" semantics). Same in reverse.
    *  3. Both ungrained: B's dimensions must be a subset of A's
    *     AND the dim counts must drop (or stay equal — see below).
    *  4. Non-time-grain coarsening: B drops a non-grain dimension
    *     from A (same timeGrain, fewer dims). Structural — every A
    *     group maps to exactly one B group.
    */
  def grainCoarsens(
    target: RollupSpec,
    source: RollupSpec): Option[String] = {
    val tGrain = target.timeGrain
    val sGrain = source.timeGrain
    val tGD = target.grainDimension
    val sGD = source.grainDimension
    // Vocabulary gate FIRST: an unknown grain label must fail loud
    // regardless of which structural arm would otherwise run (a
    // typo'd 'fortnight' must never silently pass the ordinal
    // check just because the names also differ).
    List(tGrain -> "target", sGrain -> "source").foreach { case (g, who) =>
      g.foreach { gv =>
        if (!RollupRewriter.KnownGrains.contains(gv))
          return Some(s"$who timeGrain '$gv' is not a known grain vocabulary value")
      }
    }
    (tGrain, tGD, sGrain, sGD) match {
      case (Some(tg), Some(tgd), Some(sg), Some(sgd))
          if tgd == sgd =>
        // Vocabulary already validated above; here: ordinal must
        // strictly grow (day over hour OK, hour over day is a
        // refinement, same-grain is identity — not a coarsening).
        if (grainOrdinal(tg) <= grainOrdinal(sg))
          Some(s"target grain '$tg' is NOT coarser than source grain '$sg' " +
            "(coarsening must move to a larger time bucket)")
        else None // structurally OK; dynamic completeness is connector-side
      case (Some(_), Some(_), None, _) =>
        Some("cascade source is ungrained but target is grained — " +
          "the cascade loses the source's whole-table semantics")
      case (None, _, Some(_), Some(_)) =>
        Some("cascade target is ungrained but source is grained — " +
          "the target cannot be a coarsening of a finer-grained source")
      case (None, None, None, None) =>
        // Both ungrained — coarsening must drop or keep dimensions.
        // Equal dim sets = structurally OK (identity coarsening,
        // no partial coverage risk).
        if (target.dimensions.size > source.dimensions.size)
          Some(s"ungrained target has ${target.dimensions.size} dimensions but " +
            s"source has only ${source.dimensions.size} — a coarsening must not " +
            "increase the dimension set")
        else None
      case (Some(tg), Some(tgd), Some(sg), Some(sgd)) if tgd != sgd =>
        // Both grained, different grain-dimension NAMES. The
        // canonical ADR-0031 D2 case: hourly `order_date_hour` →
        // daily `order_date_day` — the target's dim is the
        // date_trunc'd form of the source's, SAME underlying axis.
        // Without calendar metadata, core's structural proxy for
        // "same axis" is the naming convention: both names share
        // a common base token (order_date_*) and differ only by
        // the grain-suffix token. Shared-base + strictly-coarser
        // grain = structurally consistent; the date_trunc relation
        // itself is the author's declaration, verified dynamically
        // connector-side. Names sharing NO base token are different
        // axes (day grain over ship-date-hour is not a coarsening
        // of order-date-hour) — refused.
        val sameAxis = {
          val a = sgd.split('_').toList
          val b = tgd.split('_').toList
          val common = a.zip(b).takeWhile { case (x, y) => x == y }.map(_._1)
          common.size == math.min(a.size, b.size) - 1 ||
          common.size >= math.min(a.size, b.size) - 1 && a.size == b.size
        }
        if (grainOrdinal(tg) <= grainOrdinal(sg))
          Some(s"target grain '$tg' is NOT coarser than source grain '$sg'")
        else if (!sameAxis)
          Some(s"target grainDimension '$tgd' and source grainDimension '$sgd' " +
            "do not share a grain-axis naming base — different axes cannot cascade " +
            "(the date_trunc relation must be declared on the same underlying column)")
        else None
      case (None, Some(tgd), _, _) =>
        Some(s"target declares grainDimension '$tgd' but timeGrain is None — " +
          "a declared axis without a grain label cannot be cascaded")
      case (_, _, None, Some(sgd)) =>
        Some(s"source declares grainDimension '$sgd' but timeGrain is None — " +
          "an axis without a grain label cannot be a cascade source")
    }
  }

  /** Coarsening ordinal (small bucket → small number). Used for
    * ordering checks only; not a calendar arithmetic helper. */
  private def grainOrdinal(grain: String): Int = grain match {
    case "hour"    => 1
    case "day"     => 2
    case "week"    => 3
    case "month"   => 4
    case "quarter" => 5
    case "year"    => 6
    case _         => 0
  }
}
