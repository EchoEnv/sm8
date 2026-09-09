/*
 * SM8 Core — DecomposabilityAudit (ADR-0029 Gate B item 2).
 *
 * The per-measure audit that enumerates, for every measure on every
 * live rollup, which refresh-strategy predicates the measure satisfies:
 *
 *   - delta-combination (Tier 2 MERGE, ADR-0030 §D2-7): can this
 *     measure's rollup value be maintained by a row-level delta with
 *     pre/post images, or must the bucket fall back to full recompute?
 *   - cascade-eligibility (ADR-0031 §D1): can this measure be built
 *     FROM a coarser-source rollup, given the source carries the
 *     required partial-state columns?
 *
 * The two predicates SHARE the Decomposability taxonomy but have
 * DIFFERENT inputs (ADR-0031 §D6): delta-combination keys off the
 * Decomposability class + pre/post-image availability; cascade-
 * eligibility additionally requires the source to physically carry
 * the partial-state columns the class needs. This audit produces one
 * row per (rollup, measure) with both verdicts, so a single
 * enumeration pass feeds both Gate B item 2 and any future cascade
 * deployment.
 *
 * Pure data + pure functions: no IO, no Spark, no format concepts
 * (RFC §3). The connector/platform layers consume `AuditRow` values;
 * this object never touches a table.
 */
package io.sm8.core.rel

import io.sm8.core.model.{Measure, Model, RollupSpec}

/** The audit outcome for ONE (rollup, measure) pair. */
final case class DecomposabilityAuditRow(
  rollupName: String,
  measureName: String,
  fn: AggregateFn,
  decomposability: Decomposability,
  /** Tier 2 delta-combination verdict (ADR-0030 §D2-7): true iff this
    * measure can be maintained by row-level delta merge. Additive and
    * Algebraic participate (Algebraic via Welford pre+post images);
    * Positional/Holistic/Approximable do not (ADR-0030: never
    * delta-merged; those columns fall back to partition recompute). */
  deltaCombinable: Boolean,
  /** Cascade-eligibility verdict (ADR-0031 §D1): true iff this measure
    * could be built FROM a coarser source rollup — requires the class
    * to participate AND the partial-state columns to be materialized
    * on the source. `None` = no cascade source declared for this
    * rollup (the question does not arise). */
  cascadeEligible: Option[Boolean],
  /** Why the measure is excluded, when either verdict is false.
    * Diagnostic surface for the harness report (PR #350 shape). */
  exclusionReason: Option[String]
) extends Product with Serializable

object DecomposabilityAudit {

  /** The partial-state column-name prefix a SPECIFIC aggregate fn
    * requires on a cascade source. Returns PREFIXES, not full column
    * names: the cascade source carries one partial column per measure
    * FIELD, so the audit checks presence by prefix and the caller
    * resolves field→prefix.
    *
    * Per-fn, NOT per-class (R1 review heron MEDIUM+HIGH): Sum needs
    * only `sum__<F>`; Min needs only `min__<F>`; Max needs only
    * `max__<F>`; Avg/Stddev/Variance need the Welford triple.
    * Additive class returns the union of the member fns' prefixes
    * for the AUDIT row's exclusion-reason wording only.
    */
  private[rel] def requiredPartialStatesFor(fn: AggregateFn): List[String] = fn match {
    case AggregateFn.Sum  => List("sum__")
    case AggregateFn.Min  => List("min__")
    case AggregateFn.Max  => List("max__")
    case AggregateFn.Avg  => List("count__", "sum__", "m2__")
    case AggregateFn.StddevSample | AggregateFn.StddevPopulation
      | AggregateFn.VarianceSample | AggregateFn.VariancePopulation
        => List("count__", "sum__", "m2__")
    case _ => Nil
  }

  /** Per-class minimum prefixes (union of member fns) — for the
    * exclusion-reason wording only. NOT used for the boolean check
    * (which is per-fn, not per-class).
    */
  private[rel] def requiredPartialStates(d: Decomposability): List[String] = d match {
    case Decomposability.Additive  => List("sum__", "min__", "max__")
    case Decomposability.Algebraic => List("count__", "sum__", "m2__")
    case _                         => Nil
  }

  /** Whether the source has the partial-state columns this specific
    * measure needs. Per-measure (R1 review heron HIGH): a source
    * with `sum__amount` but not `m2__amount` should allow Sum cascade
    * and reject Avg cascade.
    *
    * @param fn the measure's aggregate fn (per-fn granularity: Sum
    *        needs only sum__; Avg needs the Welford triple)
    * @param field the source measure field (e.g. "amount")
    * @param sourceHasPartialStateFor caller-resolved: does the source
    *        have the named prefix for the named field?
    * @return true iff every required prefix is present on the source
    *         for this measure's field
    */
  def sourceCarriesPartialState(
    fn: AggregateFn,
    field: String,
    sourceHasPartialStateFor: (String, String) => Boolean
  ): Boolean = {
    val required = requiredPartialStatesFor(fn)
    required.forall(prefix => sourceHasPartialStateFor(field, prefix))
  }

  /** Whether a measure's class participates in Tier 2 row-level delta
    * merge (ADR-0030 §D2-7).
    *
    * @param d the decomposability class of the measure's aggregate fn
    * @return true iff the class can be maintained by row-level delta
    */
  def deltaCombinable(d: Decomposability): Boolean = d match {
    case Decomposability.Additive  => true
    case Decomposability.Algebraic => true
    case _                         => false
  }

  /** Whether a measure's class participates in cascading at all
    * (ADR-0031 §D1) — ignoring the partial-state-presence check,
    * which needs the source's materialized columns.
    *
    * @param d the decomposability class of the measure's aggregate fn
    * @return true iff the class can cascade given a suitable source
    */
  def cascadeCapable(d: Decomposability): Boolean = d match {
    case Decomposability.Additive  => true
    case Decomposability.Algebraic => true
    case _                         => false
  }

  /** Enumerate the audit for ONE rollup declaration.
    *
    * @param spec the rollup being audited
    * @param model the host model (resolves measure refs)
    * @param cascadeSource optional declared cascade source (ADR-0031
    *                      §D5: declaration lives on the caller; core
    *                      only evaluates the pure predicate)
    * @param sourceHasPartialStateFor per-measure partial-state check
    *        (field, prefix) ⇒ present-on-source. Caller resolves the
    *        source's table schema; core asks for the verdict.
    * @return one audit row per measure on the rollup
    */
  def auditRollup(
    spec: RollupSpec,
    model: Model,
    cascadeSource: Option[RollupSpec],
    sourceHasPartialStateFor: (String, String) => Boolean
  ): List[DecomposabilityAuditRow] = {
    val declared = model.measures.filter(m => spec.measures.contains(m.name))
    declared.map { measure =>
      val d = AggregateFn.decomposability(measure.expr.fn)
      val deltaOk = deltaCombinable(d)
      val field = measure.expr.input match {
        case Some(io.sm8.core.expr.Expr.FieldRef(name)) => name
        case _                                          => ""
      }
      val (cascadeOk, cascadeVerdict, exclusion) = cascadeSource match {
        case None => (None: Option[Boolean], None: Option[Boolean], None: Option[String])
        case Some(_) if !cascadeCapable(d) =>
          (Some(false), Some(false),
            Some(s"class ${d.toString} never cascades (ADR-0031 §D1)"))
        case Some(_) if !sourceCarriesPartialState(measure.expr.fn, field, sourceHasPartialStateFor) =>
          val missing = requiredPartialStates(d)
            .filterNot(p => sourceHasPartialStateFor(field, p))
          (Some(false), Some(false),
            Some(s"class ${d.toString} cascades but source missing partials for field '$field': " +
              missing.map(p => s"'$p*'").mkString(", ")))
        case Some(_) =>
          (Some(true), Some(true), None)
      }
      val reason = exclusion.orElse {
        if (!deltaOk) Some(s"class ${d.toString} never delta-merges (ADR-0030 §D2-7)") else None
      }
      DecomposabilityAuditRow(
        rollupName = spec.name,
        measureName = measure.name,
        fn = measure.expr.fn,
        decomposability = d,
        deltaCombinable = deltaOk,
        cascadeEligible = cascadeVerdict,
        exclusionReason = reason)
    }
  }

  /** Enumerate the audit for EVERY rollup on the model.
    *
    * @param model the model whose rollups are audited
    * @param cascadeSources per-rollup declared cascade source (default: none)
    * @param sourceHasPartialStateFor per-measure partial-state check
    *        (field, prefix) ⇒ present-on-source
    * @return one audit row per (rollup, measure) pair
    */
  def auditModel(
    model: Model,
    cascadeSources: RollupSpec => Option[RollupSpec] = _ => None,
    sourceHasPartialStateFor: (String, String) => Boolean = (_, _) => false
  ): List[DecomposabilityAuditRow] =
    model.rollups.flatMap { spec =>
      auditRollup(spec, model, cascadeSources(spec), sourceHasPartialStateFor)
    }

}
