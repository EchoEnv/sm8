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

  /** The partial-state column names a Decomposability class requires
    * to be materialized on a cascade source. Additive needs the raw
    * aggregated column; Algebraic needs the Welford triple; the other
    * three classes cannot cascade at all (empty = no cascade). */
  private[rel] def requiredPartialStates(d: Decomposability): List[String] = d match {
    case Decomposability.Additive  => List("sum__", "min__", "max__") // sum-type + binary-reducible
    case Decomposability.Algebraic => List("count__", "sum__", "m2__") // Welford triple
    case _                         => Nil // Positional/Holistic/Approximable never cascade
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
    * @param sourceHasPartialState does the source materialize the
    *                      partial-state columns? (connector-resolved;
    *                      ignored when cascadeSource is None)
    * @return one audit row per measure on the rollup
    */
  def auditRollup(
    spec: RollupSpec,
    model: Model,
    cascadeSource: Option[RollupSpec],
    sourceHasPartialState: Boolean
  ): List[DecomposabilityAuditRow] = {
    val declared = model.measures.filter(m => spec.measures.contains(m.name))
    declared.map { measure =>
      val d = AggregateFn.decomposability(measure.expr.fn)
      val deltaOk = deltaCombinable(d)
      val (cascadeOk, cascadeVerdict, exclusion) = cascadeSource match {
        case None => (None: Option[Boolean], None: Option[Boolean], None: Option[String])
        case Some(_) if !cascadeCapable(d) =>
          (Some(false), Some(false),
            Some(s"class ${cls(d)} never cascades (ADR-0031 §D1)"))
        case Some(_) if !sourceHasPartialState =>
          (Some(false), Some(false),
            Some(s"class ${cls(d)} cascades but source lacks the " +
              requiredPartialStates(d).filter(_.nonEmpty).map(p => s"'$p*'")
                .mkString(", ") + " partial columns"))
        case Some(_) =>
          (Some(true), Some(true), None)
      }
      val reason = exclusion.orElse {
        if (!deltaOk) Some(s"class ${cls(d)} never delta-merges (ADR-0030 §D2-7)") else None
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
    * @param partialStatePresence per-rollup source partial-state flag (default: false)
    * @return one audit row per (rollup, measure) pair
    */
  def auditModel(
    model: Model,
    cascadeSources: RollupSpec => Option[RollupSpec] = _ => None,
    partialStatePresence: RollupSpec => Boolean = _ => false
  ): List[DecomposabilityAuditRow] =
    model.rollups.flatMap { spec =>
      auditRollup(spec, model, cascadeSources(spec), partialStatePresence(spec))
    }

  private def cls(d: Decomposability): String = d match {
    case Decomposability.Additive    => "Additive"
    case Decomposability.Algebraic   => "Algebraic"
    case Decomposability.Positional  => "Positional"
    case Decomposability.Holistic    => "Holistic"
    case Decomposability.Approximable => "Approximable"
  }
}
