/*
 * SM8 Core — FreshnessPolicy (ADR-0030 D3, Tier 2).
 *
 * The declarative freshness policy an author attaches to a
 * `RollupSpec` (`freshness: Option[FreshnessPolicy]`). The policy
 * gates the `RollupBucketStale` refusal: without a policy (the
 * default), routing behavior is unchanged from pre-Tier-2 — a bucket
 * that is present but not yet final routes normally. With
 * `FinalRequired`, the connector's resolution layer refuses any
 * queried bucket whose watermark row reports non-final (ADR-0030 D3,
 * "Emission rule mirrors RollupSchemaStale": only the connector
 * instantiates the refusal; the rewriter never does).
 *
 * ==Why an ADT and not a Boolean==
 *
 * A Boolean (`requireFinal: Boolean`) cannot name the semantics and
 * cannot grow the vocabulary (a lateness-threshold policy is a
 * plausible Tier 2 follow-up). A sealed trait with one case today
 * fails loud at the match site if a sibling is added without
 * handling it (data-driven mantra 2: shape and validity are
 * separate; the shape is unconditional).
 *
 * ==Core purity contract==
 *
 * Pure data, no IO, no Spark — the watermark LOOKUP is connector
 * side (RFC §3); core only carries the verdict vocabulary
 * (`RollupRewriteRefusal.RollupBucketStale`, RollupRewriter.scala).
 */
package io.sm8.core.model

/** Declared freshness policy for a rollup (ADR-0030 D3).
  *
  * The policy decides whether non-final buckets (per the connector's
  * watermark table) refuse routing. Absent policy = route normally
  * (pre-Tier-2 behavior); `FinalRequired` = refuse non-final buckets
  * with `RollupBucketStale`.
  */
sealed trait FreshnessPolicy extends Product with Serializable

object FreshnessPolicy {

  /** Every queried bucket must be final (lateness threshold passed)
    * before the rollup serves the query. Non-final buckets yield the
    * `RollupBucketStale` refusal (connector-side emission; the
    * rewriter signature does NOT change — the verdict rides the
    * `RelOp.Scan.resolution` slot, ADR-0030 D3).
    */
  case object FinalRequired extends FreshnessPolicy
}
