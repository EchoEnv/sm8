/*
 * SM8 Core — AggregateFn (engine-portable aggregate-function ADT).
 *
 * Per the v0.1.0 IR extension plan (ADR-007 + RFC §3): aggregates
 * are a deferred concern that PR-H adds. This is the closed ADT
 * of 16 aggregate functions per the legacy design (Sum, Count,
 * CountDistinct, Avg, Min, Max, StddevSample, StddevPopulation,
 * VarianceSample, VariancePopulation, Median, PercentileContinuous,
 * PercentileDiscrete, ApproxPercentile, First, Last).
 *
 * set is fixed at compile time → sealed ADT (NOT a String or Map).
 * A `String` field would let callers pass `"sum"` / `"SUM"` /
 * `"SUMM"` (case-insensitive typos) — silent failures at
 * engine-compile time. A sealed ADT gives compiler-checked
 * exhaustiveness — the engine adapter's match statement is
 * forced to handle every case.
 *
 * aggregates (e.g. the 10 unwired in PR-K) surface as
 * `EngineError.FeatureDeferred` (typed error per the user's
 * directive "no silent no-op") — never silent failures.
 *
 * ==Rollup compositability (pre-aggregation design record, docs/adr/0022)==
 *
 * `decomposability(fn)` classifies every case by whether (and how)
 * it can be re-aggregated from pre-aggregated partial results —
 * the vocabulary a rollup router consults before re-scanning a
 * query from a rollup table. The classification is a total,
 * compiler-exhaustive function over the sealed ADT: adding a 17th
 * aggregate without filing its decomposability is a compile error.
 * The prior prose section comments carried three errors (Min/Max
 * were labeled non-additive; First was labeled additive-for-some-
 * rollups; CountDistinct sat under the additive banner while its
 * own comment said otherwise) — `AggregateFnDecomposabilitySpec`
 * guards the corrected classification.
 *
 * contract:
 * `grep -r 'org.apache.spark' sm8-core/src/main/scala/io/sm8/core/rel/AggregateFn.scala`
 */
package io.sm8.core.rel

/** How an aggregate function behaves when re-aggregated from
 * pre-aggregated partial results (rollup tables). See
 * [[AggregateFn.decomposability]] for the total classification.
 *
 * Routing contract per case (consumed by the future RollupRewriter,
 * Ticket 4 of the pre-aggregation map):
 *
 * - [[Decomposability.Additive]] — safe to serve from any
 *   coarser-grain rollup containing the query's grouping keys.
 * - [[Decomposability.Algebraic]] — safe iff the rollup stores the
 *   named partial states (e.g. Avg needs (sum, count); Stddev /
 *   Variance need (n, sum, sumSq)). v1 rollup tables must carry
 *   those columns for these functions to route.
 * - [[Decomposability.Positional]] — NOT re-aggregable without
 *   argmin/argmax (value, order-key) state; a v1 router must refuse.
 * - [[Decomposability.Holistic]] — NOT re-aggregable, period
 *   (exact median/percentile need the full distribution).
 * - [[Decomposability.Approximable]] — re-aggregable ONLY via
 *   explicit sketch state (HLL / DDsketch), and the result is then
 *   approximate. sm8 deliberately keeps exact `CountDistinct` and
 *   `ApproxPercentile` as separate ADT cases so engines cannot
 *   silently swap them — an approx substitution is a wire-contract
 *   change, never a rollup artifact (pre-aggregation ADR).
 */
sealed trait Decomposability extends Product with Serializable

object Decomposability {

 /** Re-aggregable from partials by applying the same function:
 * `f(f(a,b), c) == f(a,b,c)` (associative; for Min/Max, also
 * idempotent). Sum, Count, Min, Max. */
 case object Additive extends Decomposability

 /** Not re-aggregable by the same function, but computable
 * from a fixed set of named partial states over each group
 * (Avg from (sum, count); Stddev/Variance from (n, sum, sumSq)).
 * Per textbook decomposable-aggregate theory. */
 case object Algebraic extends Decomposability

 /** Depends on within-group ordering (First, Last). Re-aggregation
 * requires argmin/argmax (value, order-key) state that no current
 * connector exposes; routers must refuse these in v1. */
 case object Positional extends Decomposability

 /** Requires the full within-group distribution (exact Median,
 * PercentileContinuous, PercentileDiscrete). No bounded-size
 * partial state suffices; routers must refuse. */
 case object Holistic extends Decomposability

 /** Re-aggregable ONLY through explicit sketch state (HLL for
 * distinct counts, GK/DDsketch for percentiles), yielding an
 * APPROXIMATE result. CountDistinct and ApproxPercentile. Serving
 * these from a rollup silently changes exact semantics into
 * approximate ones — forbidden in v1 routing (ADR-0022). */
 case object Approximable extends Decomposability
}

sealed trait AggregateFn extends Product with Serializable

object AggregateFn {

 // -- Additive: f(f(a,b), c) == f(a,b,c). Rollup-safe from any
 // coarser grain. (Min/Max were previously mislabeled non-additive;
 // min(min(part)) = min(total) — associativity + idempotence.)

 /** Sum of values. Maps to Spark `sum`, Trino `SUM`, DuckDB `SUM`. */
 case object Sum extends AggregateFn

 /** Count of rows (or non-null values if `input` is given). Maps
 * to Spark `count`, Trino `COUNT(*)`, DuckDB `COUNT(*)`. */
 case object Count extends AggregateFn

 /** Minimum value in group. Additive (idempotent): min over
 * partial minima equals the global min. Maps to Spark `min`,
 * Trino `MIN`, DuckDB `MIN`. */
 case object Min extends AggregateFn

 /** Maximum value in group. Additive (idempotent): max over
 * partial maxima equals the global max. Maps to Spark `max`,
 * Trino `MAX`, DuckDB `MAX`. */
 case object Max extends AggregateFn

 // -- Algebraic: computable from named partial states. Safe to
 // serve from a rollup only if the rollup stores those states.

 /** Arithmetic mean: re-aggregable from (sum, count) partials.
 * Maps to Spark `avg`, Trino `AVG`, DuckDB `AVG`. */
 case object Avg extends AggregateFn

 /** Sample standard deviation (divides by N-1): re-aggregable
 * from (n, sum, sumSq) partials. Maps to Spark `stddev_samp`,
 * Trino `STDDEV_SAMP`, DuckDB `STDDEV_SAMP`. */
 case object StddevSample extends AggregateFn

 /** Population standard deviation (divides by N): re-aggregable
 * from (n, sum, sumSq) partials. Maps to Spark `stddev_pop`,
 * Trino `STDDEV_POP`, DuckDB `STDDEV_POP`. */
 case object StddevPopulation extends AggregateFn

 /** Sample variance (divides by N-1): re-aggregable from
 * (n, sum, sumSq) partials. Maps to Spark `var_samp`,
 * Trino `VAR_SAMP`, DuckDB `VAR_SAMP`. */
 case object VarianceSample extends AggregateFn

 /** Population variance (divides by N): re-aggregable from
 * (n, sum, sumSq) partials. Maps to Spark `var_pop`,
 * Trino `VAR_POP`, DuckDB `VAR_POP`. */
 case object VariancePopulation extends AggregateFn

 // -- Approximable: re-aggregable only via explicit sketch state,
 // and the result is then approximate. v1 rollup routing must
 // refuse (never silently swap exact for approximate).

 /** Count of distinct values. NOT additive: exact re-aggregation
 * requires the full per-group distinct sets; HLL sketches make it
 * re-aggregable but approximate. Maps to Spark `countDistinct`,
 * Trino `APPROX_DISTINCT`, DuckDB `COUNT(DISTINCT.)`. */
 case object CountDistinct extends AggregateFn

 /** Approximate percentile (faster, lossy). Takes one literal
 * argument. Maps to Spark `percentile_approx`, Trino
 * `APPROX_PERCENTILE`. Distinct from `Median` — the ADT
 * enforces this distinction at the type level so engines
 * can't silently swap them. */
 case object ApproxPercentile extends AggregateFn

 // -- Order-statistic / positional: NOT rollup-safe in general.
 // Routers must refuse these in v1 (see [[Decomposability]]).

 /** Exact median (50th percentile). Maps to Spark `percentile_approx(., 0.5)`
 * or `median`, Trino `MEDIAN`, DuckDB `MEDIAN`. Distinct from
 * `ApproxPercentile` — see ADR note. */
 case object Median extends AggregateFn

 /** Continuous percentile (linear interpolation). Takes one
 * literal argument (the percentile). Maps to Spark
 * `percentile_approx`, Trino `PERCENTILE_CONT`. */
 case object PercentileContinuous extends AggregateFn

 /** Discrete percentile (nearest-rank). Takes one literal
 * argument. Maps to Trino `PERCENTILE_DISC`. */
 case object PercentileDiscrete extends AggregateFn

 /** First value in group. Positional: NOT additive "for some
 * rollups" (a prior comment claimed so) — re-aggregation needs
 * argmin/argmax (value, order-key) state. Maps to
 * Spark `first`, Trino `MIN(.) FILTER (WHERE ROW_NUMBER = 1)`. */
 case object First extends AggregateFn

 /** Last value in group. Positional (see [[First]]). Maps to
 * Spark `last`, Trino `MAX(.) FILTER (WHERE ROW_NUMBER = N)`. */
 case object Last extends AggregateFn

 /** Total, compiler-exhaustive decomposability classification of
 * an aggregate function (the routing vocabulary of the
 * pre-aggregation design).
 * Adding a new `AggregateFn` case without adding its arm here is
 * a compile error — the classification cannot silently drift the
 * way the previous prose comments did.
 *
 * @param fn the aggregate function to classify
 * @return the [[Decomposability]] governing whether (and how) a
 *         rollup router may serve `fn` from pre-aggregated partials
 */
 def decomposability(fn: AggregateFn): Decomposability = fn match {
 case Sum                => Decomposability.Additive
 case Count              => Decomposability.Additive
 case Min                => Decomposability.Additive
 case Max                => Decomposability.Additive
 case Avg                => Decomposability.Algebraic
 case StddevSample       => Decomposability.Algebraic
 case StddevPopulation   => Decomposability.Algebraic
 case VarianceSample     => Decomposability.Algebraic
 case VariancePopulation => Decomposability.Algebraic
 case CountDistinct      => Decomposability.Approximable
 case ApproxPercentile   => Decomposability.Approximable
 case Median             => Decomposability.Holistic
 case PercentileContinuous => Decomposability.Holistic
 case PercentileDiscrete   => Decomposability.Holistic
 case First              => Decomposability.Positional
 case Last               => Decomposability.Positional
 }
}
