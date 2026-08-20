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
 * `/tmp/semanticdf/semanticdf-core/src/main/scala/io/semanticdf/core/rel/AggregateFn.scala`
 * with the same 16 cases.
 *
 * aggregates (e.g. the 10 unwired in PR-K) surface as
 * `EngineError.FeatureDeferred` (typed error per the user's
 * directive "no silent no-op") — never silent failures.
 *
 * contract:
 * `grep -r 'org.apache.spark' sm8-core/src/main/scala/io/sm8/core/rel/AggregateFn.scala`
 */
package io.sm8.core.rel

sealed trait AggregateFn extends Product with Serializable

object AggregateFn {

 // -- Additive (can be re-aggregated from finer-grain rollups) --

 /** Sum of values. Maps to Spark `sum`, Trino `SUM`, DuckDB `SUM`. */
 case object Sum extends AggregateFn

 /** Count of rows (or non-null values if `input` is given). Maps
 * to Spark `count`, Trino `COUNT(*)`, DuckDB `COUNT(*)`. */
 case object Count extends AggregateFn

 /** Count of distinct values. NOT additive — requires
 * distinct-count re-aggregation, more expensive than
 * Sum/Count. Maps to Spark `countDistinct`, Trino
 * `APPROX_DISTINCT`, DuckDB `COUNT(DISTINCT...)`. */
 case object CountDistinct extends AggregateFn

 /** First value in group. Additive for some rollups. Maps to
 * Spark `first`, Trino `MIN(...) FILTER (WHERE ROW_NUMBER = 1)`. */
 case object First extends AggregateFn

 // -- Non-additive (cannot be re-aggregated from finer-grain) --

 /** Minimum value in group. Maps to Spark `min`, Trino `MIN`, DuckDB `MIN`. */
 case object Min extends AggregateFn

 /** Maximum value in group. Maps to Spark `max`, Trino `MAX`, DuckDB `MAX`. */
 case object Max extends AggregateFn

 // -- Algebraic (can be re-aggregated from pre-aggregated chunks) --

 /** Arithmetic mean. Maps to Spark `avg`, Trino `AVG`, DuckDB `AVG`. */
 case object Avg extends AggregateFn

 /** Sample standard deviation (divides by N-1). Maps to Spark
 * `stddev_samp`, Trino `STDDEV_SAMP`, DuckDB `STDDEV_SAMP`. */
 case object StddevSample extends AggregateFn

 /** Population standard deviation (divides by N). Maps to Spark
 * `stddev_pop`, Trino `STDDEV_POP`, DuckDB `STDDEV_POP`. */
 case object StddevPopulation extends AggregateFn

 /** Sample variance (divides by N-1). Maps to Spark `var_samp`,
 * Trino `VAR_SAMP`, DuckDB `VAR_SAMP`. */
 case object VarianceSample extends AggregateFn

 /** Population variance (divides by N). Maps to Spark `var_pop`,
 * Trino `VAR_POP`, DuckDB `VAR_POP`. */
 case object VariancePopulation extends AggregateFn

 // -- Order-statistic (position-based; expensive at scale) --

 /** Exact median (50th percentile). Maps to Spark `percentile_approx(..., 0.5)`
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

 /** Approximate percentile (faster, lossy). Takes one literal
 * argument. Maps to Spark `percentile_approx`, Trino
 * `APPROX_PERCENTILE`. Distinct from `Median` — the ADT
 * enforces this distinction at the type level so engines
 * can't silently swap them. */
 case object ApproxPercentile extends AggregateFn

 // -- Position --

 /** Last value in group. Maps to Spark `last`, Trino
 * `MAX(...) FILTER (WHERE ROW_NUMBER = N)`. */
 case object Last extends AggregateFn
}
