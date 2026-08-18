/*
 * SM8 Cache Plugin — phantom-typed witnesses (PR-16, ADR-008-Q §PR-16).
 *
 * Per `karpathy-app-designmindset` §3.1 (Protocols before implementations):
 * the Witnesses are the implementations of the phantom-typed Protocol
 * defined in sm8-core (`TypedMeasure[M]`). They live in this plugin's
 * `object Refs` because the phantom identifier (e.g. `CacheHits`) is
 * plugin-specific.
 *
 * Per ADR-008-Q §C9 (Restate forward-looking) + `scala-jvm-safety-mindset`
 * §2: the TypedMeasure trait already `extends Serializable`; the Witnesses
 * here are case-class instances of that trait. Per
 * `scala-spark-batch-bugs-mindset` §1 (closure-safety): the Witnesses MUST
 * be defined at `object` level (singleton) so Spark UDFs can safely
 * capture them. Per `TypedDimensionClosureSafetySpec`: a method-local
 * `Refs` definition + non-Serializable enclosing local throws
 * `NotSerializableException` at executor startup.
 *
 * Per `scala-data-driven-refactor-mindset` §1 (data is data): the Witnesses
 * hold only Strings + the phantom `[M]` reference. No behavior, no state.
 */
package io.sm8.plugins.cache

import io.sm8.core.model.TypedMeasure

/**
 * Phantom-typed measure witnesses for the cache plugin.
 *
 * Wire-stable names match the existing `caches`/`hits` fields
 * referenced in the plugin's runQuery path.
 */
object Refs {

  // Phantom identifiers — sealed traits so the compiler rejects
  // typos at the call site (per scala-bug-huntingmindset §1).
  sealed trait CacheHits
  sealed trait CacheMisses

  /** AggregateFn.Count — number of cache hits. */
  val cacheHits: TypedMeasure[CacheHits] =
    TypedMeasure.count[CacheHits]("cache_hits")

  /** AggregateFn.Count — number of cache misses. */
  val cacheMisses: TypedMeasure[CacheMisses] =
    TypedMeasure.count[CacheMisses]("cache_misses")
}
