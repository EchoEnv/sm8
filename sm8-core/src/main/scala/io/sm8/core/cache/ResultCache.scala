/*
 * SM8 Platform — ResultCache trait (engine-portable journaled-form API).
 *
 * declares only the journaled-form methods used by the engine-portable
 * path. The legacy row-form `get`/`put` methods (operating on
 * `CachedResult` with `Array[Row]`) are NOT in scope — they belong
 * to the Spark-adapter layer's cache, which PR-C-final will reconcile.
 *
 * existing style): `trait` (NOT `sealed` — third-party cache impls
 * like Caffeine / Redis / DB-backed live outside `sm8-platform`)
 * + companion with `NoOp` default. NOT Scala 3 `enum`. Matches the
 * pattern set by `EngineProvider` / `EngineRegistry` (PR-C0c,
 * PR-C6) which use the same `trait extends Serializable` + open
 * extension-point rationale.
 *
 * (InMemoryResultCache impl + cache lookup in `EngineService.runQuery`)
 * overrides `getJournaled` + `putJournaledWithModelAndVersion`. The
 * `getOrComputeJournaled(key, model, version, compute)` overload
 * exists for future callers who want single-flight semantics
 * **without** the journal — the typical use is the legacy
 * `Restate.run(handler,..., Supplier)` pattern from v1.x. PR-C5b-ext-γ
 * shipped the v2.x SDK dep + a `RestatedEngineRunner` helper but did
 * NOT wrap the call sites (v2.x requires a `@Service`-handler thread
 * for journaled sub-calls; the follow-up PR does the handler wiring).
 * The cache lookup in `EngineService.runQuery` is the linear `getJournaled`
 * → `executeEngine` → `putJournaledWithModelAndVersion` pattern
 * (NOT single-flight; journal-level "single execution per journal entry"
 * guarantees at-most-once semantics without needing application-level
 * single-flight).
 *
 * `Restate.run` (PR-C5b-ext-γ) can safely capture the cache in its
 * closure. Same `Product with Serializable` contract as
 * `EngineProvider` (PR-C6) and `EngineRegistry` (PR-C6).
 */
package io.sm8.core.cache

/**
 * Pluggable destination for cached engine-portable query results.
 *
 * The engine-portable path stores `RestateCachedRow` (the wire format
 * already used by the Restate journal) — caching this form avoids
 * the redundant `Array[Row]` rebuild that the legacy row-form
 * cache incurred on every cache miss.
 *
 * == Contract ==
 *
 * - `getJournaled(key)` returns the cached value if present, else
 *  `None`. The default `NoOp` returns `None`.
 * - `putJournaledWithModelAndVersion(key, value, model, version)`
 *  records the value tagged with model + version. The default
 *  `NoOp` is a no-op. (`invalidateModel(model)` is impl-internal
 *  — concrete caches expose it on the impl, not on this trait.)
 * - The cache key is computed by the caller. Key computation
 *  lands in PR-C5b-ext-β (or wherever the engine-portable path
 *  thread the key in); this trait does not prescribe the key
 *  format.
 * - `getOrComputeJournaled(key, compute)` — single-flight
 *  read-through (see method docstring for the contract). Caches
 *  that override `getJournaled` + `putJournaledWithModelAndVersion`
 *  MUST also override this method. The default throws
 *  `UnsupportedOperationException` to surface the contract gap
 *  at the call site, matching the legacy convention.
 *
 * == Why journaled-form ==
 *
 * The Restate SDK journals `RestateCachedRow` already (PR-C5b-ext-γ
 * uses `Restate.run("query.execute", RestateCachedRow.class,...)`).
 * Caching the journaled form means cache hits don't have to
 * re-materialize the row — the journaled shape is preserved end-to-end.
 *
 * == Concurrency ==
 *
 * Implementations MUST be thread-safe. The engine-portable path
 * runs on the driver; cache access happens on the driver's
 * dispatcher thread. `Restate.run` (PR-C5b-ext-γ) captures the
 * cache in a closure — the cache must be safely shareable across
 * threads.
 *
 * == Cost note ==
 *
 * On a cache miss, the engine-portable path executes the engine
 * query. `InMemoryResultCache` (PR-C5b-ext-β) is a bounded LRU
 * keyed by the SHA-256 cache key. The default `NoOp` is the
 * production default — opt in by passing a real cache to
 * `EngineService.runQuery` (PR-C5b-ext-γ).
 */
trait ResultCache extends Serializable {

 /**
 * Look up a cached journaled-form value. Returns `None` on miss.
 *
 * Default `NoOp` returns `None`. Concrete caches (PR-C5b-ext-β)
 * override this to return cached values.
 */
 def getJournaled(key: String): Option[RestateCachedRow] = None

 /**
 * Record a journaled-form value tagged with model + version. The
 * model tag is for impl-internal invalidation (`invalidateModel`
 * on the concrete `InMemoryResultCache` — not on this trait).
 * Pass `""` for entries with no model association.
 *
 * Default `NoOp` is a no-op. Concrete caches override this to
 * maintain a sidecar map from `(model, version) → key` for O(1)
 * invalidation lookups.
 */
 def putJournaledWithModelAndVersion(
  key: String,
  value: RestateCachedRow,
  model: String,
  version: Int
 ): Unit = ()

 /**
 * Single-flight read-through (journaled form). If `key` is in the
 * cache, return the cached value. If not, invoke `compute.get()`,
 * store the result, and return it.
 *
 * <b>Contract for an override</b>:
 * - Coalesce N concurrent identical calls for the same `key`
 *  into ONE `compute.get()` invocation. The typical impl is a
 *  per-key `ConcurrentHashMap[String, CompletableFuture]`.
 * - If `compute` throws, propagate the exception to ALL waiters
 *  for this key. The cache is NOT populated on failure.
 *
 * <b>Default impl is unsatisfiable on purpose</b>: the row-form
 * `put(tag="")` leaks uninvalidateable entries. Caches that
 * override `getJournaled` + `putJournaledWithModelAndVersion` MUST
 * also override this method. The default throws
 * `UnsupportedOperationException` to surface the contract gap at
 * the call site, matching the legacy convention.
 *
 * @param key  cache key
 * @param compute supplier that produces the value on miss
 * @return the cached or freshly-computed value
 */
 def getOrComputeJournaled(
  key: String,
  compute: java.util.function.Supplier[RestateCachedRow]
 ): RestateCachedRow = {
 throw new UnsupportedOperationException(
  "ResultCache.getOrComputeJournaled: no default implementation. "
  + "Caches that override getJournaled and putJournaledWithModelAndVersion "
  + "must also override getOrComputeJournaled (see InMemoryResultCache for "
  + "the single-flight pattern)."
 )
 }
}

object ResultCache {

 /**
 * A cache that drops every put and returns `None` on every get.
 * The default. Opt in by passing a real cache to the engine-portable
 * entry-point (PR-C5b-ext-γ).
 */
 val NoOp: ResultCache = new ResultCache {}
}