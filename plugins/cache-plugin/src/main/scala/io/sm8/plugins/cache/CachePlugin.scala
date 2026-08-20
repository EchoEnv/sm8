/*
 * SM8 cache Hook Plugin — Pre/Post-execute hook pair.
 *
 * ===
 * "no Map-based rule tables": the cache lookup + populate is
 * data-driven through the SDK's Context (EngineHookRequest /
 * EngineHookResult). The Plugin holds ZERO business state beyond
 * the cache reference.
 *
 * ===
 * mantra #1 (closure-safety): `with java.io.Serializable` on the
 * Plugin AND each hook class. No SparkSession, no Iterator, no
 * Connection is ever closed over.
 *
 * ===Per RFC §8 / PR #33 (HookOrigin)===
 * Cache is engine-portable, not first-party: hooks fire at
 * HookOrigin.Core (priority 50/60). Core band [0, 99] per RFC §8.
 *
 * ===Per RFC §3 / §11 (where this lives)===
 * Plugins ship as JARs (cache is the literal example in §11).
 * This plugin is discoverable via:
 *   - `META-INF/services/io.sm8.sdk.Plugin` (Portable Portal)
 *   - `META-INF/sm8/plugin.properties` (coords for allowlist Q6=C)
 *
 * ===
 * Composition only. Each hook is a tiny element-wise transform.
 * Behavior lives in ResultCache (the contract, in sm8-core);
 * this plugin just wires the contract to the SDK Hook surface.
 *
 * ==Read-through (PreExecute priority 50)==
 * `cache.getJournaled(hookReq.cacheKey)`. On HIT: set
 * `context.stop = true` + `result = Some(EngineHookResult(pqr))`.
 * The dispatcher short-circuits the engine; PostExecute hooks
 * still fire (RFC §6 — short-circuit observable to all hooks).
 *
 * ==Write-through (PostExecute priority 60)==
 * `cache.putJournaled(key, journaledRow)`. On the HIT path the
 * result is already set; the encode + write becomes a redundant
 * write of the same data (cheap; keeps the hook shape uniform).
 * InMemoryResultCache dedupes by key.
 */
package io.sm8.plugins.cache

import io.sm8.core.cache._
import java.util.concurrent.atomic.AtomicInteger

import io.sm8.core.engine.{EngineHookRequest, EngineHookResult}
import io.sm8.sdk.{Context, Engine => SdkEngine, HookManager, HookOrigin, HookStage, Plugin, PostHook, PreHook}

final class CachePlugin(val cache: ResultCache) extends Plugin with java.io.Serializable {

  /** 
    * captured `cache` (ResultCache, extends Serializable) plus the
    * 4 AtomicInteger counters (all Serializable). */
  override def closedOverVars: Seq[String] =
    Seq("cache", "readFires", "writeFires", "hits", "misses")

  val readFires:  AtomicInteger = new AtomicInteger(0)
  val writeFires: AtomicInteger = new AtomicInteger(0)
  val hits:       AtomicInteger = new AtomicInteger(0)
  val misses:     AtomicInteger = new AtomicInteger(0)

  override def setup(engine: SdkEngine): Unit = {
    // RFC §8 / PR #33: explicit HookOrigin.Core (priority 0-99).
    // The 4-arg overload throws at the boundary if priority is
    // outside [0, 99]. Plugin authors opt-in via the 4-arg overload
    // — those staying on the 3-int overload (back-compat default
    // = FirstParty) are unaffected.
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new CacheReadPreHook(cache, readFires, hits, misses),
      priority = 50,
      origin   = HookOrigin.Core
    )
    engine.hooks.registerPostHook(
      HookStage.PostExecute,
      new CacheWritePostHook(cache, writeFires),
      priority = 60,
      origin   = HookOrigin.Core
    )
  }
}

/** PreExecute read-through. See class doc above. */
private final class CacheReadPreHook(
    cache:   ResultCache,
    counter: AtomicInteger,
    hits:    AtomicInteger,
    misses:  AtomicInteger
) extends PreHook with java.io.Serializable {

  override val name: String     = "cache-read"
  override val priority: Int    = 50
  override def stage: HookStage = HookStage.PreExecute

  override def run(context: Context): Context = {
    counter.incrementAndGet()
    context.request match {
      case hookReq: EngineHookRequest =>
        cache.getJournaled(hookReq.cacheKey) match {
          case Some(row) =>
            hits.incrementAndGet()
            val pqr = CachedRowDecoder.fromRestateCachedRowAsPortable(row)
            context.copy(
              result = Some(EngineHookResult(pqr)),
              stop   = true
            )
          case None =>
            misses.incrementAndGet()
            context
        }
      case _ =>
        // Foreign request shape — pass through unchanged.
        context
    }
  }
}

/** PostExecute write-through. */
/**
 * PostExecute write-through. Per [[io.sm8.sdk.PostHook]] `runsOnStop`
 * (PR-9, ADR-008-P §T1-D2): `false` — this is a Mutator. On a cache
 * HIT, the `cache-read` Pre-hook has already set `c.stop = true` and
 * the result is the same `PortableQueryResult` that's already in the
 * cache. Re-running the write-through would (a) duplicate the write,
 * (b) potentially re-encode the row, and (c) mask a logic error in
 * the cache layer (the pre-read should have returned the post-write).
 * The dispatcher short-circuits this hook when `c.stop` is true.
 *
 * `with java.io.Serializable` — captured `cache` (ResultCache, extends
 * Serializable) and `counter` (AtomicInteger, Serializable).
 */
private final class CacheWritePostHook(
    cache:   ResultCache,
    counter: AtomicInteger
) extends PostHook with java.io.Serializable {

  override val name: String     = "cache-write"
  override val priority: Int    = 60
  override def stage: HookStage = HookStage.PostExecute

  /** Mutator: skip on `c.stop` (cache HIT). Per PR-9 (ADR-008-P §T1-D2). */
  override def runsOnStop: Boolean = false

  override def run(context: Context): Context = {
    counter.incrementAndGet()
    context.result match {
      case Some(EngineHookResult(pqr)) =>
        val row = CachedRowDecoder.toRestateCachedRowFromPortable(pqr)
        context.request match {
          case hookReq: EngineHookRequest =>
            cache.putJournaledWithModelAndVersion(hookReq.cacheKey, row, hookReq.model.name, hookReq.model.version)
          case _ =>
        }
      case _ =>
    }
    context
  }
}
