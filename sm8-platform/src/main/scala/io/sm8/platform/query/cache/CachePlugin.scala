/*
 * SM8 Platform — CachePlugin (real cache-as-hook).
 *
 * Per [[scala-data-driven-refactor-mindset]] "no Map-based rule
 * tables": the cache lookup + populate is data-driven through
 * the SDK's Context.request (EngineHookRequest) and Context.result
 * (EngineHookResult). The Plugin holds ZERO business state beyond
 * the cache reference.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1 ("closures
 * captured by Spark UDFs / lambdas in Dataset.map must avoid
 * non-serializable refs"): with java.io.Serializable is declared
 * on the plugin AND each hook class. No SparkSession, no Iterator,
 * no Connection is ever closed over.
 *
 * Per RFC §8 origin ranges (PR #33): hooks fire at HookOrigin.Core
 * (priority 50/60) — cache is engine-portable, not first-party.
 *
 * Per [[karpathy-guidelines-mindset]]: smallest correct change —
 * this Plugin lives in sm8-platform (not a separate module) so
 * it has direct access to ResultCache + CachedRowDecoder. The
 * "plugin" pattern (SDK.Plugin trait) is preserved; the platform
 * passes it to QueryService.definition's plugins: Seq[Plugin].
 *
 * Read-through (PreExecute priority 50): cache.getJournaled.
 * On HIT, set context.stop = true + result = Some(EngineHookResult(pqr)).
 * The dispatcher skips the engine; PostExecute hooks still fire
 * (RFC §6 — short-circuit observable to all hooks).
 *
 * Write-through (PostExecute priority 60): cache.putJournaled.
 * On the HIT path, result is already set; the encode + write
 * becomes a redundant write of the same data (cheap; keeps the
 * hook shape uniform). The InMemoryResultCache dedupes by key.
 */
package io.sm8.platform.query.cache

import java.util.concurrent.atomic.AtomicInteger

import io.sm8.core.engine.{ EngineHookRequest, EngineHookResult }
import io.sm8.platform.query.{ CachedRowDecoder, ResultCache }
import io.sm8.sdk.{ Context, HookManager, HookOrigin, HookStage, Plugin, PostHook, PreHook, Engine => SdkEngine }

final class CachePlugin(val cache: ResultCache)
    extends Plugin with java.io.Serializable {

  val readFires: AtomicInteger = new AtomicInteger(0)
  val writeFires: AtomicInteger = new AtomicInteger(0)
  val hits: AtomicInteger = new AtomicInteger(0)
  val misses: AtomicInteger = new AtomicInteger(0)

  override def setup(engine: SdkEngine): Unit = {
    // RFC §8 / PR #33: explicit HookOrigin.Core (priority 0-99).
    // The 4-arg overload throws at the boundary if priority is
    // outside [0, 99]. plugin authors opt-in via the 4-arg overload
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
private final class CacheWritePostHook(
    cache:   ResultCache,
    counter: AtomicInteger
) extends PostHook with java.io.Serializable {

  override val name: String     = "cache-write"
  override val priority: Int    = 60
  override def stage: HookStage = HookStage.PostExecute

  override def run(context: Context): Context = {
    counter.incrementAndGet()
    context.result match {
      case Some(EngineHookResult(pqr)) =>
        context.request match {
          case hookReq: EngineHookRequest =>
            val row = CachedRowDecoder.toRestateCachedRowFromPortable(pqr)
            cache.putJournaledWithModelAndVersion(
              key      = hookReq.cacheKey,
              value    = row,
              model    = hookReq.model.name,
              version  = hookReq.model.version
            )
            context
          case _ =>
            // Foreign request shape — skip silently.
            context
        }
      case _ =>
        // HIT path or executor-no-result — the cache write is a
        // no-op here (row either already cached or never produced).
        context
    }
  }
}
