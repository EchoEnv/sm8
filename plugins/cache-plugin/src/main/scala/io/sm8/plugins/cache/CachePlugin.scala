/*
 * SM8 cache Hook Plugin.
 *
 * Step 9a: ships shape-correct Pre+Post hooks around the execute
 * stage. Per [[scala-data-driven-refactor-mindset]] "sealed-trait
 * dispatch": the Plugin registers Hooks via the SDK's
 * HookManager.registerPreHook / registerPostHook — the dispatch
 * itself is data-driven (HookManager.sortBy-read; see Step 4).
 *
 * Per [[scala-jvm-safety-mindset]]: no `var` in plugin internals.
 * The fire counter is an `AtomicInteger` (thread-safe, immutable
 * reference). The actual cache storage will be a `ConcurrentHashMap`
 * when the typed Result shape lands (Step 0); for now this Plugin
 * just records that the hooks fired.
 *
 * Per [[scala-error-handling-mindset]]: hook throws are RFC §9
 * fail-fast — we do NOT wrap them. The hook body is total
 * (no failure path), so no throw expected.
 *
 * Per [[karpathy-guidelines-mindset]] "smallest correct core":
 * real caching (ConcurrentHashMap lookup, ctx.result short-circuit
 * on hit, ctx.copy(result = Some(cached), stop = true)) lands when
 * the typed Result shape ships. For Step 9a we just register
 * the hooks and count fires.
 */
package io.sm8.plugins.cache

import java.util.concurrent.atomic.AtomicInteger

import io.sm8.sdk._

/**
 * Cache Hook Plugin. Per [[scala-impact-analysis-mindset]]: the
 * public API is just `setup(engine)` (inherited from `Plugin`).
 * Plugin authors depend on this via the Portal
 * (`io.sm8.plugins:cache-plugin_2.13`).
 *
 * Per [[scala-spark-batch-bugs]] mantra #1: `with java.io.Serializable`
 * so the Plugin can be captured in Spark UDFs/closures without
 * `NotSerializableException` at job time. Same pattern as the
 * Step 8 `TrinoConnector` fix.
 */
final class CachePlugin extends Plugin with java.io.Serializable {

  /** Test-visible counter — number of cache-read hook fires. */
  val readFires: AtomicInteger = new AtomicInteger(0)

  /** Test-visible counter — number of cache-write hook fires. */
  val writeFires: AtomicInteger = new AtomicInteger(0)

  override def setup(engine: Engine): Unit = {
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new CacheReadPreHook(readFires),
      priority = 100
    )
    engine.hooks.registerPostHook(
      HookStage.PostExecute,
      new CacheWritePostHook(writeFires),
      priority = 110
    )
  }
}

/**
 * PreExecute cache-read hook. Step 9a: just records the fire.
 * Real implementation: check cache, short-circuit on hit via
 * `ctx.copy(result = Some(cached), stop = true)`.
 *
 * Serializable: captured in closures must serialize cleanly.
 */
private final class CacheReadPreHook(counter: AtomicInteger)
    extends PreHook with java.io.Serializable {
  override val name: String = "cache-read"
  override val priority: Int = 100
  override def stage: HookStage = HookStage.PreExecute
  override def run(context: Context): Context = {
    counter.incrementAndGet()
    context
  }
}

/**
 * PostExecute cache-write hook. Step 9a: just records the fire.
 * Real implementation: store `context.result` in the cache, keyed
 * by the request.
 *
 * Serializable: captured in closures must serialize cleanly.
 */
private final class CacheWritePostHook(counter: AtomicInteger)
    extends PostHook with java.io.Serializable {
  override val name: String = "cache-write"
  override val priority: Int = 110
  override def stage: HookStage = HookStage.PostExecute
  override def run(context: Context): Context = {
    counter.incrementAndGet()
    context
  }
}