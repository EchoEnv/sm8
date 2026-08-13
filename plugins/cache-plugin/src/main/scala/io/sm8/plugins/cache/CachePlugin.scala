/*
 * SM8 cache Hook Plugin — stub (counter-only).
 *
 * Per [[scala-data-driven-refactor-mindset]] "sealed-trait dispatch":
 * the Plugin registers Hooks via the SDK's
 * HookManager.registerPreHook / registerPostHook.
 *
 * Per [[scala-jvm-safety-mindset]]: the fire counter is an `AtomicInteger`
 * (thread-safe, immutable reference). Real cache logic lives in
 * `io.sm8.platform.query.cache.CachePlugin` (the engine-portable
 * Plugin in sm8-platform); this artifact exists for the Plugin
 * Portal machinery (per Step 7 / Step 9 — `META-INF/services`).
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1:
 * `with java.io.Serializable` so the Plugin can be captured in
 * Spark closures without NotSerializableException at job time.
 */
package io.sm8.plugins.cache

import java.util.concurrent.atomic.AtomicInteger

import io.sm8.sdk._

final class CachePlugin extends Plugin with java.io.Serializable {

  val readFires: AtomicInteger = new AtomicInteger(0)
  val writeFires: AtomicInteger = new AtomicInteger(0)

  override def setup(engine: Engine): Unit = {
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new PreHook {
        override val name: String     = "cache-read"
        override val priority: Int    = 100
        override def stage: HookStage = HookStage.PreExecute
        override def run(c: Context): Context = { readFires.incrementAndGet(); c }
      },
      100
    )
    engine.hooks.registerPostHook(
      HookStage.PostExecute,
      new PostHook {
        override val name: String     = "cache-write"
        override val priority: Int    = 110
        override def stage: HookStage = HookStage.PostExecute
        override def run(c: Context): Context = { writeFires.incrementAndGet(); c }
      },
      110
    )
  }
}
