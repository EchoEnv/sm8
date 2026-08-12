/*
 * SM8 materialize Hook Plugin.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1: real
 * implementation will call `df.persist(level)` after the execute
 * stage. Step 9b first cut: shape-correct (counter only).
 *
 * Per [[scala-jvm-safety-mindset]] mantra #3: materialized DataFrames
 * must be `.unpersist()`-ed eventually to avoid executor-memory
 * leaks. The follow-up implementation must thread that lifecycle
 * correctly. For now, the hook just records fires.
 */
package io.sm8.plugins.materialize

import java.util.concurrent.atomic.AtomicInteger

import io.sm8.sdk._

final class MaterializePlugin extends Plugin with java.io.Serializable {

  val fires: AtomicInteger = new AtomicInteger(0)

  override def setup(engine: Engine): Unit = {
    engine.hooks.registerPostHook(
      HookStage.PostExecute,
      new MaterializePostHook(fires),
      priority = 250
    )
  }
}

private final class MaterializePostHook(counter: AtomicInteger)
    extends PostHook with java.io.Serializable {
  override val name: String = "materialize"
  override val priority: Int = 250
  override def stage: HookStage = HookStage.PostExecute
  override def run(context: Context): Context = {
    counter.incrementAndGet()
    context
  }
}