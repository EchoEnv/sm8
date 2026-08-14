/*
 * SM8 broadcast Hook Plugin.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #2: the broadcast hint
 * prevents shuffle for small DataFrames in joins. Step 9b first cut:
 * shape-correct (counter only). Real implementation will set the
 * broadcast threshold and annotate the join plan when SM8 has Spark
 * integration (deferred).
 *
 * Per [[scala-jvm-safety-mindset]]: AtomicInteger (no `var`).
 * Per [[scala-spark-batch-bugs]] mantra #1: `with java.io.Serializable`
 * for Spark-closure safety (same as Step 8 TrinoConnector + Step 9a
 * Plugins).
 */
package io.sm8.plugins.broadcast

import java.util.concurrent.atomic.AtomicInteger

import io.sm8.sdk._

/**
 * Broadcast Hook Plugin. Pre-execute hook that would set broadcast
 * hints on the join plan. Step 9b: shape-correct only — increments
 * a counter on each fire.
 */
final class BroadcastPlugin extends Plugin with java.io.Serializable {

  /** Per [[scala-spark-batch-bugs-mindset]] mantra #1. */
  override def closedOverVars: Seq[String] = Seq("fires")

  val fires: AtomicInteger = new AtomicInteger(0)

  override def setup(engine: Engine): Unit = {
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new BroadcastPreHook(fires),
      priority = 250
    )
  }
}

/**
 * PreExecute broadcast hook. Step 9b: increments a counter. Real
 * implementation will set the broadcast threshold on the SparkConf
 * before query execution.
 */
private final class BroadcastPreHook(counter: AtomicInteger)
    extends PreHook with java.io.Serializable {
  override val name: String = "broadcast"
  override val priority: Int = 250
  override def stage: HookStage = HookStage.PreExecute
  override def run(context: Context): Context = {
    counter.incrementAndGet()
    context
  }
}
