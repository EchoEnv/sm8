/*
 * SM8 skew Hook Plugin.
 *
 * aggregate": real implementation will set the AQE skew threshold
 * via `spark.sql.adaptive.skewJoin.skewedPartitionFactor`. Per the
 * plan, the SM8 core stays Spark-free; the Spark config call lives
 * inside this Plugin (which can depend on Spark). Step 9b first cut:
 * shape-correct (counter only).
 */
package io.sm8.plugins.skew

import java.util.concurrent.atomic.AtomicInteger

import io.sm8.sdk._

final class SkewPlugin extends Plugin with java.io.Serializable {

  /** Per [[scala-spark-batch-bugs-mindset]] mantra #1. */
  override def closedOverVars: Seq[String] = Seq("fires")

  val fires: AtomicInteger = new AtomicInteger(0)

  override def setup(engine: Engine): Unit = {
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new SkewPreHook(fires),
      priority = 250
    )
  }
}

private final class SkewPreHook(counter: AtomicInteger)
    extends PreHook with java.io.Serializable {
  override val name: String = "skew"
  override val priority: Int = 250
  override def stage: HookStage = HookStage.PreExecute
  override def run(context: Context): Context = {
    counter.incrementAndGet()
    context
  }
}
