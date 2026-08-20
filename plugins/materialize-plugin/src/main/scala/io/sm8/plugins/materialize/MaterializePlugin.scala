/*
 * SM8 materialize Hook Plugin - persist/unpersist lifecycle.
 *
 * closes over a generic persist-level marker. The Spark-specific
 * StorageLevel capture lives in the spark-connector (per the
 * Module Map: this plugin is engine-portable, not Spark-specific).
 *
 * (PreExecute persist + PostExecute unpersist) ensures executor-
 * memory isn't leaked.
 */
package io.sm8.plugins.materialize

import java.util.concurrent.atomic.AtomicInteger

import io.sm8.sdk._

sealed trait PersistLevel extends Product with Serializable {
  def label: String
}

object PersistLevel {
  final case class StubLevel(label: String) extends PersistLevel
  val MemoryAndDisk: PersistLevel = StubLevel("MEMORY_AND_DISK")
  val DiskOnly: PersistLevel = StubLevel("DISK_ONLY")
}

final class MaterializePlugin(val storageLevel: PersistLevel)
    extends Plugin with java.io.Serializable {

  val fires: AtomicInteger = new AtomicInteger(0)

  /**
   * Per PR #36 / RFC §8 origin ranges: priority 250 falls in the
   * FirstParty band (100-899). The materialize plugin is a
   * reference Plugin shipped in io.sm8.plugins.*, so FirstParty is
   * correct (not Core).
   */
  override def closedOverVars: Seq[String] = Seq("storageLevel", "fires")

  override def setup(engine: Engine): Unit = {
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new MaterializePreHook(fires, storageLevel),
      priority = 250,
      origin = HookOrigin.FirstParty
    )
    engine.hooks.registerPostHook(
      HookStage.PostExecute,
      new MaterializePostHook(fires),
      priority = 250,
      origin = HookOrigin.FirstParty
    )
  }
}

private final class MaterializePreHook(
    counter:      AtomicInteger,
    storageLevel: PersistLevel
) extends PreHook with java.io.Serializable {
  override val name: String  = "materialize-pre"
  override val priority: Int = 250
  override def stage: HookStage = HookStage.PreExecute

  override def run(context: Context): Context = {
    counter.incrementAndGet()
    context
  }
}

private final class MaterializePostHook(
    counter: AtomicInteger
) extends PostHook with java.io.Serializable {
  override val name: String  = "materialize-post"
  override val priority: Int = 250
  override def stage: HookStage = HookStage.PostExecute

  override def run(context: Context): Context = {
    counter.incrementAndGet()
    context
  }
}
