/*
 * SM8 Core — EngineImpl.
 *
 * Concrete implementation of the `io.sm8.sdk.Engine` trait. Lives in
 * `io.sm8.core` (internal — not SDK). Plugin authors get an Engine
 * via `EngineImpl()` or via a factory method in a future step.
 */
package io.sm8.core

import io.sm8.sdk._

/**
 * Concrete Engine. Holds the 3 registries + the Pipeline. `use(plugin)`
 * calls `plugin.setup(this)` and is forgiving (bad plugins warn, never
 * crash — per karpathy-app-design §4.2 + RFC Q6 fail-loud-but-survivable).
 */
final class EngineImpl extends Engine {

  private val _connectors:    ConnectorRegistryImpl    = new ConnectorRegistryImpl
  private val _hooks:         HookManagerImpl          = new HookManagerImpl
  private val _transformers:  TransformerRegistryImpl  = new TransformerRegistryImpl

  private val seenPlugins: scala.collection.mutable.Set[Plugin] =
    scala.collection.mutable.Set.empty

  override def use(plugin: Plugin): Engine = {
    if (seenPlugins.contains(plugin)) return this  // idempotent
    seenPlugins += plugin
    try {
      plugin.setup(this)
    } catch {
      case e: Throwable =>
        // Per karpathy-app-design §4.2: bad plugins warn, never crash.
        // Real warning sink lands when SLF4J is wired into sm8-core
        // (the pom currently has no logger; System.err is the bridge
        // until then — see scala-jvm-safety-mindset).
        System.err.println(
          s"[sm8] Plugin ${plugin.getClass.getName} failed to setup: ${e.getMessage}")
        seenPlugins -= plugin
    }
    this
  }

  override def run(request: Request): Result = {
    val pipeline = new Pipeline(_connectors, _hooks, _transformers)
    pipeline.run(request)
  }

  override def connectors: ConnectorRegistry    = _connectors
  override def hooks: HookManager               = _hooks
  override def transformers: TransformerRegistry = _transformers
}

/**
 * Factory for the default Engine implementation. Used by tests and
 * by callers who don't need a custom registry backing.
 */
object EngineImpl {
  def apply(): Engine = new EngineImpl
}