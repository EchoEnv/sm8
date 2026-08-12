/*
 * SM8 Core — EngineImpl.
 *
 * Concrete implementation of the `io.sm8.sdk.Engine` trait. Lives in
 * `io.sm8.core` (internal — not SDK). Plugin authors get an Engine
 * via `EngineImpl()` or via a factory method in a future step.
 *
 * Audit fixes (Step 3 audit):
 *   - `seenPlugins` is now a `ConcurrentHashMap.newKeySet` (was
 *     `mutable.Set[Plugin]` — non-thread-safe; per
 *     [[scala-jvm-safety-mindset]]).
 *   - `catch (Throwable)` replaced with `NonFatal` so `Error`
 *     subclasses propagate; `InterruptedException` restores the
 *     interrupt flag (per [[scala-error-handling-mindset]]).
 *   - `Pipeline` is hoisted to a `val` field — was allocated per
 *     `run(request)` (hot path; per [[scala-perf-testing-mindset]]).
 */
package io.sm8.core

import java.util.concurrent.ConcurrentHashMap

import scala.util.control.NonFatal

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

  // Hoisted from per-run allocation; the Pipeline is stateless.
  private val pipeline: Pipeline = new Pipeline(_connectors, _hooks, _transformers)

  // Thread-safe set for plugin idempotency. ConcurrentHashMap.newKeySet
  // is the only Set in the standard library that scales under writes.
  private val seenPlugins: java.util.Set[Plugin] =
    ConcurrentHashMap.newKeySet[Plugin]()

  override def use(plugin: Plugin): Engine = {
    if (!seenPlugins.add(plugin)) return this  // already seen → idempotent no-op
    try {
      plugin.setup(this)
    } catch {
      case NonFatal(e) =>
        // Per karpathy-app-design §4.2: bad plugins warn, never crash.
        // System.err is a stop-gap until SLF4J wiring (deferred to Step 7).
        System.err.println(
          s"[sm8] Plugin ${plugin.getClass.getName} failed to setup: ${e.getMessage}")
        seenPlugins.remove(plugin)
      case _: InterruptedException =>
        // Restore the interrupt flag and let the caller decide.
        seenPlugins.remove(plugin)
        Thread.currentThread().interrupt()
        throw new InterruptedException("sm8: plugin setup interrupted")
    }
    this
  }

  override def run(request: Request): Result =
    pipeline.run(request)

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