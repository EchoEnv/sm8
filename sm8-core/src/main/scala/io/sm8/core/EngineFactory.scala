/*
 * SM8 Core — EngineFactory companion.
 *
 * Sole outward-facing factory for constructing fully-wired
 * `io.sm8.sdk.Engine` instances from the adapter layer
 * (sm8-server, sm8-platform, sm8-cli, sm8-mcp). Adapters MUST
 * call `EngineFactory.create(plugins)` instead of `new EngineImpl`
 * — the concrete `EngineImpl` class is an implementation detail
 * of sm8-core, and naming it from an adapter is a layer violation
 * per RFC §3 ("adapters / core / plugins / hooks" decoupling) and
 * the AGENTS.md "Common gotchas" entry.
 *
 * ==Why this lives alongside PluginDiscovery==
 *
 * `PluginDiscovery.discoverFromConfig()` answers "what plugins
 * exist on the classpath?"; `EngineFactory.create(plugins)`
 * answers "given a plugin set, give me a wired Engine." The two
 * compose at the deployment seam (sm8-server Main.wire):
 *
 *   val plugins = PluginDiscovery.discoverFromConfig()
 *   val engine  = EngineFactory.create(plugins)
 *
 * No rename, no merge. Different layers, different concerns.
 *
 * ==Why an `object` (not a `class`)==
 *
 * The factory holds no state, has no DI surface, and the engine
 * itself is the testable unit. An `object` exposes the capability
 * at the right boundary (sm8-core) without inviting alternate
 * implementations — matching the `PluginDiscovery` style.
 *
 * ==Why the return type is the SDK `Engine` trait, not `EngineImpl`==
 *
 * Type-system layer enforcement. Adapters literally cannot name
 * the concrete class because the return type is the SDK trait.
 * This is the load-bearing part of the layer rule — the variable
 * type at the call site changes from `EngineImpl` to `Engine`,
 * and `engine.hooks` access (which both `Engine` and `EngineImpl`
 * expose) still compiles.
 *
 * ==No error return==
 *
 * `EngineImpl.use` already swallows NonFatal per the existing
 * contract (karpathy-app-design §4.2: bad plugins warn, never
 * crash). The factory inherits that contract; no new error path
 * needed here. Typed errors remain reserved for engine realization
 * (see `EngineLoader.discoverAndRealize`).
 *
 * ==No caching / no singleton==
 *
 * Each call constructs a fresh `EngineImpl`, matching
 * `PluginDiscovery.discoverFromConfig()`'s per-call semantics
 * (no leak risk across hot-reload).
 */
package io.sm8.core

import io.sm8.sdk.Engine
import io.sm8.sdk.Plugin

/**
 * Factory for constructing fully-wired `Engine` instances.
 *
 * Sole outward seam from the adapter layer for engine construction.
 * Adapters MUST NOT construct `EngineImpl` directly.
 */
object EngineFactory {

  /**
   * Construct an Engine pre-wired with the given plugins.
   *
   * @param plugins plugins to register on the engine via
   *                `engine.use(plugin)`. Empty Seq is allowed
   *                (matches the unit-test path).
   * @return the wired Engine. The return type is the SDK `Engine`
   *         trait so callers don't bind to the concrete
   *         `EngineImpl` class.
   */
  def create(plugins: Seq[Plugin]): Engine = {
    val engine = new EngineImpl
    plugins.foreach(engine.use)
    engine
  }
}
