/*
 * SM8 Core — PluginDiscovery factory.
 *
 * Per [[karpathy-app-design-mindset]] §3.1 (Protocols before
 * implementations) + the layer discipline of
 * `semantic-layer-engine-architecture.md` §3 (Core Boundary):
 * deployment modules (sm8-server) must depend INWARD on sm8-core
 * via the SDK interfaces, not by naming the concrete `EngineImpl`
 * class. This factory is the inward-facing seam that lets
 * sm8-server trigger plugin discovery without binding to the
 * concrete type — so a future refactor of `EngineImpl`
 * (rename, package move, factory swap) does not silently break
 * the deployment at compile time with no loud-fail test.
 *
 * ==Why an `object` (not a `trait` with multiple impls)==
 *
 * Discovery is a single concrete behavior driven by the
 * `sm8.plugins.allowed` classpath resource. There is no reason
 * for a swappable strategy — the resource format and the SPI
 * mechanism are fixed by RFC Q6. An `object` exposes the
 * capability at the right boundary (sm8-core) without inviting
 * alternate implementations that would have to coordinate on
 * the resource format anyway.
 *
 * ==Why this lives in sm8-core (not sm8-sdk)==
 *
 * `sm8-sdk` is the frozen contract surface for plugin authors;
 * it intentionally omits the `EngineImpl` machinery (which is
 * internal). The deployment layer needs a stable entry point
 * into that machinery, and the natural place for that entry
 * point is sm8-core itself — it's an implementation detail of
 * sm8-core, exposed via a factory for outward callers.
 *
 * ==Relationship to EngineImpl==
 *
 * Internally this delegates to `new EngineImpl().discoverFromConfig()`.
 * The delegation is intentional and documented: if the
 * discovery mechanism ever changes (e.g., moves from
 * classpath-SPI to a Restate-context lookup), only this
 * factory needs to update — deployment callers are insulated.
 *
 * No SDK type changes. PR-O5 (sm8-server layer-discipline fix)
 * consumes this.
 */
package io.sm8.core

import io.sm8.sdk.Plugin

/**
 * Factory for plugin discovery. The single outward-facing entry
 * point that lets sm8-server trigger `discoverFromConfig()`
 * without naming the concrete `EngineImpl` class.
 *
 * Per scala-data-driven-refactor-mindset §1: this is a pure
 * factory — it does no caching, holds no state, and is safe
 * to call multiple times. The underlying `EngineImpl` is
 * created per call (no leak risk across hot-reload).
 *
 * Per scala-error-handlingmindset §1 (errors are data): on a
 * misconfigured boot (malformed `sm8.plugins.allowed`, SPI
 * errors), this method logs and returns `Nil` rather than
 * throwing — the caller treats an empty plugin list as a
 * "no plugins configured" boot state, NOT as a hard failure.
 * Discovery failures should not block server startup; the
 * typed-error story is reserved for engine realization
 * (see `EngineLoader.discoverAndRealize`).
 */
object PluginDiscovery {

  /**
   * Discover Plugins via the classpath SPI, gated by the
   * `sm8.plugins.allowed` allowlist (if present on the
   * classpath; otherwise permissive discovery).
   *
   * @return the Plugins that were successfully loaded
   */
  def discoverFromConfig(): List[Plugin] =
    new EngineImpl().discoverFromConfig()
}
