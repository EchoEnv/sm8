/*
 * SM8 Semantic Graph Plugin.
 *
 * Registers two hooks that work together: the pre-resolve validator
 * detects cycles in the model's calc-measure / dimension
 * dependency graph and surfaces dangling join right-model
 * references BEFORE any Connector work; the post-resolve observer
 * publishes a typed `GraphSnapshot` to `context.meta` so that
 * out-of-band consumers (HTTP, MCP, CLI) can read the meta-
 * inspector endpoint without the plugin knowing about the
 * transport.
 *
 * Per the architecture-spec §10 Extension Points, registering a
 * hook is the right shape for cross-cutting behavior that applies
 * regardless of data source — semantic graph validation is
 * exactly that.
 *
 * Per the architecture-spec plugins.md "configuration-only" +
 * "hook-only" plugin types: this is a HOOK-ONLY plugin (no
 * adapters).
 *
 * Per  SS1 ("errors are data"):
 * the pre-resolve hook surfaces cycle as a typed
 * `EngineError.UnsupportedCapability` value (NOT a String).
 */
package io.sm8.plugins.semanticgraph

import io.sm8.sdk.{Engine, Plugin}

/**
 * SM8 semantic-graph Plugin.
 *
 * Registers `JoinPathPreHook` (pre-resolve validator) and
 * `GraphPostResolveObserver` (post-resolve observer) on the
 * engine's hook manager. Idempotent per the plugins.md Rule 1
 * ("must be idempotent-safe to call once at startup"). No
 * connection establishment here — that's the connector's job
 * (architecture-spec plugins.md Rule 1).
 */
final class SemanticGraphPlugin extends Plugin {
  /**
   * Registers the pre-resolve validator and the post-resolve
   * observer on the engine's hook manager.
   *
   * Idempotent per the plugins.md Rule 1.
   *
   * @param engine the engine on which to register the hooks
   */
  override def setup(engine: Engine): Unit = {
    // Pre-resolve validator: detects cycles in the model's calc-
    // measure / dimension dependency graph and surfaces dangling
    // join right-model references BEFORE any Connector work.
    val pre = new JoinPathPreHook
    engine.hooks.registerPreHook(pre.stage, pre, pre.priority)
    // Post-resolve observer: publishes a typed GraphSnapshot to
    // context.meta for out-of-band consumers (HTTP, MCP, CLI)
    // that read the meta-inspector endpoint.
    val post = new GraphPostResolveObserver
    engine.hooks.registerPostHook(post.stage, post, post.priority)
  }
}