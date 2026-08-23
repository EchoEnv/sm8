/*
 * SM8 Semantic Graph Plugin (PR-149, ADR-008-AI).
 *
 * Per ADR-008-AI v1.1: the plugin ships ONE hook (JoinPathPreHook).
 * Per the architecture-spec §10 Extension Points, registering a hook
 * is the right shape for cross-cutting behavior that applies
 * regardless of data source — semantic graph validation is exactly
 * that: model-shape validation that should run before any Connector
 * is touched.
 *
 * Per the architecture-spec plugins.md "configuration-only" + "hook-only"
 * plugin types: this is a HOOK-ONLY plugin (no adapters).
 *
 * Per  SS1 ("errors are data"): the hook
 * surfaces cycle as a typed EngineError.UnsupportedCapability value
 * (NOT a String).
 */
package io.sm8.plugins.semanticgraph

import io.sm8.sdk.{Engine, Plugin}

/**
 * SM8 semantic-graph Plugin (PR-149).
 *
 * Registers `JoinPathPreHook` on the engine's hook manager. Idempotent
 * per the plugins.md Rule 1 ("must be idempotent-safe to call once at
 * startup"). No connection establishment here — that's the connector's
 * job (architecture-spec plugins.md Rule 1).
 */
final class SemanticGraphPlugin extends Plugin {
  override def setup(engine: Engine): Unit = {
    val hook = new JoinPathPreHook
    engine.hooks.registerPreHook(hook.stage, hook, hook.priority)
  }
}