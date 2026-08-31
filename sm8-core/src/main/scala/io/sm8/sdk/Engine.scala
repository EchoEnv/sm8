/*
 * SM8 SDK — Engine.
 *
 * The Engine is the orchestrator. It contains no business logic (per
 * karpathy §1.2 — "The Core must not import from extension code").
 * The SDK trait here is the registry surface (HookManager +
 * TransformerRegistry); the pipeline runner, ServiceLoader discovery,
 * and the allowlist filter are provided by `io.sm8.core`
 * (`EngineImpl` + `PluginDiscovery`).
 *
 * Plugin authors interact with the Engine only through `engine.use(plugin)`
 * (per karpathy-app-design skill — Extension authors never instantiate
 * registries directly).
 *
 * The trait here is the SDK stability promise. The implementation
 * lives in `io.sm8.core.EngineImpl` and may change without SDK break.
 */
package io.sm8.sdk

/**
 * The engine is the orchestrator. Plugin authors get an Engine instance
 * passed to `Plugin.setup(engine)`; they call `engine.use(plugin)` to
 * register more Plugins (chaining) or interact with the registries
 * (`hooks`, `transformers`). The `run` method delegates to the
 * pipeline runner owned by `io.sm8.core.EngineImpl`.
 */
trait Engine {

 /**
 * Register a Plugin with the engine. The engine calls
 * `plugin.setup(this)`. Returns `this` for chaining:
 * `engine.use(p1).use(p2).use(p3)`.
 *
 * The engine guarantees (per karpathy-app-design §4.2):
 * - a Plugin whose setup throws does NOT crash the engine — it is
 *  logged as a warning and skipped;
 * - the same Plugin instance is not registered twice (idempotent).
 */
 def use(plugin: Plugin): Engine

 /**
 * Run a request through the 4-stage pipeline. Implementation lives
 * in `core.EngineImpl`.
 */
 def run(request: Request): Result

 /**
 * Hook manager. Plugins access this from `setup(engine)` to
 * register their Pre/PostHooks: `engine.hooks.registerPreHook(.)`.
 */
 def hooks: HookManager

 /**
 * Transformer registry. Plugins access this from `setup(engine)` to
 * register Transformers: `engine.transformers.register(t)`.
 * Exactly one Transformer is active at a time (Q3 = swap).
 */
 def transformers: TransformerRegistry
}