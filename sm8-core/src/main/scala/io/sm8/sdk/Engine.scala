/*
 * SM8 SDK — Engine.
 *
 * The Engine is the orchestrator. It contains no business logic (per
 * karpathy §1.2 — "The Core must not import from extension code"). It
 * holds the ConnectorRegistry + HookManager + the pipeline runner.
 *
 * For Step 1 we ship the Engine as a registry trait only — no Pipeline
 * implementation, no ServiceLoader discovery, no allowlist filter. Those
 * land in Steps 3 (Pipeline skeleton) and 7 (Portal).
 *
 * Plugin authors interact with the Engine only through `engine.use(plugin)`
 * (per karpathy-app-design skill — Extension authors never instantiate
 * registries directly).
 *
 * Frozen after Step 1 for the SDK surface. The implementation lives in
 * `io.sm8.core.EngineImpl` (added in Step 3) and may change without
 * SDK break — only the trait here is a stability promise.
 */
package io.sm8.sdk

/**
 * The engine is the orchestrator. Plugin authors get an Engine instance
 * passed to `Plugin.setup(engine)`; they call `engine.use(plugin)` to
 * register more Plugins (chaining) or interact with the registries.
 *
 * For Step 1 we expose the minimum needed to write a Plugin. The full
 * registries (`connectors`, `hooks`, `transformers`) and the pipeline
 * runner land in Step 3 (Engine skeleton).
 */
trait Engine {

  /**
   * Register a Plugin with the engine. The engine calls
   * `plugin.setup(this)`. Returns `this` for chaining:
   * `engine.use(p1).use(p2).use(p3)`.
   *
   * The engine guarantees (per karpathy-app-design §4.2):
   *   - a Plugin whose setup throws does NOT crash the engine — it is
   *     logged as a warning and skipped;
   *   - the same Plugin instance is not registered twice (idempotent).
   */
  def use(plugin: Plugin): Engine

  /**
   * Run a request through the 4-stage pipeline. For Step 1 the method
   * is declared here but its implementation is in `core.EngineImpl`
   * (Step 3). The signature is the SDK contract.
   *
   * For Step 1 we accept an abstract `Request` and return an abstract
   * `Result`. The full typed shape lands in Step 3.
   */
  def run(request: Request): Result
}