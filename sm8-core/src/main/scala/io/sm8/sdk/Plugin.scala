/*
 * SM8 SDK — Plugin.
 *
 * The unit of extension. A Plugin is a named, versioned bundle that, on
 * load, registers one or more Connectors and/or one or more Hooks with
 * the engine. It is the only thing a contributor publishes and the only
 * thing `engine.use(...)` consumes.
 *
 * Per RFC §7 + plugins.md: `Plugin.setup(engine) -> void` registers
 * adapters via `engine.connectors.register(...)` and hooks via
 * `engine.hooks.register(stage, fn, priority)`.
 *
 * Frozen after Step 1. The `setup(engine: Engine)` method signature is
 * the SDK contract. Any change is a breaking SDK change.
 */
package io.sm8.sdk

/**
 * A Plugin is the unit of extension. Implementations register Connectors
 * and Hooks with the engine during `setup`.
 *
 * Plugin authors should:
 *   - keep `setup` idempotent-safe (it is called once at startup per the
 *     RFC plugins.md Rule 1);
 *   - NOT open connections, NOT touch external systems from setup — that
 *     is the Connector's job;
 *   - hold one clear purpose (RFC plugins.md Rule 2);
 *   - NOT import other Plugins directly — read what they need from
 *     `context.meta` at hook-time (RFC plugins.md Rule 3).
 */
trait Plugin {

  /**
   * Register this Plugin's Connectors and Hooks with the engine.
   *
   * Called exactly once at startup by `Engine.use(plugin)`. Must not
   * throw under normal operation; if registration fails, return without
   * registering (the engine will log a warning, per RFC Q6 warn-and-skip).
   *
   * @param engine the engine being configured
   */
  def setup(engine: Engine): Unit
}