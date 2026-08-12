/*
 * SM8 SDK — HookManager.
 *
 * New in Step 3 (extends the SDK from 7 to 10 types).
 *
 * Holds Hooks (PreHook, PostHook, Transformer) and dispatches them
 * around the pipeline. Step 3 ships a thin skeleton; full priority
 * ordering + fail-fast dispatch lands in Step 4 (HookManager impl).
 *
 * Plugin authors call:
 *   engine.hooks.register(stage, hook, priority)
 *   engine.transformers.register(t)
 *
 * from their `setup(engine)` method.
 */
package io.sm8.sdk

/**
 * HookManager — owns Pre/PostHook registration and dispatch.
 *
 * Step 3 surface: register-only, no dispatch yet.
 * Step 4 adds: priority-ordered dispatch + fail-fast on throw.
 */
trait HookManager {

  /**
   * Register a PreHook bound to `stage` with `priority`.
   * Lower priority runs first (RFC §8). Priority range reserved by
   * origin: 0-99 core, 100-899 first-party, 900+ community.
   *
   * @return this manager, for chaining
   * @throws IllegalArgumentException if priority is negative
   */
  def registerPreHook(stage: HookStage, hook: PreHook, priority: Int): HookManager

  /**
   * Register a PostHook bound to `stage` with `priority`.
   * Same priority rules as registerPreHook.
   */
  def registerPostHook(stage: HookStage, hook: PostHook, priority: Int): HookManager

  /**
   * All PreHooks for a given stage, in priority order (lower first;
   * ties broken by registration order). Returns empty Seq in Step 3
   * (dispatch lands in Step 4). Used by the Pipeline runner.
   */
  def preHooksFor(stage: HookStage): Seq[(PreHook, Int)]

  /**
   * All PostHooks for a given stage, in priority order. Empty in
   * Step 3.
   */
  def postHooksFor(stage: HookStage): Seq[(PostHook, Int)]
}