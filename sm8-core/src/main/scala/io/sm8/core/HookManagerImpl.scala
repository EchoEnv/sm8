/*
 * SM8 Core — internal HookManager implementation.
 *
 * Audit fix (Step 3 audit): removed dormant `preHooks` / `postHooks`
 * `ListBuffer`s. They were stored but never read — `preHooksFor` /
 * `postHooksFor` returned empty regardless. Dead-but-active code
 * that misled readers (per [[debug-mantra-mindset]]).
 *
 * Step 4 reintroduces dispatch; that PR adds the proper buffer +
 * priority sort + return-by-stage. For Step 3, the registry only
 * accepts registrations and reports the names it knows about.
 */
package io.sm8.core

import io.sm8.sdk.{HookManager, HookStage, PostHook, PreHook}

final class HookManagerImpl extends HookManager {

  override def registerPreHook(stage: HookStage, hook: PreHook, priority: Int): HookManager = {
    require(priority >= 0, s"sm8: priority must be non-negative, got $priority")
    // Step 3: dispatch is a no-op; Step 4 stores the registration.
    this
  }

  override def registerPostHook(stage: HookStage, hook: PostHook, priority: Int): HookManager = {
    require(priority >= 0, s"sm8: priority must be non-negative, got $priority")
    // Step 3: dispatch is a no-op; Step 4 stores the registration.
    this
  }

  /** Step 3: returns empty. Step 4 returns priority-ordered hooks. */
  override def preHooksFor(stage: HookStage): Seq[(PreHook, Int)] = Seq.empty

  /** Step 3: returns empty. Step 4 returns priority-ordered hooks. */
  override def postHooksFor(stage: HookStage): Seq[(PostHook, Int)] = Seq.empty
}