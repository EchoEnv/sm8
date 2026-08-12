/*
 * SM8 Core — internal HookManager implementation.
 *
 * Step 3 surface: register-only, no dispatch. Pipeline runner
 * directly calls `preHooksFor` / `postHooksFor`; both return empty
 * for Step 3 (real priority dispatch lands in Step 4).
 */
package io.sm8.core

import io.sm8.sdk.{HookManager, HookStage, PostHook, PreHook}

final class HookManagerImpl extends HookManager {

  private val preHooks:  scala.collection.mutable.ListBuffer[(HookStage, PreHook, Int)]  = scala.collection.mutable.ListBuffer.empty
  private val postHooks: scala.collection.mutable.ListBuffer[(HookStage, PostHook, Int)] = scala.collection.mutable.ListBuffer.empty

  override def registerPreHook(stage: HookStage, hook: PreHook, priority: Int): HookManager = {
    require(priority >= 0, s"sm8: priority must be non-negative, got $priority")
    preHooks += ((stage, hook, priority))
    this
  }

  override def registerPostHook(stage: HookStage, hook: PostHook, priority: Int): HookManager = {
    require(priority >= 0, s"sm8: priority must be non-negative, got $priority")
    postHooks += ((stage, hook, priority))
    this
  }

  /**
   * Step 3: returns empty (dispatch lands in Step 4). Hooks are
   * stored but not invoked. The Pipeline runner checks for empty
   * preHooks/postHooks and skips dispatch — that's fine for Step 3.
   */
  override def preHooksFor(stage: HookStage): Seq[(PreHook, Int)] = Seq.empty

  /** Step 3: returns empty (dispatch lands in Step 4). */
  override def postHooksFor(stage: HookStage): Seq[(PostHook, Int)] = Seq.empty
}