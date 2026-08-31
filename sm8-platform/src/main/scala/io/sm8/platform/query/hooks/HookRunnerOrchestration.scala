/*
 * SM8 Platform — HookRunnerOrchestration (ADR-010-a v0.3).
 *
 * Drives the existing [[EngineHookDispatcher]] across all 4 pipeline
 * stages (`Parse` → `Resolve` → `Execute` → `Format`) from a single
 * entry point. The dispatcher's 2-arg `run(initial, execute)` API
 * only fires PreExecute + PostExecute hooks in production (it
 * hardcodes `PipelineStage.Execute`); the orchestration layer
 * loops over the 4 stages and delegates per stage.
 *
 * ==Single entry point (karpathy-app-design single-convention rule)==
 *
 * The orchestrator is the ONLY way production code dispatches hooks.
 * Implements [[io.sm8.sdk.HookRunner.run(initial, execute)]] verbatim
 * (no sm8-core SDK edit, no new API surface).
 *
 * ==In-tree precedent==
 *
 * `sm8-core Pipeline.run:156-179` (`Stage.All.foldLeft(initial)`
 * over one Context firing pre/post hooks at each boundary). The
 * orchestrator ports the same pattern to the platform query path.
 *
 * ==`stop=true` short-circuit (Verify-advisor point 3)==
 *
 * Honors `Context.stop` across stages: once a stage's pre-hook
 * sets `stop = true`, no subsequent stage's pre-hooks or executor
 * fires. Matches `sm8-core Pipeline.scala:165-179` which already
 * does `if (ctx.stop) ctx.copy(stage = stage)`.
 *
 * ==execute runs only at Execute==
 *
 * `execute` is supplied by the caller (the engine-portable
 * executor); it runs ONLY at `PipelineStage.Execute`. The other
 * 3 stages fire only their Pre/Post hooks (observer pattern),
 * passing `identity` as the no-op executor thunk.
 *
 * ==Efficient by construction==
 *
 * - The orchestrator is stateless. Allocate it once per
 *   `EngineImpl`; reuse for every request.
 * - The initial `Context` is the only allocation per request.
 *   Subsequent mutations reuse the case-class copy.
 * - No `var`, no `ThreadLocal`, no mutable maps.
 *
 * Per [[scala-data-driven-refactor-mindset]] "sealed-trait dispatch":
 * the only branching is the match on `stage == PipelineStage.Execute`
 * for the executor-thunk choice and the `ctx.stop` short-circuit.
 *
 * Per PR-176 NonFatal discipline: any hook throw is converted to
 * `Left(EngineError.HookFailed)` by the underlying dispatcher
 * (EngineHookDispatcher.scala:139-165 / :184-209); the orchestrator
 * inherits this — `acc.flatMap` propagates the typed error without
 * re-wrapping.
 */
package io.sm8.platform.query.hooks

import io.sm8.core.engine.EngineError
import io.sm8.sdk.{Context, HookRunner, PipelineStage}

/**
 * Orchestration layer that drives [[EngineHookDispatcher]] across
 * all 4 pipeline stages from the single [[HookRunner.run]] entry
 * point.
 *
 * Construct once via [[HookRunnerOrchestration.apply]]; invoke `run`
 * per request.
 *
 * @param dispatcher the per-stage hook dispatcher (typically built
 *                   once from `engine.hooks` after all plugins have
 *                   registered). Reuses the dispatcher's
 *                   `private[platform] runStage(stage, initial, execute)`
 *                   per stage.
 */
final class HookRunnerOrchestration private (
    dispatcher: EngineHookDispatcher
) extends HookRunner {

  /**
   * Run a request through `Parse` → `Resolve` → `Execute` → `Format`,
   * firing each stage's Pre + Post hooks in sequence.
   *
   * The supplied `execute` thunk fires ONLY at `PipelineStage.Execute`.
   * The other 3 stages use `identity` (no-op thunk); a pre-hook at
   * those stages still mutates the Context (validators, observers,
   * short-circuits) before the next stage runs.
   *
   * `Context.stop = true` short-circuits all remaining stages — once
   * any stage's pre-hook sets stop, no subsequent pre-hooks or
   * executors fire. The accumulated Context flows through the
   * orchestrator's post-hook chain so observers (audit, graph
   * snapshot) still record on the short-circuit path.
   *
   * @param initial the starting Context (must carry `request =
   *                EngineHookRequest` so pre-hooks can read
   *                `request.model` / `request.mcpRequest` /
   *                `request.cacheKey`)
   * @param execute the Execute-stage executor thunk. Returns
   *                `Right(ctx with result populated)` on success,
   *                `Left(EngineError)` on failure. Invoked exactly
   *                once per call (only at the Execute stage); other
   *                stages pass `identity`.
   * @return        the final Context (post-hooks mutated it) on
   *                success; the original typed error on failure.
   */
  override def run(
      initial: Context,
      execute: Context => Either[EngineError, Context]
  ): Either[EngineError, Context] = {
    val stages: Seq[PipelineStage] = Seq(
      PipelineStage.Parse,
      PipelineStage.Resolve,
      PipelineStage.Execute,
      PipelineStage.Format
    )

    stages.foldLeft[Either[EngineError, Context]](Right(initial)) {
      case (acc, stage) =>
        acc.flatMap { ctx =>
          // Per Verify-advisor point 3: honor `Context.stop = true`
          // across stages. If a previous pre-hook (or the
          // orchestrator's own stage boundary) set stop, skip the
          // executor + post-hook chain for this stage. The post-hook
          // for the stage that set stop has already fired (via the
          // dispatcher); observers at later stages don't double-fire.
          // Mirrors `sm8-core Pipeline.scala:165-179`.
          if (ctx.stop) Right(ctx.copy(stage = stage))
          else {
            // Only the Execute stage runs the supplied executor
            // thunk; the other 3 stages fire only their Pre + Post
            // hooks (the observer pattern). `identity` is a no-op
            // thunk that returns the context unchanged, preserving
            // the dispatcher's `Either[EngineError, Context]`
            // shape without coupling Parse/Resolve/Format to a
            // specific executor body.
            val executeFn: Context => Either[EngineError, Context] =
              if (stage == PipelineStage.Execute) execute
              else (c: Context) => Right(c)
            dispatcher.runStage(stage, ctx, executeFn)
          }
        }
    }
  }
}

object HookRunnerOrchestration {

  /**
   * Construct an orchestrator from an [[EngineHookDispatcher]].
   * Construct AFTER all plugins have called
   * `engine.hooks.registerPreHook/PostHook` for production
   * determinism.
   *
   * @param dispatcher the per-stage dispatcher
   * @return           the orchestrator (stateless; reuse for every request)
   */
  def apply(dispatcher: EngineHookDispatcher): HookRunnerOrchestration =
    new HookRunnerOrchestration(dispatcher)
}