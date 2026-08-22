/*
 * SM8 Platform — EngineHookDispatcher.
 *
 * Runs an engine-portable request through the SDK hook dispatch
 * (Pre + Post hooks around the Execute stage). This is the
 * concrete wiring that satisfies RFC §13's "a new hook can be
 * added without touching any file outside their own plugin":
 * every registered Pre/Post hook at PreExecute/PostExecute fires
 * on every cache-MISS request, in priority order, with the
 * typed `EngineHookRequest` / `EngineHookResult` visible.
 *
 * ==Efficient by construction (sm8-implementation-rules rule 2)==
 *
 * - The dispatcher is stateless. Allocate it once per EngineImpl
 *   (the EngineImpl is constructed once per QueryService.definition);
 *   reuse for every request.
 * - The initial `Context` is the only allocation per request.
 *   Subsequent mutations reuse the case-class copy.
 * - Hook lookup: `HookManager.preHooksFor` / `postHooksFor` are
 *   priority-sorted on read inside `HookManagerImpl` (per the
 *   existing implementation; see `sm8-core/.../HookManagerImpl.scala:79`).
 * - The engine-call thunk (`execute`) is `Function1[Context, Either]`,
 *   captured once per cache-MISS, invoked once.
 * - No mutable state, no `var`, no `ThreadLocal`.
 *
 * Per scala-data-driven-refactor-mindset "sealed-trait dispatch":
 * the only branching in this file is the match on `Left`/`Right`
import io.sm8.core.engine.{ EngineHookRequest, EngineHookResult }
import io.sm8.sdk.{Context, HookManager, HookStage, PipelineStage, PostHook, PreHook}
 * fail-fast. The dispatcher does NOT wrap them — they propagate
 * to the caller's `Either` via the engine-call boundary. Plugin
 * authors who want non-fatal hooks must `try/catch` inside the
 * hook function.
 *
 * Per scala-jvm-safety-mindset "null is a liar": the
 * dispatcher accepts a non-null `HookManager` at construction
 * time. `execute` is a by-name parameter that is only invoked
 * when a pre-hook does NOT set `context.stop = true`.
 */
package io.sm8.platform.query.hooks

import io.sm8.core.engine.EngineError
import io.sm8.sdk.{
  Context,
  HookManager,
  HookStage,
  PipelineStage,
  PostHook,
  PreHook
}

/**
 * Pure hook dispatcher around the engine-portable Execute stage.
 *
 * Owns no state. Construct once via [[EngineHookDispatcher.apply]];
 * invoke run per request.
 *
 * @param hooks the SDK hook manager (sorted by priority on read)
 */
final class EngineHookDispatcher private (hooks: HookManager) extends io.sm8.sdk.HookRunner {

  /**
   * Run a request through the PreExecute → execute → PostExecute
   * sequence.
   *
   * PreExecute hooks fire in priority order. If any pre-hook sets
   * `context.stop = true`, the executor is skipped and the
   * accumulated `Context` flows through PostExecute hooks (so
   * audit/log/observer plugins still fire on the short-circuit
   * path).
   *
   * PostExecute hooks fire in priority order AFTER the executor
   * (or after the short-circuit). They may mutate the result
   * (observer plugins record state; mutator plugins cap rows).
   *
   * Per RFC §9 fail-fast: hook throws are NOT caught here — they
   * propagate to the caller's `EngineService.executeEngine`
   * boundary, which converts them to `EngineError` via the
   * `RuntimeException`-catching block already present there.
   */
  def run(
      initial: Context,
      execute: Context => Either[EngineError, Context]
  ): Either[EngineError, Context] = {
    val stage: PipelineStage = PipelineStage.Execute

    val afterPreE: Either[EngineError, Context] =
      firePre(stage, initial)

    afterPreE.flatMap { afterPre =>
      if (afterPre.stop) {
        // Short-circuit: skip executor, still fire post-hooks so
        // observers (audit, log) see the cached/halted path.
        // The Context carries whatever the pre-hook set
        // (`result` may be set; the dispatcher's contract is
        // "pre-hook-responsible for the result shape").
        firePost(stage, afterPre)
      } else {
        execute(afterPre).flatMap { withResult =>
          firePost(stage, withResult)
        }
      }
    }
  }

  /**
   * Fire every Pre-hook registered for `stage` in priority order.
   *
   * Per ADR-008-AF v1.0: a Pre-hook that throws is converted to a typed
   * `EngineError.HookFailed` so the caller can pattern-match on the
   * hook identity (name + priority + stage) + the underlying message.
   * The hook `message` is sanitized via `Option(e.getMessage).getOrElse(...)`
   * to avoid JVM-internal variable-name leaks.
   */
  private def firePre(stage: PipelineStage, ctx: Context): Either[EngineError, Context] = {
    val hookStage: HookStage = preStageFor(stage)
    val pre: Seq[(PreHook, Int)] = hooks.preHooksFor(hookStage)
    pre.foldLeft[Either[EngineError, Context]](Right(ctx)) { case (acc, (h, p)) =>
      acc.flatMap { c =>
        if (c.stop) Right(c)
        else try Right(h.run(c)) catch {
          case e: Throwable => Left(EngineError.HookFailed(
            engine = "<dispatcher>",
            name = h.name,
            priority = p,
            stage = hookStage.toString,
            message = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
          ))
        }
      }
    }
  }

  /**
   * Fire every Post-hook registered for `stage` in priority order.
   *
   * Per RFC `hooks.md`: Post-hook runnability on the short-circuit path
   * is type-dependent. Observer / side-effect hooks (audit, metrics,
   * cache-read acknowledgment) MUST fire even after `c.stop = true`.
   * Mutator hooks (cache-write, row-cap, schema rename) MUST skip when
   * `c.stop` — re-running them on the cached/HIT path would either
   * duplicate writes or silently mutate the stored Result.
   *
   * Per PR-9 (ADR-008-P §T1-D2 + §"What's Next"): the discriminator
   * is `PostHook.runsOnStop` (default `true` = Observer; override
   * `false` for Mutator). PR-7 (AR-P1-7) marked the Connector trait
   * deprecated; this flag finishes the symmetrical classification
   * for post-hook intent (RFC `hooks.md` enumerates 5 types; 3 of
   * them post-fire).
   */
  private def firePost(stage: PipelineStage, ctx: Context): Either[EngineError, Context] = {
    val hookStage: HookStage = postStageFor(stage)
    val post: Seq[(PostHook, Int)] = hooks.postHooksFor(hookStage)
    post.foldLeft[Either[EngineError, Context]](Right(ctx)) { case (acc, (h, p)) =>
      acc.flatMap { c =>
        if (c.stop && !h.runsOnStop) Right(c)
        else try Right(h.run(c)) catch {
          case e: Throwable => Left(EngineError.HookFailed(
            engine = "<dispatcher>",
            name = h.name,
            priority = p,
            stage = hookStage.toString,
            message = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
          ))
        }
      }
    }
  }

  /** Map a pipeline stage to its corresponding PreExecute HookStage. */
  private def preStageFor(stage: PipelineStage): HookStage = stage match {
    case PipelineStage.Parse   => HookStage.PreParse
    case PipelineStage.Resolve => HookStage.PreResolve
    case PipelineStage.Execute => HookStage.PreExecute
    case PipelineStage.Format  => HookStage.PreFormat
  }

  /** Map a pipeline stage to its corresponding PostExecute HookStage. */
  private def postStageFor(stage: PipelineStage): HookStage = stage match {
    case PipelineStage.Parse   => HookStage.PostParse
    case PipelineStage.Resolve => HookStage.PostResolve
    case PipelineStage.Execute => HookStage.PostExecute
    case PipelineStage.Format  => HookStage.PostFormat
  }
}

object EngineHookDispatcher {

  /**
   * Construct a dispatcher from a [[HookManager]]. The HookManager
   * is held by the dispatcher; plugins registered on it AFTER
   * construction will not be seen by an already-constructed
   * dispatcher... wait — actually they WILL be seen, because
   * `hooks.preHooksFor(hookStage)` reads-through to the live
   * mutable map inside `HookManagerImpl` (registered hooks are
   * sorted on read). Re-registration adds entries; subsequent
   * `preHooksFor` calls reflect the new state.
   *
   * Construct the dispatcher AFTER all plugins have called
   * `engine.hooks.registerPreHook/PostHook` for production
   * determinism.
   */
  def apply(hooks: HookManager): EngineHookDispatcher =
    new EngineHookDispatcher(hooks)

  /**
   * No-op dispatcher: fires zero hooks. Useful for unit tests
   * that don't exercise the hook path; preserves the
   * `Either[EngineError, Context]` shape without coupling test
   * code to plugin setup.
   */
  val NoOp: EngineHookDispatcher =
    new EngineHookDispatcher(new HookManager {
      override def registerPreHook(stage: HookStage, hook: PreHook, priority: Int): HookManager = this
      override def registerPostHook(stage: HookStage, hook: PostHook, priority: Int): HookManager = this
      override def preHooksFor(stage: HookStage): Seq[(PreHook, Int)] = Seq.empty
      override def postHooksFor(stage: HookStage): Seq[(PostHook, Int)] = Seq.empty
    })
}
