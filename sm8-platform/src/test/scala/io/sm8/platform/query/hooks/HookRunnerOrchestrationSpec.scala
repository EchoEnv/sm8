/*
 * SM8 Platform — HookRunnerOrchestrationSpec (ADR-010-a v0.3).
 *
 * Validates the orchestration layer above `EngineHookDispatcher`:
 *  1. Driving a request through the orchestrator fires Pre + Post
 *     hooks at all 4 stages (`Parse`, `Resolve`, `Execute`, `Format`).
 *  2. `Context.stop = true` from one stage's pre-hook short-circuits
 *     all SUBSEQUENT stages (per Verify-advisor point 3).
 *  3. The `execute` thunk fires exactly ONCE — only at
 *     `PipelineStage.Execute`. The other 3 stages use `identity`
 *     (no-op) as the executor thunk.
 *
 * Per [[debug-mantra-mindset]] §5 (verify): each acceptance criterion
 * is a falsifiable assertion against observable side effects
 * (AtomicInteger counters on stub hooks), not against internal
 * state.
 */
package io.sm8.platform.query.hooks

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.sm8.core.HookManagerImpl
import io.sm8.sdk.{Context, HookStage, PipelineStage, PostHook, PreHook}

class HookRunnerOrchestrationSpec extends AnyFunSuite with Matchers {

  /**
   * Build a stub hook that increments a counter on `run`.
   * Used to assert pre/post hooks fire at each pipeline stage.
   */
  private final class StubPre(hookName: String, hookPriority: Int, hookStage: HookStage)
      extends PreHook {
    override val name: String = hookName
    override val priority: Int = hookPriority
    override val stage: HookStage = hookStage
    val fires: AtomicInteger = new AtomicInteger(0)
    override def run(c: Context): Context = {
      fires.incrementAndGet()
      c
    }
  }

  private final class StubPost(hookName: String, hookPriority: Int, hookStage: HookStage)
      extends PostHook {
    override val name: String = hookName
    override val priority: Int = hookPriority
    override val stage: HookStage = hookStage
    val fires: AtomicInteger = new AtomicInteger(0)
    override def run(c: Context): Context = {
      fires.incrementAndGet()
      c
    }
  }

  /**
   * Build a stub Pre-hook that sets `stop = true` and writes a meta
   * key on `run`. Used to verify the `stop=true` short-circuit
   * propagates across stages.
   */
  private final class StopPre(hookName: String, hookPriority: Int, hookStage: HookStage,
      metaKey: String, metaValue: String)
      extends PreHook {
    override val name: String = hookName
    override val priority: Int = hookPriority
    override val stage: HookStage = hookStage
    override def run(c: Context): Context =
      c.copy(stop = true, meta = c.meta + (metaKey -> metaValue))
  }
  /** A trivial initial Context (the orchestrator mutates it). */
  private def initialCtx: Context = Context(
    stage   = PipelineStage.Parse,
    request = new io.sm8.sdk.Request {},
    result  = None,
    meta    = Map.empty,
    stop    = false
  )

  test("orchestrator drives all 4 stages (Parse, Resolve, Execute, Format)") {
    // One stub Pre + Post per stage registered via HookManager.
    val hm = new HookManagerImpl
    val parsePre   = new StubPre("parse-pre",   100, HookStage.PreParse)
    val parsePost  = new StubPost("parse-post", 100, HookStage.PostParse)
    val resolvePre = new StubPre("resolve-pre", 100, HookStage.PreResolve)
    val resolvePost= new StubPost("resolve-post",100, HookStage.PostResolve)
    val execPre    = new StubPre("exec-pre",    100, HookStage.PreExecute)
    val execPost   = new StubPost("exec-post",  100, HookStage.PostExecute)
    val formatPre  = new StubPre("format-pre",  100, HookStage.PreFormat)
    val formatPost = new StubPost("format-post",100, HookStage.PostFormat)
    Seq(parsePre, parsePost, resolvePre, resolvePost,
        execPre, execPost, formatPre, formatPost).foreach { h =>
      h match {
        case p: PreHook  => hm.registerPreHook(p.stage,  p, p.priority)
        case p: PostHook => hm.registerPostHook(p.stage, p, p.priority)
      }
    }

    val dispatcher = EngineHookDispatcher(hm)
    val orchestrator = HookRunnerOrchestration(dispatcher)

    val executeFires = new AtomicInteger(0)
    val execute: Context => io.sm8.core.engine.EngineError Either Context = { ctx =>
      executeFires.incrementAndGet()
      Right(ctx)
    }
    val result = orchestrator.run(initialCtx, execute)

    result.isRight shouldBe true
    // All 4 Pre + 4 Post hooks fired.
    parsePre.fires.get()    shouldBe 1
    parsePost.fires.get()   shouldBe 1
    resolvePre.fires.get()  shouldBe 1
    resolvePost.fires.get() shouldBe 1
    execPre.fires.get()     shouldBe 1
    execPost.fires.get()    shouldBe 1
    formatPre.fires.get()   shouldBe 1
    formatPost.fires.get()  shouldBe 1
  }

  test("orchestrator: `execute` runs exactly once, only at Execute stage") {
    // Per ADR-010-a v0.3: only the Execute stage runs the executor
    // thunk; the other 3 stages use `identity` (no-op). The
    // orchestrator MUST NOT invoke `execute` at Parse/Resolve/Format.
    val hm = new HookManagerImpl
    val dispatcher = EngineHookDispatcher(hm)
    val orchestrator = HookRunnerOrchestration(dispatcher)

    val executeFires = new AtomicInteger(0)
    val execute: Context => io.sm8.core.engine.EngineError Either Context = { ctx =>
      executeFires.incrementAndGet()
      Right(ctx)
    }
    val result = orchestrator.run(initialCtx, execute)

    result.isRight shouldBe true
    // `execute` invoked exactly once across all 4 stages.
    executeFires.get() shouldBe 1
  }

  test("orchestrator: stop=true from a PreResolve hook short-circuits Execute + Format stages") {
    // Per Verify-advisor point 3: `Context.stop = true` set by one
    // stage's pre-hook must prevent later stages (Execute, Format)
    // from firing their pre-hooks / executor / post-hooks. This is
    // the regression test for the orchestrator's stop-propagation
    // discipline.
    val hm = new HookManagerImpl
    // PreResolve stub: sets stop=true and writes meta key.
    val stopPre = new StopPre("resolve-stop", 100, HookStage.PreResolve,
      metaKey = "test.stopped", metaValue = "yes")
    hm.registerPreHook(stopPre.stage, stopPre, stopPre.priority)

    // Execute + Format stub hooks: should NEVER fire because the
    // PreResolve short-circuited the pipeline.
    val execPre = new StubPre("exec-pre",  100, HookStage.PreExecute)
    val execPost= new StubPost("exec-post",100, HookStage.PostExecute)
    val formatPre = new StubPre("format-pre",  100, HookStage.PreFormat)
    val formatPost= new StubPost("format-post",100, HookStage.PostFormat)
    Seq(execPre, execPost, formatPre, formatPost).foreach { h =>
      h match {
        case p: PreHook  => hm.registerPreHook(p.stage,  p, p.priority)
        case p: PostHook => hm.registerPostHook(p.stage, p, p.priority)
      }
    }

    val dispatcher = EngineHookDispatcher(hm)
    val orchestrator = HookRunnerOrchestration(dispatcher)

    val executeFires = new AtomicInteger(0)
    val execute: Context => io.sm8.core.engine.EngineError Either Context = { ctx =>
      executeFires.incrementAndGet()
      Right(ctx)
    }
    val result = orchestrator.run(initialCtx, execute)

    result.isRight shouldBe true
    // Execute stage did NOT run: no executor call, no Execute-stage
    // pre/post hooks fired.
    executeFires.get() shouldBe 0
    execPre.fires.get()   shouldBe 0
    execPost.fires.get()  shouldBe 0
    // Format stage did NOT run.
    formatPre.fires.get()  shouldBe 0
    formatPost.fires.get() shouldBe 0
  }

  test("orchestrator: stop=true from PreParse short-circuits all 3 later stages") {
    // The short-circuit applies to ALL later stages, not just the
    // immediate next one. Set stop at PreParse (the first stage) and
    // assert nothing after it fires.
    val hm = new HookManagerImpl
    val stopPre = new StopPre("parse-stop", 100, HookStage.PreParse,
      metaKey = "test.stopped", metaValue = "yes")
    hm.registerPreHook(stopPre.stage, stopPre, stopPre.priority)

    val resolvePre = new StubPre("resolve-pre",   100, HookStage.PreResolve)
    val execPre    = new StubPre("exec-pre",      100, HookStage.PreExecute)
    val formatPre  = new StubPre("format-pre",    100, HookStage.PreFormat)
    Seq(resolvePre, execPre, formatPre).foreach { p =>
      hm.registerPreHook(p.stage, p, p.priority)
    }

    val dispatcher = EngineHookDispatcher(hm)
    val orchestrator = HookRunnerOrchestration(dispatcher)

    val executeFires = new AtomicInteger(0)
    val execute: Context => io.sm8.core.engine.EngineError Either Context = { ctx =>
      executeFires.incrementAndGet()
      Right(ctx)
    }
    val result = orchestrator.run(initialCtx, execute)

    result.isRight shouldBe true
    executeFires.get()    shouldBe 0
    resolvePre.fires.get() shouldBe 0
    execPre.fires.get()    shouldBe 0
    formatPre.fires.get()  shouldBe 0
  }

  test("orchestrator: no hooks registered -> orchestrator is a transparent pass-through") {
    // Backward-compat: with an empty HookManager, the orchestrator
    // fires zero hooks and returns the context with the executor's
    // result applied (only at Execute stage).
    val hm = new HookManagerImpl
    val dispatcher = EngineHookDispatcher(hm)
    val orchestrator = HookRunnerOrchestration(dispatcher)

    val executeFires = new AtomicInteger(0)
    val execute: Context => io.sm8.core.engine.EngineError Either Context = { ctx =>
      executeFires.incrementAndGet()
      Right(ctx.copy(stage = PipelineStage.Format))
    }
    val result = orchestrator.run(initialCtx, execute)

    result.isRight shouldBe true
    executeFires.get() shouldBe 1
    // Final stage reflects the executor's stage tag.
    result.toOption.get.stage shouldBe PipelineStage.Format
  }
}