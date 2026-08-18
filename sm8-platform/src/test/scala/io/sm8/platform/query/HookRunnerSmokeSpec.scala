/*
 * SM8 Platform — HookRunner smoke test (PR-3b per ADR-008-P §C1).
 *
 * Per [[karpathy-app-design-mindset]] §1.3 (plugins observable
 * end-to-end): a hook runner that doesn't fire observable side effects
 * is a silent no-op. This test proves the HookRunner / EngineHookDispatcher
 * contract: when registered, Pre/Post hooks fire on every execute.
 *
 * Per ADR-008-P §C1 spec line 315: smoke test that registers a
 * CachePlugin + InMemoryResultCache, runs the same query twice, asserts
 * the second query short-circuits via CachePlugin.hits. The full
 * CachePlugin smoke test (in `plugins/cache-plugin/src/test/.../CachePluginSpec`)
 * covers that path end-to-end. Here we test the runner contract in
 * isolation: a stub PreHook that increments an AtomicInteger; the
 * test asserts the counter increments after `runner.run(...)` is called.
 *
 * Per [[scala-spark-batch-bugs-mindset]] §1 (closure-safety): the
 * PreHook.run closure captures only the AtomicInteger (Serializable).
 * No SparkSession / DataFrame / HookManager refs.
 */
package io.sm8.platform.query

import java.util.concurrent.atomic.AtomicInteger

import io.sm8.core.{EngineImpl, HookManagerImpl}
import io.sm8.platform.query.hooks.EngineHookDispatcher
import io.sm8.sdk.{Context, HookStage, PipelineStage, PreHook}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HookRunnerSmokeSpec extends AnyFunSuite with Matchers {

  test("PR-3b: EngineHookDispatcher fires registered PreExecute hooks " +
       "on every run. The contract is observably end-to-end.") {
    // Minimal wiring: HookManager + a stub PreHook + the dispatcher.
    val hookManager = new HookManagerImpl
    val hookFires = new AtomicInteger(0)
    val stubPre = new PreHook {
      override val name: String = "smoke-stub-pre"
      override val priority: Int = 50
      override val stage: HookStage = HookStage.PreExecute
      override def run(context: Context): Context = {
        hookFires.incrementAndGet()
        context
      }
    }
    hookManager.registerPreHook(HookStage.PreExecute, stubPre, priority = 50)
    val dispatcher = EngineHookDispatcher(hookManager)

    // Build a Context and run the dispatcher with a no-op execute thunk.
    // The contract: the stub PreExecute fires exactly once per run.
    val initialCtx: Context = Context(
      request = io.sm8.core.engine.EngineHookRequest(
        model    = io.sm8.core.model.Model.of(
          name    = "smoke",
          version = 1,
          source  = io.sm8.core.model.SourceRef.ByName(table = "smoke"),
          status  = io.sm8.core.model.ModelStatus.Draft,
          defaultPolicies = io.sm8.core.model.ModelPolicyDefaults(
            io.sm8.core.model.MaterializePolicy.None,
            io.sm8.core.model.CachePolicy.NoCache,
            io.sm8.core.model.AuditPolicy.NoAudit,
          ),
          dimensions = Nil, measures = Nil,
        ).toOption.get,
        mcpRequest = io.sm8.core.engine.QueryRequest(model = "smoke"),
        cacheKey   = "smoke",
      ),
      stage   = PipelineStage.Execute,
    )
    val result = dispatcher.run(initialCtx, { ctx =>
      // No-op execute thunk: return the context unchanged.
      Right(ctx)
    })

    // The contract: result is Right (the dispatcher returned a
    // post-hooks-mutated Context), and the stub PreExecute fired
    // exactly once.
    result.isRight shouldBe true
    hookFires.get() shouldBe 1
  }

}
