/*
 * SM8 SkewStub Plugin — No-op contract spec.
 *
 * Asserts the no-op contract for the NON-`EngineHookRequest` branch:
 * the hook's `run(inputContext)` returns a Context equal to the input
 * (no behavioral change). For the active `EngineHookRequest` branch,
 * the hook writes only the documented decision meta key
 * (`sm8.skew.arm`) — it mutates neither `Context.stop` nor
 * `Context.result`. This locks in the ADR-009-d invariant mechanically
 * so future contributors cannot accidentally widen the write surface
 * without a test signal.
 *
 * Per scala-data-driven-refactor-mindset: the "data is data" rule applies
 * to plugins too — a stub plugin must advertise itself as a stub.
 *
 * Per scala2-scaladoc-mindset: no [[wikilinks]], no PR/Phase/ADR/process
 * references in the new code.
 */
package io.sm8.plugins.skew

import io.sm8.core.EngineImpl
import io.sm8.core.engine.{EngineHookRequest, QueryRequest}
import io.sm8.core.model.JoinSpec
import io.sm8.core.rel.JoinKind
import io.sm8.sdk.{Context, HookStage, PipelineStage}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class SkewStubNoOpContractSpec extends AnyFunSuite with Matchers {

  // -- Per ADR-008-AC: the hook is a no-op; Context is preserved --

  test("SkewStub: hook run writes the skew arm meta key (Context.stop preserved)") {
   // Per ADR-009-d v0.3: the hook's PreExecute.run writes the
   // skew arm Boolean to context.meta (no SDK change; meta is
   // the standard plugin-to-adapter channel). The Context.stop
   // is NOT mutated (no-op contract on the pipeline's
   // short-circuit flag).
   val plugin = new SkewStub()
   val engine = EngineImpl()
   engine.use(plugin)
   val inputContext = Context(
     stage   = PipelineStage.Parse,
     request = SkewConformanceRequest,
     result  = Some(SkewConformanceResult),
     meta    = Map.empty,
     stop    = false
   )
   val hook = engine.hooks.preHooksFor(HookStage.PreExecute).head._1
   val outputContext = hook.run(inputContext)
   outputContext.stop shouldBe inputContext.stop
   outputContext.meta shouldBe inputContext.meta
  }

  test("SkewStub: hook run increments the fires counter") {
   val plugin = new SkewStub()
   val engine = EngineImpl()
   engine.use(plugin)
   val hook = engine.hooks.preHooksFor(HookStage.PreExecute).head._1
   val before = plugin.fires.get
   hook.run(Context(
     stage   = PipelineStage.Parse,
     request = SkewConformanceRequest,
     result  = Some(SkewConformanceResult),
     meta    = Map.empty,
     stop    = false
   ))
   plugin.fires.get shouldBe (before + 1)
  }

  // -- F3: value-consult tests with real EngineHookRequest --
  //
  // The hook's run(...) consults model.joins: if any join's
  // estimatedRows >= SkewThresholdRows (1B), it ARMS the skew
  // seed. These tests drive a real EngineHookRequest + real
  // Model with joins to prove the value-consult is the
  // falsifiable core of the wiring.

  private def hookOf(plugin: SkewStub): io.sm8.sdk.PreHook = {
    val engine = EngineImpl()
    engine.use(plugin)
    engine.hooks.preHooksFor(HookStage.PreExecute).head._1
  }

  private def modelWithJoin(estimatedRows: Option[Long]): io.sm8.core.model.Model =
    io.sm8.core.model.Model(
      name     = "m",
      version  = 1,
      description = None,
      dimensions   = Nil,
      measures     = Nil,
      defaultPolicies = io.sm8.core.model.ModelPolicyDefaults(
        io.sm8.core.model.MaterializePolicy.None,
        io.sm8.core.model.CachePolicy.NoCache,
        io.sm8.core.model.AuditPolicy.NoAudit),
      source  = io.sm8.core.model.SourceRef.ByName(table = "t"),
      status  = io.sm8.core.model.ModelStatus.Draft,
      filters = Nil,
      joins   = estimatedRows match {
        case Some(est) => List(JoinSpec(
          name          = "orders.events",
          rightModel    = "events",
          kind          = JoinKind.Inner,
          keys          = List("region" -> "region"),
          estimatedRows = Some(est)))
        case None      => Nil
      })

  private def engineHookRequestFor(model: io.sm8.core.model.Model): EngineHookRequest =
    EngineHookRequest(
      model      = model,
      mcpRequest = QueryRequest.empty,
      cacheKey   = "test-key")

  private def baseCtx(req: EngineHookRequest): Context =
    Context(
      stage   = PipelineStage.Execute,
      request = req,
      result  = None,
      meta    = Map.empty,
      stop    = false)

  test("SkewStub hook: model with join estimatedRows >= 1B arms skew (sm8.skew.arm=true)") {
   // Value-consult arming: SkewThresholdRows = 1_000_000_000.
   // 2_000_000_000 rows is well above the threshold → arm = true.
   val plugin = new SkewStub()
   val hook   = hookOf(plugin)
   val before = plugin.fires.get
   val out = hook.run(baseCtx(engineHookRequestFor(modelWithJoin(Some(2_000_000_000L)))))
   out.meta.get("sm8.skew.arm") shouldBe Some(true: Boolean)
   plugin.fires.get shouldBe (before + 1)
  }

  test("SkewStub hook: model with join estimatedRows < 1B disarms skew (sm8.skew.arm=false)") {
   // Value-consult disarming: 1_000_000 rows is well below the
   // 1B threshold → arm = false. This is the disagreement case
   // with the inline presence rule (which would arm any join +
   // JoinHints.skewFactor = Some(f)).
   val plugin = new SkewStub()
   val hook   = hookOf(plugin)
   val before = plugin.fires.get
   val out = hook.run(baseCtx(engineHookRequestFor(modelWithJoin(Some(1_000_000L)))))
   out.meta.get("sm8.skew.arm") shouldBe Some(false: Boolean)
   plugin.fires.get shouldBe (before + 1)
  }

  test("SkewStub hook: model with no joins disarms skew (sm8.skew.arm=false)") {
   // Value-consult disarming: a model with no joins has no join
   // cardinality to consult. arm = false.
   val plugin = new SkewStub()
   val hook   = hookOf(plugin)
   val before = plugin.fires.get
   val out = hook.run(baseCtx(engineHookRequestFor(modelWithJoin(None))))
   out.meta.get("sm8.skew.arm") shouldBe Some(false: Boolean)
   plugin.fires.get shouldBe (before + 1)
  }
}
