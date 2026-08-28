/*
 * SM8 MaterializeStub Plugin — No-op contract spec.
 *
 * Asserts the no-op contract: the hook's `run(inputContext)` returns a
 * Context equal to the input (no behavioral change), and the counter is
 * incremented. This locks in the no-op invariant mechanically so future
 * contributors cannot accidentally add real behavior without a test signal.
 *
 * Per scala-data-driven-refactor-mindset: the "data is data" rule applies
 * to plugins too — a stub plugin must advertise itself as a stub.
 *
 * Per scala2-scaladoc-mindset: no [[wikilinks]], no PR/Phase/ADR/process
 * references in the new code.
 */
package io.sm8.plugins.materialize

import io.sm8.core.EngineImpl
import io.sm8.sdk.{Context, HookStage, PipelineStage}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class MaterializeStubNoOpContractSpec extends AnyFunSuite with Matchers {

  // -- Per ADR-008-AC: the hook is a no-op; Context is preserved --

  test("MaterializeStub: hook run is a no-op (Context returned unchanged)") {
    val plugin = new MaterializeStub(PersistLevel.MemoryAndDisk)
    val engine = EngineImpl()
    engine.use(plugin)
    val inputContext = Context(
      stage   = PipelineStage.Parse,
      request = MaterializeConformanceRequest,
      result  = Some(MaterializeConformanceResult),
      meta    = Map.empty,
      stop    = false
    )
    val hook = engine.hooks.preHooksFor(HookStage.PreExecute).head._1
    val outputContext = hook.run(inputContext)
    outputContext shouldBe inputContext
  }

  test("MaterializeStub: hook run increments the fires counter") {
    val plugin = new MaterializeStub(PersistLevel.MemoryAndDisk)
    val engine = EngineImpl()
    engine.use(plugin)
    val hook = engine.hooks.preHooksFor(HookStage.PreExecute).head._1
    val before = plugin.fires.get
    hook.run(Context(
      stage   = PipelineStage.Parse,
      request = MaterializeConformanceRequest,
      result  = Some(MaterializeConformanceResult),
      meta    = Map.empty,
      stop    = false
    ))
    plugin.fires.get shouldBe (before + 1)
  }
}
