/*
 * SM8 materialize Plugin — test.
 *
 * Updated for PR #36's lifecycle contract: the plugin now takes a
 * PersistLevel (engine-portable marker) constructor arg, and
 * registers BOTH the PreExecute persist + PostExecute unpersist
 * hooks. The pair is the  mantra #3
 * contract - a regression that registers only one half breaks the
 * materialize lifecycle.
 */
package io.sm8.plugins.materialize

import io.sm8.core.EngineImpl
import io.sm8.sdk.HookStage

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MaterializeStubSpec extends AnyFlatSpec with Matchers {

  "MaterializeStub.setup" should "register the lifecycle pair (PreExecute persist + PostExecute unpersist)" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new MaterializeStub(PersistLevel.MemoryAndDisk)
    engine.use(plugin)

    val preHooks  = engine.hooks.preHooksFor(HookStage.PreExecute)
    val postHooks = engine.hooks.postHooksFor(HookStage.PostExecute)

    preHooks.map(_._1.name) shouldBe List("materialize-pre-stub")
    postHooks.map(_._1.name) shouldBe List("materialize-post-stub")
  }

  it should "fire twice per engine.run (counter increments on each fire)" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new MaterializeStub(PersistLevel.MemoryAndDisk)
    engine.use(plugin)

    engine.run(new io.sm8.sdk.Request {})
    // Both the PreExecute persist hook AND the PostExecute unpersist
    // hook fire on each engine.run - that's the lifecycle pair.
    plugin.fires.get() shouldBe 2
  }
}
