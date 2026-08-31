/*
 * SM8 row-cap Plugin — test.
 */
package io.sm8.plugins.rowcap

import io.sm8.core.EngineImpl
import io.sm8.sdk.HookStage

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RowCapStubSpec extends AnyFlatSpec with Matchers {

  "RowCapStub.setup" should "register a single Post-hook at PostExecute" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new RowCapStub(RowCapConfig(maxRows = 100))
    engine.use(plugin)

    engine.hooks.postHooksFor(HookStage.PostExecute).map(_._1.name) shouldBe List("row-cap-stub")
  }

  it should "reject negative maxRows at the boundary" in {
    an [IllegalArgumentException] should be thrownBy RowCapConfig(maxRows = -1)
  }

  it should "fire once per engine.run" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new RowCapStub(RowCapConfig(maxRows = 100))
    engine.use(plugin)

    engine.run(new io.sm8.sdk.Request {})
    plugin.fires.get() shouldBe 1
  }
}