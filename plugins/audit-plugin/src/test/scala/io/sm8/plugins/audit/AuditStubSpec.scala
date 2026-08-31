/*
 * SM8 audit Plugin — test.
 */
package io.sm8.plugins.audit

import io.sm8.core.EngineImpl
import io.sm8.sdk.HookStage

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AuditStubSpec extends AnyFlatSpec with Matchers {

  "AuditStub.setup" should "register a single Post-hook at PostFormat" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new AuditStub
    engine.use(plugin)

    engine.hooks.postHooksFor(HookStage.PostFormat).map(_._1.name) shouldBe List("audit-stub")
  }

  it should "fire once per engine.run" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new AuditStub
    engine.use(plugin)

    engine.run(new io.sm8.sdk.Request {})
    plugin.fires.get() shouldBe 1
  }
}