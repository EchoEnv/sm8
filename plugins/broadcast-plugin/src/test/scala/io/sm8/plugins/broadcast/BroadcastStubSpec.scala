/*
 * SM8 broadcast Plugin — test.
 */
package io.sm8.plugins.broadcast

import io.sm8.core.{ConnectorRequest, EngineImpl}
import io.sm8.sdk.HookStage

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BroadcastStubSpec extends AnyFlatSpec with Matchers {

  "BroadcastStub.setup" should "register a single Pre-hook at PreExecute" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new BroadcastStub
    engine.use(plugin)
    engine.hooks.preHooksFor(HookStage.PreExecute).map(_._1.name) shouldBe List("broadcast-stub")
  }

  it should "fire once per engine.run" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new BroadcastStub
    engine.use(plugin)

    val stub = new io.sm8.sdk.Connector {
      override def name: String = "stub"
      override def connect(config: io.sm8.sdk.ConnectorConfig): Unit = ()
      override def query(request: io.sm8.sdk.SemanticQuery): io.sm8.sdk.ResultRows =
        io.sm8.sdk.ResultRows(Vector.empty)
      override def schema(): io.sm8.sdk.ConnectorSchema = io.sm8.sdk.ConnectorSchema(Nil)
    }
    engine.connectors.register(stub)

    engine.run(ConnectorRequest("stub", new io.sm8.sdk.SemanticQuery {}))
    plugin.fires.get() shouldBe 1
  }
}