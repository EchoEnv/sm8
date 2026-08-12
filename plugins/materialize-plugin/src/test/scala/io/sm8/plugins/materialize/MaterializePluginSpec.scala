/*
 * SM8 materialize Plugin — test.
 */
package io.sm8.plugins.materialize

import io.sm8.core.{ConnectorRequest, EngineImpl}
import io.sm8.sdk.HookStage

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MaterializePluginSpec extends AnyFlatSpec with Matchers {

  "MaterializePlugin.setup" should "register a single Post-hook at PostExecute" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new MaterializePlugin
    engine.use(plugin)
    engine.hooks.postHooksFor(HookStage.PostExecute).map(_._1.name) shouldBe List("materialize")
  }

  it should "fire once per engine.run" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new MaterializePlugin
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