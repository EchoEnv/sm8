/*
 * SM8 cache Plugin — test.
 *
 * Per [[debug-mantra-mindset]]: assert real behavior — Plugin
 * registers the right hooks, hooks fire when engine.run executes.
 *
 * Per [[scala-impact-analysis-mindset]]: tests are the SDK
 * stability promise. The Plugin trait is frozen; we don't change
 * it here, only verify it.
 */
package io.sm8.plugins.cache

import io.sm8.core.{ConnectorRequest, EngineImpl}
import io.sm8.sdk.HookStage

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CachePluginSpec extends AnyFlatSpec with Matchers {

  "CachePlugin.setup" should "register one Pre-hook at PreExecute and one Post-hook at PostExecute" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new CachePlugin
    engine.use(plugin)

    engine.hooks.preHooksFor(HookStage.PreExecute).map(_._1.name) shouldBe List("cache-read")
    engine.hooks.postHooksFor(HookStage.PostExecute).map(_._1.name) shouldBe List("cache-write")
  }

  it should "fire both hooks once per engine.run" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new CachePlugin
    engine.use(plugin)

    // We need a real Connector for engine.run to reach the execute stage.
    // StubConnector from sm8-core's test-jar isn't on this module's
    // classpath (no test-jar dep). Use a minimal inline Connector
    // that satisfies the contract shape.
    val stub = new io.sm8.sdk.Connector {
      override def name: String = "stub"
      override def connect(config: io.sm8.sdk.ConnectorConfig): Unit = ()
      override def query(request: io.sm8.sdk.SemanticQuery): io.sm8.sdk.ResultRows =
        io.sm8.sdk.ResultRows(Vector.empty)
      override def schema(): io.sm8.sdk.ConnectorSchema =
        io.sm8.sdk.ConnectorSchema(Nil)
    }
    engine.connectors.register(stub)

    engine.run(ConnectorRequest("stub", new io.sm8.sdk.SemanticQuery {}))

    plugin.readFires.get() shouldBe 1
    plugin.writeFires.get() shouldBe 1
  }
}