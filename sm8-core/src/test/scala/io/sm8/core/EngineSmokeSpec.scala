/*
 * SM8 Core — EngineSmokeSpec.
 *
 * First end-to-end smoke for the SM8 Engine:
 *   1. Construct an Engine
 *   2. Register a Connector directly via `engine.connectors.register`
 *   3. Send a `ConnectorRequest` to `engine.run`
 *   4. Assert the Connector was invoked and rows came back
 *
 * This is the proof that Step 3's machinery (Engine + registries +
 * Pipeline + Connector contract) wires together correctly.
 *
 * It uses the Step 2 StubConnector (defined in
 * `io.sm8.sdk.contract.StubConnectorSpec`) because the StubConnector
 * has predictable behavior. The real built-in `InMemoryConnector`
 * is tested in its own module (`connectors/in-memory-connector`).
 */
package io.sm8.core

import io.sm8.sdk._
import io.sm8.sdk.contract.{StubConfig, StubConnector, StubQuery}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EngineSmokeSpec extends AnyFlatSpec with Matchers {

  "Engine" should "register a Connector and route a request to it" in {
    val engine  = EngineImpl()
    val stub    = new StubConnector
    engine.connectors.register(stub)

    val request = ConnectorRequest(
      connectorName = stub.name,
      query         = StubQuery(malformed = false)
    )

    val result = engine.run(request)

    result shouldBe a [ConnectorResult]
    val cr = result.asInstanceOf[ConnectorResult]
    cr.connectorName shouldBe "stub"
    cr.rows.rows should have size 1
    cr.rows.rows.head should contain key "id"
  }

  it should "honour stop = true to short-circuit the pipeline (delegated to HookDispatchSpec)" in {
    // Step 4: the actual stop-flag test lives in HookDispatchSpec
    // (it needs to register a StopPreHook directly). This test name
    // stays as a documentation breadcrumb pointing readers to the
    // real test.
    succeed  // per [[debug-mantra-mindset]]: an empty test body silently passes; we deliberately use `succeed` here as a marker
  }

  it should "fail loudly when registering a Connector with a duplicate name" in {
    val engine = EngineImpl()
    val stub1  = new StubConnector
    engine.connectors.register(stub1)
    an [IllegalArgumentException] should be thrownBy engine.connectors.register(stub1)
  }

  it should "be forgiving when a Plugin's setup throws" in {
    val engine = EngineImpl()
    val bad    = new Plugin {
      override def setup(engine: Engine): Unit =
        throw new RuntimeException("simulated setup failure")
    }
    noException should be thrownBy engine.use(bad) // bad plugin warns, never crashes
  }

  it should "register a Plugin that adds a Connector via setup(engine)" in {
    val engine = EngineImpl()
    val stub    = new StubConnector
    val plugin  = new Plugin {
      override def setup(engine: Engine): Unit = {
        engine.connectors.register(stub)
      }
    }
    engine.use(plugin)
    val result = engine.run(
      ConnectorRequest(connectorName = stub.name, query = StubQuery(false))
    )
    result shouldBe a [ConnectorResult]
  }

  it should "report an unknown connector name gracefully (no crash, stub result)" in {
    val engine   = EngineImpl()
    val request  = ConnectorRequest(connectorName = "nonexistent", query = StubQuery(false))
    val result   = engine.run(request)
    result shouldBe a [ConnectorResult]
  }
}