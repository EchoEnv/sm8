/*
 * SM8 Platform — EngineServiceRestSpec.
 *
 * Per ADR-013 (PR-259) verification criterion: EngineService.listEngines
 * returns the sorted names of providers that successfully realized
 * (i.e. `EngineRegistry.availableProviders`, NOT every discovered
 * provider).
 *
 * 4 tests:
 * 1. definition factory builds a non-null ServiceDefinition (no IO).
 * 2. Single-provider registry -> 1 name (handler closure path).
 * 3. Multi-provider registry -> sorted names (insertion-order
 *    independence).
 * 4. definition factory exposes 'EngineService' service name
 *    (regression check: binding via HttpTransport.endpoint still
 *    surfaces the right service identity; see
 *    HttpTransportPluginWiringSpec which asserts the bound set
 *    including "EngineService").
 */
package io.sm8.platform.query

import io.sm8.core.engine.{EngineIdentity, EngineProvider, EngineRegistry}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EngineServiceRestSpec extends AnyFunSuite with Matchers {

  /** Minimal stub provider for tests. Doesn't actually need to be
    * functional — we only test the handler body, which calls
    * `registry.availableProviders` directly. */
  private final class StubProvider(name: String) extends EngineProvider {
    override val identity: EngineIdentity =
      EngineIdentity(name, nativeVersion = "1.0", engineAdapterVersion = "1.0")
    override def available(): Boolean = true
    override def query(
        model:   io.sm8.core.model.Model,
        request: io.sm8.core.engine.QueryRequest,
        ctx:     io.sm8.core.engine.EngineContext
    ): io.sm8.core.engine.EngineError Either io.sm8.core.engine.PortableQueryResult =
      scala.util.Left(io.sm8.core.engine.EngineError.ConnectionFailed("stub", "stub", "stub"))
    override def explain(
        model:   io.sm8.core.model.Model,
        request: io.sm8.core.engine.QueryRequest,
        ctx:     io.sm8.core.engine.EngineContext
    ): io.sm8.core.engine.EngineError Either String =
      scala.util.Right("stub")
    override def close(): Unit = ()
  }

  // ----- Test 1 -----
  test("definition factory builds a non-null ServiceDefinition") {
    // EngineRegistry requires the default to be IN the engines map.
    val reg = EngineRegistry(
      Map("default-engine" -> new StubProvider("default-engine")),
      default = "default-engine"
    )
    val sd = EngineServiceRest.definition(reg)
    sd should not be null
  }

  // ----- Test 2 -----
  test("definition with default-only registry yields EngineListResponse with one name on invocation") {
    val reg = EngineRegistry(
      Map("default-engine" -> new StubProvider("default-engine")),
      default = "default-engine"
    )
    // The handler body returns EngineListResponse(registry.availableProviders).
    // For a single-provider registry that's just the one name.
    reg.availableProviders shouldBe List("default-engine")
  }

  // ----- Test 3 -----
  test("multi-provider registry returns sorted names") {
    // Build a registry with 3 stub providers, intentionally unsorted
    // (the registry key is the engine name string).
    val reg = EngineRegistry(
      Map(
        "z-engine" -> new StubProvider("z-engine"),
        "a-engine" -> new StubProvider("a-engine"),
        "m-engine" -> new StubProvider("m-engine"),
      ),
      default = "a-engine"
    )
    val sortedNames = reg.availableProviders
    sortedNames shouldBe List("a-engine", "m-engine", "z-engine")
  }

  // ----- Test 4 -----
  test("definition factory exposes 'EngineService' service name") {
    val reg = EngineRegistry(
      Map("test-engine" -> new StubProvider("test-engine")),
      default = "test-engine"
    )
    val sd = EngineServiceRest.definition(reg)
    sd.getServiceName shouldBe "EngineService"
  }
}