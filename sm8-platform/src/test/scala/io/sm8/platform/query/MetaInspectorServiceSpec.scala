/*
 * SM8 Platform — MetaInspectorServiceSpec.
 *
 * Exercises the wire shape of the transport layer's
 * `MetaInspectorService`: the service is a GENERIC `context.meta`
 * reader, so the spec is independent of any specific plugin's
 * value schema. The wire DTOs (`MetaRequest` + `MetaResponse`)
 * carry no plugin-specific knowledge; the plugin owns the
 * value schema, the transport only commits to "round-trip via
 * Jackson with `DefaultScalaModule`".
 */
package io.sm8.platform.query

import io.sm8.core.engine.EngineRegistry
import io.sm8.core.model._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class MetaInspectorServiceSpec extends AnyFunSuite with Matchers {

  // -- Fixtures --

  /** A simple Model fixture for the registration call. The
   * `MetaInspectorService.definition(...)` signature requires a
   * `Model` (for handler shape) but the model itself is not
   * exercised by the unit test (the engineFn is stubbed). */
  private val model: Model = Model
    .of(
      name = "x",
      version = 1,
      description = None,
      dimensions = List.empty,
      measures = List.empty,
      defaultPolicies = ModelPolicyDefaults(
        MaterializePolicy.None,
        CachePolicy.NoCache,
        AuditPolicy.NoAudit
      ),
      source = SourceRef.byName("in-memory", "x"),
      status = ModelStatus.Published,
      filters = List.empty,
      calculatedMeasures = List.empty,
      joins = List.empty
    )
    .toOption
    .get

  /** Minimal engine so `EngineRegistry(Map(dflt))` satisfies its
    * `require(default in engines)` invariant. The registry is only
    * ever passed to `MetaInspectorService.definition` (the handler
    * never selects an engine), so the stub is never invoked. */
  private final class UnusedStubEngine extends io.sm8.core.engine.EngineProvider {
    override val identity: io.sm8.core.engine.EngineIdentity =
      io.sm8.core.engine.EngineIdentity("default", "0.0.0", "0.0.0")
    override val available: Boolean = true
    override def query(
        model: io.sm8.core.model.Model,
        request: io.sm8.core.engine.QueryRequest,
        ctx: io.sm8.core.engine.EngineContext
    ): io.sm8.core.engine.EngineError Either io.sm8.core.engine.PortableQueryResult = ???
    override def explain(
        model: io.sm8.core.model.Model,
        request: io.sm8.core.engine.QueryRequest,
        ctx: io.sm8.core.engine.EngineContext
    ): io.sm8.core.engine.EngineError Either String = ???
  }

  private val emptyRegistry: EngineRegistry = EngineRegistry(Map("default" -> new UnusedStubEngine), "default")

  // -- Tests --

  test("MetaInspectorService.definition returns a ServiceDefinition with name MetaInspectorService") {
    val defn =
      MetaInspectorService.definition(model, emptyRegistry, () => Map.empty)
    defn.getServiceName shouldBe "MetaInspectorService"
  }

  test("MetaInspectorService.definition exposes both getMeta AND getMetaByPrefix handlers") {
    // Per [[ADR-010-a]] (PR-252 commit 2): MetaInspectorService gained
    // the `getMetaByPrefix` handler so callers can do batch introspection
    // (e.g. "show me all `sm8.cache.*` keys in one call").
    val defn =
      MetaInspectorService.definition(model, emptyRegistry, () => Map.empty)
    val handlerNames = defn.getHandlers.asScala.map(_.getName).toSet
    handlerNames shouldBe Set("getMeta", "getMetaByPrefix")
  }
  test("MetaRequest is a 1-field case class (only the key)") {
    val req = MetaRequest(key = "io.sm8.foo:bar")
    req.key shouldBe "io.sm8.foo:bar"
  }

  test("MetaResponse carries key, present, and Option[Map] value") {
    val resp = MetaResponse(
      key = "k",
      present = true,
      value = Some(Map("x" -> 1))
    )
    resp.key shouldBe "k"
    resp.present shouldBe true
    resp.value shouldBe Some(Map("x" -> 1))
  }

  test("MetaResponse for an absent key has present=false and value=None") {
    val resp = MetaResponse(key = "missing", present = false, value = None)
    resp.present shouldBe false
    resp.value shouldBe None
  }

  // ------------------------------------------------------------------
  // getMetaByPrefix handler tests (sm8-pr252 commit 2)
  // ------------------------------------------------------------------

  test("MetaByPrefixRequest is a 1-field case class (only the prefix)") {
    val req = MetaByPrefixRequest(prefix = "sm8.cache")
    req.prefix shouldBe "sm8.cache"
  }

  test("MetaEntry carries key + value (no 'present' field — prefix filter implies presence)") {
    val entry = MetaEntry(key = "sm8.cache.policy", value = Map("tier" -> "ReadOnly"))
    entry.key shouldBe "sm8.cache.policy"
    entry.value shouldBe Map("tier" -> "ReadOnly")
  }

  test("MetaByPrefixResponse carries prefix + count + entries") {
    val resp = MetaByPrefixResponse(
      prefix = "sm8.cache",
      count  = 2,
      entries = Seq(
        MetaEntry(key = "sm8.cache.policy", value = Map("tier" -> "ReadOnly")),
        MetaEntry(key = "sm8.cache.lastHit", value = Map("key" -> "k1"))
      )
    )
    resp.prefix shouldBe "sm8.cache"
    resp.count shouldBe 2
    resp.entries.size shouldBe 2
  }

  // ------------------------------------------------------------------
  // Direct tests of the extracted filterByPrefix helper. Per the spec
  // design rule: the handler closure delegates to filterByPrefix, so
  // testing the helper directly is the canonical way to assert the
  // filter+sort+wrap logic.
  // ------------------------------------------------------------------

  test("filterByPrefix returns only keys matching the prefix") {
    val meta = Map[String, Any](
      "sm8.cache.policy"   -> Map("tier" -> "ReadOnly"),
      "sm8.cache.lastHit"  -> Map("key" -> "k1"),
      "io.sm8.plugins.foo" -> "primitive-value",
      "sm8.query.foo"      -> 42
    )
    val out = MetaInspectorService.filterByPrefix(meta, "sm8.cache")
    out.size shouldBe 2
    out.map(_.key).toSet shouldBe Set("sm8.cache.policy", "sm8.cache.lastHit")
  }

  test("filterByPrefix returns keys in lexicographic order (stable)") {
    val meta = Map[String, Any](
      "z.last" -> 1,
      "a.first" -> 2,
      "m.middle" -> 3
    )
    val out = MetaInspectorService.filterByPrefix(meta, "")
    out.map(_.key) shouldBe Seq("a.first", "m.middle", "z.last")
  }

  test("filterByPrefix with empty prefix matches all keys") {
    val meta = Map[String, Any]("a" -> 1, "b" -> 2, "c" -> 3)
    MetaInspectorService.filterByPrefix(meta, "").size shouldBe 3
  }

  test("filterByPrefix with prefix that matches no keys returns empty Seq") {
    val meta = Map[String, Any]("sm8.cache.policy" -> Map.empty[String, Any])
    MetaInspectorService.filterByPrefix(meta, "io.sm9").size shouldBe 0
  }

  test("filterByPrefix wraps non-Map values as Map(\"value\" -> v) for wire uniformity") {
    val meta = Map[String, Any](
      "sm8.cache.policy"   -> Map("tier" -> "ReadOnly"),
      "sm8.cache.lastHit"  -> "k1",
      "sm8.cache.count"    -> 42L,
      "sm8.cache.enabled"  -> true
    )
    val out = MetaInspectorService.filterByPrefix(meta, "sm8.cache")
    out.size shouldBe 4
    out.find(_.key == "sm8.cache.policy").get.value shouldBe Map("tier" -> "ReadOnly")
    out.find(_.key == "sm8.cache.lastHit").get.value shouldBe Map("value" -> "k1")
    out.find(_.key == "sm8.cache.count").get.value shouldBe Map("value" -> 42L)
    out.find(_.key == "sm8.cache.enabled").get.value shouldBe Map("value" -> true)
  }
}