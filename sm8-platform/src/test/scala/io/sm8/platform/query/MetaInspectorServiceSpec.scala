/*
 * SM8 Platform — MetaInspectorServiceSpec (PR-152, ADR-008-AI follow-up).
 *
 * Per the architect's 2026-08-23 design review
 * (`docs/review/graph-display-design-review.md`): the transport
 * lib's `MetaInspectorService` is a GENERIC `context.meta` reader.
 * The wire DTOs (`MetaRequest` + `MetaResponse`) carry no
 * plugin-specific knowledge. This spec exercises the wire shape
 * and the typed projection (`value: Map[String, Any]`).
 *
 * Per  SS1 (smallest correct
 * test footprint): the engineFn is stubbed to return a fixed
 * `Map[String, Any]`. No Restate runtime, no Docker, no
 * Testcontainers — same pattern as `QueryServiceSpec`.
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

  private val emptyRegistry: EngineRegistry = EngineRegistry(Map.empty, "default")

  // -- Tests --

  test("MetaInspectorService.definition returns a ServiceDefinition with name MetaInspectorService") {
    val defn =
      MetaInspectorService.definition(model, emptyRegistry, () => Map.empty)
    defn.getServiceName shouldBe "MetaInspectorService"
  }

  test("MetaInspectorService.definition exposes a single handler named getMeta") {
    val defn =
      MetaInspectorService.definition(model, emptyRegistry, () => Map.empty)
    defn.getHandlers.asScala.map(_.getName).toList shouldBe List("getMeta")
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
}