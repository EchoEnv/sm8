/*
 * SM8 Trino Connector — Replay-safety / determinism spec (PR-F per ADR-007).
 *
 * Per ADR-007 §PR-F: per-provider strategy. For Trino-stub, the
 * `query` implementation returns `Left(EngineError.FeatureDeferred(...))`
 * unconditionally. Determinism is trivially true — the same input
 * yields the same `Left` every time.
 *
 * Per [[scala-error-handling-mindset]]: the stub never silently
 * no-ops. Every call surfaces a typed error. Determinism +
 * error-propagation = the durable shape.
 *
 * Per [[scala-jvm-safety-mindset]]: no static / ThreadLocal state.
 */
package io.sm8.connectors.trino

import io.sm8.core.engine.{EngineContext, QueryRequest}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TrinoEngineProviderReplaySafetySpec extends AnyFunSuite with Matchers {

  test("FeatureDeferred is deterministic by construction (Restate-replay-safe)") {
    val provider = new TrinoEngineProvider()
    val request = QueryRequest(model = "test", limit = Some(100L))
    val ctx = EngineContext.defaultContext

    val a = provider.query(null, request, ctx)
    val b = provider.query(null, request, ctx)

    // Both calls return the same Left(FeatureDeferred(...)). Structural
    // equality on Either[EngineError, PortableQueryResult] holds because
    // the FeatureDeferred case class is the same data every time.
    a shouldBe b
    a.isLeft shouldBe true
  }

  test("determinism holds across many invocations (100 calls)") {
    val provider = new TrinoEngineProvider()
    val request = QueryRequest(model = "test")
    val ctx = EngineContext.defaultContext

    val first = provider.query(null, request, ctx)
    for (_ <- 1 to 100) {
      provider.query(null, request, ctx) shouldBe first
    }
  }

  test("explain is also deterministic (Restate-replay-safe on plan side)") {
    // Trino stub's explain uses s"trino plan for ${model.name} (stub)";
    // pass a real Model so it doesn't NPE on null dereference.
    import io.sm8.core.model.{Model, ModelPolicyDefaults, ModelStatus, SourceRef}
    val model = Model.of(
      name    = "test",
      version = 1,
      source  = SourceRef.ByName(table = "t"),
      status  = ModelStatus.Draft,
      defaultPolicies = ModelPolicyDefaults(
        io.sm8.core.model.MaterializePolicy.None,
        io.sm8.core.model.CachePolicy.NoCache,
        io.sm8.core.model.AuditPolicy.NoAudit),
      dimensions = Nil,
      measures   = Nil
    ).toOption.get

    val provider = new TrinoEngineProvider()
    val request = QueryRequest(model = "test")

    val a = provider.explain(model, request, null)
    val b = provider.explain(model, request, null)

    a shouldBe b
  }
}
