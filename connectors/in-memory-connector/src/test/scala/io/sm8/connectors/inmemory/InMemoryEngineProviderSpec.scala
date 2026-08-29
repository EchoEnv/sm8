/*
 * SM8 in-memory Connector — engine-provider parity spec (PR-B).
 *
 * PR-200 (audit follow-up M4): extends the falsifiable spec coverage
 * for `decideUnsupported` — the existing 3 tests passed `null` for
 * `ctx`, which means the honor-or-reject branch was never exercised
 * by the spec (only by `InMemoryEngineReplaySafetySpec`). The
 * additions below route a real `EngineContext(decisionHints = …)`
 * through `query(...)` and assert that the adapter returns the
 * typed `UnsupportedCapability` named by the platform meta key —
 * proving the per-field decision (broadcastArmed,
 * broadcastThresholdBytes, skewArmed) is honored-or-rejected at the
 * adapter boundary. Per `scala-bug-hunting` §1, a falsifiable spec
 * must exercise the branch under test; passing `null` made the
 * test tautological.
 */
package io.sm8.connectors.inmemory

import io.sm8.core.engine.{DecisionHints, EngineContext, EngineError, PortableQueryResult, QueryRequest}
import io.sm8.core.model.{Model, SourceRef}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class InMemoryEngineProviderSpec extends AnyFunSuite with Matchers {

  test("always available (reference engine on a bare classpath)") {
    val p = new InMemoryEngineProvider()
    p.available shouldBe true
    p.identity.name shouldBe "in-memory"
  }

  test("realize(any url) → None (in-memory has no URL grammar; already realized)") {
    val p = new InMemoryEngineProvider()
    p.realize("anything") shouldBe None
    p.realize("local[*]") shouldBe None
  }

  test("query returns an empty PortableQueryResult with engine marker") {
    val p = new InMemoryEngineProvider()
    val out = p.query(null, null, null)
    out.isRight shouldBe true
    out.toOption.get.metadata("engine") shouldBe "in-memory"
  }

  // ---------------------------------------------------------------------------
  // PR-200 (audit follow-up M4): falsifiable coverage for
  // `decideUnsupported` (ADR-009-d item 13 honor-or-reject rule).
  //
  // The original spec passed `null` for ctx, which short-circuited
  // `decideUnsupported` to `None` regardless of the decision. The
  // tests below route a real EngineContext with each DecisionHints
  // field set through `query(...)` and assert the typed
  // UnsupportedCapability surfaces with the platform meta-key.
  // Deterministic order matches the implementation:
  // broadcastArmed → broadcastThresholdBytes → skewArmed.
  // ---------------------------------------------------------------------------

  private def emptyModel: Model = Model.of(
    name    = "test-model",
    version = 1,
    source  = SourceRef.byName("in-memory", "test"),
  ).toOption.get

  private def queryWithDecision(dh: DecisionHints): Either[EngineError, PortableQueryResult] =
    new InMemoryEngineProvider().query(
      model   = emptyModel,
      request = QueryRequest(model = "test-model"),
      ctx     = EngineContext.defaultContext.copy(decisionHints = Some(dh))
    )

  test("query with DecisionHints(broadcastArmed = Some(true)) returns typed UnsupportedCapability(\"sm8.broadcast.arm\")") {
    // Falsifiable: if `decideUnsupported` silently dropped a decided
    // field, this test would return the empty PortableQueryResult
    // (Right with metadata = "in-memory"), NOT a typed error.
    val out = queryWithDecision(DecisionHints(broadcastArmed = Some(true)))
    out.isLeft shouldBe true
    val err = out.swap.toOption.get
    err shouldBe a[EngineError.UnsupportedCapability]
    err.engine shouldBe "in-memory-connector"
    err.asInstanceOf[EngineError.UnsupportedCapability].capability shouldBe "sm8.broadcast.arm"
  }

  test("query with DecisionHints(broadcastThresholdBytes = Some(...)) returns typed UnsupportedCapability(\"sm8.broadcast.thresholdBytes\")") {
    // Falsifiable: only fires when broadcastArmed is None. Order in
    // the implementation is broadcastArmed → broadcastThresholdBytes
    // → skewArmed.
    val out = queryWithDecision(DecisionHints(broadcastThresholdBytes = Some(10L * 1024L * 1024L)))
    out.isLeft shouldBe true
    out.swap.toOption.get
      .asInstanceOf[EngineError.UnsupportedCapability]
      .capability shouldBe "sm8.broadcast.thresholdBytes"
  }

  test("query with DecisionHints(skewArmed = Some(true)) returns typed UnsupportedCapability(\"sm8.skew.arm\")") {
    // Falsifiable: only fires when broadcastArmed AND
    // broadcastThresholdBytes are None (last in the order).
    val out = queryWithDecision(DecisionHints(skewArmed = Some(true)))
    out.isLeft shouldBe true
    out.swap.toOption.get
      .asInstanceOf[EngineError.UnsupportedCapability]
      .capability shouldBe "sm8.skew.arm"
  }

  test("query with all DecisionHints None (default fold) returns empty PortableQueryResult") {
    // Falsifiable: the no-oracle path must yield the prior
    // empty-success behavior — an adapter that returned a typed
    // error on the empty fold would break every bare-deploy path.
    val out = queryWithDecision(DecisionHints())
    out.isRight shouldBe true
    out.toOption.get.metadata("engine") shouldBe "in-memory"
  }

  test("query with decisionHints = None (no plugin registered) returns empty PortableQueryResult") {
    // Falsifiable: legacy / bare-deploy paths bypass the oracle
    // fold entirely. The adapter must accept ctx.decisionHints = None
    // as "no oracle registered" — distinct from "oracle armed".
    val out = new InMemoryEngineProvider().query(
      model   = emptyModel,
      request = QueryRequest(model = "test-model"),
      ctx     = EngineContext.defaultContext.copy(decisionHints = None)
    )
    out.isRight shouldBe true
    out.toOption.get.metadata("engine") shouldBe "in-memory"
  }
}
