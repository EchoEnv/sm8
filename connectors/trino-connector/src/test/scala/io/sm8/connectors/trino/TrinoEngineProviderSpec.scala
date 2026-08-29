/*
 * SM8 Trino Connector — typed realize() spec (PR-B parity).
 *
 * PR-202 (audit follow-up Bundle A4): extends the falsifiable spec
 * coverage for `decideUnsupported` — the prior `query on stub` test
 * passed `null` for `ctx`, which means the honor-or-reject branch was
 * never exercised directly here. The only prior end-to-end coverage
 * was the cross-engine integration spec at
 * `sm8-platform/src/test/scala/io/sm8/platform/query/
 * CrossEngineDecisionHintsConsumptionSpec.scala:103-126`, which
 * exercises `broadcastArmed → UnsupportedCapability("sm8.broadcast.arm")`
 * via `EngineService.runQueryWithHooks` (NOT a direct `provider.query`
 * call). The additions below are the first DIRECT unit coverage of
 * `decideUnsupported` for the Trino adapter, mirroring
 * `InMemoryEngineProviderSpec`'s 6 tests 1:1 (same deterministic
 * order broadcastArmed → broadcastThresholdBytes → skewArmed; same
 * platform meta keys). Per `scala-bug-hunting` §1, a falsifiable spec
 * must exercise the branch under test; passing `null` made the
 * original test tautological.
 *
 * Per `scala-data-driven-refactor` §3, the duplicated decision-logic
 * between in-memory and trino is a known refactor target (extract a
 * shared `DecisionHintsPolicy` helper in sm8-core). It is intentionally
 * NOT refactored here: this PR is the smallest-correct-change that
 * closes the cross-engine inconsistency (Bundle A4). A follow-up
 * refactor PR can extract the shared helper once both adapters'
 * behavior is locked in by tests.
 */
package io.sm8.connectors.trino

import io.sm8.core.engine.{DecisionHints, EngineContext, EngineError, PortableQueryResult, QueryRequest}
import io.sm8.core.model.{Model, SourceRef}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TrinoEngineProviderSpec extends AnyFunSuite with Matchers {

  test("no-arg ctor: contract-gap stub (available = false)") {
    val p = new TrinoEngineProvider()
    p.available shouldBe false
    p.identity.name shouldBe "trino"
  }

  test("realize(jdbc:trino://...) → Some with available = true") {
    val realized = new TrinoEngineProvider().realize("jdbc:trino://host:8080/catalog")
    realized shouldBe defined
    realized.get.available shouldBe true
    realized.get.identity.name shouldBe "trino"
  }

  test("realize(non-trino URL) → None (per-connector grammar)") {
    new TrinoEngineProvider().realize("spark://host:7077") shouldBe None
    new TrinoEngineProvider().realize("local[*]") shouldBe None
    new TrinoEngineProvider().realize("") shouldBe None
    new TrinoEngineProvider().realize(null) shouldBe None
  }

  test("query on stub: typed FeatureDeferred error (never a silent no-op)") {
    val stub = new TrinoEngineProvider()
    stub.available shouldBe false
    // The stub itself never queries; the realized stub defers loudly.
    // This test passes `null` for `ctx` to lock in the pre-PR-202
    // behavior on the no-oracle fold (decideUnsupported returns None
    // for null ctx, so query falls through to FeatureDeferred).
    val realized = stub.realize("jdbc:trino://h:8080").get
    realized.query(null, null, null).isLeft shouldBe true
    realized.query(null, null, null).swap.toOption.get match {
      case fd: EngineError.FeatureDeferred =>
        fd.engine shouldBe "trino"
        fd.feature shouldBe "query"
      case other =>
        fail(s"expected FeatureDeferred, got ${other.getClass.getSimpleName}: $other")
    }
  }

  // ---------------------------------------------------------------------------
  // PR-202 (audit follow-up Bundle A4): falsifiable coverage for
  // `decideUnsupported` (ADR-009-d item 13 honor-or-reject rule).
  //
  // The original spec passed `null` for ctx, which short-circuited
  // `decideUnsupported` to `None` regardless of the decision. The
  // tests below route a real EngineContext with each DecisionHints
  // field set through `query(...)` and assert the typed
  // UnsupportedCapability surfaces with the platform meta-key BEFORE
  // the generic FeatureDeferred stub error. Deterministic order
  // matches the implementation: broadcastArmed →
  // broadcastThresholdBytes → skewArmed.
  // ---------------------------------------------------------------------------

  private def emptyModel: Model = Model.of(
    name    = "test-model",
    version = 1,
    source  = SourceRef.byName("trino", "test"),
  ).toOption.get

  private def queryWithDecision(dh: DecisionHints): Either[EngineError, PortableQueryResult] =
    new TrinoEngineProvider().query(
      model   = emptyModel,
      request = QueryRequest(model = "test-model"),
      ctx     = EngineContext.defaultContext.copy(decisionHints = Some(dh))
    )

  test("query with DecisionHints(broadcastArmed = Some(true)) returns typed UnsupportedCapability(\"sm8.broadcast.arm\")") {
    // Falsifiable: if `decideUnsupported` silently dropped a decided
    // field, this test would return the FeatureDeferred stub error
    // (Left with engine = "trino", feature = "query"), NOT a typed
    // UnsupportedCapability. Locks in that decided fields are surfaced
    // BEFORE the stub deferral.
    val out = queryWithDecision(DecisionHints(broadcastArmed = Some(true)))
    out.isLeft shouldBe true
    out.swap.toOption.get match {
      case uc: EngineError.UnsupportedCapability =>
        uc.engine shouldBe "trino-connector"
        uc.capability shouldBe "sm8.broadcast.arm"
      case other =>
        fail(s"expected UnsupportedCapability, got ${other.getClass.getSimpleName}: $other")
    }
  }

  test("query with DecisionHints(broadcastArmed = Some(false)) also surfaces UnsupportedCapability (locks in `isDefined` behavior)") {
    // PR-200 review MEDIUM-2 (cross-engine): the implementation uses
    // `dh.broadcastArmed.isDefined` (matches BOTH Some(true) AND
    // Some(false)). Per DecisionHints.scala:8 semantics, Some(false)
    // is "oracle disarmed" — arguably a no-op decision, not an
    // unsupported one. This test LOCKS IN the current behavior so
    // any future change to `.contains(true)` would surface here as
    // a test diff, not a silent semantic shift. Smallest-correct-
    // change per karpathy-guidelines — the implementation fix
    // (Option B) is tracked as a separate follow-up.
    val out = queryWithDecision(DecisionHints(broadcastArmed = Some(false)))
    out.isLeft shouldBe true
    out.swap.toOption.get match {
      case uc: EngineError.UnsupportedCapability =>
        uc.capability shouldBe "sm8.broadcast.arm"
      case other =>
        fail(s"expected UnsupportedCapability, got ${other.getClass.getSimpleName}: $other")
    }
  }

  test("query with DecisionHints(broadcastThresholdBytes = Some(...)) returns typed UnsupportedCapability(\"sm8.broadcast.thresholdBytes\")") {
    // Falsifiable: only fires when broadcastArmed is None. Order in
    // the implementation is broadcastArmed → broadcastThresholdBytes
    // → skewArmed.
    val out = queryWithDecision(DecisionHints(broadcastThresholdBytes = Some(10L * 1024L * 1024L)))
    out.isLeft shouldBe true
    out.swap.toOption.get match {
      case uc: EngineError.UnsupportedCapability =>
        uc.engine shouldBe "trino-connector"
        uc.capability shouldBe "sm8.broadcast.thresholdBytes"
      case other =>
        fail(s"expected UnsupportedCapability, got ${other.getClass.getSimpleName}: $other")
    }
  }

  test("query with DecisionHints(skewArmed = Some(true)) returns typed UnsupportedCapability(\"sm8.skew.arm\")") {
    // Falsifiable: only fires when broadcastArmed AND
    // broadcastThresholdBytes are None (last in the order).
    val out = queryWithDecision(DecisionHints(skewArmed = Some(true)))
    out.isLeft shouldBe true
    out.swap.toOption.get match {
      case uc: EngineError.UnsupportedCapability =>
        uc.engine shouldBe "trino-connector"
        uc.capability shouldBe "sm8.skew.arm"
      case other =>
        fail(s"expected UnsupportedCapability, got ${other.getClass.getSimpleName}: $other")
    }
  }

  test("query with all DecisionHints None (default fold) returns FeatureDeferred (no UnsupportedCapability surfaced)") {
    // Falsifiable: the no-oracle path must fall through to the
    // generic stub error — an adapter that returned a typed
    // UnsupportedCapability on the empty fold would break every
    // bare-deploy path that calls Trino without a broadcast/skew
    // plugin registered. Distinct from the in-memory case, which
    // returns the empty PortableQueryResult; Trino is a stub that
    // always defers.
    val out = queryWithDecision(DecisionHints())
    out.isLeft shouldBe true
    out.swap.toOption.get match {
      case fd: EngineError.FeatureDeferred =>
        fd.engine shouldBe "trino"
        fd.feature shouldBe "query"
      case other =>
        fail(s"expected FeatureDeferred, got ${other.getClass.getSimpleName}: $other")
    }
  }

  test("query with decisionHints = None (no plugin registered) returns FeatureDeferred") {
    // Falsifiable: legacy / bare-deploy paths bypass the oracle
    // fold entirely. The adapter must accept ctx.decisionHints = None
    // as "no oracle registered" — distinct from "oracle armed".
    // Trino's empty-oracle fallback is FeatureDeferred, not the
    // empty PortableQueryResult that in-memory returns.
    val out = new TrinoEngineProvider().query(
      model   = emptyModel,
      request = QueryRequest(model = "test-model"),
      ctx     = EngineContext.defaultContext.copy(decisionHints = None)
    )
    out.isLeft shouldBe true
    out.swap.toOption.get match {
      case fd: EngineError.FeatureDeferred =>
        fd.engine shouldBe "trino"
      case other =>
        fail(s"expected FeatureDeferred, got ${other.getClass.getSimpleName}: $other")
    }
  }
}
