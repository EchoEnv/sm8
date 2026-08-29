/*
 * SM8 Core — DecisionHintsPolicy spec.
 *
 * PR-204 (refactor): falsifiable coverage for the extracted helper.
 * The pre-PR-204 behavior is locked in by the adapter specs
 * (InMemoryEngineProviderSpec + TrinoEngineProviderSpec), each of
 * which exercises the helper with the adapter-specific engine
 * field / display name. This spec covers the helper itself with
 * parameterized engine values so any future caller gets the same
 * contract verified directly (rather than only transitively
 * through an adapter).
 *
 * Falsifiability: every test would fail if
 * `DecisionHintsPolicy.honorOrReject` returned a different
 * UnsupportedCapability shape (different engine / capability /
 * message) than the spec asserts.
 */
package io.sm8.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DecisionHintsPolicySpec extends AnyFunSuite with Matchers {

  // ---------------------------------------------------------------------------
  // firstDecidedCapabilityKey — deterministic ordering
  // (broadcastArmed → broadcastThresholdBytes → skewArmed)
  // ---------------------------------------------------------------------------

  test("firstDecidedCapabilityKey with all None fields returns None") {
    DecisionHintsPolicy.firstDecidedCapabilityKey(DecisionHints()) shouldBe None
  }

  test("firstDecidedCapabilityKey with broadcastArmed alone returns sm8.broadcast.arm") {
    DecisionHintsPolicy.firstDecidedCapabilityKey(
      DecisionHints(broadcastArmed = Some(true))) shouldBe Some("sm8.broadcast.arm")
  }

  test("firstDecidedCapabilityKey with broadcastArmed takes precedence over thresholdBytes and skewArmed") {
    DecisionHintsPolicy.firstDecidedCapabilityKey(DecisionHints(
      broadcastArmed           = Some(true),
      broadcastThresholdBytes  = Some(1024L),
      skewArmed                = Some(true)
    )) shouldBe Some("sm8.broadcast.arm")
  }

  test("firstDecidedCapabilityKey with thresholdBytes alone returns sm8.broadcast.thresholdBytes") {
    DecisionHintsPolicy.firstDecidedCapabilityKey(
      DecisionHints(broadcastThresholdBytes = Some(10L * 1024L * 1024L))) shouldBe
      Some("sm8.broadcast.thresholdBytes")
  }

  test("firstDecidedCapabilityKey with thresholdBytes (no broadcastArmed) takes precedence over skewArmed") {
    DecisionHintsPolicy.firstDecidedCapabilityKey(DecisionHints(
      broadcastThresholdBytes = Some(1024L),
      skewArmed              = Some(true)
    )) shouldBe Some("sm8.broadcast.thresholdBytes")
  }

  test("firstDecidedCapabilityKey with skewArmed alone returns sm8.skew.arm") {
    DecisionHintsPolicy.firstDecidedCapabilityKey(
      DecisionHints(skewArmed = Some(true))) shouldBe Some("sm8.skew.arm")
  }

  test("firstDecidedCapabilityKey isDefined semantics — Some(false) is a real decision") {
    // Per DecisionHints.scala:8, Some(false) is "oracle disarmed" — a
    // real plugin decision, not a no-op. The pre-PR-204 implementation
    // used `.isDefined` (matches BOTH Some(true) AND Some(false)).
    // This test LOCKS IN that behavior so any future change to
    // `.contains(true)` surfaces here as a test diff, not a silent
    // semantic shift. PR-200 review MEDIUM-2 tracked the
    // `.contains(true)` alternative as a separate follow-up.
    DecisionHintsPolicy.firstDecidedCapabilityKey(
      DecisionHints(broadcastArmed = Some(false))) shouldBe Some("sm8.broadcast.arm")
  }

  // ---------------------------------------------------------------------------
  // honorOrReject — ctx / decisionHints / engine field / message contract
  // ---------------------------------------------------------------------------

  test("honorOrReject with null ctx returns None (preserves pre-refactor null short-circuit)") {
    DecisionHintsPolicy.honorOrReject(null, "in-memory-connector", "in-memory engine") shouldBe None
    DecisionHintsPolicy.honorOrReject(null, "trino-connector", "trino engine") shouldBe None
  }

  test("honorOrReject with decisionHints = None returns None") {
    val ctx = EngineContext.defaultContext.copy(decisionHints = None)
    DecisionHintsPolicy.honorOrReject(ctx, "in-memory-connector", "in-memory engine") shouldBe None
  }

  test("honorOrReject with all DecisionHints None returns None (empty fold)") {
    val ctx = EngineContext.defaultContext.copy(decisionHints = Some(DecisionHints()))
    DecisionHintsPolicy.honorOrReject(ctx, "in-memory-connector", "in-memory engine") shouldBe None
  }

  test("honorOrReject builds engine-specific UnsupportedCapability (in-memory flavor)") {
    val ctx = EngineContext.defaultContext.copy(
      decisionHints = Some(DecisionHints(broadcastArmed = Some(true))))
    val out = DecisionHintsPolicy.honorOrReject(ctx, "in-memory-connector", "in-memory engine")
    out.isDefined shouldBe true
    out.get match {
      case uc: EngineError.UnsupportedCapability =>
        uc.engine shouldBe "in-memory-connector"
        uc.capability shouldBe "sm8.broadcast.arm"
        uc.message should include("in-memory engine cannot honor decided field 'sm8.broadcast.arm'")
        uc.message should include("route to an engine with a native broadcast/skew config")
      case other =>
        fail(s"expected UnsupportedCapability, got ${other.getClass.getSimpleName}: $other")
    }
  }

  test("honorOrReject builds engine-specific UnsupportedCapability (trino flavor)") {
    // Same decision shape, different engine parameters → different
    // engine field + message body. The deterministic key and shared
    // message suffix remain identical.
    val ctx = EngineContext.defaultContext.copy(
      decisionHints = Some(DecisionHints(broadcastArmed = Some(true))))
    val out = DecisionHintsPolicy.honorOrReject(ctx, "trino-connector", "trino engine")
    out.isDefined shouldBe true
    out.get match {
      case uc: EngineError.UnsupportedCapability =>
        uc.engine shouldBe "trino-connector"
        uc.capability shouldBe "sm8.broadcast.arm"
        uc.message should include("trino engine cannot honor decided field 'sm8.broadcast.arm'")
        uc.message should include("route to an engine with a native broadcast/skew config")
      case other =>
        fail(s"expected UnsupportedCapability, got ${other.getClass.getSimpleName}: $other")
    }
  }

  test("honorOrReject with all three fields decided picks broadcastArmed (deterministic order)") {
    // Locks in the platform contract: even when all three fields are
    // decided, the helper reports the broadcastArmed key. A
    // future change that, say, reports the skewArmed key here would
    // surface as a test diff.
    val ctx = EngineContext.defaultContext.copy(decisionHints = Some(DecisionHints(
      broadcastArmed           = Some(true),
      broadcastThresholdBytes  = Some(1024L),
      skewArmed                = Some(true)
    )))
    DecisionHintsPolicy.honorOrReject(ctx, "in-memory-connector", "in-memory engine") match {
      case Some(uc: EngineError.UnsupportedCapability) =>
        uc.capability shouldBe "sm8.broadcast.arm"
      case other =>
        fail(s"expected Some(UnsupportedCapability(sm8.broadcast.arm)), got $other")
    }
  }
}
