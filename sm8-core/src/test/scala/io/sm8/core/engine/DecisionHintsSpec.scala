/*
 * SM8 Core — DecisionHintsSpec.
 *
 * Per-query decision oracle (ADR-009-d v0.3): the typed value that
 * carries a plugin's PreExecute hook decision across the boundary
 * into the spark connector. Pure-data ADT tests; no engine wiring.
 */
package io.sm8.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DecisionHintsSpec extends AnyFunSuite with Matchers {

  test("DecisionHints default constructor: all fields None") {
    val h = DecisionHints()
    h.broadcastArmed shouldBe None
    h.skewArmed shouldBe None
    h.broadcastThresholdBytes shouldBe None
  }

  test("DecisionHints all-fields-set: round-trip equality") {
    val h = DecisionHints(
      broadcastArmed          = Some(true),
      skewArmed               = Some(false),
      broadcastThresholdBytes = Some(10L * 1024L * 1024L))
    h.broadcastArmed shouldBe Some(true)
    h.skewArmed shouldBe Some(false)
    h.broadcastThresholdBytes shouldBe Some(10485760L)
    // Case-class equality (data-driven; no behavior)
    val h2 = DecisionHints(Some(true), Some(false), Some(10485760L))
    h shouldBe h2
  }

  test("DecisionHints partial-set: independent fields carry through independently") {
    // A plugin may register only the broadcast decision; the
    // skew decision stays None (inline fallback fires).
    val h = DecisionHints(broadcastArmed = Some(true))
    h.broadcastArmed shouldBe Some(true)
    h.skewArmed shouldBe None
    h.broadcastThresholdBytes shouldBe None
  }
}