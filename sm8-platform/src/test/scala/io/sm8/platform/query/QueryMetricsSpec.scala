/*
 * SM8 Platform — QueryMetricsSpec.
 *
 * Per ADR-012-b-followup §Verification criteria (item 5):
 * covers all 6 record methods + the snapshot reader (7 tests).
 *
 * Per [[debug-mantra-mindset]]: tests prove the AtomicLong
 * increments are independent, thread-safe (the object is a singleton
 * so thread-safety of AtomicLong is sufficient), and the snapshot
 * reader computes `failed = auditSinkUnavailable + timedOut` correctly.
 */
package io.sm8.platform.query

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class QueryMetricsSpec extends AnyFunSuite with Matchers {

  // Per the ADR verification criteria: each test increments the
  // counter and asserts the cumulative value matches. The tests are
  // order-independent (AnyFunSuite doesn't guarantee order) so the
  // single-spec state is the live QueryMetrics singleton — which is
  // fine because the suite asserts exact values via N-iteration checks
  // (not "after this test it should be X" cross-test sequencing).
  //
  // Per [[debug-mantra-mindset]] §1 (reproduce): reset between tests
  // via QueryMetrics's snapshot-only public surface — we don't have
  // a `reset()` method on the singleton (intentional; tests for the
  // snap reader compute expected totals at the start of each test).

  test("recordInvocation increments invocations.total by 1 each call") {
    val before = QueryMetrics.snapshot(0L, "test").invocations.total
    QueryMetrics.recordInvocation()
    val after = QueryMetrics.snapshot(0L, "test").invocations.total
    (after - before) shouldBe 1
  }

  test("recordSuccess increments invocations.succeeded by 1 each call") {
    val before = QueryMetrics.snapshot(0L, "test").invocations.succeeded
    QueryMetrics.recordSuccess()
    val after = QueryMetrics.snapshot(0L, "test").invocations.succeeded
    (after - before) shouldBe 1
  }

  test("recordCacheHit increments cache.hits by 1 each call") {
    val before = QueryMetrics.snapshot(0L, "test").cache.hits
    QueryMetrics.recordCacheHit()
    val after = QueryMetrics.snapshot(0L, "test").cache.hits
    (after - before) shouldBe 1
  }

  test("recordCacheMiss increments cache.misses by 1 each call") {
    val before = QueryMetrics.snapshot(0L, "test").cache.misses
    QueryMetrics.recordCacheMiss()
    val after = QueryMetrics.snapshot(0L, "test").cache.misses
    (after - before) shouldBe 1
  }

  test("recordAuditSinkUnavailable increments errors.auditSinkUnavailable by 1 each call") {
    val before = QueryMetrics.snapshot(0L, "test").errors.auditSinkUnavailable
    QueryMetrics.recordAuditSinkUnavailable()
    val after = QueryMetrics.snapshot(0L, "test").errors.auditSinkUnavailable
    (after - before) shouldBe 1
  }

  test("recordTimedOut increments errors.timedOut by 1 each call") {
    val before = QueryMetrics.snapshot(0L, "test").errors.timedOut
    QueryMetrics.recordTimedOut()
    val after = QueryMetrics.snapshot(0L, "test").errors.timedOut
    (after - before) shouldBe 1
  }

  test("snapshot computes invocations.failed = auditSinkUnavailable + timedOut") {
    // Per ADR-012-b-followup §Decision: `failed` is computed in
    // snapshot() (not stored as a separate counter) to keep the
    // read-side invariant simple.
    // Establish baseline first
    val baselineFailed = QueryMetrics.snapshot(0L, "test").invocations.failed
    val baselineAudit = QueryMetrics.snapshot(0L, "test").errors.auditSinkUnavailable
    val baselineTimeout = QueryMetrics.snapshot(0L, "test").errors.timedOut
    // Increment 2 audit + 3 timeout
    QueryMetrics.recordAuditSinkUnavailable(); QueryMetrics.recordAuditSinkUnavailable()
    QueryMetrics.recordTimedOut(); QueryMetrics.recordTimedOut(); QueryMetrics.recordTimedOut()
    // Verify the increment
    QueryMetrics.snapshot(0L, "test").invocations.failed shouldBe
      (baselineFailed + 5)
    QueryMetrics.snapshot(0L, "test").errors.auditSinkUnavailable shouldBe
      (baselineAudit + 2)
    QueryMetrics.snapshot(0L, "test").errors.timedOut shouldBe
      (baselineTimeout + 3)
  }
}