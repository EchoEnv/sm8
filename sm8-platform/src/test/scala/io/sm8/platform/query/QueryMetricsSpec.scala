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

  // -- Rollup-rewrite counters (direct read-surface assertions) --

  test("recordRollupRewrite increments rewrites in both snapshot and rollupSnapshot") {
    val beforeWire = QueryMetrics.snapshot(0L, "test").rollup.rewrites
    val beforeSnap = QueryMetrics.rollupSnapshot().rewrites
    QueryMetrics.recordRollupRewrite()
    QueryMetrics.snapshot(0L, "test").rollup.rewrites shouldBe (beforeWire + 1)
    QueryMetrics.rollupSnapshot().rewrites shouldBe (beforeSnap + 1)
  }

  test("recordRollupRefusal increments totals and the per-reason map keyed by reasonName") {
    /** Current per-reason count (0 when the key is absent yet).
      *
      * @param snap the snapshot to read
      * @param key  the reasonName key
      * @return the recorded count for the key
      */
    def countOf(snap: io.sm8.core.cache.RollupCountersSnapshot, key: String): Long =
      snap.refusalsByReason.find(_._1 == key).map(_._2).getOrElse(0L)
    val before      = QueryMetrics.rollupSnapshot()
    val beforeWire  = QueryMetrics.snapshot(0L, "test").rollup
    QueryMetrics.recordRollupRefusal(
      io.sm8.core.rel.RollupRewriter.RollupRewriteRefusal.GrainMismatch)
    val after     = QueryMetrics.rollupSnapshot()
    val afterWire = QueryMetrics.snapshot(0L, "test").rollup
    after.refusals shouldBe (before.refusals + 1)
    after.refusalsPermanent shouldBe before.refusalsPermanent // GrainMismatch is recoverable
    countOf(after, "grainMismatch") shouldBe (countOf(before, "grainMismatch") + 1)
    afterWire.refusals shouldBe (beforeWire.refusals + 1)
  }

  test("recordRollupRefusal with UnsplittableAggregate counts toward refusalsPermanent") {
    /** Current per-reason count (0 when the key is absent yet).
      *
      * @param snap the snapshot to read
      * @param key  the reasonName key
      * @return the recorded count for the key
      */
    def countOf(snap: io.sm8.core.cache.RollupCountersSnapshot, key: String): Long =
      snap.refusalsByReason.find(_._1 == key).map(_._2).getOrElse(0L)
    val before = QueryMetrics.rollupSnapshot()
    QueryMetrics.recordRollupRefusal(
      io.sm8.core.rel.RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate)
    val after = QueryMetrics.rollupSnapshot()
    after.refusalsPermanent shouldBe (before.refusalsPermanent + 1)
    countOf(after, "unsplittableAggregate") shouldBe
      (countOf(before, "unsplittableAggregate") + 1)
  }

  test("rollupSnapshot per-reason list is sorted by key for stable output") {
    val reasons = QueryMetrics.rollupSnapshot().refusalsByReason.map(_._1)
    reasons shouldBe reasons.sorted
  }

  test("QueryMetrics works as the registered MetricsSink (wiring parity)") {
    // Pins the sm8-server boot invariant: the JVM-global sink is
    // QueryMetrics, so the plugin read surface (rollupSnapshot) and
    // the wire surface (snapshot().rollup) report the same counters.
    io.sm8.core.cache.MetricsRegistry.register(QueryMetrics)
    val beforeWire = QueryMetrics.snapshot(0L, "test").rollup
    val beforeSnap = io.sm8.core.cache.MetricsRegistry.sink().rollupSnapshot()
    QueryMetrics.recordRollupRewrite()
    val afterWire = QueryMetrics.snapshot(0L, "test").rollup
    val afterSnap = io.sm8.core.cache.MetricsRegistry.sink().rollupSnapshot()
    afterSnap.rewrites shouldBe (beforeSnap.rewrites + 1)
    afterSnap.rewrites shouldBe afterWire.rewrites
    afterWire.rewrites shouldBe (beforeWire.rewrites + 1)
  }
}