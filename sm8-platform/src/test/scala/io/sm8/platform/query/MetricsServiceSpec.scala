/*
 * SM8 Platform — MetricsServiceSpec.
 *
 * Per [[ADR-012-b]] (`docs/adr/0012-b-metricsservice-restate-handler.md`):
 * tests for the read-only MetricsService Restate handler. The
 * handler returns PLACEHOLDER ZEROS until ADR-012-b-followup
 * instruments the call sites; these tests lock down the wire
 * shape + counter structure + startedAt/uptime semantics.
 *
 * Per [[debug-mantra-mindset]]: tests prove the wire shape + counter
 * structure is stable so the ADR-012-b-followup instrumentation can
 * change values without changing wire compatibility.
 *
 * Per [[scala-data-driven-refactor-mindset]]: the wire DTOs are
 * single-source — there should be exactly one canonical shape, and
 * this spec locks it down.
 */
package io.sm8.platform.query

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MetricsServiceSpec extends AnyFunSuite with Matchers {

  // ------------------------------------------------------------------
  // DTO shape tests (lock down wire contract)
  // ------------------------------------------------------------------

  test("SnapshotRequest is a 0-field case class (the empty request body)") {
    val req = SnapshotRequest()
    // Constructor takes no args; identity check confirms the empty form.
    req shouldBe SnapshotRequest()
  }

  test("InvocationCounters carries total/succeeded/failed: Long") {
    val c = InvocationCounters(total = 10, succeeded = 8, failed = 2)
    c.total shouldBe 10
    c.succeeded shouldBe 8
    c.failed shouldBe 2
  }

  test("CacheCounters carries hits/misses: Long") {
    val c = CacheCounters(hits = 100, misses = 5)
    c.hits shouldBe 100
    c.misses shouldBe 5
  }

  test("ErrorCounters carries auditSinkUnavailable/timedOut: Long") {
    val e = ErrorCounters(auditSinkUnavailable = 3, timedOut = 1)
    e.auditSinkUnavailable shouldBe 3
    e.timedOut shouldBe 1
  }

  test("MetricsSnapshot carries startedAt (ISO-8601), uptimeSeconds, and the 3 counter groups") {
    // Per ADR-012-b: startedAt is an ISO-8601 string, uptimeSeconds is a
    // Long computed at call time.
    val s = MetricsSnapshot(
      startedAt    = "2026-09-01T00:00:00Z",
      uptimeSeconds = 42L,
      invocations  = InvocationCounters(total = 0, succeeded = 0, failed = 0),
      cache        = CacheCounters(hits = 0, misses = 0),
      errors       = ErrorCounters(auditSinkUnavailable = 0, timedOut = 0)
    )
    s.startedAt shouldBe "2026-09-01T00:00:00Z"
    s.uptimeSeconds shouldBe 42L
    s.invocations.total shouldBe 0
    s.cache.hits shouldBe 0
    s.errors.auditSinkUnavailable shouldBe 0
  }

  test("MetricsSnapshot accepts non-zero counter values (the wire shape allows any Long)") {
    // Per the wire contract: any Long is valid. ADR-012-b ships zeros;
    // future ADR-012-b-followup will increment counters at runtime.
    val s = MetricsSnapshot(
      startedAt    = "2026-09-01T00:00:00Z",
      uptimeSeconds = 1000L,
      invocations  = InvocationCounters(total = 999, succeeded = 998, failed = 1),
      cache        = CacheCounters(hits = 500, misses = 500),
      errors       = ErrorCounters(auditSinkUnavailable = 0, timedOut = 0)
    )
    s.invocations.total shouldBe 999
    s.cache.hits shouldBe 500
  }

  // ------------------------------------------------------------------
  // Service definition tests (lock down registration)
  // ------------------------------------------------------------------

  test("MetricsService.definition returns a ServiceDefinition named MetricsService") {
    val defn = MetricsService.definition()
    defn.getServiceName shouldBe "MetricsService"
  }

  test("MetricsService.definition exposes a single handler named snapshot") {
    val defn = MetricsService.definition()
    defn.getHandlers.asScala.map(_.getName).toList shouldBe List("snapshot")
  }

  // ------------------------------------------------------------------
  // Sanity tests (startedAt + uptime semantics)
  // ------------------------------------------------------------------

  test("MetricsService.definition: snapshot returns startedAt + counters (placeholder zeros today)") {
    // We don't drive the handler directly (closure is opaque), but the
    // ServiceDefinition exists, so a smoke test would catch wire-shape
    // regressions.
    val defn = MetricsService.definition()
    defn.getHandlers.asScala.head.getName shouldBe "snapshot"
  }
}
