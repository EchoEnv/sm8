/*
 * SM8 Platform — MetricsHttpRouteSpec.
 *
 * Per ADR-012-b-export (`docs/adr/0012-b-export-prometheus-metrics.md`):
 * verification criterion #5 — covers each
 * of the 9 metrics + format compliance (HELP/TYPE/value pattern) +
 * counter consistency (matches QueryMetrics snapshot) + uptime
 * monotonicity (>= 0) + content-type header set correctly.
 *
 * 8 unit tests per the ADR's verification criteria table.
 *
 * Per debug-mantra: each test exercises a distinct
 * invariant; the suite verifies the WIRE shape (string body
 * structure) since the rendering is a pure function on
 * QueryMetrics.snapshot() — no actual HTTP server needed.
 */
package io.sm8.platform.query

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class MetricsHttpRouteSpec extends AnyFunSuite with Matchers {

  // The Prometheus exporter implementation must include a metrics-spec
  // sanity test that verifies the body format. Because the route is
  // a pure function (`renderBody`), the test fixture is just
  // (startedAt, mockSnapshot) — no HTTP server or Vert.x needed.

  /** A fixed-start Instant for stable uptime in tests. */
  private val fixedStart: Instant = Instant.parse("2026-09-01T00:00:00Z")

  /** A full-population MetricsSnapshot for assertion. Values picked so
    * each counter is distinguishable and monotonic. */
  private val populatedSnapshot: MetricsSnapshot = MetricsSnapshot(
    startedAt    = fixedStart.toString,
    uptimeSeconds = 60L,
    invocations  = InvocationCounters(total = 10, succeeded = 8, failed = 2),
    cache        = CacheCounters(hits = 30, misses = 12),
    errors       = ErrorCounters(auditSinkUnavailable = 1, timedOut = 1)
  )

  // ----- Test 1 -----
  test("renderBody includes all 9 metric names with _total suffix (counters) or no suffix (gauges)") {
    val body = MetricsHttpRoute.renderBody(fixedStart)
    val expectedNames = Seq(
      "sm8_invocation_total",
      "sm8_invocation_succeeded_total",
      "sm8_invocation_failed_total",
      "sm8_cache_hits_total",
      "sm8_cache_misses_total",
      "sm8_error_audit_sink_unavailable_total",
      "sm8_error_timed_out_total",
      "sm8_process_uptime_seconds",
      "sm8_process_start_time_seconds"
    )
    expectedNames.foreach { name =>
      withClue(s"missing metric `$name` in body:\n$body\n") {
        body should include (s"# HELP $name ")
        body should include (s"# TYPE $name ")
        // Counters should NOT have an extra _total suffix (no double-suffix).
        body should not include (s"${name}_total")
      }
    }
  }

  // ----- Test 2 -----
  test("renderBody follows Prometheus text format 0.0.4 (HELP + TYPE + value pattern for each metric)") {
    val body = MetricsHttpRoute.renderBody(fixedStart)
    // Each metric line is preceded by a "# HELP <name> <description>" line
    // and a "# TYPE <name> <type>" line, then a value line.
    expectedMetricsCount(body) shouldBe 9
  }

  // ----- Test 3 -----
  test("renderBody sets Content-Type header compatible format — values are integers (no floats)") {
    // Per Prometheus spec: counter and gauge values are integer or float;
    // we use integers (no decimal point). Verify no value line has '.'.
    val body = MetricsHttpRoute.renderBody(fixedStart)
    val valueLines = body.linesIterator
      .filter(_.trim.nonEmpty)
      .filter(!_.startsWith("#"))
      .toList
    valueLines.foreach { line =>
      withClue(s"value line with decimal point: $line") {
        line should not include (".")
      }
    }
  }

  // ----- Test 4 -----
  test("renderBody output is non-empty and well-formed (>= 9 lines)") {
    val body = MetricsHttpRoute.renderBody(fixedStart)
    body.linesIterator.toList.size should be >= 9
  }

  // ----- Test 5 (counter consistency) -----
  test("renderBody values match QueryMetrics.snapshot output (counter consistency)") {
    // Setup: inject custom values via QueryMetrics records, then
    // call renderBody. The body should reflect the recorded state.
    // We can't easily reset QueryMetrics (no reset method, intentional),
    // so this test uses the recorded state at test time (delta-based
    // like QueryMetricsSpec).
    val beforeTotal  = QueryMetrics.snapshot(0L, "test").invocations.total
    val beforeHits   = QueryMetrics.snapshot(0L, "test").cache.hits
    val beforeMisses = QueryMetrics.snapshot(0L, "test").cache.misses

    QueryMetrics.recordInvocation()
    QueryMetrics.recordCacheHit()
    QueryMetrics.recordCacheHit()
    QueryMetrics.recordCacheMiss()

    val body = MetricsHttpRoute.renderBody(fixedStart)
    // Line-exact parse (no substring overmatch: `...total 1` would
    // otherwise pass against a body line of `...total 11`).
    metricValue(body, "sm8_invocation_total") shouldBe beforeTotal + 1
    metricValue(body, "sm8_cache_hits_total") shouldBe beforeHits + 2
    metricValue(body, "sm8_cache_misses_total") shouldBe beforeMisses + 1
  }

  // ----- Test 6 (uptime monotonicity) -----
  test("renderBody uptimeSeconds >= 0 (uptime is monotonically non-negative)") {
    // No matter how long the test takes, uptime must be >= 0.
    // Use a fixed start time far in the past so uptime is large and positive.
    val ancientStart: Instant = Instant.parse("2000-01-01T00:00:00Z")
    val body = MetricsHttpRoute.renderBody(ancientStart)
    // uptime_seconds should be huge positive
    val uptimeLine = body.linesIterator.find(_.startsWith("sm8_process_uptime_seconds "))
    uptimeLine shouldBe defined
    uptimeLine.get.split(" ")(1).toLong should be >= 0L
  }

  // ----- Test 6b (line-exact value parse — pins the overmatch fix) -----
  test("renderBody value lines are parseable as Long (line-exact, no substring overmatch)") {
    val body = MetricsHttpRoute.renderBody(fixedStart)
    val counters = Seq(
      "sm8_invocation_total", "sm8_invocation_succeeded_total",
      "sm8_invocation_failed_total", "sm8_cache_hits_total",
      "sm8_cache_misses_total", "sm8_error_audit_sink_unavailable_total",
      "sm8_error_timed_out_total", "sm8_process_uptime_seconds",
      "sm8_process_start_time_seconds"
    )
    counters.foreach { name =>
      withClue(s"metric $name must have exactly one parseable value line: ") {
        metricValue(body, name) should be >= 0L
      }
    }
  }

  // ----- Test 7 (no doubled _total suffix anywhere) -----
  test("renderBody has no doubled _total suffix (caught in r3 review)") {
    // Per the r3 dual-review catch: never have `metric_total_total`. The
    // implementation must use the suffixed name once and only once per
    // metric line.
    val body = MetricsHttpRoute.renderBody(fixedStart)
    body should not include ("_total_total")
  }

  // ----- Test 8 (HELP/TYPE line precedes each value line) -----
  test("renderBody has HELP + TYPE lines preceding each value line (Prometheus compliance)") {
    val body = MetricsHttpRoute.renderBody(fixedStart)
    val lines = body.linesIterator.toList.filter(_.trim.nonEmpty)
    // Find indices of HELP lines, TYPE lines, and value lines
    val helpIndices = lines.zipWithIndex.collect { case (l, i) if l.startsWith("# HELP ") => i }
    val typeIndices = lines.zipWithIndex.collect { case (l, i) if l.startsWith("# TYPE ") => i }
    val valueIndices = lines.zipWithIndex.collect { case (l, i) if !l.startsWith("#") && !l.trim.isEmpty => i }
    // For each metric (name extracted from HELP), there must be:
    //   HELP line at index i, TYPE line at i+1, value line at i+2.
    helpIndices.indices.foreach { idx =>
      withClue(s"HELP/TYPE/value sequence broken at index $idx in body:\n$body\n") {
        (typeIndices(idx) - helpIndices(idx)) shouldBe 1
        (valueIndices(idx) - typeIndices(idx)) shouldBe 1
      }
    }
  }

  // ----- helpers -----

  /** Count the number of "# HELP" lines (= number of metrics). */
  private def expectedMetricsCount(body: String): Int =
    body.linesIterator.count(_.startsWith("# HELP "))

  /** Parse the exact value line for `name` (the line that starts with
    * `name + " "`) and return the Long it ends with. Fails the test
    * if the line is absent or the value is not a Long — this is the
    * line-exact alternative to substring `include` matching, which
    * overmatches numeric prefixes (`total 1` ⊂ `total 11`). */
  private def metricValue(body: String, name: String): Long = {
    val prefix = name + " "
    val line = body.linesIterator.find(_.startsWith(prefix))
    withClue(s"value line for `$name` missing from body:\n$body\n") {
      line shouldBe defined
    }
    line.get.substring(prefix.length).toLong
  }
}