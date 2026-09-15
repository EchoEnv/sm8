/*
 * SM8 Platform — RollupFreshnessGauge spec (issue #425 PR 1).
 *
 * Verifies the /metrics freshness family end-to-end at the render
 * layer: gauge presence/absence by reader installation, per-rollup
 * rows, never-refreshed NaN rendering, malformed-timestamp
 * isolation, and probe-failure degradation.
 *
 * The reader is installed/cleared per test via
 * QueryMetrics.installRollupFreshnessReader (global state — each
 * test resets it in finally so test order cannot leak).
 */
package io.sm8.platform.query

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class RollupFreshnessGaugeSpec extends AnyFunSuite with Matchers {

  private val nowMs = Instant.parse("2026-09-15T12:00:00Z").toEpochMilli

  private def render(): String =
    MetricsHttpRoute.renderBody(Instant.ofEpochMilli(nowMs), Some(nowMs))

  private def withReader[A](entries: Either[String, List[RollupFreshnessSnapshot.Entry]])(f: => A): A = {
    QueryMetrics.installRollupFreshnessReader(() => entries)
    try f
    finally QueryMetrics.installRollupFreshnessReader(null)
  }

  test("no reader installed → freshness family absent from /metrics") {
    QueryMetrics.installRollupFreshnessReader(null)
    val body = render()
    body should not include ("sm8_rollup_freshness")
  }

  test("reader with one fresh rollup → three gauges, freshness=1, age computed") {
    withReader(Right(List(
      RollupFreshnessSnapshot.Entry("by_day", "2026-09-15T11:30:00Z", allFinal = true, bucketCount = 24L)
    ))) {
      val body = render()
      body should include ("""sm8_rollup_freshness{rollup="by_day"} 1""")
      body should include ("""sm8_rollup_freshness_age_seconds{rollup="by_day"} 1800""")
      body should include ("""sm8_rollup_freshness_buckets{rollup="by_day"} 24""")
    }
  }

  test("never-refreshed rollup (empty timestamp) → freshness=0, age=NaN") {
    withReader(Right(List(
      RollupFreshnessSnapshot.Entry("never", "", allFinal = false, bucketCount = 0L)
    ))) {
      val body = render()
      body should include ("""sm8_rollup_freshness{rollup="never"} 0""")
      body should include ("""sm8_rollup_freshness_age_seconds{rollup="never"} NaN""")
      body should include ("""sm8_rollup_freshness_buckets{rollup="never"} 0""")
    }
  }

  test("malformed timestamp on ONE rollup → NaN for that rollup, family intact") {
    withReader(Right(List(
      RollupFreshnessSnapshot.Entry("good", "2026-09-15T11:59:00Z", allFinal = true, bucketCount = 3L),
      RollupFreshnessSnapshot.Entry("bad", "not-a-timestamp", allFinal = false, bucketCount = 2L)
    ))) {
      val body = render()
      body should include ("""sm8_rollup_freshness{rollup="good"} 1""")
      body should include ("""sm8_rollup_freshness_age_seconds{rollup="good"} 60""")
      body should include ("""sm8_rollup_freshness_age_seconds{rollup="bad"} NaN""")
    }
  }

  test("probe failure → probe_failed family, not silence") {
    withReader(Left("watermark table unreadable")) {
      val body = render()
      body should include ("sm8_rollup_freshness_probe_failed 1")
    }
  }

  test("multiple rollups render as separate labeled rows") {
    withReader(Right(List(
      RollupFreshnessSnapshot.Entry("a", "2026-09-15T11:59:00Z", allFinal = true, bucketCount = 1L),
      RollupFreshnessSnapshot.Entry("b", "", allFinal = false, bucketCount = 7L)
    ))) {
      val body = render()
      body should include ("""{rollup="a"}""")
      body should include ("""{rollup="b"}""")
    }
  }
}
