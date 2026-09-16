package io.sm8.cli

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Regression pin for monkey's HIGH: a NaN freshness/buckets sample
  * must degrade to 0, not crash the verb with NumberFormatException.
  */
class NaNStatusSpec extends AnyFunSuite with Matchers {
  test("NaN freshness/buckets degrade to 0, verb survives") {
    val body = """sm8_rollup_freshness{rollup="na"} NaN
sm8_rollup_freshness_age_seconds{rollup="na"} NaN
sm8_rollup_freshness_buckets{rollup="na"} NaN
sm8_rollup_rewrites_total 0"""
    val freshness = Main.parsePrometheusLabeled(body, "sm8_rollup_freshness")
    val buckets = Main.parsePrometheusLabeled(body, "sm8_rollup_freshness_buckets")
    // The wire value is the string "NaN"; the toLong would throw.
    freshness.get("na") shouldBe Some("NaN")
    noException should be thrownBy Main.safeLong(freshness.get("na"))
    Main.safeLong(freshness.get("na")) shouldBe 0L
    Main.safeLong(buckets.get("na")) shouldBe 0L
    Main.safeLong(None) shouldBe 0L
  }
}
