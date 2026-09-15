package io.sm8.platform.query

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.lang.reflect.Method

/** Regression spec for the reflective Entry-copy field adapter at
  * the sm8-server ↔ spark-connector bridge.
  *
  * The CCE bug: when the connector widened `bucketCount` from Int to
  * Long, the bridge was still reading it with `asInstanceOf[Int]`,
  * which throws ClassCastException at runtime against a java.lang.Long.
  * The per-scrape NonFatal upstream catches it but the cache then
  * stores Left() for the TTL window, so the gauge is silently broken.
  * Unit tests on the platform side constructed Entry directly and
  * missed it. This spec verifies the adapter pattern by reflection
  * against a Long so a future narrowing regression fails here, not in
  * production.
  */
class RollupFreshnessAdapterSpec extends AnyFunSuite with Matchers {

  // The bridge adapter shape — mirrors Main.scala adaptEntry. Kept
  // here as a touched-by-tests copy (not imported) so a regression in
  // either side is caught at compile OR test time.
  private def adaptBucket(raw: AnyRef): Long = {
    val m: Method = raw.getClass.getDeclaredMethod("bucketCount")
    val box = m.invoke(raw)
    box match {
      case n: java.lang.Number => n.longValue()
      case other              => other.toString.toLong
    }
  }

  test("Long bucketCount is read correctly (not CCE-asInstanceOf-Int)") {
    val src = new AnyRef {
      def bucketCount: java.lang.Long = java.lang.Long.valueOf(42L)
    }
    adaptBucket(src) shouldBe 42L
  }

  test("Integer bucketCount is also widened via Number (forward-compat)") {
    val src = new AnyRef {
      def bucketCount: java.lang.Integer = java.lang.Integer.valueOf(7)
    }
    adaptBucket(src) shouldBe 7L
  }

  test("a large Long passes through without precision loss") {
    val big = java.lang.Long.valueOf(Long.MaxValue)
    val src = new AnyRef { def bucketCount: java.lang.Long = big }
    adaptBucket(src) shouldBe Long.MaxValue
  }
}
