/*
 * SM8 Spark Connector — P2 cluster regression test for site 1.
 *
 * PR-176 NonFatal discipline by topic (PR-176 was the 8 P1 fix wave
 * that established `case NonFatal(e) => ...` at IO boundaries; `Error`
 * subclasses propagate). The legacy `SparkEngineProviderDescriptor.realize`
 * path was a `case _: Throwable => None` swallow that pre-dated PR-176.
 * P2 cluster narrows it to `NonFatal(_) => None` so a JVM `Error` from
 * `SparkSession.builder().getOrCreate()` (e.g. `OutOfMemoryError` from
 * an over-allocated driver) propagates to the caller instead of being
 * silently converted to `None`.
 *
 * This spec asserts the narrowed behavior:
 *  1. A non-fatal `AnalysisException` from the Spark factory still
 *     returns `None` (the legacy contract — backwards-compat with PR-O4g
 *     callers that only inspect `Option[EngineProvider]`).
 *  2. A fatal `Error` (here: `OutOfMemoryError`) from the Spark factory
 *     ESCAPES the catch — proving the discipline-narrowing holds at this
 *     site. Without `NonFatal`, the `Throwable` catch would have swallowed
 *     OOM into `None` (the very failure mode PR-176 was introduced to
 *     close at `EngineImpl.scala:50` and `EngineService.scala:258-264`).
 *
 * The exception is injected via a `protected[spark]` seam
 * (`newSparkSession`) — production behavior is unchanged
 * (`SparkSession.builder().master(url).getOrCreate()`).
 */
package io.sm8.connectors.spark

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkEngineProviderDescriptorRealizeSpec extends AnyFunSuite with Matchers {

  /** Test-only descriptor that injects a controlled SparkSession
    * factory. Production behavior is `SparkSession.builder().master(url).getOrCreate()`. */
  private final class SeamedDescriptor(
      sessionFactory: String => SparkSession)
    extends SparkEngineProviderDescriptor {
    override protected[spark] def newSparkSession(master: String): SparkSession =
      sessionFactory(master)
  }

  test("realize: NonFatal AnalysisException from the Spark factory still returns None (legacy contract)") {
    // The pre-P2 shape was `case _: Throwable => None` — which
    // silently swallowed OOM/StackOverflow too. The narrowed shape
    // is `case NonFatal(_) => None`, so an `AnalysisException`
    // (the typical "bad master URL" fault) still returns `None`.
    // This is the backward-compat regression guard for the PR-O4g
    // callers that only inspect `Option[EngineProvider]`.
    val descriptor = new SeamedDescriptor(_ =>
      throw new org.apache.spark.sql.AnalysisException(
        "sm8-test: simulated SparkSession.builder().getOrCreate() failure",
        Map.empty[String, String]))
    descriptor.realize("local[1]") shouldBe None
  }

  test("realize: Error subclass from the Spark factory PROPAGATES (PR-176 NonFatal discipline)") {
    // PR-176 NonFatal discipline (cite by topic): the catch is
    // narrowed to `NonFatal(_)`, so an `OutOfMemoryError` from the
    // Spark factory escapes to the caller. Before this fix, the
    // `case _: Throwable => None` would have silently converted
    // OOM into `None` — a fatally-broken JVM producing a clean-looking
    // missing-provider result. This is the regression guard.
    val descriptor = new SeamedDescriptor(_ =>
      throw new OutOfMemoryError("sm8-test: simulated driver OOM during getOrCreate"))
    val thrown = intercept[OutOfMemoryError] {
      descriptor.realize("local[1]")
    }
    thrown.getMessage should include ("simulated driver OOM")
  }
}
