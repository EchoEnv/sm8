/*
 * SM8 Spark Connector — P2 cluster regression test for site 2.
 *
 * PR-176 NonFatal discipline by topic (PR-176 was the 8 P1 fix wave
 * that established `case NonFatal(e) => ...` at IO boundaries; `Error`
 * subclasses propagate). The `realizeTyped` path pre-P2 had a
 * `case e: Throwable` catch — broader than NonFatal — that would
 * silently swallow JVM `Error` subclasses into a typed
 * `EngineError.ConnectionFailed` instead of letting them propagate.
 *
 * P2 cluster narrows the catch to `NonFatal(e)` + adds an explicit
 * `AnalysisException` branch (preserves the cause class in the
 * `message` string). The narrow discipline mirrors
 * `MinimalRelOpLowerer.scala:194-200` (the canonical narrowing
 * example for spark-connector IO boundaries).
 *
 * This spec asserts:
 *  1. A non-fatal `AnalysisException` from the Spark factory returns
 *     `Left(ConnectionFailed)` with a message that identifies the
 *     cause class (the operator-diagnostic value of the branch).
 *  2. A fatal `Error` (here: `OutOfMemoryError`) from the Spark
 *     factory ESCAPES the catch — proving the narrowing discipline
 *     holds at this site. Without NonFatal, the Throwable catch
 *     would have silently typed the OOM as `ConnectionFailed`.
 *
 * The exception is injected via the `protected[spark]` seam
 * (`newSparkSession`) — production behavior is unchanged
 * (`SparkSession.builder().master(url).getOrCreate()`).
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineError, EngineUrl}

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkEngineProviderDescriptorRealizeTypedSpec extends AnyFunSuite with Matchers {

  /** Test-only descriptor that injects a controlled SparkSession
    * factory. Production behavior is `SparkSession.builder().master(url).getOrCreate()`. */
  private final class SeamedDescriptor(
      sessionFactory: String => SparkSession)
    extends SparkEngineProviderDescriptor {
    override protected[spark] def newSparkSession(master: String): SparkSession =
      sessionFactory(master)
  }

  test("realizeTyped: NonFatal failure -> Left(ConnectionFailed) with cause-class in message") {
    // The post-P2 shape narrows the catch to `NonFatal(e)` so a JVM
    // Error subclass would propagate, but a plain NonFatal (RuntimeException)
    // is still typed as `Left(ConnectionFailed)` with the cause-class in
    // the message — operators can diagnose root cause without an attached
    // exception. The narrow discipline mirrors `MinimalRelOpLowerer.scala:194-200`.
    val descriptor = new SeamedDescriptor(_ =>
      throw new RuntimeException("sm8-test: simulated SparkSession.builder().getOrCreate() failure"))
    val out = descriptor.realizeTyped(EngineUrl.Spark(master = "local[1]"))
    out.isLeft shouldBe true
    val err = out.left.toOption.get
    err shouldBe a [EngineError.ConnectionFailed]
    err.message should include ("RuntimeException")
    err.message should include ("sm8-test: simulated")
  }
  test("realizeTyped: Error subclass from the Spark factory PROPAGATES (PR-176 NonFatal discipline)") {
    // PR-176 NonFatal discipline (cite by topic): the catch is
    // narrowed to `NonFatal(e)`, so an `OutOfMemoryError` from the
    // Spark factory escapes to the caller — NOT silently typed
    // as `Left(ConnectionFailed)`. Before this fix, the
    // `case e: Throwable` would have swallowed OOM into a typed
    // error (the same failure mode `EngineHookDispatcher` had
    // before PR-176).
    val descriptor = new SeamedDescriptor(_ =>
      throw new OutOfMemoryError("sm8-test: simulated driver OOM during getOrCreate"))
    val thrown = intercept[OutOfMemoryError] {
      descriptor.realizeTyped(EngineUrl.Spark(master = "local[1]"))
    }
    thrown.getMessage should include ("simulated driver OOM")
  }
}
