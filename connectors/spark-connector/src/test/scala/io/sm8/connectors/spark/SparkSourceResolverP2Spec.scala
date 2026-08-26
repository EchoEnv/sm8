/*
 * SM8 Spark Connector — P2 cluster regression tests for site 3
 * (SparkSourceResolver.resolveByName + resolveByPath).
 *
 * Pre-P2 shape: `catch { case _: Exception => None }` — would
 * silently swallow ANY exception (including `InterruptedException`,
 * `OutOfMemoryError`, spark RPC faults) into a clean-looking
 * `UnsupportedCapability(SourceRef.ByName.resolve)` typed Left.
 *
 * P2 narrows to `catch { case _: org.apache.spark.sql.AnalysisException => None }` —
 * the specific Spark exception raised when a table is not in the active
 * catalog (or a path is not parseable as the given format). The narrow
 * discipline mirrors `MinimalRelOpLowerer.scala:213-216` (the canonical
 * narrowing example for spark-connector IO boundaries). Other NonFatal
 * failures propagate to `EngineService.executeEngine:258-264`'s
 * `NonFatal -> ProviderInvocationFailed` typed conversion; `Error`
 * subclasses propagate to the caller per the PR-176 discipline.
 *
 * This spec asserts:
 *  1. An `AnalysisException` (real Spark: `spark.table("nonexistent")`)
 *     still returns `Left(UnsupportedCapability)` (regression guard
 *     for the existing test at `SparkSourceResolverSpec.scala:88`).
 *  2. An `InterruptedException` PROPAGATES — proving the narrowing
 *     discipline holds (the previous `case _: Exception` would have
 *     swallowed it silently into `UnsupportedCapability`).
 *
 * The `InterruptedException` is injected via the `protected[spark]`
 * seam `loadTableByName` — production behavior is unchanged
 * (`spark.table(name)`).
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineError, EngineIdentity}
import io.sm8.core.model.SourceRef

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkSourceResolverP2Spec extends AnyFunSuite with Matchers {

  private val identity: EngineIdentity = EngineIdentity(
    name = "sm8-test", nativeVersion = "3.5", engineAdapterVersion = "0.1.0",
  )

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("spark-source-resolver-p2-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

  /** Test-only resolver that injects a controlled `loadTableByName`
    * factory. Production behavior is `spark.table(name)`. */
  private final class SeamedResolver(
      spark: SparkSession,
      tableLoader: String => org.apache.spark.sql.DataFrame)
    extends SparkSourceResolver(spark) {
    override protected[spark] def loadTableByName(name: String): org.apache.spark.sql.DataFrame =
      tableLoader(name)
  }

  test("resolveByName: AnalysisException -> Left(UnsupportedCapability) (narrowed-catch contract)") {
    // Regression guard for the existing `ByName: missing table`
    // test in `SparkSourceResolverSpec.scala:88`. Pre-P2 the
    // catch was `case _: Exception => None`; post-P2 the
    // catch is `case _: AnalysisException => None`. The
    // observable contract is identical for AnalysisException
    // (the only class Spark's `spark.table` raises when a table
    // is missing in the active catalog).
    val spark = buildSpark()
    try {
      val resolver = new SparkSourceResolver(spark)
      val out = resolver.resolve(SourceRef.ByName(table = "no_such_p2_table"), identity)
      out.isLeft shouldBe true
      val err = out.left.toOption.get
      err shouldBe a [EngineError.UnsupportedCapability]
      err match {
        case EngineError.UnsupportedCapability(_, cap, msg) =>
          cap shouldBe "SourceRef.ByName.resolve"
          msg should include ("no_such_p2_table")
        case other => fail(s"expected UnsupportedCapability, got $other")
      }
    } finally {
      spark.stop()
    }
  }

  test("resolveByName: InterruptedException PROPAGATES (PR-176 NonFatal discipline)") {
    // PR-176 NonFatal discipline (cite by topic): the catch is
    // narrowed to `AnalysisException`, so an `InterruptedException`
    // (the typical "cancellation requested" signal from a Restate
    // journal-cancellation hook) escapes to the caller. Pre-P2,
    // the `case _: Exception => None` swallow would have silently
    // typed it as `UnsupportedCapability` and the cancellation
    // would be lost. This is the regression guard.
    val spark = buildSpark()
    try {
      val resolver = new SeamedResolver(spark, _ =>
        throw new InterruptedException("sm8-test: simulated cancellation"))
      val thrown = intercept[InterruptedException] {
        resolver.resolve(SourceRef.ByName(table = "any"), identity)
      }
      thrown.getMessage should include ("simulated cancellation")
    } finally {
      spark.stop()
    }
  }
}
