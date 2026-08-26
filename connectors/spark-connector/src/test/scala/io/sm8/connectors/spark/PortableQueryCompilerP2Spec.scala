/*
 * SM8 Spark Connector — P2 cluster regression tests for site 4
 * (PortableQueryCompiler.resolveSource).
 *
 * Pre-P2 shape: `case _: Exception => Left(UnsupportedCapability)` (3 sites:
 * outer ByName fall-through, inner ByName fall-through, ByPath) — would
 * silently swallow ANY NonFatal (InterruptedException, Spark RPC faults)
 * into a typed Left.
 *
 * P2 narrows all three to `case _: AnalysisException => Left(...)` —
 * the specific Spark exception raised when a table is not in the active
 * catalog (or a path is not parseable as the given format). The narrow
 * discipline mirrors the canonical MinimalRelOpLowerer.scala:194-218
 * pattern. Other NonFatal failures propagate to
 * EngineService.executeEngine:258-264's NonFatal -> ProviderInvocationFailed
 * typed conversion; Error subclasses propagate to the caller per the
 * PR-176 discipline. The PortableQueryCompiler.scala:311,316 join-right
 * catches already narrow correctly — this site enforces the
 * single-convention rule across the file.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, EngineError}
import io.sm8.core.model.SourceRef

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PortableQueryCompilerP2Spec extends AnyFunSuite with Matchers {

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("p2-portable-query-compiler-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

  /** Construct a minimal Model with the given source. */
  private def makeModel(source: SourceRef): io.sm8.core.model.Model =
    io.sm8.core.model.Model.of(
      name    = "p2-model",
      version = 1,
      source  = source,
      status  = io.sm8.core.model.ModelStatus.Draft,
      defaultPolicies = io.sm8.core.model.ModelPolicyDefaults(
        materialize = io.sm8.core.model.MaterializePolicy.None,
        cache = io.sm8.core.model.CachePolicy.NoCache,
        audit = io.sm8.core.model.AuditPolicy.NoAudit,
      ),
    ).toOption.get

  /** Test-only compiler that injects a controlled `readTableByName`
    * factory. Production behavior is `spark.table(name)`. */
  private final class SeamedCompiler(
      spark: SparkSession,
      tableLoader: String => org.apache.spark.sql.DataFrame)
    extends PortableQueryCompiler(spark) {
    override protected[spark] def readTableByName(name: String): org.apache.spark.sql.DataFrame =
      tableLoader(name)
  }

  test("resolveSource/ByName: AnalysisException -> Left(UnsupportedCapability) (narrowed-catch contract)") {
    // Regression guard: a missing table in the active catalog must
    // surface as a typed Left(UnsupportedCapability), preserving the
    // observable contract that pre-P2 callers relied on. The
    // narrowing to AnalysisException is observationally identical
    // for the common case (real spark.table on a missing table
    // throws AnalysisException).
    val spark = buildSpark()
    try {
      val compiler = new PortableQueryCompiler(spark)
      val model = makeModel(SourceRef.ByName(table = "no_such_p2_table"))
      val out = compiler.compile(model, EngineContext.defaultContext)
      out.isLeft shouldBe true
      val err = out.left.toOption.get
      err shouldBe a [EngineError.UnsupportedCapability]
    } finally {
      spark.stop()
    }
  }

  test("resolveSource/ByName: InterruptedException PROPAGATES (PR-176 NonFatal discipline)") {
    // PR-176 NonFatal discipline (cite by topic): the catch is
    // narrowed to `AnalysisException`, so an `InterruptedException`
    // (the typical "cancellation requested" signal) escapes to
    // the caller. Pre-P2, the `case _: Exception => ...` would
    // have swallowed it into a typed Left and lost the cancellation.
    val spark = buildSpark()
    try {
      val compiler = new SeamedCompiler(spark, _ =>
        throw new InterruptedException("sm8-test: simulated cancellation"))
      val model = makeModel(SourceRef.ByName(table = "any"))
      val thrown = intercept[InterruptedException] {
        compiler.compile(model, EngineContext.defaultContext)
      }
      thrown.getMessage should include ("simulated cancellation")
    } finally {
      spark.stop()
    }
  }
}
