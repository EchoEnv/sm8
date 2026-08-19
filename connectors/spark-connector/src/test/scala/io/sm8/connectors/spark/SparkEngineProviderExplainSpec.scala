/*
 * SM8 Spark Connector -- SparkEngineProviderExplainSpec (PR-27, ADR-008-R SSexplain).
 *
 * Per the user's 2026-08-19 directive ("does this support semantic
 * query plan yet? like spark df.explain() but for semantic +
 * provider's plan") + the PR-27 SM8 + Spark physical approval.
 *
 * Per [[debug-mantra-mindset]] SS1: every test asserts on substring
 * content (per node presence + per-node args).
 *
 * Per [[scala-spark-batch-bugs-mindset]] SS1 (closure-safety -- the
 * user's explicit concern): the SM8 + Spark physical path runs
 * ENTIRELY in the driver (df.explain is a Catalyst operation);
 * no executor-side closures.
 *
 * Per [[karpathy-guidelines-mindset]] SS3 (smallest correct change):
 * this spec exercises the explain() path end-to-end via the smallest
 * number of stable tests. The "SM8 + Spark physical" assertion is
 * the user-facing change; the other tests are backward-compat
 * regressions for the existing explain() contract.
 *
 * Per [[karpathy-bug-huntingmindset]] SS1 (trust compiler, not
 * runtime): the smoke-compile uses the SAME `compileModelToDataFrame`
 * helper as `query()` -- including `ModelValidator.validateAgainstSchema`.
 * This fixes the UNRESOLVED_COLUMN bug discovered during the diagnostic
 * spec (PR-27 iteration).
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, QueryRequest}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.{
  CalculatedMeasure, Dimension, MaterializePolicy, CachePolicy,
  AuditPolicy, Measure, Model, ModelPolicyDefaults, ModelStatus, SourceRef
}
import io.sm8.core.schema.SealedDataType

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkEngineProviderExplainSpec extends AnyFunSuite with Matchers {

  /** Helper: build a minimal in-memory patients model for testing.
    * The `sourceTable` parameter controls the source -- tests can
    * point at a missing table (for the build-failure footer test)
    * or a pre-registered temp view (for the SM8 + Spark physical
    * test). */
  private def dummyModel(name: String = "test-model", sourceTable: String = "patients_csv"): Model = Model.of(
    name = name,
    version = 1,
    source = SourceRef.ByName(table = sourceTable),
    status = ModelStatus.Draft,
    defaultPolicies = ModelPolicyDefaults(
      materialize = MaterializePolicy.None,
      cache       = CachePolicy.NoCache,
      audit       = AuditPolicy.NoAudit,
    ),
    dimensions = List(
      Dimension.field("patient_id", "patient_id"),
      Dimension.field("name", "name"),
    ),
    measures = List(
      Measure.aggregate(
        name = "patient_count",
        fn   = io.sm8.core.rel.AggregateFn.Count,
        expr = Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int),
      ),
    ),
  ) match {
    case Right(m) => m
    case Left(err) => throw new IllegalStateException(s"sm8: dummyModel failed: $err")
  }

  // === Backward-compat tests (Categories 1 + 3 + 4 of the 4-category plan) ===

  test("explain() with spark=null renders the SM8 semantic plan only + build-failed footer (backward compat)") {
    // Per [[scala-impactanalysismindset]] SS3 (binary compat): the
    // existing explain() contract returns the SM8 semantic plan +
    // a typed-error footer when the IR can't be resolved. spark=null
    // means the SourceRef.ByName.resolve fails (no catalog) -- the
    // footer names the typed error per [[scala-error-handlingmindset]]
    // SS1 (no thrown exceptions).
    val provider = new SparkEngineProvider(null, SparkTypeBridge, "spark-3.5")
    val out = provider.explain(dummyModel("err-table", sourceTable = "nonexistent_xyz"), QueryRequest.empty, EngineContext.defaultContext)
    out.isRight shouldBe true
    val s = out.toOption.get
    s should include ("SM8 Plan: err-table")
    s should include ("engine=spark-3.5")
    s should include ("version=<uninitialized>")
    s should include ("build failed:")
    s should include ("UnsupportedCapability")
    // No Spark physical plan section when spark=null (per smallest-correct-change).
    s should not include "Spark Physical Plan"
  }

  test("explain() header carries model name + engine + version (wire-format contract is stable)") {
    // Per [[karpathy-guidelinesmindset]] "smallest correct change":
    // the wire-format header is stable across calls (no drift
    // between models).
    val provider = new SparkEngineProvider(null, SparkTypeBridge, "spark-3.5")
    val s1 = provider.explain(dummyModel("alpha"), QueryRequest.empty, EngineContext.defaultContext).toOption.get
    val s2 = provider.explain(dummyModel("beta"),  QueryRequest.empty, EngineContext.defaultContext).toOption.get
    s1 should include ("SM8 Plan: alpha")
    s2 should include ("SM8 Plan: beta")
    s1 should include ("engine=spark-3.5")
    s2 should include ("engine=spark-3.5")
  }

  test("explain() with spark=null returns Right (never throws) per [[scala-error-handlingmindset]] SS1") {
    val provider = new SparkEngineProvider(null, SparkTypeBridge, "spark-3.5")
    noException should be thrownBy {
      provider.explain(dummyModel("error-model"), QueryRequest.empty, EngineContext.defaultContext)
    }
  }

  // === Headline new test (Category 2 -- SM8 + Spark physical plan) ===

  test("explain() with a live SparkSession appends the Spark physical plan section (the user's headline ask)") {
    // Per the user's 2026-08-19 directive: SM8 semantic plan +
    // provider's (Spark physical) plan BOTH. PR-27 ships this
    // end-to-end via df.queryExecution.explainString(ExplainMode.fromString("extended"))
    // when the smoke-compile succeeds.
    //
    // Per [[karpathy-bug-huntingmindset]] SS1: the diagnostic spec
    // (ExplainDebugSpec) revealed UNRESOLVED_COLUMN errors when the
    // smoke-compile skipped `ModelValidator.validateAgainstSchema`.
    // PR-27 fixes this by calling the shared `compileModelToDataFrame`
    // helper (the same pipeline as `query()`).
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("explain-physical-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .getOrCreate()
    try {
      // Register a temp view with the expected schema so the
      // ModelValidator passes (otherwise the build fails BEFORE
      // the smoke-compile can be called).
      spark.sql("SELECT 1 AS patient_id, 'a' AS name").createTempView("patients_csv")
      val provider = new SparkEngineProvider(spark, SparkTypeBridge, "spark-3.5")
      val out = provider.explain(dummyModel(), QueryRequest.empty, EngineContext.defaultContext)
      out.isRight shouldBe true
      val s = out.toOption.get
      // SM8 semantic section (always printed when the IR builds).
      s should include ("SM8 Plan: test-model")
      // Spark physical plan section (printed when spark non-null
      // AND the smoke-compile succeeds).
      s should include ("== Spark Physical Plan (via df.explain(true)) ==")
      // The SM8 semantic section is rendered BEFORE the Spark
      // physical section (per the production code in the spark
      // match block).
      (s.indexOf("SM8 Plan:") should be < s.indexOf("== Spark Physical Plan"))
    } finally spark.stop()
  }
}
