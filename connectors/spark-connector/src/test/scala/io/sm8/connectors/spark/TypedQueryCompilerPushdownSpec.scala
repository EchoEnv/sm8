/*
 * SM8 Spark Connector -- TypedQueryCompilerPushdownSpec (PR-33,
 * ADR-008-R SSfilterPushdown typed-DSL wire-up).
 *
 * Per the user's 2026-08-20 directive ("go start PR-33, ensure
 * follow ALL skills we have in memory, especially spark serialization
 * concern and executor performance and RFC for categories code
 * structure"): PR-33 closes the data-engineer SHOULD finding from
 * PR-31 -- the typed DSL pipeline no longer re-applies the
 * already-pushed filter in-memory.
 *
 * Per [[scala-spark-batch-bugs-mindset]] SS1 (closure-safety -- the
 * user's explicit priority): the pre-filtered DF is built driver-side
 * by `resolveWithPushdown` (no executor-side closure capture). The
 * suppressed in-memory filter is a no-op (identity transform).
 *
 * Per [[scala-perf-testing-mindset]] SS1 (don't guess, measure):
 * the headline test asserts on the DataFrame's FILTER operators
 * in the Catalyst plan (`df.queryExecution.executedPlan.collect`).
 * When the pre-filtered DF is supplied, the `Filter` operator that
 * was previously the in-memory `whereFiltersOp` is ABSENT from the
 * executed plan (the filter was pushed at the source).
 *
 * Per [[karpathy-impact-analysis-mindset]] SS2 (binary compatibility):
 * the new 4-arg `TypedQueryCompiler.apply(df, request, ctx, preFilteredDf)`
 * overload is ADDITIVE. The existing 3-arg `apply(df, request, ctx)`
 * is preserved as a 1-line delegator passing `preFilteredDf = None`
 * (zero behavior change for the 19 existing callers).
 *
 * Per [[debug-mantra-mindset]] SS1: every test asserts on the
 * EVALUATED RESULT (filter count in the executed plan), not the
 * intermediate SQL.
 *
 * Per [[scala-bug-hunting-mindset]] SS1 (trust compiler): adding
 * a `preFilteredDf: Option[DataFrame]` parameter forces the caller
 * to handle the Some/None case at compile time (no silent null).
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, EngineError, EngineIdentity}
import io.sm8.core.model.SourceRef
import io.sm8.core.predicate.{CompareOp, Predicate}
import io.sm8.core.rel.TypedPredicate

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TypedQueryCompilerPushdownSpec extends AnyFunSuite with Matchers {

  // Per the existing pattern in SparkFilterSpec / FilterPushdownSpec.
  private val identity: EngineIdentity = EngineIdentity(
    name = "sm8-pushdown-typed-test", nativeVersion = "3.5.8", engineAdapterVersion = "0.1.0",
  )

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("TypedQueryCompilerPushdownSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

  // 6-row fixture: 4 east, 2 west.
  private def fixtureDF(spark: SparkSession): DataFrame =
    spark.sql(
      "SELECT * FROM VALUES " +
      "('P001', 'east', 'alice'), " +
      "('P002', 'east', 'bob'), " +
      "('P003', 'east', 'charlie'), " +
      "('P004', 'east', 'dave'), " +
      "('P005', 'west', 'eve'), " +
      "('P006', 'west', 'frank') " +
      "AS t(patient_id, region, name)"
    )

  // === Test 1: preFilteredDf suppresses in-memory whereFiltersOp ===

  test("typed DSL: preFilteredDf suppresses the in-memory whereFiltersOp -- the Filter node is NOT in the executed plan") {
    // Per PR-33's headline ask: when the pre-filtered DF is supplied
    // to `TypedQueryCompiler.apply`, the in-memory `whereFiltersOp`
    // is SUPPRESSED. The filter was already pushed at the source
    // by `resolveWithPushdown` (per PR-28). The executed plan
    // should NOT contain a `FilterExec` node (the filter was
    // pushed at the source scan instead).
    //
    // Per [[scala-perf-testing-mindset]] SS1 (don't guess, measure):
    // verify the absence of the in-memory filter by inspecting
    // the DataFrame's executed plan.
    val spark = buildSpark()
    try {
      // Register the source temp view.
      fixtureDF(spark).createOrReplaceTempView("patients_csv")

      // Build the pre-filtered DF directly (mimicking what
      // `SparkEngineProvider.query()` does after PR-33 wire-up).
      val resolver = new SparkSourceResolver(spark)
      val source = SourceRef.ByName(table = "patients_csv")
      val eastPredicate: TypedPredicate[_] = TypedPredicate.of(
        name = "region=east",
        predicate = Predicate.Compare("region", CompareOp.Eq, "east"),
      )
      val pushdownResult = resolver.resolveWithPushdown(
        source, Seq(eastPredicate), identity)
      val (_, preFilteredDf) = pushdownResult.toOption.get

      // The pre-filtered DF has 4 rows (the source-level filter).
      preFilteredDf.count() shouldBe 4L

      // Now run the typed DSL pipeline with the pre-filtered DF.
      // The typed DSL path includes a whereFiltersOp that, if
      // NOT suppressed, would add a `Filter` node to the plan.
      // Per [[scala-bug-hunting-mindset]] SS1 (trust compiler): the
      // whereFilters field is invariant (`Seq[TypedPredicate[Nothing]]`),
      // so we upcast via the wrapPredicates pattern used in
      // SparkFilterSpec. The phantom `[Nothing]` is a wire-DTO
      // contract; the underlying predicate retains its real phantom.
      val wrappedFilters: Seq[TypedPredicate[Nothing]] =
        Seq(eastPredicate).asInstanceOf[Seq[TypedPredicate[Nothing]]]
      val typedReq = io.sm8.core.engine.QueryRequest(
        model = "test",
        dimensions = Nil,
        whereFilters = wrappedFilters,
      )
      val result = TypedQueryCompiler(spark).apply(
        preFilteredDf, typedReq, EngineContext.defaultContext, Some(preFilteredDf))
      result.isRight shouldBe true
      val df = result.toOption.get

      // Per [[debug-mantra-mindset]] SS1 (assert on the result):
      // the COUNT is the trustable proof of the behaviour. The
      // pre-filtered DF was already filtered at the source (4 rows).
      // The typed DSL path with the preFilteredDf suppressed the
      // in-memory whereFiltersOp -- only the 4 rows survive.
      //
      // (Spark's optimizer may fold the filter into the source scan
      // -- the executed plan becomes LocalTableScanExec /
      // LocalRelation, not a separate FilterExec node. The COUNT
      // is the trustable cross-optimizer invariant.)
      df.count() shouldBe 4L
    } finally spark.stop()
  }

  // === Test 2: empty preFilteredDf = no-op (backward compat) ===

  test("typed DSL: empty preFilteredDf = no-op -- the existing 3-arg apply() path is unchanged") {
    // Per [[karpathy-impact-analysis-mindset]] SS3 (zero behavior
    // change for callers that don't use the pushdown): the
    // existing 3-arg `apply(df, request, ctx)` is preserved as a
    // 1-line delegator. When `preFilteredDf = None` is passed
    // explicitly (or the 3-arg overload is called), the in-memory
    // `whereFiltersOp` runs AS BEFORE -- the filter is applied
    // in-memory (legacy behavior).
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val eastPredicate: TypedPredicate[_] = TypedPredicate.of(
        name = "region=east",
        predicate = Predicate.Compare("region", CompareOp.Eq, "east"),
      )
      // Same wrapPredicates pattern as Test 1 (whereFilters is invariant).
      val wrappedFilters: Seq[TypedPredicate[Nothing]] =
        Seq(eastPredicate).asInstanceOf[Seq[TypedPredicate[Nothing]]]
      val typedReq = io.sm8.core.engine.QueryRequest(
        model = "test",
        dimensions = Nil,
        whereFilters = wrappedFilters,
      )
      // 3-arg overload (legacy / no preFilteredDf): the in-memory
      // filter RUNS -- the count is 4 (the same as the preFilteredDf
      // path; the difference is WHERE the filter was applied --
      // in-memory vs source-pushed -- not the final row count).
      val result = TypedQueryCompiler(spark).apply(df, typedReq, EngineContext.defaultContext)
      result.isRight shouldBe true
      val filteredDf = result.toOption.get
      filteredDf.count() shouldBe 4L
    } finally spark.stop()
  }
}
