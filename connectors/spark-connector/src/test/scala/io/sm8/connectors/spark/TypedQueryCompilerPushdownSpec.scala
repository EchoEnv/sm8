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
 * the headline test asserts on the count (the trustable
 * cross-optimizer invariant). To distinguish suppression from
 * double-application, the typed request uses a DIFFERENT predicate
 * than the pre-filtered DF.
 *
 * Per [[scala-impact-analysis-mindset]] SS2 (binary compatibility):
 * the new 4-arg `TypedQueryCompiler.apply(df, request, ctx, preFilteredDf)`
 * overload is ADDITIVE. The existing 3-arg `apply(df, request, ctx)`
 * is preserved as a 1-line delegator passing `preFilteredDf = None`.
 *
 * Per [[scala-bug-hunting-mindset]] SS1 (trust compiler): adding
 * a `preFilteredDf: Option[DataFrame]` parameter forces the caller
 * to handle the Some/None case at compile time (no silent null).
 *
 * Per [[debug-mantra-mindset]] SS1: every test asserts on the
 * EVALUATED RESULT (the count), not the intermediate SQL.
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

  test("typed DSL: preFilteredDf suppresses the in-memory whereFiltersOp -- a DIFFERENT typed-request predicate proves the suppression") {
    // Per PR-33's headline ask: when the pre-filtered DF is supplied
    // to `TypedQueryCompiler.apply`, the in-memory `whereFiltersOp`
    // is SUPPRESSED. The filter was already pushed at the source
    // by `resolveWithPushdown` (per PR-28).
    //
    // Per [[scala-perf-testing-mindset]] SS1 (don't guess, measure):
    // the count is the trustable cross-optimizer invariant. To
    // distinguish suppression from double-application, the typed
    // request uses a DIFFERENT predicate than the pre-filtered DF.
    //
    //   - Pre-filtered DF: region=east (4 rows: alice, bob, charlie, dave)
    //   - Typed request: region=west (intentionally different)
    //
    // - Suppression path: count = 4 (the typed request's `region=west`
    //   is NOT applied because the in-memory filter is suppressed)
    // - Double-application path: count = 0 (the typed request's
    //   `region=west` is applied to the 4 east rows -> 0 rows)
    //
    // The test asserts count == 4 (the suppression path).
    val spark = buildSpark()
    try {
      // Register the source temp view.
      fixtureDF(spark).createOrReplaceTempView("patients_csv")

      val resolver = new SparkSourceResolver(spark)
      val source = SourceRef.ByName(table = "patients_csv")

      // 1. Build the pre-filtered DF: region=east (4 rows).
      val eastPredicate: TypedPredicate[_] = TypedPredicate.of(
        name = "region=east",
        predicate = Predicate.Compare("region", CompareOp.Eq, "east"),
      )
      val pushdownResult = resolver.resolveWithPushdown(
        source, Seq(eastPredicate), identity)
      val (_, preFilteredDf) = pushdownResult.toOption.get
      preFilteredDf.count() shouldBe 4L

      // 2. Run the typed DSL pipeline with a DIFFERENT predicate
      // (region=west). The in-memory whereFiltersOp, if NOT
      // suppressed, would zero out all rows.
      val westPredicate: TypedPredicate[_] = TypedPredicate.of(
        name = "region=west",
        predicate = Predicate.Compare("region", CompareOp.Eq, "west"),
      )
      val wrappedFilters: Seq[TypedPredicate[Nothing]] =
        Seq(westPredicate).asInstanceOf[Seq[TypedPredicate[Nothing]]]
      val typedReq = io.sm8.core.engine.QueryRequest(
        model = "test",
        dimensions = Nil,
        whereFilters = wrappedFilters,
      )
      val result = TypedQueryCompiler(spark).apply(
        preFilteredDf, typedReq, EngineContext.defaultContext, Some(preFilteredDf))
      result.isRight shouldBe true
      val df = result.toOption.get

      // 3. ASSERT: count = 4 (suppression path). Count = 0 would
      // mean the in-memory whereFiltersOp was NOT suppressed.
      df.count() shouldBe 4L
    } finally spark.stop()
  }

  // === Test 2: empty preFilteredDf = no-op (backward compat) ===

  test("typed DSL: empty preFilteredDf = no-op -- the 4-arg overload with None behaves identically to the 3-arg overload") {
    // Per [[karpathy-impact-analysis-mindset]] SS3 (zero behavior
    // change for callers that don't use the pushdown): the new
    // 4-arg overload with `preFilteredDf = None` is the LEGACY
    // behavior. The 3-arg and 4-arg (with None) overloads MUST
    // produce the same row count.
    //
    // Per [[karpathy-bug-hunting-mindset]] SS1 (trust compiler):
    // the 4-arg overload is a typed Either -- the count is the
    // trustable proof of identity.
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val eastPredicate: TypedPredicate[_] = TypedPredicate.of(
        name = "region=east",
        predicate = Predicate.Compare("region", CompareOp.Eq, "east"),
      )
      val wrappedFilters: Seq[TypedPredicate[Nothing]] =
        Seq(eastPredicate).asInstanceOf[Seq[TypedPredicate[Nothing]]]
      val typedReq = io.sm8.core.engine.QueryRequest(
        model = "test",
        dimensions = Nil,
        whereFilters = wrappedFilters,
      )

      // 3-arg overload (legacy): the in-memory filter RUNS.
      val legacyResult = TypedQueryCompiler(spark).apply(df, typedReq, EngineContext.defaultContext)
      legacyResult.isRight shouldBe true
      val legacyCount = legacyResult.toOption.get.count()
      legacyCount shouldBe 4L

      // 4-arg overload with None: the in-memory filter ALSO RUNS.
      // The count is the same as the 3-arg overload.
      val newResult = TypedQueryCompiler(spark).apply(
        df, typedReq, EngineContext.defaultContext, None)
      newResult.isRight shouldBe true
      val newCount = newResult.toOption.get.count()
      newCount shouldBe 4L

      // The 3-arg and 4-arg (with None) overloads MUST produce the
      // same row count -- this is the backward-compat invariant.
      newCount shouldBe legacyCount
    } finally spark.stop()
  }
}
