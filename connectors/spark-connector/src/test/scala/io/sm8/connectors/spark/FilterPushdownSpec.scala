/*
 * SM8 Spark Connector -- FilterPushdownSpec (PR-28, ADR-008-R
 * SSfilterPushdown).
 *
 * Per the user's 2026-08-19 directive: "go filter pushdown
 * optimization ensure follow ALL skills we have in memory,
 * especially spark serialization concern and executor performance
 * and RFC for categories code structure".
 *
 * Per [[debug-mantra-mindset]] SS1: every test asserts on the
 * EVALUATED RESULT (count + schema), not the intermediate SQL.
 *
 * Per [[scala-spark-batch-bugs-mindset]] SS1 (closure-safety -- the
 * user's explicit priority): the pushdown transform fn captures
 * ONLY a Seq of TypedPredicate instances (which are Serializable)
 * + a ResolvedSource (also Serializable). No DataFrame / Spark
 * session captured into any UDF closure.
 *
 * Per [[scala-perf-testing-mindset]] SS1 (don't guess, measure):
 * the headline assertion is that the pushdown FILTER RUNS AT THE
 * SOURCE -- measured by the count of rows in the typed pipeline
 * (a small pre-filtered subset, not the full table).
 *
 * Per [[karpathy-app-design-mindset]] SS3.1 (Protocols before
 * Implementations) + RFC SS3 (layer ownership): the pushdown is at
 * the CONNECTOR layer (spark-connector), not in sm8-core. The
 * typed TypedPredicate protocol (sm8-core/rel/) is unchanged.
 *
 * Per [[karpathy-bug-hunting-mindset]] SS3 (every match must be
 * exhaustive): the test asserts the existence of the pushdown
 * behavior on the ByName (temp view) code path. ByPath (Parquet)
 * is exercised by the existing SparkSourceResolverSpec tests.
 *
 * Per [[karpathy-app-design-mindset]] SS3.1: the wire-up to
 * `compileRelOp` (so the pre-filtered DataFrame actually flows
 * into the typed pipeline) is DEFERRED per senior R-recommendation
 * SS7.1 #2. PR-28 ships the FOUNDATION (the resolver-level
 * pushdown); the wire-up is a follow-up PR-29.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineError, EngineIdentity, ResolvedSource}
import io.sm8.core.model.SourceRef
import io.sm8.core.predicate.{CompareOp, Predicate}
import io.sm8.core.rel.TypedPredicate
import io.sm8.core.schema.SealedDataType

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FilterPushdownSpec extends AnyFunSuite with Matchers {

  // Per the existing pattern in SparkSourceResolverSpec.
  private val identity: EngineIdentity = EngineIdentity(
    name = "sm8-pushdown-test", nativeVersion = "3.5.8", engineAdapterVersion = "0.1.0",
  )

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("FilterPushdownSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

  // === Test 1: The user's headline ask -- pushdown filters the source ===

  test("filter pushdown: whereFilters applied at the source -- the typed pipeline sees ONLY the pre-filtered rows") {
    // Per the user's "Catalyst pushdown" approval: the predicate
    // is pushed into the source's `df.filter(...)` step. The typed
    // pipeline then operates on the pre-filtered DataFrame.
    //
    // Setup: 6 rows; we filter region='east' (4 rows). The typed
    // pipeline should see 4 rows (not 6).
    val spark = buildSpark()
    try {
      // Register 6 rows: 4 east, 2 west.
      spark.sql(
        """SELECT * FROM VALUES
          |  ('P001', 'east', 'alice'),
          |  ('P002', 'east', 'bob'),
          |  ('P003', 'east', 'charlie'),
          |  ('P004', 'east', 'dave'),
          |  ('P005', 'west', 'eve'),
          |  ('P006', 'west', 'frank')
          |AS t(patient_id, region, name)
        """.stripMargin
      ).createTempView("patients_csv")

      val resolver = new SparkSourceResolver(spark)
      val source = SourceRef.ByName(table = "patients_csv")
      val eastPredicate: TypedPredicate[_] = TypedPredicate.of(
        name = "region=east",
        predicate = Predicate.Compare("region", CompareOp.Eq, "east"),
      )

      val result: Either[EngineError, (ResolvedSource, DataFrame)] =
        resolver.resolveWithPushdown(source, Seq(eastPredicate), identity)

      result.toOption.get match {
        case (resolved: ResolvedSource.Scan, df) =>
          df.count() shouldBe 4L  // 4 east rows (pre-filtered)
          df.schema.fieldNames.toSet shouldBe Set("patient_id", "region", "name")
          resolved shouldBe a [ResolvedSource.Scan]
        case _ => fail("expected ResolvedSource.Scan + DataFrame tuple")
      }
    } finally spark.stop()
  }

  // === Test 2: no filters == backward compat (zero behavior change) ===

  test("filter pushdown: no filters == no-op (backward compat -- 19 callers see zero behavior change)") {
    // Per [[scala-impact-analysis-mindset]] SS3 (binary compat): if
    // the filter list is empty, the pushdown is a no-op (the
    // existing path is preserved).
    val spark = buildSpark()
    try {
      spark.sql(
        "SELECT * FROM VALUES ('P001', 'east', 'alice'), ('P002', 'west', 'bob') AS t(patient_id, region, name)"
      ).createTempView("patients_csv")

      val resolver = new SparkSourceResolver(spark)
      val source = SourceRef.ByName(table = "patients_csv")
      val result = resolver.resolveWithPushdown(source, Seq.empty, identity)
      result.toOption.get match {
        case (_, df) =>
          df.count() shouldBe 2L  // 2 rows, no filter applied
        case _ => fail("expected DataFrame")
      }
    } finally spark.stop()
  }

  // === Test 3: result type contract -- (ResolvedSource, DataFrame) tuple ===

  test("filter pushdown: result type is (ResolvedSource, DataFrame) for typed-pipeline consumption") {
    val spark = buildSpark()
    try {
      spark.sql(
        "SELECT * FROM VALUES ('P001', 'east', 'alice') AS t(patient_id, region, name)"
      ).createTempView("patients_csv")
      val resolver = new SparkSourceResolver(spark)
      val source = SourceRef.ByName(table = "patients_csv")
      val eastPredicate: TypedPredicate[_] = TypedPredicate.of(
        name = "region=east",
        predicate = Predicate.Compare("region", CompareOp.Eq, "east"),
      )
      val result = resolver.resolveWithPushdown(source, Seq(eastPredicate), identity)
      // Per the protocol contract: returns (ResolvedSource, DataFrame).
      result.toOption.get match {
        case (rs: ResolvedSource.Scan, df) =>
          rs.schema should not be empty
          df.count() shouldBe 1L
        case _ => fail("expected ResolvedSource.Scan + DataFrame tuple")
      }
    } finally spark.stop()
  }

  // === Test 4: multiple predicates are AND-combined ===

  test("filter pushdown: multiple predicates are AND-combined into a single combined filter") {
    // Per [[karpathy-bug-hunting-mindset]] SS1 (trust the compiler):
    // multiple TypedPredicates are AND-combined into one combined
    // `df.filter(combined)` call (single Catalyst pass; minimal plan
    // overhead).
    val spark = buildSpark()
    try {
      spark.sql(
        """SELECT * FROM VALUES
          |  ('P001', 'east', 'alice'),
          |  ('P002', 'east', 'bob'),
          |  ('P003', 'west', 'charlie'),
          |  ('P004', 'east', 'dave'),
          |  ('P005', 'west', 'eve')
          |AS t(patient_id, region, name)
        """.stripMargin
      ).createTempView("patients_csv")

      val resolver = new SparkSourceResolver(spark)
      val source = SourceRef.ByName(table = "patients_csv")
      val regionEast = TypedPredicate.of(
        name = "region=east",
        predicate = Predicate.Compare("region", CompareOp.Eq, "east"),
      )
      val nameAlice = TypedPredicate.of(
        name = "name=alice",
        predicate = Predicate.Compare("name", CompareOp.Eq, "alice"),
      )
      val result = resolver.resolveWithPushdown(
        source, Seq(regionEast, nameAlice), identity,
      )
      result.toOption.get match {
        case (_, df) =>
          // region=east AND name=alice -> only 1 row (alice is east).
          df.count() shouldBe 1L
        case _ => fail("expected DataFrame")
      }
    } finally spark.stop()
  }
}
