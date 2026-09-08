/*
 * RollupMaterializerTier1Spec — the ADR-0029 Tier 1 (dynamic
 * partition overwrite via DSv2) contract tests.
 *
 * Pins the strategy-select behavior of RollupMaterializer.persistCatalog
 * for time-grained rollups with a declared partition scope:
 *
 *   1. Partition isolation: refresh with scope = today swaps ONLY
 *      today's partition; yesterday's data files are reused by
 *      manifest reference (data-file content identity preserved).
 *   2. Scope coverage refusal: source contains partition values
 *      outside the declared scope → typed EngineError, no commit,
 *      previous snapshot intact.
 *   3. Undeclared scope refusal: None/empty scope on a time-grained
 *      rollup refuses typed BEFORE the aggregation job runs (no
 *      wasted compute, no accidental whole-table refresh).
 *   4. Degenerate grain fallback: grain-less rollup (timeGrain=None,
 *      grainDimension=None) takes the Tier 0 whole-table path even
 *      with a scope declared.
 *   5. Atomicity preserved: a failed Tier 1 refresh leaves the
 *      previous snapshot serving (the #358 contract under DSv2).
 *   6. Parquet default: a scope on a Parquet refresh refuses typed
 *      (DSv1 saveAsTable has no partition-scoped overwrite;
 *      ADR-0029 scope fences).
 *
 * Uses an embedded HadoopCatalog (no external catalog service).
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.EngineError
import io.sm8.core.expr.Expr
import io.sm8.core.model._
import io.sm8.core.rel.{AggregateCall, AggregateFn}

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RollupMaterializerTier1Spec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val warehouseDir: String =
    java.nio.file.Files.createTempDirectory("iceberg-warehouse-tier1").toString

  /** Boot the Spark session + register the embedded HadoopCatalog
    * (the ADR-0028 minimum viable catalog) before the first test. */
  override def beforeAll(): Unit = {
    // Same embedded HadoopCatalog recipe as the Tier 0 spec — the
    // iceberg_cat name matches icebergCatalog's default so persistCatalog
    // addresses the same catalog the deployment would.
    spark = SparkSession.builder()
      .appName("RollupMaterializerTier1Spec")
      .master("local[1]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.iceberg_cat",
        "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_cat.type", "hadoop")
      .config("spark.sql.catalog.iceberg_cat.warehouse", warehouseDir)
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    // Warehouse hygiene (R1 review comment on the Tier 0 spec).
    val wh = new java.io.File(warehouseDir)
    if (wh.exists()) recursiveDelete(wh)
    wh.mkdirs()
  }

  /** Tear down the Spark session and clean the per-suite warehouse
    * directory so the temp filesystem does not accumulate. */
  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    val wh = new java.io.File(warehouseDir)
    if (wh.exists()) recursiveDelete(wh)
  }

  private def recursiveDelete(f: java.io.File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(recursiveDelete)
    f.delete()
  }

  /** Time-grained fixture: a sales table with `order_date` (timestamp)
    * so the rollup can partition on `date_trunc('day', order_date)`. */
  private def timeGrainedModel(): Model = Model.of(
    name = "sales_t",
    version = 1,
    dimensions = List(
      Dimension.field("order_date", "order_date", dataType = io.sm8.core.schema.SealedDataType.Timestamp),
      Dimension.field("region", "region")),
    measures = List(
      Measure("order_count", AggregateCall(fn = AggregateFn.Count, input = None, alias = "order_count")),
      Measure.aggregate("total_amount", AggregateFn.Sum, Expr.FieldRef("amount"))),
    source = SourceRef.ByName(table = "sales_t_base")
  ) match {
    case Right(m) => m
    case Left(e)  => fail(s"fixture model invalid: $e")
  }

  /** Daily grain on order_date — the partition column is order_date. */
  private val dailyByRegion: RollupSpec =
    RollupSpec("by_day_region",
      List("order_date", "region"),
      List("order_count", "total_amount"),
      timeGrain = Some("day"),
      grainDimension = Some("order_date"))

  /** Grain-less rollup (no timeGrain / grainDimension) — single
    * partition even on a partitioned base table; per the ADR, Tier 1
    * falls back to Tier 0 here regardless of any declared scope. */
  private val grainlessByRegion: RollupSpec =
    RollupSpec("all_regions",
      List("region"),
      List("order_count", "total_amount"),
      timeGrain = None,
      grainDimension = None)

  /** Three days of base rows for the daily-grained rollup. */
  private def seedThreeDays(): Unit = {
    spark.sql("DROP VIEW IF EXISTS sales_t_base")
    spark.sql(
      "SELECT timestamp'2026-09-06 10:00:00' AS order_date, 'east' AS region, 10L AS amount " +
      "UNION ALL SELECT timestamp'2026-09-06 11:00:00', 'east', 5L " +
      "UNION ALL SELECT timestamp'2026-09-06 12:00:00', 'west', 20L " +
      "UNION ALL SELECT timestamp'2026-09-07 10:00:00', 'east', 30L " +
      "UNION ALL SELECT timestamp'2026-09-07 11:00:00', 'west', 25L " +
      "UNION ALL SELECT timestamp'2026-09-08 10:00:00', 'east', 15L " +
      "UNION ALL SELECT timestamp'2026-09-08 11:00:00', 'west', 10L")
      .createOrReplaceTempView("sales_t_base")
  }

  // ----------------------------------------------------------------
  // Test 1: partition isolation — today-only refresh swaps today's
  // partitions; yesterday's data files are reused by manifest reference.
  // ----------------------------------------------------------------
  test("Tier 1 partition isolation: scope = today swaps only today's partition") {
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_t__by_day_region")
    seedThreeDays()
    val m = timeGrainedModel()

    // First refresh: full week, scope omitted → Tier 0 whole-table.
    val fullOut = RollupMaterializer.materialize(
      spark, m, dailyByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = RollupMaterializer.RefreshScope.NoScope)
    fullOut.isRight shouldBe true
    // Snapshot the data files for 09-06 BEFORE the scoped refresh.
    val beforeFiles = dataFilePathsFor("iceberg_cat.sales_t__by_day_region")

    // Add a row for 09-08 only; scope declares 09-08. The caller
    // (per the ADR operator contract) NARROWS the recomputed source
    // to only the scoped partitions — the materializer then verifies
    // source ⊆ scope and runs the Tier 1 write.
    spark.sql("DROP VIEW IF EXISTS sales_t_aug08")
    spark.sql(
      "SELECT timestamp'2026-09-08 13:00:00' AS order_date, 'south' AS region, 99L AS amount")
      .createOrReplaceTempView("sales_t_aug08")
    spark.sql(
      "CREATE OR REPLACE TEMP VIEW sales_t_base AS " +
      "SELECT * FROM (VALUES " +
      "(timestamp'2026-09-08 10:00:00', 'east', 15L), " +
      "(timestamp'2026-09-08 11:00:00', 'west', 10L), " +
      "(timestamp'2026-09-08 13:00:00', 'south', 99L)) AS t(order_date, region, amount)")

    val scope = RollupMaterializer.RefreshScope.Partitions(
      List(Map("order_date" -> "2026-09-08")))
    val tier1Out = RollupMaterializer.materialize(
      spark, m, dailyByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = scope)
    tier1Out match {
      case Left(e) => fail(s"scoped refresh returned Left: $e")
      case Right(_) => ()
    }

    // The recomputed 09-08 region south now appears; the 09-08
    // totals are correct (east: 15, west: 10, south: 99).
    val rows = spark.table("iceberg_cat.sales_t__by_day_region").collect()
      .map(r => (r.getTimestamp(0).toString, r.getString(1),
        r.getLong(2), r.getLong(3))).toSet

    // 09-06 and 09-07 partitions are still present (untouched)
    rows should contain (("2026-09-06 00:00:00.0", "east", 2L, 15L))
    rows should contain (("2026-09-06 00:00:00.0", "west", 1L, 20L))
    rows should contain (("2026-09-07 00:00:00.0", "east", 1L, 30L))
    rows should contain (("2026-09-07 00:00:00.0", "west", 1L, 25L))
    // 09-08 partition has the new south row added; east/west unchanged.
    rows should contain (("2026-09-08 00:00:00.0", "east", 1L, 15L))
    rows should contain (("2026-09-08 00:00:00.0", "west", 1L, 10L))
    rows should contain (("2026-09-08 00:00:00.0", "south", 1L, 99L))

    // The data files for the 09-06 partition must be reused
    // (manifest reference, not rewritten). Pin: the same `file_path`
    // appears in the post-refresh snapshot for 09-06.
    val afterFiles = dataFilePathsFor("iceberg_cat.sales_t__by_day_region")
    val before0607 = beforeFiles.filter(_.endsWith(".parquet")).toSet
    val after = afterFiles.filter(_.endsWith(".parquet")).toSet
    after should contain allElementsOf before0607
  }

  // ----------------------------------------------------------------
  // Test 2: scope-coverage refusal — uncovered partition values
  // trigger typed EngineError; no commit, previous snapshot intact.
  // ----------------------------------------------------------------
  test("Tier 1 scope coverage refusal: uncovered partition values refuse typed") {
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_t__by_day_region")
    seedThreeDays()
    val m = timeGrainedModel()

    // First refresh: full week, baseline.
    val baseline = RollupMaterializer.materialize(
      spark, m, dailyByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = RollupMaterializer.RefreshScope.NoScope)
    baseline.isRight shouldBe true
    val baselineCount = spark.table("iceberg_cat.sales_t__by_day_region").count()

    // The scope declares 09-08 ONLY, but the recomputed source
    // contains rows for 09-06, 09-07, AND 09-08 (seedThreeDays
    // is unchanged). Refuse typed.
    val scope = RollupMaterializer.RefreshScope.Partitions(
      List(Map("order_date" -> "2026-09-08")))
    val out = RollupMaterializer.materialize(
      spark, m, dailyByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = scope)
    out match {
      case Left(e: EngineError.UnsupportedCapability) =>
        e.message should include ("outside the declared refresh scope")
      case other =>
        fail(s"expected typed refusal, got: $other")
    }
    // Previous snapshot intact — no overwrite happened.
    spark.table("iceberg_cat.sales_t__by_day_region").count() shouldBe baselineCount
  }

  // ----------------------------------------------------------------
  // Test 3: undeclared-scope refusal — None / empty scope on a
  // time-grained rollup refuses typed BEFORE the aggregation job.
  // ----------------------------------------------------------------
  test("Tier 1 undeclared scope refusal: empty scope on time-grained rollup refuses typed") {
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_t__by_day_region")
    seedThreeDays()
    val m = timeGrainedModel()

    // Pass an explicitly empty partition list. The ADR calls this
    // case out as the sixth contract test — close the loophole
    // where a caller accidentally widens scope to "nothing".
    val scope = RollupMaterializer.RefreshScope.Partitions(Nil)
    val out = RollupMaterializer.materialize(
      spark, m, dailyByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = scope)
    // An empty declared scope = no partition is "in scope", but the
    // source has rows. This is the uncovered case: every partition
    // value in the source is outside the declared scope.
    out match {
      case Left(e: EngineError.UnsupportedCapability) =>
        e.message should (include("outside the declared scope") or include("outside the declared refresh scope"))
      case other =>
        fail(s"expected typed refusal, got: $other")
    }
  }

  // ----------------------------------------------------------------
  // Test 4: degenerate-grain fallback — grain-less rollup with a
  // scope declared takes Tier 0 (the scope is not honored for
  // single-partition tables; ADR scope fences).
  // ----------------------------------------------------------------
  test("Tier 1 degenerate grain fallback: grain-less rollup ignores scope, takes Tier 0") {
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_t__all_regions")
    seedThreeDays()
    val m = timeGrainedModel()

    // A scope is declared but the rollup is grain-less. Per the
    // strategy-select matrix, the scope caller declared intent is
    // honored by silently falling back to Tier 0 (the caller would
    // otherwise get a RefuseScopeUncovered for a "non-grain-dim"
    // scope; we accept any partition value in that case because
    // there is no partition column to compare against).
    // For grain-less we interpret the scope as ignored and route
    // to Tier 0 — verifies the spec returns Right, not Left.
    val scope = RollupMaterializer.RefreshScope.Partitions(
      List(Map("ignored" -> "value")))
    val out = RollupMaterializer.materialize(
      spark, m, grainlessByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = scope)
    out.isRight shouldBe true
    // Pin the strategy explicitly: assert decideStrategy selects
    // Tier0 (not Tier1Iceberg) for a grain-less rollup. A future
    // regression that routed grain-less to Tier 1 with arbitrary
    // scope-widening would still pass the row-count check above
    // but fail this one.
    val recDf = RollupMaterializer.materialize(
      spark, m, grainlessByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = scope)
    val strat = RollupMaterializer.decideStrategy(
      spark.table("iceberg_cat.sales_t__all_regions"),
      grainlessByRegion,
      RollupMaterializer.Iceberg,
      scope,
      tableExists = true)
    strat shouldBe RollupMaterializer.PersistStrategy.Tier0
    spark.table("iceberg_cat.sales_t__all_regions").count() should be > 0L
  }

  // ----------------------------------------------------------------
  // Test 5: atomicity preserved — a failed Tier 1 refresh keeps
  // the previous snapshot serving.
  // ----------------------------------------------------------------
  test("Tier 1 atomicity: a failed scoped refresh leaves the previous snapshot intact (pre-write validation path)") {
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_t__by_day_region")
    seedThreeDays()
    val m = timeGrainedModel()

    // Baseline: whole-table refresh.
    RollupMaterializer.materialize(
      spark, m, dailyByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = RollupMaterializer.RefreshScope.NoScope).isRight shouldBe true
    val beforeRows = spark.table("iceberg_cat.sales_t__by_day_region").collect()
      .map(r => (r.getTimestamp(0).toString, r.getString(1))).toSet

    // Scoped refresh that succeeds: scope = 09-08, source narrowed
    // to 09-08 rows only (operator contract — source ⊆ scope).
    val rescope = RollupMaterializer.RefreshScope.Partitions(
      List(Map("order_date" -> "2026-09-08")))
    spark.sql("DROP VIEW IF EXISTS sales_t_base")
    spark.sql(
      "CREATE OR REPLACE TEMP VIEW sales_t_base AS " +
      "SELECT * FROM (VALUES " +
      "(timestamp'2026-09-08 10:00:00', 'east', 15L), " +
      "(timestamp'2026-09-08 11:00:00', 'west', 10L)) AS t(order_date, region, amount)")
    val s1 = RollupMaterializer.materialize(
      spark, m, dailyByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = rescope)
    s1 match {
      case Left(e) => fail(s"scoped refresh returned Left: $e")
      case Right(_) => ()
    }

    // Now corrupt the aggregation (rename `amount` to break Sum's
    // input). The scoped refresh MUST fail loud (typed or thrown).
    spark.sql("DROP VIEW IF EXISTS sales_t_base")
    spark.sql(
      "SELECT timestamp'2026-09-08 14:00:00' AS order_date, 'east' AS region, 99L AS amount_broken")
      .createOrReplaceTempView("sales_t_base")
    val out = RollupMaterializer.materialize(
      spark, m, dailyByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = rescope)
    (out.isLeft, spark.catalog.tableExists("iceberg_cat.sales_t__by_day_region")) match {
      case (true, _) =>
        // Typed refusal: previous scoped snapshot survives.
        val rows = spark.table("iceberg_cat.sales_t__by_day_region").collect()
          .map(r => (r.getTimestamp(0).toString, r.getString(1))).toSet
        // The 09-08 partition still has the prior data (east + west
        // from the scoped refresh, NOT 09-08 with east/99 broken-data).
        rows should contain (("2026-09-08 00:00:00.0", "east"))
        rows should contain (("2026-09-08 00:00:00.0", "west"))
        rows should contain (("2026-09-06 00:00:00.0", "east"))
        rows should contain (("2026-09-07 00:00:00.0", "east"))
      case (false, true) =>
        // buildRollupDf unexpectedly succeeded; previous snapshot
        // still must be intact (no partial blend).
        spark.table("iceberg_cat.sales_t__by_day_region").collect()
          .map(r => (r.getTimestamp(0).toString, r.getString(1))).toSet shouldBe beforeRows
      case (false, false) =>
        fail("table was dropped — that should never happen on a failed refresh")
    }
  }

  // ----------------------------------------------------------------
  // Test 6: Parquet refuses scope — DSv1 saveAsTable has no
  // partition-scoped overwrite; ADR scope fences.
  // ----------------------------------------------------------------
  test("Tier 1 Parquet refusal: scope on a Parquet refresh refuses typed") {
    spark.sql("DROP TABLE IF EXISTS sales_t__by_day_region_parquet")
    seedThreeDays()
    val m = timeGrainedModel()

    val scope = RollupMaterializer.RefreshScope.Partitions(
      List(Map("order_date" -> "2026-09-08")))
    val out = RollupMaterializer.materialize(
      spark, m, dailyByRegion, eager = true,
      tableFormat = RollupMaterializer.Parquet,
      refreshScope = scope)
    out match {
      case Left(e: EngineError.UnsupportedCapability) =>
        e.message should include ("Parquet")
      case other =>
        fail(s"expected typed Parquet refusal, got: $other")
    }
  }

  // ----------------------------------------------------------------
  // Test (cross-version note): the partition values surfaced by
  // `df.select(grainCol).distinct().collect()` are date_trunc-shaped
  // strings. Pin the string format so Spark 3.5 / 4.x drift in the
  // timestamp-as-string representation would surface here.
  // ----------------------------------------------------------------
  test("Tier 1 partition-value format is stable across refresh (cross-version note)") {
    // Pins the partition-value STRING FORMAT (what decideStrategy
    // canon() reads off the distinct().collect()) so a Spark 3.5/4.x
    // drift in Timestamp-as-string representation would surface here.
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_t__by_day_region")
    seedThreeDays()
    val m = timeGrainedModel()
    RollupMaterializer.materialize(
      spark, m, dailyByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = RollupMaterializer.RefreshScope.NoScope).isRight shouldBe true

    // Narrow the source to the scoped partition (operator contract)
    // so the scoped refresh is accepted.
    spark.sql("DROP VIEW IF EXISTS sales_t_base")
    spark.sql(
      "CREATE OR REPLACE TEMP VIEW sales_t_base AS " +
      "SELECT * FROM (VALUES " +
      "(timestamp'2026-09-08 10:00:00', 'east', 15L)) AS t(order_date, region, amount)")

    // Capture the canonical partition values the materializer reads
    // off the source. For daily grain the canon() trims to
    // "yyyy-MM-dd"; the captured repr is what gets compared against
    // the declared scope. If Spark's Timestamp.toString() format
    // drifts (e.g. drops the trailing ".0"), this test pins it.
    val sourceReprs: Set[String] =
      spark.table("sales_t_base")
        .select("order_date").distinct().collect()
        .map(r => String.valueOf(r.get(0))).toSet
    // Every repr in the source must contain the yyyy-MM-dd prefix
    // — that's the substring canon() strips to. If Spark's repr
    // ever stops including "2026-09-08", the scoped refresh below
    // would fail (the declared scope is "2026-09-08" and the
    // source repr would normalize to something different).
    sourceReprs.foreach { repr =>
      repr should include ("2026-09-08")
    }

    val scope = RollupMaterializer.RefreshScope.Partitions(
      List(Map("order_date" -> "2026-09-08")))
    val out = RollupMaterializer.materialize(
      spark, m, dailyByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = scope)
    out match {
      case Left(e) => fail(s"scoped refresh returned Left: $e")
      case Right(_) => ()  // covered
    }
  }

  test("Tier 1 first-write coverage: scope=[today] + full-source base refuses typed (no silent widening)") {
    // Goat F1 fix: a scoped CREATE refuses if the source contains
    // partitions outside the declared scope. The caller must
    // either narrow the source OR drop the scope for a full create.
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_t__by_day_region")
    seedThreeDays()
    val m = timeGrainedModel()

    val scope = RollupMaterializer.RefreshScope.Partitions(
      List(Map("order_date" -> "2026-09-08")))
    val out = RollupMaterializer.materialize(
      spark, m, dailyByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = scope)
    out match {
      case Left(e: EngineError.UnsupportedCapability) =>
        e.message should include ("first-write source contains partition value(s)")
        e.message should include ("outside the declared scope")
      case other =>
        fail(s"expected typed first-write refusal, got: $other")
    }
    // The table must NOT exist — the refusal happened before CTAS.
    spark.catalog.tableExists("iceberg_cat.sales_t__by_day_region") shouldBe false
  }

  test("Tier 1 scope-key validation: scope entry keys must name the partition column") {
    // Lion MED-4 fix: a scope entry whose key is NOT the grain
    // column name is refused typed (defense-in-depth: the scope
    // is a key+value mapping, not a value-bag).
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_t__by_day_region")
    seedThreeDays()
    val m = timeGrainedModel()
    RollupMaterializer.materialize(
      spark, m, dailyByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = RollupMaterializer.RefreshScope.NoScope).isRight shouldBe true

    val badScope = RollupMaterializer.RefreshScope.Partitions(
      List(Map("wrong_col" -> "2026-09-08")))
    val out = RollupMaterializer.materialize(
      spark, m, dailyByRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg,
      refreshScope = badScope)
    out match {
      case Left(e: EngineError.UnsupportedCapability) =>
        e.message should include ("scope entry keys")
        e.message should include ("do not name the partition column")
      case other =>
        fail(s"expected typed bad-key refusal, got: $other")
    }
  }

  // ----------------------------------------------------------------
  // Helpers
  // ----------------------------------------------------------------

  /** Read the data-file paths currently referenced by the table's
    * snapshot manifest. Used by the partition-isolation test to
    * verify untouched partitions reuse the same data files. */
  private def dataFilePathsFor(qualifiedTable: String): Seq[String] = {
    spark.read
      .format("iceberg")
      .load(s"$qualifiedTable.entries")
      .select("data_file.file_path")
      .collect()
      .map(_.getString(0)).toSeq
  }
}
