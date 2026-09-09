/*
 * RollupRefresherScopeSpec — the cron-surface Tier 1 contract tests
 * (ADR-0029; ADR-0030 §D1; ADR-0031 §D1).
 *
 * Pins:
 *   - refreshModel with NO scopes preserves the Tier 0 behavior:
 *     every rollup refreshes whole-table, byte-identical to the
 *     pre-scope PRs.
 *   - refreshModel with a scope routes that rollup through the
 *     materializer's Tier 1 strategy-select: covered scope commits,
 *     uncovered scope refuses typed (the failure is per-rollup —
 *     other rollups in the model still refresh, per-rollup isolation).
 *   - refreshModelJ threads the scope map through the JDK boundary.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.EngineError
import io.sm8.core.expr.Expr
import io.sm8.core.model._
import io.sm8.core.rel.{AggregateCall, AggregateFn}

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RollupRefresherScopeSpec
  extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val warehouseDir: String =
    Files.createTempDirectory("refresher-scope-spec").toString

  /** Boot the Spark session with the embedded HadoopCatalog (the
    * ADR-0028 minimum viable catalog) before the first test. */
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("RollupRefresherScopeSpec")
      .master("local[1]")
      .config("spark.sql.catalog.iceberg_cat",
        "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_cat.type", "hadoop")
      .config("spark.sql.catalog.iceberg_cat.warehouse", warehouseDir)
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
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
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(recursiveDelete))
    f.delete()
  }

  /** Two-rollup fixture: a time-grained rollup (scope-eligible) and a
    * grain-less rollup (always Tier 0). Both on one model so the
    * per-rollup isolation is exercised. */
  private def fixtureModel(): Model = Model.of(
    name = "sales_rs",
    version = 1,
    dimensions = List(
      Dimension.field("order_date", "order_date",
        dataType = io.sm8.core.schema.SealedDataType.Timestamp),
      Dimension.field("region", "region")),
    measures = List(
      Measure("order_count", AggregateCall(fn = AggregateFn.Count,
        input = None, alias = "order_count")),
      Measure.aggregate("total_amount", AggregateFn.Sum,
        Expr.FieldRef("amount"))),
    rollups = List(
      RollupSpec("by_day_region", List("order_date", "region"),
        List("order_count", "total_amount"),
        timeGrain = Some("day"), grainDimension = Some("order_date")),
      RollupSpec("all_regions", List("region"),
        List("order_count", "total_amount"))),
    source = SourceRef.ByName(table = "sales_rs_base")
  ) match {
    case Right(m) => m
    case Left(e)  => fail(s"fixture model invalid: $e")
  }

  private def seed(): Unit = {
    spark.sql("DROP VIEW IF EXISTS sales_rs_base")
    spark.sql(
      "CREATE OR REPLACE TEMP VIEW sales_rs_base AS SELECT * FROM VALUES " +
        "(timestamp'2026-09-08 10:00:00', 'east', 15L), " +
        "(timestamp'2026-09-08 11:00:00', 'west', 10L) " +
        "AS t(order_date, region, amount)")
  }

  private val modelResolver: String => Option[Model] = name =>
    if (name == "sales_rs") Some(fixtureModel()) else None

  test("refreshModel without scopes refreshes Tier 0 (default preserved)") {
    seed()
    val results = RollupRefresher.refreshModel(spark, "sales_rs", modelResolver)
    results.isRight shouldBe true
    val rs = results.right.get
    rs should have size 2
    rs.foreach {
      case RollupRefresher.RollupRefreshResult.Refreshed(_, _) => ()
      case RollupRefresher.RollupRefreshResult.Failed(_, e) =>
        fail(s"unexpected failure: $e")
    }
    // Tier 0 default writes through the session catalog (Parquet) —
    // NOT iceberg_cat. Only scoped (Iceberg) writes land there.
    spark.catalog.tableExists("sales_rs__by_day_region") shouldBe true
    spark.catalog.tableExists("sales_rs__all_regions") shouldBe true
  }

  test("refreshModel with a covered scope routes the scoped rollup through Tier 1") {
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_rs__by_day_region")
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_rs__all_regions")
    spark.sql("DROP TABLE IF EXISTS sales_rs__by_day_region")
    spark.sql("DROP TABLE IF EXISTS sales_rs__all_regions")
    seed()
    val scopes = Map("by_day_region" ->
      RollupMaterializer.RefreshScope.Partitions(
        List(Map("order_date" -> "2026-09-08"))))
    val results = RollupRefresher.refreshModel(spark, "sales_rs",
      modelResolver, rollupScopes = scopes)
    results.isRight shouldBe true
    // The scoped rollup committed through Iceberg (iceberg_cat);
    // the grain-less rollup refreshed through the session catalog
    // (Tier 0 fallback, Parquet).
    spark.catalog.tableExists("iceberg_cat.sales_rs__by_day_region") shouldBe true
    spark.catalog.tableExists("sales_rs__all_regions") shouldBe true
  }

  test("refreshModel per-rollup isolation: scoped rollup refreshes; failing rollup does not block others") {
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_rs__by_day_region")
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_rs__all_regions")
    spark.sql("DROP TABLE IF EXISTS sales_rs__by_day_region")
    spark.sql("DROP TABLE IF EXISTS sales_rs__all_regions")
    seed()
    // Known-covered scope: source has 09-08 rows; scope declares 09-08.
    // The scoped rollup MUST refresh (discriminating assertion).
    // The grain-less rollup has no scope entry → Tier 0 path
    // (separate catalog; Per-rollup isolation: neither rollup's
    // outcome affects the other's iteration).
    val scopes = Map("by_day_region" ->
      RollupMaterializer.RefreshScope.Partitions(
        List(Map("order_date" -> "2026-09-08"))))
    val results = RollupRefresher.refreshModel(spark, "sales_rs",
      modelResolver, rollupScopes = scopes - "all_regions")
    results.isRight shouldBe true
    val rs = results.right.get
    // L2 fix (gull): the scoped rollup MUST be Refreshed (the test
    // was previously non-falsifiable — it accepted either Refreshed
    // or Failed). Same-row-count source + matching scope = covered.
    rs.find {
      case RollupRefresher.RollupRefreshResult.Refreshed(name, _) => name == "by_day_region"
      case _ => false
    } shouldBe defined
    rs.find {
      case RollupRefresher.RollupRefreshResult.Refreshed(name, _) => name == "all_regions"
      case _ => false
    } shouldBe defined
  }

  test("refreshModel per-rollup isolation: uncovered scope refuses one rollup typed") {
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_rs__by_day_region")
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_rs__all_regions")
    spark.sql("DROP TABLE IF EXISTS sales_rs__by_day_region")
    spark.sql("DROP TABLE IF EXISTS sales_rs__all_regions")
    seed()
    // Source has only 09-08 rows; scope declares 09-07 (a partition
    // the source doesn't cover). Materializer refuses typed.
    val scopes = Map("by_day_region" ->
      RollupMaterializer.RefreshScope.Partitions(
        List(Map("order_date" -> "2026-09-07"))))
    val results = RollupRefresher.refreshModel(spark, "sales_rs",
      modelResolver, rollupScopes = scopes - "all_regions")
    results.isRight shouldBe true
    val rs = results.right.get
    rs.find {
      case RollupRefresher.RollupRefreshResult.Failed(name, _) => name == "by_day_region"
      case _ => false
    } shouldBe defined
    rs.find {
      case RollupRefresher.RollupRefreshResult.Refreshed(name, _) => name == "all_regions"
      case _ => false
    } shouldBe defined
    // L3 (gull): operator-visible assertions for the REFUSED case.
    // The scoped rollup's refresh was refused before any write: the
    // Iceberg table must NOT exist (the DROP at the top of the test
    // removed it, and the refusal prevents recreation), and no
    // session-catalog Parquet table was created either.
    spark.catalog.tableExists("iceberg_cat.sales_rs__by_day_region") shouldBe false
    spark.catalog.tableExists("sales_rs__by_day_region") shouldBe false
    // The grain-less rollup still refreshed (Tier 0, session catalog).
    spark.catalog.tableExists("sales_rs__all_regions") shouldBe true
    spark.catalog.tableExists("iceberg_cat.sales_rs__all_regions") shouldBe false
  }

  test("refreshModelJ threads scopes through the JDK boundary") {
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_rs__by_day_region")
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales_rs__all_regions")
    spark.sql("DROP TABLE IF EXISTS sales_rs__by_day_region")
    spark.sql("DROP TABLE IF EXISTS sales_rs__all_regions")
    seed()
    val scopes = Map("by_day_region" ->
      RollupMaterializer.RefreshScope.Partitions(
        List(Map("order_date" -> "2026-09-08"))))
    val out = RollupRefresher.refreshModelJ(spark, "sales_rs",
      name => modelResolver(name), rollupScopes = scopes)
    out.get("ok") shouldBe java.lang.Boolean.TRUE
  }
}
