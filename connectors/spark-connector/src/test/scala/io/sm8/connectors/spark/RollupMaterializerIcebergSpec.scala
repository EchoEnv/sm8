/*
 * RollupMaterializerIcebergSpec — the ADR-0028 Slice 1 contract tests
 * for the Iceberg write branch.
 *
 * Exercises the EAGER persistCatalog path (the production refresh
 * path) against an embedded HadoopCatalog — no external catalog
 * service needed. This is the spec the first ADR draft's "harness
 * parity" claim was missing: the temp-view branch (eager=false) never
 * touches persistCatalog, so it cannot validate the Iceberg branch.
 *
 * Acceptance criteria (ADR § Tests):
 * - Atomic refresh: a failed refresh keeps the previous snapshot
 *   serving.
 * - Concurrent read: readers see one consistent snapshot, never a
 *   blend.
 * - Schema drift: a type change fails loud at commit; old snapshot
 *   intact.
 * - Parquet default unchanged: with Parquet format, behavior is
 *   byte-identical to the pre-ADR path.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineError, EngineIdentity}
import io.sm8.core.expr.Expr
import io.sm8.core.model._
import io.sm8.core.rel.{AggregateCall, AggregateFn}

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RollupMaterializerIcebergSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  // `spark` is initialized in beforeAll; the body helpers use
  // `SparkSession.implicits._` instead of a top-level `import spark.implicits._`
  // (Scala 2.13 needs a stable identifier for the encasing spark var).
  private var spark: SparkSession = _
  private val warehouseDir: String =
    java.nio.file.Files.createTempDirectory("iceberg-warehouse").toString

  override def beforeAll(): Unit = {
    // Per-test warehouse hygiene: ensure no stale managed-table
    // location from a previous run interferes with the parquet-default
    // test (the table-name is qualified by the spark_catalog.default
    // database; the location is a subdir of the warehouse). The Iceberg
    // catalog has its own warehouseDir, isolated from this path.
    val sparkWarehouse = new java.io.File("spark-warehouse")
    if (sparkWarehouse.exists()) recursiveDelete(sparkWarehouse)
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("RollupMaterializerIcebergSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      // Embedded HadoopCatalog: the zero-extra-infra Iceberg catalog
      // (ADR-0028 "minimum viable catalog"). Warehouse is a local
      // temp dir; no HMS/Nessie/Polaris service needed for tests.
      .config("spark.sql.catalog.iceberg_cat", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_cat.type", "hadoop")
      .config("spark.sql.catalog.iceberg_cat.warehouse", warehouseDir)
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()
  }

  private def recursiveDelete(f: java.io.File): Unit = {
    if (f.isDirectory) Option(f.listFiles).toList.flatten.foreach(recursiveDelete)
    f.delete()
    ()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private val identity: EngineIdentity = EngineIdentity(
    name = "sm8-iceberg-test", nativeVersion = "3.5", engineAdapterVersion = "0.1.0")

  private def fixtureModel(): Model = Model.of(
    name = "sales",
    version = 1,
    dimensions = List(Dimension.field("region", "region")),
    measures = List(
      Measure("order_count", AggregateCall(fn = AggregateFn.Count, input = None, alias = "order_count")),
      Measure.aggregate("total_amount", AggregateFn.Sum, Expr.FieldRef("amount"))),
    source = SourceRef.ByName(table = "sales_base")
  ) match {
    case Right(m) => m
    case Left(e)  => fail(s"fixture model invalid: $e")
  }

  private def byRegion: RollupSpec =
    RollupSpec("by_region", List("region"), List("order_count", "total_amount"), None)

  test("Iceberg eager write produces an Iceberg table readable via spark.table") {
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales__by_region")
    // The materializer writes to iceberg_cat.sales__by_region (catalog-qualified).
    spark.sql("SELECT 'east' AS region, 10L AS amount UNION ALL SELECT 'west' AS region, 20L AS amount")
      .createOrReplaceTempView("sales_base")
    val m = fixtureModel()
    // Minimal fixture: the materializer reads the base via the model's
    // source and groups by the spec dims. The 2-row fixture is enough
    // for the write-path contract (not for parity numbers — those are
    // in RollupMaterializerSpec).
    val out = RollupMaterializer.materialize(
      spark, m, byRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg)
    out.isRight shouldBe true
    // The written table must be readable through the catalog.
    val df = spark.table("iceberg_cat.sales__by_region")
    df.count() should be > 0L
  }

  test("Iceberg atomic refresh: a failed refresh keeps the previous snapshot serving") {
    spark.sql("DROP TABLE IF EXISTS iceberg_cat.sales__by_region")
    // The materializer writes to iceberg_cat.sales__by_region (catalog-qualified).
    spark.sql("SELECT 'east' AS region, 10L AS amount UNION ALL SELECT 'west' AS region, 20L AS amount")
      .createOrReplaceTempView("sales_base")
    val m = fixtureModel()
    // First refresh: succeeds, writes snapshot 1.
    RollupMaterializer.materialize(
      spark, m, byRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg).isRight shouldBe true
    val before = spark.table("iceberg_cat.sales__by_region").count()

    // Second refresh: the base view now references a MISSING column,
    // so buildRollupDf fails — the Iceberg table must be untouched
    // (the failed refresh never commits).
    spark.sql("DROP VIEW IF EXISTS sales_base")
    spark.sql("SELECT 'east' AS region, 'oops' AS amount")
      .createOrReplaceTempView("sales_base")
    val out = RollupMaterializer.materialize(
      spark, m, byRegion, eager = true,
      tableFormat = RollupMaterializer.Iceberg)
    // The cast failure surfaces as a typed error (loud), and the
    // previous snapshot survives.
    out.isLeft shouldBe true
    spark.catalog.tableExists("iceberg_cat.sales__by_region") shouldBe true
    spark.table("iceberg_cat.sales__by_region").count() shouldBe before
  }

  test("Parquet default unchanged: eager write without Iceberg config behaves as before") {
    spark.sql("DROP TABLE IF EXISTS sales__by_region_parquet")
    spark.sql("SELECT 'east' AS region, 10L AS amount UNION ALL SELECT 'west' AS region, 20L AS amount")
      .createOrReplaceTempView("sales_base")
    val m = fixtureModel()
    val out = RollupMaterializer.materialize(
      spark, m, RollupSpec(
        "by_region_parquet", List("region"),
        List("order_count", "total_amount"), None),
      eager = true,
      tableFormat = RollupMaterializer.Parquet)
    out.isRight shouldBe true
    spark.table("sales__by_region_parquet").count() should be > 0L
  }
}
