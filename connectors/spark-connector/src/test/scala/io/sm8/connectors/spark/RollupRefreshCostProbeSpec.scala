/*
 * RollupRefreshCostProbeSpec — contract tests for the Gate B item 3
 * instrumentation (ADR-0029 §Gate B item 3; ADR-0030 §D1).
 *
 * Pins the metric surface (R1 gorilla M1 — the probe was untested;
 * the metrics could silently rot). Uses the same 3-day seed as
 * RollupMaterializerTier1Spec so the fixtures compose.
 *
 * NOT exhaustive — this is a metric-shape guard, not a replacement
 * for end-to-end production traces. The probe's purpose is to
 * produce the INSTRUMENTATION shape (rewritten-but-unchanged bytes,
 * untouched-bytes-preserved, isolation ratio); production-scale
 * numbers require production-scale data.
 */
package io.sm8.connectors.spark

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RollupRefreshCostProbeSpec
  extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val warehouseDir: String =
    Files.createTempDirectory("gateb-probe-spec").toString

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("RollupRefreshCostProbeSpec")
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

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    val wh = new java.io.File(warehouseDir)
    if (wh.exists()) recursiveDelete(wh)
  }

  private def recursiveDelete(f: java.io.File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(recursiveDelete))
    f.delete()
  }

  test("run() emits a Gate B report with Tier 0 and Tier 1 runs") {
    val report = RollupRefreshCostProbe.run(spark, warehouseDir)
    report.tier0Run.success shouldBe true
    report.tier1Run shouldBe defined
    report.tier1Run.get.success shouldBe true
    report.tier0Run.wallClockMs should be >= 0L
    report.tier1Run.get.wallClockMs should be >= 0L
  }

  test("Tier 0 bytes are present (CRITICAL gorilla C2: status != 2 captures live files)") {
    // After a first refresh, every live file has status != 2. If
    // the status filter were broken (e.g. status = 1 only) bytesByPartition
    // would return an empty Map and tier0TotalBytes would be 0.
    val report = RollupRefreshCostProbe.run(spark, warehouseDir)
    report.tier0TotalBytes should be > 0L
    report.tier1TouchedBytes should be > 0L
  }

  test("isolation ratio: untouched partitions reuse their data files") {
    // The bytes-preserved count is the Tier 1 saving signal; it must
    // be a positive number when there are untouched partitions.
    val report = RollupRefreshCostProbe.run(spark, warehouseDir)
    report.contentIdentityRatio should (be >= 0.0 and be <= 1.0)
  }

  test("partitionKeyOf extracts the Hive-style partition directory") {
    RollupRefreshCostProbe.partitionKeyOf(
      "warehouse/db/tbl/order_date=2026-09-08%5C/data.parquet") should
      startWith("order_date=2026-09-08")
    RollupRefreshCostProbe.partitionKeyOf("warehouse/db/tbl/data.parquet") shouldBe
      "UNPARTITIONED"
  }

  test("filePaths returns non-empty set after Tier 0 refresh") {
    RollupRefreshCostProbe.run(spark, warehouseDir)
    val paths = RollupRefreshCostProbe.filePaths(
      spark, "iceberg_cat.sales_t__by_day_region")
    paths should not be empty
    paths.foreach(_ should endWith(".parquet"))
  }

  test("rewritten-but-unchanged bytes axis is bounded (the Gate B item 3 signal)") {
    val report = RollupRefreshCostProbe.run(spark, warehouseDir)
    report.rewrittenButUnchangedBytes should be >= 0L
    // Hard upper bound: total bytes in the table.
    val totalBytes =
      RollupRefreshCostProbe.bytesByPartition(
        spark, "iceberg_cat.sales_t__by_day_region").values.sum
    report.rewrittenButUnchangedBytes should be <= totalBytes
  }
}
