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

  /** Boot the Spark session with the embedded HadoopCatalog
    * (mirrors the production recipe: iceberg_cat default name,
    * warehouse-local temp dir, IcebergSparkSessionExtensions).
    * Per-test warehouse hygiene: clean the dir before each suite. */
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

  /** Tear down the Spark session and remove the warehouse directory. */
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
    // Discriminating assertion (final-gate rhino): touched must be a
    // STRICT SUBSET of total — if the filter accidentally inverted
    // (18bcb4b regression), touched would equal the UNTOUCHED byte
    // sum, which is strictly less than total in this fixture (two
    // untouched partitions outweigh one touched).
    report.tier1TouchedBytes should be < report.tier0TotalBytes
    // And rewritten-but-unchanged is bounded by touched (it is a
    // subset of the scoped partition files).
    report.rewrittenButUnchangedBytes should be <= report.tier1TouchedBytes
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

  test("runModel: scope key derives from grainDimension (R1 ibis H1 regression test)") {
    // H1 regression test: a rollup with a non-order_date grainDimension
    // must have its scope matched by that grainDimension's name, not
    // by the hardcoded order_date prefix. Seed two partition dates
    // under a non-standard grainDimension name (event_date).
    val model = io.sm8.core.model.Model.of(
      name = "ship_t",
      version = 1,
      dimensions = List(
        io.sm8.core.model.Dimension.field("event_date", "event_date",
          dataType = io.sm8.core.schema.SealedDataType.Timestamp),
        io.sm8.core.model.Dimension.field("region", "region")),
      measures = List(
        io.sm8.core.model.Measure("order_count",
          io.sm8.core.rel.AggregateCall(fn = io.sm8.core.rel.AggregateFn.Count,
            input = None, alias = "order_count")),
        io.sm8.core.model.Measure.aggregate("total_amount",
          io.sm8.core.rel.AggregateFn.Sum,
          io.sm8.core.expr.Expr.FieldRef("amount"))),
      source = io.sm8.core.model.SourceRef.ByName(table = "ship_base")
    ) match {
      case Right(m) => m
      case Left(e)  => fail(s"fixture model invalid: $e")
    }
    val spec = io.sm8.core.model.RollupSpec(
      "by_day_region", List("event_date", "region"),
      List("order_count", "total_amount"),
      timeGrain = Some("day"), grainDimension = Some("event_date"))

    spark.sql("DROP VIEW IF EXISTS ship_base")
    // Refresh the catalog so the freshly created view resolves
    // immediately (sequential tests share the Spark session; stale
    // catalog metadata from the prior test's tables can shadow).
    spark.catalog.refreshTable("iceberg_cat.sales_t__by_day_region")
    spark.sql(
      "CREATE OR REPLACE TEMP VIEW ship_base AS SELECT * FROM VALUES " +
        "(timestamp'2026-09-08 10:00:00', 'east', 15L), " +
        "(timestamp'2026-09-08 11:00:00', 'west', 10L) " +
        "AS t(event_date, region, amount)")

    val scope = RollupMaterializer.RefreshScope.Partitions(
      List(Map("event_date" -> "2026-09-08")))
    val report = RollupRefreshCostProbe.runModel(spark, model, spec, scope)
    // Non-zero touched bytes means the scopeDates matched the
    // event_date-partitioned files (the bug would have zeroed these).
    report.tier1TouchedBytes should be > 0L
    report.rewrittenButUnchangedBytes should be >= 0L
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
  test("localDateOf decodes URL-encoded minute-precision UTC instants (rhino M3)") {
    // 17:00Z = 2026-09-08 00:00 +07 (Asia/Bangkok) — the UTC-shifted
    // directory name decodes to the NEXT local day. The assertions
    // pin the host-TZ-dependent decode; on a UTC host the minute-
    // precision case would decode to 09-07 instead (this fixture
    // assumes +07, matching the CI box; if that changes, update).
    RollupRefreshCostProbe.localDateOf("order_date=2026-09-07T17%3A00Z") shouldBe "2026-09-08"
    // Plain date form (no shift)
    RollupRefreshCostProbe.localDateOf("order_date=2026-09-07") shouldBe "2026-09-07"
  }

  test("contentIdentityRatio equals 1.0 for healthy Tier 1 isolation (rhino M4)") {
    // Bounds-only assertions let a 0.5 ratio pass; healthy isolation
    // on this fixture MUST be exactly 1.0 (untouched partitions are
    // byte-identical). (R2 rhino M4.)
    val report = RollupRefreshCostProbe.run(spark, warehouseDir)
    report.contentIdentityRatio shouldBe 1.0
  }

}

