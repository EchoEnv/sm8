/*
 * SM8 Spark Connector — RollupSnapshotDiffExtractorSpec (ADR-0030 D5
 * amendment; spec §7 contract tests).
 *
 * Drives the extractor + flag wiring against a synthetic Iceberg
 * table built by the test, then asserts the ADT classification for
 * each lineage scenario in §7. Same HadoopCatalog fixture as
 * RollupMergeTier2Spec — production swap-out is the catalog config,
 * not the rest of this harness.
 */
package io.sm8.connectors.spark

import io.sm8.core.rollup.{AmbiguityReason, SnapshotDelta}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class RollupSnapshotDiffExtractorSpec
  extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val warehouseDir: String =
    Files.createTempDirectory("d5-extractor-spec").toString

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("RollupSnapshotDiffExtractorSpec")
      .master("local[1]")
      .config("spark.sql.catalog.iceberg_cat_d5",
        "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_cat_d5.type", "hadoop")
      .config("spark.sql.catalog.iceberg_cat_d5.warehouse", warehouseDir)
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

  /** Create a fresh (empty) Iceberg table; returns the qualified
    * name. Uses `saveAsTable` shape as `RollupRefreshCostProbeSpec`
    * so the catalog registers the table without a `default.` prefix.
    * The initial CREATE carries no data — the first `appendMore`
    * creates the first DATA snapshot, so the extractor's from=0 walk
    * sees a clean append. */
  private def freshTable(name: String): String = {
    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("id",
        org.apache.spark.sql.types.LongType, nullable = false)))
    spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      schema)
      .write.format("iceberg").mode("overwrite")
      .saveAsTable(s"iceberg_cat_d5.$name")
    s"iceberg_cat_d5.$name"
  }

  /** Append rows (creating a fresh append snapshot); returns the
    * new last-commit snapshot id. Schema must match the table's
    * (single `id` column). */
  private def appendMore(name: String, extraRows: Int): Long = {
    spark.range(extraRows.toLong).toDF("id")
      .writeTo(s"iceberg_cat_d5.$name").append()
    // TRUE head via the Iceberg table API (NOT RollupWatermark.currentSnapshotId:
    // its .max heuristic picks the numerically-largest id, which is NOT
    // guaranteed to be the current snapshot — ids are random 63-bit).
    org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, s"iceberg_cat_d5.$name")
      .currentSnapshot().snapshotId()
  }

  /** spec §7 test 1: append-only lineage → Appended. Empty table
    * created first (no data snapshot), then a clean append — the
    * extractor's from=0 walk sees exactly one append step.
    * The instrumentation print reveals the actual `operation()` string
    * Iceberg emits, so we ground-truth our operation-vocabulary
    * classification (the API docs are thin here). */
  test("append-only lineage returns Appended (spec §7 test 1)") {
    val q = freshTable("apd_only")
    appendMore("apd_only", 100)
    val r = RollupSnapshotDiffExtractor.extract(spark, q, 0L)
    r.isRight shouldBe true
    r.toOption.get match {
      case SnapshotDelta.Appended(rows, files) =>
        rows should be >= 0L
        files should not be empty
      case other =>
        fail(s"expected Appended, got $other")
    }
  }

  /** spec §7 test 9a: already-consumed lineage → NoDataChange. */
  test("already-consumed lineage returns NoDataChange (spec §7 test 9a)") {
    val q = freshTable("apd_consumed")
    appendMore("apd_consumed", 50)
    val head = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, q)
      .currentSnapshot().snapshotId()
    val r = RollupSnapshotDiffExtractor.extract(spark, q, head)
    r.isRight shouldBe true
    r.toOption.get shouldBe SnapshotDelta.NoDataChange
  }

  /** spec §6 narrowing prelude: when NoDataChange returns, the
    * refresher feeds `spark.emptyDataFrame` into executeMerge. The
    * MERGE source-view must be safe with zero rows in Spark 3.5 +
    * Iceberg 1.5.2 (heron Q5: this was untested). The fixture tries an
    * actual MERGE on a populated Iceberg target using an empty temp
    * view; the target's row count is unchanged (Idempotency contract,
    * spec §7 9b). */
  test("empty-DataFrame MERGE target no-op on Iceberg 1.5.2 (heron Q5)") {
    val q = freshTable("empty_merge")
    appendMore("empty_merge", 25)
    val rollupQualified = q
    val before = spark.read.format("iceberg").load(rollupQualified).count()
    // Build a temp view of the empty schema, then issue the MERGE.
    val schema = spark.read.format("iceberg").load(rollupQualified).schema
    val empty = spark.createDataFrame(
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], schema)
    empty.createOrReplaceTempView("empty_merge_src")
    val keyCol = schema.fields.head.name
    spark.sql(
      s"""MERGE INTO $rollupQualified t
         |USING empty_merge_src s
         |ON t.`$keyCol` = s.`$keyCol`
         |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
    val after = spark.read.format("iceberg").load(rollupQualified).count()
    spark.catalog.dropTempView("empty_merge_src")
    after shouldBe before // no-op merge preserves row count
  }

  /** Disabled-by-default flag check: with the flag off, the
    * refresher's narrowing is a no-op (the existing full recompute
    * path runs). This is the production safety guarantee: until a
    * flag is set, behavior is identical to pre-D5. */
  test("snapshotDiffEnabled defaults to false (flag production safety)") {
    val prior = System.getProperty("sm8.rollup.tier2.snapshotDiff.enabled")
    try {
      System.clearProperty("sm8.rollup.tier2.snapshotDiff.enabled")
      spark.conf.unset("spark.sm8.rollup.tier2.snapshotDiff.enabled")
      RollupMergeRefresher.snapshotDiffEnabled(spark) shouldBe false
    } finally if (prior == null) System.clearProperty("sm8.rollup.tier2.snapshotDiff.enabled")
      else System.setProperty("sm8.rollup.tier2.snapshotDiff.enabled", prior)
  }

  test("snapshotDiffEnabled honors System property true") {
    val prior = System.getProperty("sm8.rollup.tier2.snapshotDiff.enabled")
    try {
      System.setProperty("sm8.rollup.tier2.snapshotDiff.enabled", "true")
      RollupMergeRefresher.snapshotDiffEnabled(spark) shouldBe true
    } finally if (prior == null) System.clearProperty("sm8.rollup.tier2.snapshotDiff.enabled")
      else System.setProperty("sm8.rollup.tier2.snapshotDiff.enabled", prior)
  }

  test("snapshotDiffEnabled honors SparkConf true") {
    val prior = System.getProperty("sm8.rollup.tier2.snapshotDiff.enabled")
    try {
      System.clearProperty("sm8.rollup.tier2.snapshotDiff.enabled")
      spark.conf.set("spark.sm8.rollup.tier2.snapshotDiff.enabled", "true")
      RollupMergeRefresher.snapshotDiffEnabled(spark) shouldBe true
    } finally {
      if (prior == null) System.clearProperty("sm8.rollup.tier2.snapshotDiff.enabled")
      else System.setProperty("sm8.rollup.tier2.snapshotDiff.enabled", prior)
      spark.conf.unset("spark.sm8.rollup.tier2.snapshotDiff.enabled")
    }
  }

  test("System property overrides SparkConf (ops-level override)") {
    val prior = System.getProperty("sm8.rollup.tier2.snapshotDiff.enabled")
    try {
      spark.conf.set("spark.sm8.rollup.tier2.snapshotDiff.enabled", "false")
      System.setProperty("sm8.rollup.tier2.snapshotDiff.enabled", "true")
      RollupMergeRefresher.snapshotDiffEnabled(spark) shouldBe true
    } finally {
      if (prior == null) System.clearProperty("sm8.rollup.tier2.snapshotDiff.enabled")
      else System.setProperty("sm8.rollup.tier2.snapshotDiff.enabled", prior)
      spark.conf.unset("spark.sm8.rollup.tier2.snapshotDiff.enabled")
    }
  }

  private def recursiveDelete(f: java.io.File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(recursiveDelete))
    f.delete()
  }
}
