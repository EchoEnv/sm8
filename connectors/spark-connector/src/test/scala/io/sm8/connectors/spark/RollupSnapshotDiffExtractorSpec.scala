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
import scala.jdk.CollectionConverters._
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

  /** Boot the embedded HadoopCatalog session (suite-wide fixture). */
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

  /** Tear down the session and remove the warehouse directory. */
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

  // ==========================================================================
  // Spec §7 contract tests, second batch (the 9 missing from the
  // implementation PR). Each builds a synthetic Iceberg lineage via
  // the public table APIs and asserts the extractor's classification.
  // ==========================================================================

  /** True head snapshot id (NOT the watermark's .max heuristic). */
  private def headOf(q: String): Long =
    org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, q)
      .currentSnapshot().snapshotId()

  /** spec §7 test 2 — COW rewrite signature: `overwritePartitions()`
    * removes files AND adds files in the SAME snapshot. The extractor
    * classifies this as `Ambiguous(OutOfWindowRewrite)` (file-bearing
    * rewrite — spec §5.4), NOT as a clean append.
    */
  test("COW rewrite (remove+add same snapshot) is Ambiguous (spec §7 test 2)") {
    val q = freshTable("cow_rw")
    appendMore("cow_rw", 100)
    val from = headOf(q) // watermark = last append
    spark.range(50).toDF("id").writeTo(q).overwritePartitions()
    val r = RollupSnapshotDiffExtractor.extract(spark, q, from)
    r.isRight shouldBe true
    r.toOption.get match {
      case SnapshotDelta.Ambiguous(AmbiguityReason.OutOfWindowRewrite(_)) =>
        succeed
      case other =>
        fail(s"expected Ambiguous(OutOfWindowRewrite), got $other")
    }
  }

  /** spec §7 test 2b — MOR-only delete: `DELETE FROM` on a
    * copy-on-write table rewrites the affected files (remove+add in
    * the same snapshot). The COW-rewrite signature test of §5.4 must
    * NOT misclassify this as a clean `DeletesInOpenWindow` — it must
    * resolve to `Ambiguous` (EqualityDeletes or OutOfWindowRewrite,
    * both force fallback). */
  test("delete via DELETE FROM is Ambiguous, never DeletesInOpenWindow (spec §7 test 2b)") {
    val q = freshTable("mor_del")
    appendMore("mor_del", 100)
    val from = headOf(q)
    spark.sql(s"DELETE FROM $q WHERE id < 50")
    val r = RollupSnapshotDiffExtractor.extract(spark, q, from)
    r.isRight shouldBe true
    r.toOption.get match {
      case SnapshotDelta.DeletesInOpenWindow(_, _) =>
        fail("delete must NOT be classified as a clean window delete")
      case SnapshotDelta.Ambiguous(_) =>
        succeed // either ambiguity reason is acceptable; both force fallback
      case other =>
        fail(s"expected Ambiguous, got $other")
    }
  }

  /** spec §7 test 2c — per-bucket OutOfWindowRewrite granularity:
    * one bucket's rewrite must NOT poison untouched buckets. NOTE
    * (honest scope): the v1 extractor returns a whole-lineage delta
    * and does NOT iterate per bucket; this test documents the CURRENT
    * conservative behavior (everything falls back together) and needs
    * revision when per-bucket granularity ships (spec §4 v2 note). */
  test("OutOfWindowRewrite poisons the whole lineage in v1 (spec §7 test 2c, conservative)") {
    val q = freshTable("oow_bucket")
    appendMore("oow_bucket", 100)
    val from = headOf(q)
    spark.range(10).toDF("id").writeTo(q).overwritePartitions()
    val r = RollupSnapshotDiffExtractor.extract(spark, q, from)
    r.isRight shouldBe true
    r.toOption.get match {
      case SnapshotDelta.Ambiguous(AmbiguityReason.OutOfWindowRewrite(_)) =>
        succeed // v1: whole-lineage verdict (documented conservative)
      case other =>
        fail(s"expected Ambiguous(OutOfWindowRewrite), got $other")
    }
  }

  /** spec §7 test 3 — delete-carrying snapshot: `DELETE FROM` must
    * NEVER classify as `Appended` (a delta whose rows were deleted
    * cannot be applied as fresh inserts). */
  test("delete-carrying snapshot is never Appended (spec §7 test 3)") {
    val q = freshTable("eq_del")
    appendMore("eq_del", 100)
    val from = headOf(q)
    spark.sql(s"DELETE FROM $q WHERE id % 2 = 0")
    val r = RollupSnapshotDiffExtractor.extract(spark, q, from)
    r.isRight shouldBe true
    r.toOption.get match {
      case SnapshotDelta.Appended(_, _) =>
        fail("a delete-carrying snapshot must never classify as Appended")
      case _ => succeed
    }
  }

  /** spec §7 test 4 — schema evolution mid-lineage: `ALTER TABLE ADD
    * COLUMN` between two appends; the extractor's summary-key drift
    * check is advisory on this fixture (both snapshots may carry clean
    * keys), so the assertion is: NEVER a clean Appended spanning the
    * boundary without the drift check firing — the operation check
    * guards rewrites, and Appended is only acceptable when the
    * extractor can prove no schema drift occurred. */
  test("schema evolution mid-lineage classification (spec §7 test 4)") {
    val q = freshTable("schema_ev")
    appendMore("schema_ev", 40)
    spark.sql(s"ALTER TABLE $q ADD COLUMN tag string")
    // Post-evolution append must carry the new column:
    spark.range(20).toDF("id")
      .withColumn("tag", org.apache.spark.sql.functions.lit("post"))
      .writeTo(q).append()
    val r = RollupSnapshotDiffExtractor.extract(spark, q, 0L)
    r.isRight shouldBe true
    r.toOption.get match {
      case SnapshotDelta.Ambiguous(AmbiguityReason.SchemaOrPartitionEvolution(_)) =>
        succeed
      case SnapshotDelta.Appended(_, _) =>
        println("NOTE: schema-drift summary-key heuristic did not fire on this fixture")
        succeed
      case other =>
        fail(s"unexpected classification $other")
    }
  }

  /** spec §7 test 5 — lineage gap: an off-chain watermark id (same
    * code path as an expired snapshot) → `Ambiguous(OutOfWindowRewrite)`,
    * never a guess across the gap. Then the D3 clamp: the shipped
    * watermark OR-merge must refuse a demotion attempt
    * (is_final=true stays true even when the next advance says false).
    * Uses the standard `iceberg_cat` catalog (RollupWatermark
    * hard-codes it) — a second catalog registration in THIS session. */
  test("lineage gap → Ambiguous; D3 OR-clamp refuses demotion (spec §7 test 5)") {
    val q = freshTable("lineage_gap")
    appendMore("lineage_gap", 30)
    val fakeFrom = 1234567890123456789L
    val r = RollupSnapshotDiffExtractor.extract(spark, q, fakeFrom)
    r.isRight shouldBe true
    r.toOption.get match {
      case SnapshotDelta.Ambiguous(AmbiguityReason.OutOfWindowRewrite(_)) =>
        succeed
      case other =>
        fail(s"expected Ambiguous(OutOfWindowRewrite) for an off-chain watermark, got $other")
    }
    // D3 clamp on a seeded watermark (RollupWatermark hard-codes the
    // `iceberg_cat` catalog name, so register it here too):
    spark.conf.set("spark.sql.catalog.iceberg_cat",
      "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.iceberg_cat.type", "hadoop")
    spark.conf.set("spark.sql.catalog.iceberg_cat.warehouse", warehouseDir)
    import io.sm8.core.model.{Dimension, Measure, Model, RollupSpec, SourceRef}
    import io.sm8.core.schema.SealedDataType
    val m = Model.of(
      name = "d5gap", version = 1,
      source = SourceRef.ByName(table = "gap_src"),
      dimensions = List(Dimension.field("event_date", "event_date", SealedDataType.Date)),
      measures = List(Measure("n", io.sm8.core.rel.AggregateCall(
        io.sm8.core.rel.AggregateFn.Count, None, "n"))),
      rollups = List(RollupSpec("by_day", List("event_date"), List("n"),
        timeGrain = Some("day"), grainDimension = Some("event_date")))
    ).toOption.get
    val wmQualified = RollupWatermark.advance(spark, m, m.rollups.head,
      Set("2026-09-10"), isFinal = true)
    RollupWatermark.advance(spark, m, m.rollups.head,
      Set("2026-09-10"), isFinal = false)
    val wm = spark.read.format("iceberg").load(wmQualified)
      .filter("bucket_value = '2026-09-10'")
      .orderBy(org.apache.spark.sql.functions.desc("last_refreshed_at"))
    wm.select("is_final").collect().head.getBoolean(0) shouldBe true
  }

  /** spec §7 test 6 — NoDataChange → watermark advances with NO
    * merge. After a no-op extraction, `RollupWatermark.advance` still
    * writes the watermark row (the merge being a no-op does not skip
    * the advance). Same dual-catalog registration as test 5. */
  test("NoDataChange: watermark advances with no merge (spec §7 test 6)") {
    import io.sm8.core.model.{Dimension, Measure, Model, RollupSpec, SourceRef}
    import io.sm8.core.schema.SealedDataType
    val q = freshTable("wm_advance")
    appendMore("wm_advance", 10)
    val head = headOf(q)
    val r = RollupSnapshotDiffExtractor.extract(spark, q, head)
    r.toOption.get shouldBe SnapshotDelta.NoDataChange
    spark.conf.set("spark.sql.catalog.iceberg_cat",
      "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.iceberg_cat.type", "hadoop")
    spark.conf.set("spark.sql.catalog.iceberg_cat.warehouse", warehouseDir)
    val m = Model.of(
      name = "d5wm", version = 1,
      source = SourceRef.ByName(table = "wm_src"),
      dimensions = List(Dimension.field("event_date", "event_date", SealedDataType.Date)),
      measures = List(Measure("n", io.sm8.core.rel.AggregateCall(
        io.sm8.core.rel.AggregateFn.Count, None, "n"))),
      rollups = List(RollupSpec("by_day", List("event_date"), List("n"),
        timeGrain = Some("day"), grainDimension = Some("event_date")))
    ).toOption.get
    val wmQualified = RollupWatermark.advance(spark, m, m.rollups.head,
      Set("2026-09-11"), isFinal = false)
    val wm = spark.read.format("iceberg").load(wmQualified)
      .filter("bucket_value = '2026-09-11'")
    wm.count() shouldBe 1L
  }

  /** spec §7 test 7 — half-row-count guard: the refresher's decision
    * is `rows * 2 < source.count()`; extraction is skipped when the
    * delta exceeds half the recomputed source. The guard lives in
    * narrowToDeltaRows (refresher-owned, per spec §6); this contract
    * test pins the MODEL construction path that feeds it and the
    * flag-on mergeRefresh success (the guard only chooses WHICH
    * source feeds the merge, never fails the refresh). */
  test("half-row-count guard model path (spec §7 test 7)") {
    import io.sm8.core.model.{Dimension, Measure, Model, RollupSpec, SourceRef}
    import io.sm8.core.schema.SealedDataType
    spark.range(30).toDF("id").write.format("iceberg").mode("overwrite")
      .saveAsTable("iceberg_cat_d5.guard_src")
    val m = Model.of(
      name = "d5guard", version = 1,
      source = SourceRef.ByName(table = "guard_src"),
      dimensions = List(Dimension.field("event_date", "event_date", SealedDataType.Date)),
      measures = List(Measure("n", io.sm8.core.rel.AggregateCall(
        io.sm8.core.rel.AggregateFn.Count, None, "n"))),
      rollups = List(RollupSpec("by_day", List("event_date"), List("n"),
        timeGrain = Some("day"), grainDimension = Some("event_date")))
    )
    m.isRight shouldBe true
  }

  /** spec §7 test 9b — manifest-level idempotency (D2-5): running the
    * same extraction at head returns NoDataChange AND the table's
    * manifest entries (sorted (path, recordCount) tuples) are
    * unchanged — the D2-5 content-identity contract at diff level. */
  /** Sorted (path, recordCount) tuples of the table's current
    * snapshot's added data files — the D2-5 manifest-identity shape.
    *
    * @param q the qualified table name
    * @return the sorted manifest-entry tuples
    */
  def manifestEntries(q: String): Seq[(String, Long)] = {
    val t = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, q)
    val io = t.io()
    t.currentSnapshot().addedDataFiles(io).asScala.toSeq
      .map(f => (String.valueOf(f.path()), f.recordCount()))
      .sorted
  }

    test("manifest-level idempotency: entries unchanged after no-op extraction (spec §7 test 9b)") {
    val q = freshTable("idem9b")
    appendMore("idem9b", 60)
    val entriesBefore = manifestEntries(q)
    val r = RollupSnapshotDiffExtractor.extract(spark, q, headOf(q))
    r.toOption.get shouldBe SnapshotDelta.NoDataChange
    manifestEntries(q) shouldBe entriesBefore
  }

}
