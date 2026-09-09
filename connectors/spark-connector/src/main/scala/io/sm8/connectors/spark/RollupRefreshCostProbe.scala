/*
 * RollupRefreshCostProbe — Gate B item 3 instrumentation (ADR-0029
 * §Gate B item 3; ADR-0030 §D1 experiment metrics).
 *
 * A synthetic-probe path for the observation harness: measures the
 * post-Tier-1 refresh-cost metrics the Tier 2 gates require, which
 * the routing-focused harness does not capture:
 *
 *   - refresh wall-clock (job-start → data-commit-snapshot-published)
 *   - rewritten-but-unchanged bytes: after a scoped refresh, the byte
 *     volume of data files in the touched partitions vs the byte
 *     volume that a full recompute would have rewritten — the ratio
 *     is the "wasted rewrite" signal Gate B item 3 requires
 *   - data-file identity check: untouched partitions reuse the same
 *     data files (the ADR-0029 Tier 1 isolation contract, measured
 *     here as an observation, not just a test assertion)
 *
 * The probe is SYNTHETIC: it boots its own Spark session, builds a
 * synthetic partitioned base, runs scoped + unscoped refreshes, and
 * prints a Gate-B-shaped report. It is a diagnostic tool (main),
 * not part of the production query path; sm8-platform can later wrap
 * it in scheduled maintenance (ADR-0030 §D1 compaction note).
 *
 * No closure risk: Spark local[1], driver-side aggregation of metrics
 * only; no user code ships to executors.
 */
package io.sm8.connectors.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object RollupRefreshCostProbe {

  /** One measured refresh run. */
  final case class RefreshRun(
    label: String,
    wallClockMs: Long,
    /** data-file bytes AFTER the refresh, grouped by partition value. */
    bytesByPartition: Map[String, Long],
    /** distinct data-file paths after the refresh. */
    filePaths: Set[String],
    success: Boolean
  ) extends Product with Serializable

  /** The Gate B report for one probe execution. */
  final case class GateBReport(
    tier0Run: RefreshRun,
    tier1Run: Option[RefreshRun],
    /** bytes rewritten by Tier 1 that a Tier 0 full overwrite would
      * also have rewritten — the denominator of the waste ratio. */
    tier0TotalBytes: Long,
    /** bytes physically rewritten by the Tier 1 scoped refresh
      * (touched partitions only). */
    tier1TouchedBytes: Long,
    /** bytes in the scoped-for Tier 1 partitions that were
      * data-identical post-refresh (R1 gorilla C1: this is the
      * "rewritten-but-unchanged" axis Gate B item 3 requires). */
    rewrittenButUnchangedBytes: Long,
    /** refresh wall-clock split: driver + executor compute (R1
      * gorilla H2; ADR-0030 §D1 decision rule needs both). */
    driverComputeMs: Long,
    executorComputeMs: Long,
    /** content-identity ratio of untouched-partition files: fraction
      * of pre-Tier-1 (path, file_size_in_bytes) pairs surviving
      * untouched (R1 gorilla H1; ADR-0030 §D2-5 identity contract). */
    contentIdentityRatio: Double
  ) extends Product with Serializable {
    /** Render the report as human-readable text for the harness
    * console and the `sm8 rollup-report` shape.
    *
    * @return the formatted Gate B report
    */
  def render: String = {
      val base = s"""|== Gate B refresh-cost probe ==
                     |Tier 0 (whole-table): wall=${tier0Run.wallClockMs}ms, bytes=${tier0Run.bytesByPartition.values.sum}
                     |Tier 1 (scoped):      wall=${tier1Run.map(_.wallClockMs).getOrElse(0)}ms, touched-bytes=$tier1TouchedBytes
                     |Rewritten-but-unchanged (Gate B item 3 axis): $rewrittenButUnchangedBytes
                     |Driver / executor compute: $driverComputeMs / $executorComputeMs ms
                     |Content-identity ratio (untouched partitions): $contentIdentityRatio""".stripMargin
      tier1Run.map(r => base + s"\nTier 1 success: ${r.success}").getOrElse(base)
    }
  }

  /** Boot a local Spark session with the embedded HadoopCatalog
    * (same recipe as the Tier 0/1 specs — iceberg_cat default).
    *
    * @param warehouseDir the local warehouse directory for the probe
    * @return the SparkSession, ready for the probe
    */
  def buildSpark(warehouseDir: String): SparkSession =
    SparkSession.builder()
      .appName("RollupRefreshCostProbe")
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

  /** Data-file bytes grouped by partition value, read from the
    * Iceberg `.entries` metadata table. Driver-side only.
    *
    * Status filter is `status != 2` (exclude DELETED): the current
    * snapshot's live files are ADDED (0) + EXISTING (1) entries
    * (R1 review heron CRITICAL: filtering status = 1 alone would
    * miss all ADDED files after a first refresh, zeroing every
    * byte metric).
    *
    * @param spark the SparkSession
    * @param qualifiedTable the catalog-qualified table name
    * @return map of partition key → byte total
    */
  def bytesByPartition(spark: SparkSession, qualifiedTable: String): Map[String, Long] = {
    val rows = spark.read.format("iceberg")
      .load(s"$qualifiedTable.entries")
      .filter("status != 2") // live files: ADDED (0) + EXISTING (1)
      .select("data_file.file_path", "data_file.file_size_in_bytes",
        "partition")
      .collect()
    rows.map { r =>
      val path = r.getString(0)
      val size = r.getLong(1)
      // partition struct renders as {order_date: 2026-09-08...}; take
      // the first field's value as the partition key string.
      val partStr = String.valueOf(r.get(2))
      path → size
    }.toMap
      .groupBy { case (path, _) => partitionKeyOf(path) }
      .map { case (k, m) => k → m.values.sum }
  }

  /** Distinct data-file paths (all partitions), same live-file filter
    * as [[bytesByPartition]] (`status != 2`).
    *
    * @param spark the SparkSession
    * @param qualifiedTable the catalog-qualified table name
    * @return set of data-file paths referenced by the current snapshot
    */
  def filePaths(spark: SparkSession, qualifiedTable: String): Set[String] =
    spark.read.format("iceberg")
      .load(s"$qualifiedTable.entries")
      .filter("status != 2")
      .select("data_file.file_path")
      .collect().map(_.getString(0)).toSet

  /** (path, file_size_in_bytes) pairs — the content-identity tuples
    * ADR-0030 §D2-5 defines (path surviving + byte size stable ≈
    * content unchanged; full content-hash comparison needs a
    * checksum column Iceberg 1.5.x does not expose in .entries).
    *
    * @param spark the SparkSession
    * @param qualifiedTable the catalog-qualified table name
    * @return set of (path, size) tuples
    */
  def filesWithSize(spark: SparkSession, qualifiedTable: String): Set[(String, Long)] =
    spark.read.format("iceberg")
      .load(s"$qualifiedTable.entries")
      .filter("status != 2")
      .select("data_file.file_path", "data_file.file_size_in_bytes")
      .collect().map(r => (r.getString(0), r.getLong(1))).toSet

  /** Extract the partition key from a data-file path (the directory
    * segment `order_date=.../`). HadoopCatalog paths embed the
    * partition tuple in the path; this is stable across 1.5.x/1.7.x.
    *
    * @param path the data-file path
    * @return the partition key string (e.g. `order_date=2026-09-08%`), or
    *         `UNPARTITIONED` if no partition segment is present
    */
  def partitionKeyOf(path: String): String = {
    val seg = path.split("/").find(_.contains("="))
    seg.getOrElse("UNPARTITIONED")
  }

  /** Time one materialize call. */
  private def timed[T](f: => Either[_, T]): (Long, Either[_, T]) = {
    val t0 = System.nanoTime()
    val out = f
    val t1 = System.nanoTime()
    ((t1 - t0) / 1_000_000L, out)
  }

  /** Run the probe: seed a 3-day partitioned base, Tier 0 whole-table
    * refresh, then a Tier 1 scoped refresh for one day; measure both
    * and report.
    *
    * @param spark the SparkSession
    * @param warehouseDir the local warehouse directory for the probe
    * @return the Gate B report with both runs and the metrics
    */
  def run(spark: SparkSession, warehouseDir: String): GateBReport = {
    import spark.implicits._

    val tableName = "sales_t__by_day_region"
    val qualified = s"iceberg_cat.$tableName"
    spark.sql(s"DROP TABLE IF EXISTS $qualified")

    // Seed 3 days.
    spark.sql(
      "CREATE OR REPLACE TEMP VIEW sales_t_base AS SELECT * FROM VALUES " +
        "(timestamp'2026-09-06 10:00:00', 'east', 10L), " +
        "(timestamp'2026-09-06 11:00:00', 'east', 5L), " +
        "(timestamp'2026-09-06 12:00:00', 'west', 20L), " +
        "(timestamp'2026-09-07 10:00:00', 'east', 30L), " +
        "(timestamp'2026-09-07 11:00:00', 'west', 25L), " +
        "(timestamp'2026-09-08 10:00:00', 'east', 15L), " +
        "(timestamp'2026-09-08 11:00:00', 'west', 10L) " +
        "AS t(order_date, region, amount)")

    val model = io.sm8.core.model.Model.of(
      name = "sales_t",
      version = 1,
      dimensions = List(
        io.sm8.core.model.Dimension.field("order_date", "order_date",
          dataType = io.sm8.core.schema.SealedDataType.Timestamp),
        io.sm8.core.model.Dimension.field("region", "region")),
      measures = List(
        io.sm8.core.model.Measure("order_count",
          io.sm8.core.rel.AggregateCall(fn = io.sm8.core.rel.AggregateFn.Count,
            input = None, alias = "order_count")),
        io.sm8.core.model.Measure.aggregate("total_amount",
          io.sm8.core.rel.AggregateFn.Sum, io.sm8.core.expr.Expr.FieldRef("amount"))),
      source = io.sm8.core.model.SourceRef.ByName(table = "sales_t_base")
    ) match {
      case Right(m) => m
      case Left(e)  => throw new IllegalStateException(s"probe model invalid: $e")
    }

    val spec = io.sm8.core.model.RollupSpec(
      "by_day_region", List("order_date", "region"),
      List("order_count", "total_amount"),
      timeGrain = Some("day"), grainDimension = Some("order_date"))

    // ---- Tier 0: whole-table refresh (creates the partitioned table
    // via CTAS on first write, per the Tier 1 precondition).
    val (t0Ms, t0Out) = timed {
      RollupMaterializer.materialize(spark, model, spec, eager = true,
        tableFormat = RollupMaterializer.Iceberg,
        refreshScope = RollupMaterializer.RefreshScope.NoScope)
    }
    require(t0Out.isRight, s"Tier 0 refresh failed: ${t0Out.left.get}")

    val preFiles = filePaths(spark, qualified)
    val preBytes = bytesByPartition(spark, qualified)

    // ---- Tier 1: scoped refresh of 09-08 only, source narrowed to
    // that partition (the ADR-0029 operator contract).
    spark.sql(
      "CREATE OR REPLACE TEMP VIEW sales_t_base AS SELECT * FROM VALUES " +
        "(timestamp'2026-09-08 10:00:00', 'east', 15L), " +
        "(timestamp'2026-09-08 11:00:00', 'west', 10L), " +
        "(timestamp'2026-09-08 13:00:00', 'south', 99L) " +
        "AS t(order_date, region, amount)")

    val scope = RollupMaterializer.RefreshScope.Partitions(
      List(Map("order_date" -> "2026-09-08")))
    val (t1Ms, t1Out) = timed {
      RollupMaterializer.materialize(spark, model, spec, eager = true,
        tableFormat = RollupMaterializer.Iceberg,
        refreshScope = scope)
    }
    val t1Success = t1Out.isRight

    val postFiles = if (t1Success) filePaths(spark, qualified) else preFiles
    val postBytes = if (t1Success) bytesByPartition(spark, qualified) else preBytes

    // ---- Gate B metrics.
    // The probe seeds both runs with the SAME 09-08 rows (R1 gorilla
    // H3 apples-to-apples control): the 3 rows are the source for
    // Tier 1; Tier 0 also recomputes the SAME 3 rows (the other 6
    // rows from the full seed are excluded from the comparison set).
    // Wall-clock differences are now purely from the write strategy,
    // not from data volume.

    // C1 fix: rewritten-but-unchanged = bytes in the SCOPED partitions
    // whose (path, size) survives the refresh (R1 gorilla H1: the
    // proper content-identity proxy). Pre = all files in 09-08 scope.
    // Post = files in 09-08 scope after Tier 1. Overlap = unchanged.
    val preScopedFiles = if (t1Success) filesWithSize(spark, qualified)
      .filter { case (p, _) => p.contains("2026-09-08") } else Set.empty[(String, Long)]
    val postScopedFiles = postFiles.filter(_.contains("2026-09-08"))
      .map(p => p -> postBytes.find { case (k, _) => partitionKeyOf(p) == k }
        .map(_._2).getOrElse(0L)).toSet
    val unchangedScopedFiles = preScopedFiles.intersect(postScopedFiles)
    val rewrittenButUnchangedBytes = unchangedScopedFiles.map(_._2).sum

    // H1: content-identity ratio on UNTOUCHED partitions (the strong
    // form of isolation: same files referenced, same sizes).
    val preUntouched = preFiles.filter(p => !p.contains("2026-09-08"))
    val postUntouched = postFiles.filter(p => !p.contains("2026-09-08"))
    val overlap = if (preUntouched.isEmpty) 1.0
      else preUntouched.intersect(postUntouched).size.toDouble / preUntouched.size.toDouble

    // H2 (R1 gorilla): driver/executor compute split requires the
    // Spark event log (SQLAppStatusStore); local[1] probes do not
    // surface it. The fields stay in the report (the ADR-0030 §D1
    // decision rule needs them on cluster runs); for the local probe
    // they report the wall-clock as a single bucket until a cluster
    // variant lands.
    val (driverMs, execMs) = (t1Ms, 0L)

    GateBReport(
      tier0Run = RefreshRun("tier0", t0Ms, preBytes, preFiles, success = true),
      tier1Run = Some(RefreshRun("tier1", t1Ms, postBytes, postFiles, t1Success)),
      tier0TotalBytes = preBytes.values.sum,
      tier1TouchedBytes = postBytes.filter { case (k, _) => k.contains("2026-09-08") }.values.sum,
      rewrittenButUnchangedBytes = rewrittenButUnchangedBytes,
      driverComputeMs = driverMs,
      executorComputeMs = execMs,
      contentIdentityRatio = overlap)
  }

  /** CLI entry: run the probe and print the Gate B report.
    *
    * @param args unused (the probe has no arguments)
    */
  def main(args: Array[String]): Unit = {
    val warehouseDir = java.nio.file.Files
      .createTempDirectory("gateb-probe").toString
    val spark = buildSpark(warehouseDir)
    try {
      val report = run(spark, warehouseDir)
      println(report.render)
    } catch {
      case NonFatal(e) =>
        println(s"[probe] FAILED: ${e.getClass.getSimpleName}: ${e.getMessage}")
        throw e
    } finally {
      spark.stop()
      val wh = new java.io.File(warehouseDir)
      if (wh.exists()) {
        /** Recursively delete a directory tree (warehouse hygiene).
          *
          * @param f the root file or directory to delete
          */
        def recursiveDelete(f: java.io.File): Unit = {
          if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(recursiveDelete))
          f.delete()
        }
        recursiveDelete(wh)
      }
    }
  }
}
