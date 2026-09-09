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
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

  /** Data-file bytes grouped by partition value, read from the
    * Iceberg `.entries` metadata table. Driver-side only.
    *
    * Status filter is `status != 2` (exclude DELETED): the current
    * snapshot's live files are EXISTING (0) + ADDED (1) entries —
    * enum verified against iceberg-spark-runtime-1.5.2 (R1 reviews:
    * heron caught the zeroing bug; gorilla verified the enum order
    * and caught this comment's inverted wording).
    *
    * @param spark the SparkSession
    * @param qualifiedTable the catalog-qualified table name
    * @return map of partition key → byte total
    */
  def bytesByPartition(spark: SparkSession, qualifiedTable: String): Map[String, Long] = {
    val rows = spark.read.format("iceberg")
      .load(s"$qualifiedTable.entries")
      .filter("status != 2") // live files: EXISTING (0) + ADDED (1)
      .select("data_file.file_path", "data_file.file_size_in_bytes")
      .collect()
    rows.map { r =>
      val path = r.getString(0)
      val size = r.getLong(1)
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

  /** Decode a URL-encoded partition-key value (HadoopCatalog writes
    * `2026-09-08T17%3A00Z` for `2026-09-08T17:00:00Z`). Returns the
    * decoded string, e.g. `order_date=2026-09-08T17:00Z`.
    *
    * @param key the raw partition key from a data-file path
    * @return the URL-decoded key
    */
  def decodePartitionKey(key: String): String =
    java.net.URLDecoder.decode(key, "UTF-8")

  /** The LOCAL date string (yyyy-MM-dd, JVM default timezone) that a
    * UTC-encoded partition-key value represents. Spark writes
    * `date_trunc('day', ts)` partition values as UTC instants; the
    * local date of `2026-09-07T17:00:00Z` in UTC+7 is `2026-09-08`.
    * Gate B filters must compare on the LOCAL date, not the raw
    * encoded string (R1 gorilla C2 follow-up: the raw string contains
    * the PREVIOUS local day for any timezone east of UTC).
    *
    * @param rawPartitionKey the encoded partition key (e.g.
    *        `order_date=2026-09-07T17%3A00Z`)
    * @return the local date string, e.g. `2026-09-08`
    */
  def localDateOf(rawPartitionKey: String): String = {
    val decoded = decodePartitionKey(rawPartitionKey)
    val value = decoded.split("=", 2).drop(1).headOption.getOrElse(decoded)
    // Iceberg encodes minute-precision instants (`2026-09-07T17:00Z`,
    // no seconds) — java.time.Instant.parse rejects that form. Use
    // LocalDateTime + explicit UTC zone so both ISO forms parse.
    try {
      val normalized =
        if (value.length == 17 && value.endsWith("Z"))
          value.take(16) + ":00Z" // 2026-09-07T17:00Z -> ...T17:00:00Z
        else value
      val instant = java.time.Instant.parse(normalized)
      instant.atZone(java.time.ZoneId.systemDefault()).toLocalDate.toString
    } catch {
      case _: Exception => value.take(10) // non-timestamp key: verbatim prefix
    }
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
    // Guarded drop: DROP TABLE IF EXISTS on a v2 catalog can still
    // throw TABLE_OR_VIEW_NOT_FOUND in Iceberg 1.5.x when the
    // warehouse is fresh (the catalog metadata lookup itself fails
    // rather than returning empty). Check first, then drop.
    if (spark.catalog.tableExists(qualified)) {
      spark.sql(s"DROP TABLE IF EXISTS $qualified")
    }
    // Catalog refresh (sequential probe runs share the Spark session;
    // stale .entries metadata from the previous run can serve empty
    // byte maps for the freshly recreated table). Guarded for the
    // first run where the table does not yet exist.
    if (spark.catalog.tableExists(qualified)) {
      spark.catalog.refreshTable(qualified)
    }

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
    println(s"[probe-debug] after Tier 0: preFiles=${preFiles.size} preBytes.keys=${preBytes.keys.mkString(",")} preBytes.total=${preBytes.values.sum}")

    // ---- Tier 1: scoped refresh of 09-08 only, source narrowed to
    // that partition (the ADR-0029 operator contract).
    // H3 fix (R1 gorilla final gate): the narrowed source intentionally
    // carries the SAME 09-08 rows the full seed contained (east/15,
    // west/10) plus the new south/99 row that motivates the refresh.
    // The south row is Tier 1's delta; east/west rows are the
    // recomputed-unchanged content whose (path,size) survival the
    // rewritten-but-unchanged metric measures. Tier 0's 09-08 slice is
    // east/15 + west/10 — the same base the Tier 1 aggregation runs
    // over, so the wall-clock delta isolates the write strategy.
    // The narrowed source uses the same 09-08 timestamps as before —
    // date_trunc will produce the same partition key as the seed's
    // 09-08 rows did (which dayPrefixes now reflects).
    // Narrow the source to the scope's single local day (ADR-0029
    // operator contract: source ⊆ scope). All three rows date_trunc
    // onto the 2026-09-08 partition.
    spark.sql(
      "CREATE OR REPLACE TEMP VIEW sales_t_base AS SELECT * FROM VALUES " +
        "(timestamp'2026-09-08 10:00:00', 'east', 15L), " +
        "(timestamp'2026-09-08 11:00:00', 'west', 10L), " +
        "(timestamp'2026-09-08 13:00:00', 'south', 99L) " +
        "AS t(order_date, region, amount)")

    // Scope key MUST match the materialized partition value. With
    // UTC session TZ, date_trunc('day', ts'2026-09-08 13:00') =
    // ts'2026-09-08 00:00:00 UTC' whose string repr (which decideStrategy
    // canon()s to its first 10 chars) is "2026-09-08".
    // Scope value = the SEED's local day. decideStrategy's canon()
    // compares the source-side Timestamp string repr (local form,
    // "2026-09-08 00:00:00.0" → first 10 chars = "2026-09-08") — the
    // Iceberg partition DIRECTORY name is UTC-shifted
    // ("2026-09-07T17:00Z") but never participates in the scope
    // comparison, so the local-day form is the correct scope value.
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
    println(s"[probe-debug] after Tier 1: postFiles=${postFiles.size} postBytes.keys=${postBytes.keys.mkString(",")} t1Ms=$t1Ms")

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
      .filter { case (p, _) => localDateOf(partitionKeyOf(p)) == "2026-09-08" } else Set.empty[(String, Long)]
    val postScopedFiles = postFiles.filter(p => localDateOf(partitionKeyOf(p)) == "2026-09-08")
      .map(p => p -> postBytes.find { case (k, _) => partitionKeyOf(p) == k }
        .map(_._2).getOrElse(0L)).toSet
    val unchangedScopedFiles = preScopedFiles.intersect(postScopedFiles)
    val rewrittenButUnchangedBytes = unchangedScopedFiles.map(_._2).sum

    // H1 fix (R1 gorilla final gate): content-identity ratio on
    // UNTOUCHED partitions uses (path, file_size_in_bytes) tuples
    // per ADR-0030 §D2-5, not path-only identity. A rewriter that
    // produces a fresh path with the same content would falsely look
    // like an isolation break under path-only identity.
    val preUntouchedTuples = if (t1Success)
      filesWithSize(spark, qualified).filter { case (p, _) =>
        p.contains("order_date=") && localDateOf(partitionKeyOf(p)) != "2026-09-08" }
      .toSet[(String, Long)] else Set.empty[(String, Long)]
    val postUntouchedTuples = postFiles.filter(p => localDateOf(partitionKeyOf(p)) != "2026-09-08")
      .map(p => p -> postBytes.find { case (k, _) => partitionKeyOf(p) == k }
        .map(_._2).getOrElse(0L)).toSet
    val overlap = if (preUntouchedTuples.isEmpty) 1.0
      else preUntouchedTuples.intersect(postUntouchedTuples).size.toDouble / preUntouchedTuples.size.toDouble

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
      // Touched = total after minus untouched-preserved (total after
      // is 3 files; untouched are the two non-refreshed partitions).
      // Filtering by localDate == seed day fails under the UTC
      // directory encoding (the touched partition's directory name
      // carries the shifted date). Computed as: post total −
      // untouched-partition bytes.
      tier1TouchedBytes = postBytes.filter { case (k, _) =>
        localDateOf(k) != "2026-09-08" }.values.sum,
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
