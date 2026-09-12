/*
 * SM8 ops — Gate B representative-workload stager.
 *
 * Synthesizes the `rep_events` base table into the Gate B bootstrap
 * warehouse (embedded HadoopCatalog) so GateBTraceRunner --model-path
 * (live-model mode) can measure a scoped + whole-table refresh against
 * it. Sized for the 7.5 GiB dev host: the row count is a fixed
 * constant (jvm-safety mantra 3: nothing here grows unbounded).
 *
 * [[RowsPerDay]] is operator-tunable via the GATEB_ROWS_PER_DAY env
 * var (default 150000 ≈ 1.05M rows). The 2026-09-12 aarch64 SIGSEGV
 * investigation (hs_err: G1 WeakProcessor / SymbolTable / netty
 * directBufferAddress — 3 distinct crash sites across Temurin 17,
 * Corretto 17, and Temurin 21) showed the Tier 0 whole-table rewrite
 * of 1.05M rows is the crash trigger on this host; a smaller staging
 * run (e.g. GATEB_ROWS_PER_DAY=10000 → 70k rows) completes cleanly
 * and still exercises the full Tier 0 → Tier 1 code path. Row-count
 * scaling does NOT change which code path runs — only the magnitude
 * of the measured wall-clock — so a small run is a valid harness
 * smoke-test (production Gate B numbers need the full representative
 * scale on appropriately provisioned hardware, per ADR-0029).
 *
 * Run via the connector test classpath (spark-shell style through
 * scala-cli is NOT installed on this host); the established pattern is
 * a throwaway spec — see scripts/gate-b-stage.sh which wraps this.
 */
package io.sm8.connectors.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GateBStageWorkload {
  val Days = 7
  /** Rows per day partition; env-tunable for small hosts (see class
    * Scaladoc: the 1.05M-row default SIGSEGVs during Tier 0 on the
    * 7.5 GiB aarch64 dev host — smaller values keep the same code
    * path at a runnable magnitude). */
  val RowsPerDay: Int =
    sys.env.get("GATEB_ROWS_PER_DAY").flatMap(v => v.toIntOption)
      .getOrElse(150000)
  val Regions = Seq("emea", "apac", "nama", "latam")

  def main(args: Array[String]): Unit = {
    val warehouse = args.headOption.getOrElse(
      throw new IllegalArgumentException("usage: GateBStageWorkload <warehouse-dir>"))
    val spark = SparkSession.builder()
      .appName("gateb-stage-workload")
      .master("local[2]")
      // spark_catalog's warehouse is pinned to the SAME bootstrap dir
      // the GateBTraceRunner session uses — so the unqualified
      // spark.table("rep_events") lookup in RollupRefreshCostProbe
      // resolves against this staging session's writes regardless of
      // the runner process's CWD (a plain ./spark-warehouse default is
      // CWD-relative and would land wherever the stager happened to
      // run from).
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.sql.catalog.iceberg_cat",
        "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_cat.type", "hadoop")
      .config("spark.sql.catalog.iceberg_cat.warehouse", warehouse)
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try {
      // Deterministic pseudo-random amounts (no RNG dependency: hash-based).
      val base = spark.range(RowsPerDay.toLong * Days)
        .withColumn("day_off", (col("id") / RowsPerDay).cast("int"))
        .withColumn("event_date", date_add(lit(java.sql.Date.valueOf("2026-09-01")), col("day_off")))
        .withColumn("region", element_at(
          array(lit("emea"), lit("apac"), lit("nama"), lit("latam")),
          (col("id") % 4 + 1).cast("int")))
        .withColumn("amount", (col("id") % 997) / 10.0)
        .select("event_date", "region", "amount")
      // Write the base once, against iceberg_cat (the only catalog
      // configured). The RollupRefreshCostProbe live-model path reads
      // it via `spark.table(<unqualified>)`, which resolves to
      // iceberg_cat.default.rep_events (Spark's catalog lookup
      // returns the only configured catalog for unqualified names).
      // Earlier revisions also wrote a parquet copy under
      // spark_catalog.default.rep_events at the same path, which
      // always failed with LOCATION_ALREADY_EXISTS on a fresh
      // warehouse and was masking the JVM crash we'd been chasing.
      base.write.format("iceberg").mode("overwrite")
        .saveAsTable("iceberg_cat.rep_events")
      val n = spark.table("iceberg_cat.rep_events").count()
      println(s"GATEB-STAGE: rep_events written, rows=$n, days=$Days, regions=${Regions.size}")
      require(n == RowsPerDay.toLong * Days, s"row count mismatch: $n")
      println("GATEB-STAGE: PASS")
    } finally spark.stop()
  }
}
