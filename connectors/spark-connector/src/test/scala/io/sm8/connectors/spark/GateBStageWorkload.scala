/*
 * SM8 ops — Gate B representative-workload stager.
 *
 * Synthesizes the `rep_events` base table into the Gate B bootstrap
 * warehouse (embedded HadoopCatalog) so GateBTraceRunner --model-path
 * (live-model mode) can measure a scoped + whole-table refresh against
 * it. Sized for the 7.5 GiB dev host: ~1M rows over 7 days × 4 regions
 * (jvm-safety mantra 3: nothing here grows unbounded; the row count is
 * a fixed constant).
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
  val RowsPerDay = 150000 // ~1.05M rows total
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
      // Write the base under BOTH catalogs:
      //   - iceberg_cat.rep_events: matches the connector's rollup-side
      //     discovery (the rollup table is created via iceberg_cat)
      //   - spark_catalog.default.rep_events (parquet): matches the
      //     probe's unqualified spark.table("rep_events") call in
      //     RollupRefreshCostProbe.runModel. Without this second write
      //     the probe reports "table not found" against spark_catalog.
      base.write.format("iceberg").mode("overwrite")
        .saveAsTable("iceberg_cat.rep_events")
      // The unqualified spark_catalog registration is a CONVENIENCE for
      // the probe's spark.table("rep_events") pre-count (best-effort,
      // failure tolerated) — the model yaml now uses the qualified
      // iceberg_cat.rep_events name, so the probe's real path is the
      // iceberg table above. Try/catch: a leftover spark-warehouse dir
      // from a prior run makes the managed-table registration fail
      // (LOCATION_ALREADY_EXISTS); the iceberg table is what matters.
      try {
        base.write.mode("overwrite").saveAsTable("default.rep_events")
      } catch {
        case e: Throwable if e.getMessage != null &&
            e.getMessage.contains("LOCATION_ALREADY_EXISTS") =>
          println("GATEB-STAGE: default.rep_events registration skipped " +
            "(leftover spark-warehouse location; probe pre-count degraded, " +
            "probe path unaffected — model uses the qualified name)")
      }
      val n = spark.table("iceberg_cat.rep_events").count()
      println(s"GATEB-STAGE: rep_events written, rows=$n, days=$Days, regions=${Regions.size}")
      require(n == RowsPerDay.toLong * Days, s"row count mismatch: $n")
      println("GATEB-STAGE: PASS")
    } finally spark.stop()
  }
}
