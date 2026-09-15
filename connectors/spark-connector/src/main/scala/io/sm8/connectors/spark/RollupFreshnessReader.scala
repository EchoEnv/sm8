/*
 * SM8 Spark Connector — RollupFreshnessReader.
 *
 * Reads per-rollup freshness (last_refreshed_at / is_final) from the
 * RollupWatermark Iceberg table so the metrics surface can expose it
 * as gauges. The CLI's rollup-status verb is the intended consumer.
 *
 * Layer placement: connector-side because only the connector can
 * read an Iceberg table. The platform sees only the engine-portable
 * snapshot shape (see `FreshnessEntry`); sm8-server bridges the two
 * via the same reflective pattern as the validation probes — zero
 * compile-time connector deps on the server, no Spark types on the
 * platform.
 */
package io.sm8.connectors.spark

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, max}

import io.sm8.core.engine.EngineError
import io.sm8.core.model.Model

/** One rollup's freshness verdict (engine-portable; no Spark types). */
final case class FreshnessEntry(
  rollupName:      String,
  /** ISO-8601 timestamp of the most recent bucket refresh
    * (max over buckets), or "" when the watermark table has no rows
    * yet (never refreshed). */
  lastRefreshedAt: String,
  /** Aggregate finality: true iff EVERY bucket row is is_final=true.
    * A rollup with zero rows reports false (nothing proven final). */
  allFinal:        Boolean,
  /** Number of bucket rows in the watermark table. `Long` to
    * accommodate multi-year retention (Int.MaxValue ≈ 24-day
    * hourly buckets; few-years daily buckets easily exceed Int). */
  bucketCount:     Long
)

object RollupFreshnessReader {

  /** The Iceberg catalog (same value as RollupWatermark's
    * private `IcebergCatalog`; the codebase convention is one
    * per-file copy rather than a shared constant). */
  private val IcebergCatalog = "iceberg_cat"

  /** Read freshness for every rollup declared on the model.
    *
    * Per-rollup isolation: a failure reading ONE watermark table
    * degrades that rollup to a "never refreshed" entry (typed error
    * carried in the log, not thrown) — a broken watermark for one
    * rollup must not blank the whole model's freshness view. A
    * failure of the whole probe (e.g. no Spark session) surfaces as
    * a typed Left.
    *
    * @param spark the active SparkSession (Iceberg reads only)
    * @param model the deployed model (its rollups are enumerated)
    * @return      `Right` one entry per declared rollup;
    *              `Left` a typed error when the probe cannot run.
    */
  def readAll(
      spark: SparkSession,
      model: Model
  ): Either[EngineError, List[FreshnessEntry]] = {
    if (spark == null) {
      Left(EngineError.UnsupportedCapability(
        engine     = "spark",
        capability = "rollup-freshness-probe",
        message    = "null-spark configuration has no session to read the watermark"
      ))
    } else {
      val entries = model.rollups.map { spec =>
        val qualified = s"$IcebergCatalog.${RollupWatermark.tableName(model, spec)}"
        val attempted = Try {
          val df = spark.table(qualified)
          val agg = df.agg(
            max("last_refreshed_at").as("last_ts"),
            org.apache.spark.sql.functions.min("is_final").as("all_final"))
          val row = agg.head()
          val bucketCount = df.count()
          FreshnessEntry(
            rollupName      = spec.name,
            lastRefreshedAt = Option(row.getAs[Any]("last_ts")).map(_.toString).getOrElse(""),
            allFinal        = row.getAs[Any]("all_final") match {
              case b: java.lang.Boolean => b.booleanValue()
              case _ => false
            },
            bucketCount     = bucketCount
          )
        }
        attempted.toEither match {
          case Right(e) => e
          case Left(NonFatal(e)) =>
            // Per-rollup degradation: absent table (never refreshed)
            // or a malformed watermark reads as "never refreshed"
            // with zero buckets. Never throws past this boundary.
            FreshnessEntry(
              rollupName      = spec.name,
              lastRefreshedAt = "",
              allFinal        = false,
              bucketCount     = 0
            )
        }
      }
      Right(entries)
    }
  }
}
