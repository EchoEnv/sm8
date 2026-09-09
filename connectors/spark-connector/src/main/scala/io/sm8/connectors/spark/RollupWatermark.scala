/*
 * SM8 Spark Connector — RollupWatermark (ADR-0030 D3, Tier 2).
 *
 * The refresh watermark table: one row per (model, rollup, bucket)
 * recording that bucket's freshness verdict — `is_final` (lateness
 * window closed; no further re-processing expected),
 * `last_refreshed_at` (driver wall clock), `last_commit_snapshot_id`
 * (the Iceberg snapshot id of the data commit that produced the
 * state — diagnostic joins, and the staleness re-derivation rule:
 * a watermark row whose snapshot id does not match the rollup's
 * current snapshot is stale and re-derived on the next refresh).
 *
 * ==Sequential, not atomic (honest limits)==
 *
 * Two commits to two Iceberg tables are NEVER cross-table atomic
 * (ADR-0030 D3, teal C2). The watermark commit FOLLOWS the data
 * commit and only after it returned success; a crash between them
 * leaves the watermark stale-but-valid — data is current, the
 * watermark simply does not yet reflect it, and it never claims
 * MORE final than the data (monotonicity: is_final never regresses
 * true → false for a given bucket; the merge is an OR, not a
 * blind overwrite).
 *
 * ==Core boundary (RFC §3)==
 *
 * The watermark table lives entirely connector side. Core consumes
 * freshness verdicts ONLY through the refusal vocabulary
 * (`RollupRewriteRefusal.RollupBucketStale`) — the rewriter never
 * instantiates that case; this object's `nonFinalBuckets` feeds the
 * connector's resolution layer, which does.
 *
 * ==Why a table (not a table property / metastore tag)==
 *
 * Per-bucket rows with query semantics: the resolution layer asks
 * "are THESE buckets final?" per query. A property blob would need
 * parsing on every lookup and caps at metastore metadata size.
 */
package io.sm8.connectors.spark

import io.sm8.core.model.{Model, RollupSpec}
import io.sm8.core.rel.RollupRewriter

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Create, read, and advance the rollup watermark table.
  *
  * All entry points are idempotent-safe: `ensureTable` is
  * create-if-absent; `advance` merges with is_final-OR monotonicity.
  */
object RollupWatermark {

  /** The Iceberg catalog (shared with RollupMaterializer). */
  private val IcebergCatalog = "iceberg_cat"

  /** The watermark table name for a rollup (deterministic sibling:
    * `<model>__<rollup>__watermark` in the same catalog). */
  def tableName(model: Model, spec: RollupSpec): String =
    s"${RollupRewriter.rollupTableName(model, spec)}__watermark"

  /** Ensure the watermark table exists (create-if-absent,
    * driver-side DDL; idempotent — second call is a no-op check). */
  def ensureTable(spark: SparkSession, model: Model, spec: RollupSpec): String = {
    val qualified = s"$IcebergCatalog.${tableName(model, spec)}"
    if (!spark.catalog.tableExists(qualified)) {
      val empty = spark.createDataFrame(
        java.util.Collections.emptyList[org.apache.spark.sql.Row](),
        StructType(Seq(
          StructField("model_name", StringType, nullable = false),
          StructField("rollup_name", StringType, nullable = false),
          StructField("bucket_value", StringType, nullable = false),
          StructField("is_final", BooleanType, nullable = false),
          StructField("last_refreshed_at", TimestampType),
          StructField("last_commit_snapshot_id", LongType))))
      empty.writeTo(qualified).create()
    }
    qualified
  }

  /** The buckets a query would touch that are NOT final.
    *
    * The resolution-layer read (D3): given the rollup and the
    * queried bucket values, return the subset whose watermark rows
    * are absent (never refreshed — treat as non-final, fail-safe)
    * or explicitly non-final. A bucket with NO row is non-final by
    * absence (never assume freshness from silence).
    *
    * @param spark       the session
    * @param model       the host model
    * @param spec        the rollup declaration
    * @param queryBuckets the canonical bucket values the query touches
    * @return the non-final subset of `queryBuckets` (empty = all final)
    */
  def nonFinalBuckets(
    spark: SparkSession,
    model: Model,
    spec: RollupSpec,
    queryBuckets: Set[String]): Set[String] = {
    val qualified = s"$IcebergCatalog.${tableName(model, spec)}"
    if (!spark.catalog.tableExists(qualified)) queryBuckets // absent table = nothing proven final
    else {
      val rows = spark.table(qualified)
        .filter(col("model_name") === lit(model.name) &&
                col("rollup_name") === lit(spec.name))
        .select("bucket_value", "is_final")
        .collect()
      val finalOnes = rows.filter(_.getBoolean(1)).map(_.getString(0)).toSet
      queryBuckets -- finalOnes
    }
  }

  /** Build the RollupBucketStale refusal for the non-final subset,
    * if the rollup's policy demands finality (ADR-0030 D3 emission
    * rule: policy-gated; policy-less rollups route normally). This
    * is the connector-side instantiation site the rewriter never
    * performs.
    *
    * @return Some(refusal) when the policy is FinalRequired and at
    *         least one queried bucket is non-final; None otherwise
    */
  def stalenessRefusal(
    spark: SparkSession,
    model: Model,
    spec: RollupSpec,
    queryBuckets: Set[String])
  : Option[io.sm8.core.rel.RollupRewriter.RollupRewriteRefusal.RollupBucketStale] =
    spec.freshness match {
      case Some(io.sm8.core.model.FreshnessPolicy.FinalRequired) =>
        val nonFinal = nonFinalBuckets(spark, model, spec, queryBuckets)
        if (nonFinal.isEmpty) None
        else Some(RollupRewriter.RollupRewriteRefusal
          .RollupBucketStale(nonFinal.map(RollupRewriter.RollupRewriteRefusal.BucketKey(_))))
      case _ => None // no policy = pre-Tier-2 routing (D3 default)
    }

  /** Advance the watermark AFTER a successful data commit (D3
    * ordering: sequential, watermark follows data, never before).
    *
    * Monotonicity contract: `is_final` only ever moves false → true
    * for a bucket (the merge ORs the incoming verdict with the
    * stored one; a refresh that would demote finality is a no-op on
    * that column). `last_commit_snapshot_id` records the rollup
    * table's CURRENT snapshot (the data commit just landed).
    *
    * @param spark      the session
    * @param model      the host model
    * @param spec       the rollup declaration
    * @param buckets    the bucket values this refresh covered
    * @param isFinal    whether the lateness window closed for them
    * @return the qualified watermark table name (written)
    */
  def advance(
    spark: SparkSession,
    model: Model,
    spec: RollupSpec,
    buckets: Set[String],
    isFinal: Boolean): String = {
    val qualified = ensureTable(spark, model, spec)
    val now = java.time.Instant.now()
    val snapshotId = currentSnapshotId(spark,
      s"$IcebergCatalog.${RollupRewriter.rollupTableName(model, spec)}")
    import spark.implicits._
    val incoming = buckets.toSeq.map(b =>
      (model.name, spec.name, b, isFinal,
       java.sql.Timestamp.from(now), snapshotId)).toDF(
      "model_name", "rollup_name", "bucket_value", "is_final",
      "last_refreshed_at", "last_commit_snapshot_id")
    val view = s"tier2_wm_src_${spec.name}"
    incoming.createOrReplaceTempView(view)
    // Monotone OR-merge on is_final: matched rows keep finality if
    // they already had it (a demotion attempt is a no-op on that
    // column — the contract's "never regresses" clause, enforced by
    // the merge shape itself, not by caller discipline).
    spark.sql(
      s"""MERGE INTO $qualified t
         |USING $view s
         |ON t.model_name = s.model_name AND t.rollup_name = s.rollup_name
         |   AND t.bucket_value = s.bucket_value
         |WHEN MATCHED THEN UPDATE SET
         |  t.is_final = t.is_final OR s.is_final,
         |  t.last_refreshed_at = s.last_refreshed_at,
         |  t.last_commit_snapshot_id = s.last_commit_snapshot_id
         |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
    spark.catalog.dropTempView(view)
    qualified
  }

  /** The rollup table's current Iceberg snapshot id (0 when the
    * table is too new to expose it — the column is diagnostic, not
    * correctness-critical; D3's re-derivation treats mismatches as
    * stale, and 0 never matches, which is the conservative read). */
  private[spark] def currentSnapshotId(
    spark: SparkSession,
    qualifiedTable: String): Long =
    try {
      val rows = spark.read.format("iceberg")
        .load(s"$qualifiedTable.snapshots")
        .select(col("snapshot_id"))
        .collect()
      if (rows.isEmpty) 0L else rows.map(_.getLong(0)).max
    } catch {
      case scala.util.control.NonFatal(_) => 0L
    }
}
