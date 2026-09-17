package com.example.flight

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

import io.sm8.core.model.ModelValidator
import io.sm8.core.engine.{
  EngineContext, EngineIdentity, EngineProvider, QueryRequest,
  ResultValue
}
import io.sm8.core.expr.ExprSugar._
import io.sm8.core.query.QueryBuilder
import io.sm8.connectors.spark.SparkSourceResolver
import io.sm8.core.model.{
  CalculatedMeasure, Dimension, FreshnessPolicy, Measure, Model, ModelBuilder,
  ModelStatus, RollupSpec, SourceRef
}
import io.sm8.core.rel.AggregateFn

/** sm8 flight-delays example — the full time-series rollup freshness
  * lifecycle on a day-grained flights table.
  *
  * Scenario (an airline ops team):
  *  1. INGEST batch 1 (flights for Sep 7-9) into a temp view.
  *  2. DECLARE the model via the `ModelBuilder` DSL with a daily ALOS-style
  *    rollup (avg delay per airline/day) carrying `FreshnessPolicy.FinalRequired`.
  *  3. MATERIALIZE the rollup, then LATCH the watermark final for past days
  *    (the ops action that marks "this day is closed" in a real deployment).
  *  4. QUERY avg delay by airline/day — the router serves it from the rollup.
  *  5. INGEST batch 2 (Sep 10, a late-arriving day) into the base view.
  *     The Sep 10 bucket is NOT yet latched final: the freshness gate now
  *    refuses routing for it (`RollupBucketStale`) and the query falls back
  *    to the base table (honest fail-closed semantics, not silent staleness).
  *  6. REFRESH the rollup for the late bucket (Tier-2 `mergeRefresh` +
  *    watermark advance — the D3 ordering: watermark follows the data commit),
  *    then re-query: routing succeeds again on a now-final rollup.
  *  7. Print a Prometheus-format metrics summary (rollup routing counters).
  *
  * Every step prints an explicit log line, so the full lifecycle is
  * readable straight from `mvn scala:run` output.
  */
object Main {

  private val Logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  // ====== Ingest (batch readers) ======

  /** Read a CSV batch (with header) and cast the date column.
    *
    * flight_date is cast to `date` at the read boundary so every
    * downstream stage sees one type (schema drift caught once, at
    * the edge).
    */
  private def readCsv(spark: SparkSession, filename: String): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"data/$filename")
      .withColumn("flight_date", col("flight_date").cast("date"))

  // ====== Declare (ModelBuilder DSL) ======

  /** Build the flights `Model` via the fluent `ModelBuilder` DSL.
    *
    * Declares one rollup: daily average delay per airline, day-grained on
    * `flight_date`, with `FreshnessPolicy.FinalRequired` — the freshness
    * gate that makes STEP 5's stale-bucket refusal possible. The validator
    * requires a freshness policy to carry BOTH `timeGrain` and
    * `grainDimension` (a staleness verdict is per bucket; a grain-less
    * rollup has no buckets to gate).
    */
  private def buildFlightsModel(): Model = {
    val dimensions: List[Dimension] = List(
      Dimension.field("flight_id", "flight_id"),
      Dimension.field("airline", "airline"),
      Dimension.field("origin", "origin"),
      Dimension.field("destination", "destination"),
      // flight_date declares `Date` so it can serve as the rollup's
      // grain axis (calendar truncation requires a temporal type).
      Dimension.field("flight_date", "flight_date", io.sm8.core.schema.SealedDataType.Date),
      Dimension.field("cancelled", "cancelled"))

    val measures: List[Measure] = List(
      Measure.aggregate("flight_count", "flight_id".countStar),
      Measure.aggregate("total_delay", "delay_minutes".asField.sum),
      Measure.aggregate("cancelled_count",
        AggregateFn.Sum,
        io.sm8.core.expr.Expr.CaseWhen(
          branches = List(
            ("cancelled".asField === "true".asVarchar) -> 1.asInt),
          otherwise = 0.asInt)))

    val calculatedMeasures: List[CalculatedMeasure] = List(
      CalculatedMeasure(
        name = "avg_delay",
        expr = "total_delay".measure / "flight_count".measure))

    // NOTE: the rollup intentionally has NO calculated-measure refs
    // (avg_delay is derived, not re-aggregable) — ModelValidator
    // rejects calculated refs in rollups. total_delay + flight_count
    // are additive base measures, so avg-delay is derivable after
    // routing: sum(partial sums) / sum(partial counts).
    val dailyDelayRollup = RollupSpec(
      name = "daily_delay_by_airline",
      dimensions = List("airline", "flight_date"),
      measures = List("total_delay", "flight_count"),
      timeGrain = Some("day"),
      grainDimension = Some("flight_date"),
      freshness = Some(FreshnessPolicy.FinalRequired))

    ModelBuilder()
      .withName("flights")
      .withVersion(1)
      .withSource(SourceRef.ByName(table = "flights_clean_csv"))
      .withStatus(ModelStatus.Draft)
      .withDimensions(dimensions)
      .withMeasures(measures)
      .withCalculatedMeasures(calculatedMeasures)
      .withRollup(dailyDelayRollup)
      .build match {
      case Right(m) => m
      case Left(err) =>
        throw new IllegalStateException(s"sm8: failed to build flights Model: $err")
    }
  }

  // ====== Materialize + watermark (Tier-2 seals) ======

  /** Materialize the declared rollup as an Iceberg table and latch the
    * watermark final for every bucket strictly before today.
    *
    * This is the operator action a real deployment schedules after each
    * daily close ("the day is done, mark it final"). Tier-2 semantics:
    * the watermark commit FOLLOWS the data commit, and `is_final` never
    * regresses (the merge is an OR, not an overwrite).
    */
  private def materializeAndSeal(spark: SparkSession, model: Model): Unit = {
    model.rollups.foreach { spec =>
      io.sm8.connectors.spark.RollupMaterializer.materialize(
        spark, model, spec, eager = true,
        tableFormat = io.sm8.connectors.spark.RollupMaterializer.Iceberg) match {
        case Right(table) =>
          Logger.info(s"  rollup '${spec.name}' materialized as Iceberg table: $table")
        case Left(err) =>
          throw new IllegalStateException(
            s"sm8: rollup '${spec.name}' materialization failed: $err")
      }
      // Seal every PAST day present in the base (v1 finality heuristic:
      // buckets strictly before today are final; today/future stay open).
      val pastBuckets: Set[String] =
        spark.table("flights_clean_csv")
          .select("flight_date")
          .filter(col("flight_date") < current_date())
          .distinct()
          .collect().map(_.getDate(0).toString).toSet
      if (pastBuckets.nonEmpty) {
        io.sm8.connectors.spark.RollupWatermark.advance(
          spark, model, spec, pastBuckets, isFinal = true)
        Logger.info(s"  watermark sealed final: ${pastBuckets.mkString(", ")}")
      }
    }
  }

  // ====== Refresh (Tier-2 scoped refresh) ======

  /** Tier-2 refresh for the late bucket: mergeRefresh (Iceberg merge into
    * the rollup) followed by the watermark advance over the same scope.
    *
    * The watermark advance happens only AFTER a successful data
    * commit (see [[ADR-0030]] D3 ordering); `mergeRefresh` returns
    * the buckets it covered so the caller advances exactly what was
    * rebuilt (no scope widening).
    */
  private def refreshLateBucket(
      spark: SparkSession, model: Model, bucket: String): Unit = {
    model.rollups.foreach { spec =>
      io.sm8.connectors.spark.RollupMergeRefresher.mergeRefresh(
        spark, model, spec, List(bucket)) match {
        case Right(io.sm8.connectors.spark.RollupMergeRefresher.MergeRefreshResult.Merged(rollup, table, _, _)) =>
          Logger.info(s"  mergeRefresh '$rollup' scope [$bucket]: $table")
          io.sm8.connectors.spark.RollupWatermark.advance(
            spark, model, spec, Set(bucket), isFinal = true)
          Logger.info(s"  watermark advanced final for [$bucket]")
        case Left(err) =>
          throw new IllegalStateException(
            s"sm8: mergeRefresh '${spec.name}' failed for [$bucket]: $err")
      }
    }
  }

  // ====== Query helper ======

  /** Run one query and print the row count + sample, tagging whether the
    * plan routed to the rollup (from the sink counters) or the base table.
    */
  private def runQuery(
      label: String,
      provider: EngineProvider,
      model: Model,
      request: QueryRequest): Unit = {
    Logger.info(s"--- $label ---")
    provider.query(model, request, EngineContext.defaultContext) match {
      case Right(pqr) =>
        Logger.info(s"  rows: ${pqr.rows.size}")
        pqr.rows.take(10).zipWithIndex.foreach { case (row, i) =>
          Logger.info(s"  [$i] " + row.values.map {
            case ResultValue.StringV(s)  => s
            case ResultValue.IntV(n)     => n.toString
            case ResultValue.DoubleV(d)  => d.toString
            case ResultValue.DecimalV(d) => d.toString
            case ResultValue.NullV       => "null"
            case ResultValue.BoolV(b)    => b.toString
            case other                   => other.toString
          }.mkString(", "))
        }
        if (pqr.rows.size > 10) Logger.info(s"... ${pqr.rows.size - 10} more rows")
      case Left(err) =>
        Logger.error(s"  sm8 query FAILED: ${err.getClass.getSimpleName}: $err")
    }
  }

  // ====== Metrics sink ======

  /** Example-only `MetricsSink`: rollup-routing counters via the core
    * `MetricsRegistry` seam (the same events the spark-connector routing
    * fold fires; a real deployment exposes them on `--metrics-port`).
    *
    * Thread-safety: AtomicLong counters, same discipline as
    * sm8-platform `QueryMetrics`.
    */
  private final class FlightMetricsSink extends io.sm8.core.cache.MetricsSink {
    private val rollupHits   = new java.util.concurrent.atomic.AtomicLong(0)
    private val rollupMisses = new java.util.concurrent.atomic.AtomicLong(0)

    /** Called by the spark-connector routing fold when a query is rewritten to a rollup. */
    override def recordRollupRewrite(): Unit = { rollupHits.incrementAndGet(); () }

    /** Called by the spark-connector routing fold when a query is NOT rewritten;
      * the typed `reason` names which matcher (group set, grain, aggregates,
      * filters, freshness) refused the rewrite.
      *
      * @param reason the typed refusal reason from the rewriter
      */
    override def recordRollupRefusal(
        reason: io.sm8.core.rel.RollupRewriter.RollupRewriteRefusal): Unit = {
      rollupMisses.incrementAndGet(); ()
    }

    /** One-shot Prometheus-style summary (printed at end of run).
      *
      * @return the counters rendered in Prometheus text format, two
      *         `counter` series plus `# HELP`/`# TYPE` headers.
      */
    def summary: String =
      s"""# HELP flight_rollup_rewrite_total queries routed to a rollup
         |# TYPE flight_rollup_rewrite_total counter
         |flight_rollup_rewrite_total ${rollupHits.get}
         |# HELP flight_rollup_refusal_total queries NOT routed (typed reason on the sink API)
         |# TYPE flight_rollup_refusal_total counter
         |flight_rollup_refusal_total ${rollupMisses.get}""".stripMargin
  }

  // ====== main ======

  /** Lifecycle entry point. Runs the full rollup-freshness story on a
    * local Spark session against a temp-dir Iceberg catalog:
    * ingest -> declare -> materialize+seal -> query -> late batch ->
    * stale refusal -> refresh -> query -> metrics summary. Exits 0
    * on success; throws on any typed engine error.
    *
    * @param args unused (the example takes no flags; mirrors
    *               hospital-cleaning's shape)
    */
  def main(args: Array[String]): Unit = {
    val sink = new FlightMetricsSink
    io.sm8.core.cache.MetricsRegistry.register(sink)

    val warehouseDir = java.nio.file.Files.createTempDirectory("sm8-flight-iceberg").toString

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("sm8-flight-delays")
      // iceberg_cat: the Tier-2 (Iceberg) catalog the rollup + watermark
      // tables live in. hadoop catalog = no external metastore needed.
      .config("spark.sql.catalog.iceberg_cat",
        "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_cat.type", "hadoop")
      .config("spark.sql.catalog.iceberg_cat.warehouse", warehouseDir)
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    try {
      Logger.info("=" * 70)
      Logger.info("sm8 flight-delays example — rollup freshness lifecycle")
      Logger.info("=" * 70)

      // Driver-side: batch 1 ingest (Sep 7-9) -- just a CSV read into a Spark temp view.
      Logger.info("STEP 1: INGEST batch 1 (Sep 7-9)")
      val batch1 = readCsv(spark, "flights_batch1.csv")
      batch1.createOrReplaceTempView("flights_clean_csv")
      Logger.info(s"  rows: ${batch1.count()}")

      // Driver-side: model declaration via ModelBuilder DSL.
      Logger.info("STEP 2: DECLARE model (ModelBuilder DSL + FinalRequired day-grain rollup)")
      val flights = buildFlightsModel()
      Logger.info(s"  model: ${flights.name} v${flights.version}, rollups: ${flights.rollups.map(_.name).mkString(", ")}")

      // Model integrity check (what `sm8 validate` runs first).
      ModelValidator.validate(flights) match {
        case Right(_) => Logger.info("  model validation: ok")
        case Left(errs) =>
          throw new IllegalStateException(s"sm8: model validation failed: $errs")
      }

      // Driver-side: rollup materialize + watermark seal.
      Logger.info("STEP 3: MATERIALIZE rollup (Iceberg) + seal past buckets final")
      materializeAndSeal(spark, flights)

      val descriptor = new io.sm8.connectors.spark.SparkEngineProviderDescriptor
      val provider: io.sm8.connectors.spark.SparkEngineProvider =
        descriptor.realize("local[*]") match {
          case Some(p) => p.asInstanceOf[io.sm8.connectors.spark.SparkEngineProvider]
          case None => throw new IllegalStateException(
            "sm8: SparkEngineProviderDescriptor.realize(local[*]) returned None")
        }

      // Driver-side: first query after sealing.
      Logger.info("STEP 4: QUERY daily avg delay (all past buckets final)")
      runQuery(
        "Q1: avg delay by airline/day, Sep 7-9",
        provider, flights,
        QueryRequest(model = flights.name,
          dimensions = Seq("airline", "flight_date"),
          measures = Seq("avg_delay")))

      // Driver-side: late-batch ingest triggering the freshness gate.
      Logger.info("STEP 5: INGEST batch 2 (Sep 10, late-arriving) — bucket NOT sealed")
      val batch2 = readCsv(spark, "flights_batch2.csv")
      val combined = batch1.union(batch2)
      combined.createOrReplaceTempView("flights_clean_csv")
      Logger.info(s"  rows now: ${combined.count()} (batch2 adds ${batch2.count()})")

      Logger.info("STEP 5a: QUERY again (Sep 10 bucket open -> RollupBucketStale, base-table fallback)")
      runQuery(
        "Q2: avg delay by airline/day WITH the open Sep 10 bucket",
        provider, flights,
        QueryRequest(model = flights.name,
          dimensions = Seq("airline", "flight_date"),
          measures = Seq("avg_delay")))

      // The typed refusal, read directly (what `sm8 rollup-status` and
      // the MCP validate_query tool surface to operators):
      // The typed refusal read directly (what `sm8 rollup-status` and
      // the MCP `validate_query` tool surface to operators):
      // RollupWatermark.stalenessRefusal is the same connector-only seam
      // the routing fold invokes per query. We probe it explicitly here
      // (the provider.query path above falls back to the base table on
      // the avg_los-projection peel refusal before it ever reaches the
      // freshness gate — see the README's "Honest limitations" note).
      io.sm8.connectors.spark.RollupWatermark.stalenessRefusal(
        spark, flights, flights.rollups.head, Set("2026-09-10")) match {
        case Some(refusal) =>
          Logger.info(s"  typed refusal for bucket 2026-09-10: ${refusal.getClass.getSimpleName}(buckets=${refusal.buckets.mkString(",")})")
        case None =>
          Logger.info("  (no staleness refusal — bucket already final)")
      }

      // Driver-side: Tier-2 scoped refresh.
      Logger.info("STEP 6: REFRESH Sep 10 bucket (mergeRefresh + watermark advance)")
      refreshLateBucket(spark, flights, "2026-09-10")

      Logger.info("STEP 6a: QUERY again (Sep 10 now sealed)")
      runQuery(
        "Q3: avg delay by airline/day AFTER refresh",
        provider, flights,
        QueryRequest(model = flights.name,
          dimensions = Seq("airline", "flight_date"),
          measures = Seq("avg_delay")))

      // Driver-side: end-of-run metrics summary.
      Logger.info("=" * 70)
      Logger.info("Metrics summary (Prometheus text format; the same counters a")
      Logger.info("real deployment exposes via `sm8-server --metrics-port`):")
      Logger.info(sink.summary)
      Logger.info("=" * 70)
      Logger.info("Lifecycle complete: seal -> route -> stale refusal -> refresh -> route.")
    } catch {
      case t: Throwable =>
        Logger.error(s"sm8 flight-delays example FAILED: ${t.getClass.getSimpleName}: ${t.getMessage}")
        throw t
    } finally {
      spark.stop()
    }
  }
}
