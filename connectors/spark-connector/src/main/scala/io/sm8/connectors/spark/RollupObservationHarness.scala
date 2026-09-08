/*
 * Rollup routing observation harness (design: docs/measurement/).
 *
 * Boots Spark local[1], creates a CSV-shaped base table, materializes
 * a declared rollup, then drives a MIX of rollup-eligible and
 * rollup-ineligible queries through the REAL SparkEngineProvider.query()
 * path (routing fold + telemetry included). At the end it prints the
 * counter snapshot in the same shape `sm8 rollup-report` would show,
 * so the reading can be compared against the handbook's interpretation
 * rubric directly.
 *
 * NOT a test — a runnable diagnostic. Usage (from the repo root):
 *
 *   mvn -pl connectors/spark-connector -am compile
 *   java -cp "connectors/spark-connector/target/classes:sm8-core/target/classes:$(cat /tmp/spark-cp.txt)" \
 *     io.sm8.connectors.spark.RollupObservationHarness
 *
 * The script scripts/rollup-observe.sh wraps this (classpath build +
 * invocation) so operators do not hand-assemble the classpath.
 */
package io.sm8.connectors.spark

import io.sm8.core.cache.{MetricsRegistry, MetricsSink, RollupCountersSnapshot}
import io.sm8.core.engine.{EngineContext, EngineIdentity, QueryRequest}
import io.sm8.core.rel.RollupRewriter

import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._

/** File-scope fixture row (Spark encoder requirement). */
final case class ObsSale(region: String, item: String, amount: Long, units: Int)

object RollupObservationHarness {

  private val Identity = EngineIdentity(
    name = "sm8-observation", nativeVersion = "3.5", engineAdapterVersion = "0.1.0")

  private val Regions = List("east", "west", "north", "south")

  /** 400 deterministic rows: 4 regions x 5 items x 20 repeats. */
  private val Sales: List[ObsSale] =
    (0 until 400).map { i =>
      ObsSale(
        region = Regions(i % 4),
        item = s"item-${i % 5}",
        amount = (i * 13L) % 1000L + 1L,
        units = (i % 9) + 1)
    }.toList

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("rollup-observation-harness")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

  /** A real sink so the harness can read what it recorded (the
    * production sink is sm8-platform's QueryMetrics; here we use the
    * same MetricsSink contract with an in-harness implementation so
    * the harness has zero platform dependency). */
  private class HarnessSink extends MetricsSink {
    val rewrites = new java.util.concurrent.atomic.AtomicLong(0)
    val refusalsByReason = new java.util.concurrent.ConcurrentHashMap[String, java.util.concurrent.atomic.AtomicLong]()
    /** Counts one rewrite event (a plan was routed to a rollup). */
    override def recordRollupRewrite(): Unit = rewrites.incrementAndGet()
    /** Counts one refusal under its stable reason label.
      *
      * @param reason the typed refusal from the rewriter
      */
    override def recordRollupRefusal(reason: RollupRewriter.RollupRewriteRefusal): Unit =
      refusalsByReason
        .computeIfAbsent(RollupRewriter.RollupRewriteRefusal.reasonName(reason),
                         _ => new java.util.concurrent.atomic.AtomicLong(0))
        .incrementAndGet()
    /** Reads the counters as an immutable snapshot.
      *
      * @return the rollup counter snapshot
      */
    override def rollupSnapshot(): RollupCountersSnapshot = {
      RollupCountersSnapshot(
        rewrites          = rewrites.get,
        refusals          = refusalsByReason.asScala.values.map(_.get).sum,
        refusalsPermanent = 0L,
        refusalsByReason  = refusalsByReason.asScala.toList
                              .map { case (k, v) => (k, v.get) }
                              .sortBy { case (k, _) => k })
    }
  }

  /** The query mix: each entry is (label, will-route). The `willRoute`
    * flag documents the EXPECTED outcome so a mismatch between the
    * harness's expectation and the actual counter reading is visible
    * in the final diff (the harness prints both). */
  private val QueryMix: List[(String, Boolean, QueryRequest)] = {
    /** Build a single QueryRequest with the given dimensions/measures.
      *
      * @param dimensions the dimensions to group by (empty = global)
      * @param measures the measures to aggregate
      * @return the typed QueryRequest for the fixture model
      */
    def req(dimensions: List[String], measures: List[String]): QueryRequest =
      QueryRequest(model = "sales", dimensions = dimensions, measures = measures)
    List(
      ("rollup-eligible: group by region",             true,  req(List("region"), List("order_count", "total_amount"))),
      ("rollup-eligible: group by region (repeat)",    true,  req(List("region"), List("order_count", "total_amount"))),
      ("rollup-eligible: group by region, sum only",   true,  req(List("region"), List("total_amount"))),
      ("ineligible: group by item (not a rollup dim)", false, req(List("item"),   List("order_count"))),
      ("ineligible: global aggregate (no dims)",       false, req(List(),         List("order_count"))),
      ("ineligible: both dims (finer than rollup)",    false, req(List("region", "item"), List("order_count"))),
    )
  }

  /** The harness entry point. Boots a Spark local[1] session,
    * registers the in-process sink, runs the query mix through the
    * production query() path, then prints the report.
    *
    * @param args reserved for future flags (e.g. --json)
    */
  def main(args: Array[String]): Unit = {
    // Memory guard (per the execution checklist): refuse to boot when
    // the box is already near the ceiling — a Spark local[1] session
    // adds roughly 1-1.5 GB RSS on top of the current footprint.
    val memLine = java.nio.file.Files
      .readAllLines(java.nio.file.Paths.get("/proc/meminfo")).asScala
      .find(_.startsWith("MemTotal")).map(_.trim).getOrElse("")
    if (memLine.isEmpty) {
      // /proc/meminfo unreadable (non-Linux): proceed, the JVM will
      // fail on its own if memory is truly exhausted.
      ()
    }
    val usedPct = {
      val lines = java.nio.file.Files
        .readAllLines(java.nio.file.Paths.get("/proc/meminfo")).asScala
      /** Parse a `/proc/meminfo` size value (e.g. `MemTotal: 7730 kB`).
        *
        * @param key the line prefix (e.g. `"MemTotal:"`)
        * @return the value in kB, or 0 if the line is missing
        */
      def kb(key: String): Long =
        lines.find(_.startsWith(key)).map(_.trim.split("\\s+")(1).toLong).getOrElse(0L)
      val total = kb("MemTotal:")
      val available = kb("MemAvailable:")
      if (total == 0L) 0L else ((total - available) * 100) / total
    }
    if (usedPct >= 85) {
      println(s"[harness] ABORT: RAM at ${usedPct}% (>= 85% guard). Free memory first, then re-run.")
      System.exit(2)
    }
    println(s"[harness] RAM at ${usedPct}% — proceeding (guard: <85%)")

    val spark = buildSpark()
    val sink = new HarnessSink
    MetricsRegistry.register(sink)
    try {
      import spark.implicits._
      Sales.toDF("region", "item", "amount", "units").createOrReplaceTempView("sales_base")

      val model = io.sm8.core.model.Model.of(
        name = "sales",
        version = 1,
        dimensions = List(io.sm8.core.model.Dimension.field("region", "region")),
        measures = List(
          io.sm8.core.model.Measure(
            "order_count",
            io.sm8.core.rel.AggregateCall(fn = io.sm8.core.rel.AggregateFn.Count, input = None, alias = "order_count")),
          io.sm8.core.model.Measure.aggregate(
            "total_amount", io.sm8.core.rel.AggregateFn.Sum, io.sm8.core.expr.Expr.FieldRef("amount"))),
        defaultPolicies = io.sm8.core.model.ModelPolicyDefaults(
          materialize = io.sm8.core.model.MaterializePolicy.None,
          cache = io.sm8.core.model.CachePolicy.NoCache,
          audit = io.sm8.core.model.AuditPolicy.NoAudit),
        source = io.sm8.core.model.SourceRef.ByName(table = "sales_base"),
        rollups = List(io.sm8.core.model.RollupSpec(
          "by_region", List("region"), List("order_count", "total_amount"), None))
      ) match {
        case Right(m) => m
        case Left(e)  => throw new IllegalStateException(s"fixture model invalid: $e")
      }

      // Materialize the real rollup via the production write path.
      RollupMaterializer.materialize(spark, model, io.sm8.core.model.RollupSpec(
        "by_region", List("region"), List("order_count", "total_amount"), None)) match {
        case Right(name) => println(s"[harness] materialized rollup: $name")
        case Left(e)     => throw new IllegalStateException(s"materialize failed: $e")
      }

      val provider = new SparkEngineProvider(spark, SparkTypeBridge, "sm8-observation")
      val ctx = EngineContext.defaultContext

      println(s"[harness] driving ${QueryMix.size} queries through the production query() path ...")
      val outcomes = QueryMix.map { case (label, expectedRoute, request) =>
        val result = provider.query(model, request, ctx)
        val ok = result.isRight
        println(s"[harness] ${label.padTo(50, ' ' )} ok=$ok (expected-route=$expectedRoute)")
        (label, expectedRoute, ok)
      }

      // Final reading in the `sm8 rollup-report` shape.
      val snap = sink.rollupSnapshot()
      println()
      println("== rollup routing report (harness reading) ==")
      println(s"  rewrites:            ${snap.rewrites}")
      println(s"  refusals:            ${snap.refusals}")
      println(s"  refusals permanent:  ${snap.refusalsPermanent}")
      if (snap.refusalsByReason.isEmpty) {
        println("  (no per-reason refusals recorded)")
      } else {
        println("  refusals by reason (ranked):")
        snap.refusalsByReason.foreach { case (reason, count) =>
          val share = if (snap.refusals == 0L) "0%" else s"${count * 100 / snap.refusals}%"
          println(f"    ${reason.padTo(28, ' ')} $count  ($share)")
        }
      }
      val expectedRewrites = QueryMix.count(_._2)
      val expectedRefusals = QueryMix.size - expectedRewrites
      println()
      println(s"  EXPECTED (from the query mix): rewrites=$expectedRewrites refusals=$expectedRefusals")
      val routeMatch = snap.rewrites == expectedRewrites && snap.refusals == expectedRefusals
      println(s"  ROUTING MATCH: $routeMatch")
      val allOk = outcomes.forall(_._3) && routeMatch
      println(s"  HARNESS VERDICT: ${if (allOk) "PASS" else "FAIL"}")
      if (!allOk) System.exit(1)
    } finally {
      MetricsRegistry.register(MetricsSink.NoOp)
      spark.stop()
    }
  }
}
