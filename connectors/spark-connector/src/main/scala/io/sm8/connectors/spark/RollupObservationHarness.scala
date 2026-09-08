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

  /** The query mix: each entry is (label, model, request, will-route).
    * Each model has a different declared-dim shape vs the rollup it
    * carries, so the mix hits every major refusal reason (the plans
    * QueryBuilder.build emits are MODEL-shaped, not request-shaped —
    * request.dimensions variation cannot trigger refusal, so the
    * MODEL variation must). */
  private val QueryMix: List[(String, String, QueryRequest, Boolean)] = {
    /** Build a QueryRequest targeting a given model.
      *
      * @param model the model name to query
      * @param dimensions the dimensions to group by (empty = global)
      * @param measures the measures to aggregate
      * @return the typed QueryRequest
      */
    def req(model: String, dimensions: List[String], measures: List[String]): QueryRequest =
      QueryRequest(model = model, dimensions = dimensions, measures = measures)
    List(
      // sales: model dims = [region]; rollup covers [region].
      // All shapes derive plan groupBy=[region]; all SHOULD route.
      ("rollup-eligible: sales group by region",
        "sales", req("sales", List("region"), List("order_count", "total_amount")), true),
      ("rollup-eligible: sales group by region, repeat",
        "sales", req("sales", List("region"), List("order_count", "total_amount")), true),
      ("rollup-eligible: sales global aggregate",
        "sales", req("sales", List(), List("order_count")), true),
      ("rollup-eligible: sales subset-of rollup dims",
        "sales", req("sales", List("region"), List("total_amount")), true),
      // sales_by_item: model dims = [region, item]; rollup covers
      // [region] only. The plan groups by [region, item]; the
      // rewriter refuses because `item` is not a rollup dim.
      ("ineligible: sales_by_item group by region+item (item not in rollup)",
        "sales_by_item", req("sales_by_item", List("region", "item"), List("order_count")), false),
      // sales_by_item: global aggregate — the model still carries
      // [region, item] in the IR; rollup covers only [region].
      ("ineligible: sales_by_item global aggregate (rollup only covers region)",
        "sales_by_item", req("sales_by_item", List(), List("order_count")), false),
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

      /** Build the sales model with the given declared dimensions.
        * Two variants drive the query mix: `sales` (region only —
        * the rollup covers it) and `sales_by_item` (region + item —
        * the rollup does NOT cover item, so those queries refuse).
        *
        * @param name the model name (and the QueryRequest target)
        * @param withItemDim true adds the `item` dimension (used by
        *                    the ineligible queries)
        * @return the built model
        */
      def buildModel(name: String, withItemDim: Boolean): io.sm8.core.model.Model = {
        val dims =
          List(io.sm8.core.model.Dimension.field("region", "region")) ++
            (if (withItemDim) List(io.sm8.core.model.Dimension.field("item", "item")) else Nil)
        io.sm8.core.model.Model.of(
          name = name,
          version = 1,
          dimensions = dims,
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
          case Left(e)  => throw new IllegalStateException(s"fixture model $name invalid: $e")
        }
      }
      val salesModel       = buildModel("sales", withItemDim = false)
      val salesByItemModel = buildModel("sales_by_item", withItemDim = true)
      val models: Map[String, io.sm8.core.model.Model] =
        Map("sales" -> salesModel, "sales_by_item" -> salesByItemModel)

      // Materialize the real rollup via the production write path.
      RollupMaterializer.materialize(spark, salesModel, io.sm8.core.model.RollupSpec(
        "by_region", List("region"), List("order_count", "total_amount"), None)) match {
        case Right(name) => println(s"[harness] materialized rollup: $name")
        case Left(e)     => throw new IllegalStateException(s"materialize failed: $e")
      }

      val provider = new SparkEngineProvider(spark, SparkTypeBridge, "sm8-observation")
      val ctx = EngineContext.defaultContext

      println(s"[harness] driving ${QueryMix.size} queries through the production query() path ...")
      val outcomes = QueryMix.map { case (label, modelName, request, expectedRoute) =>
        val model = models(modelName)
        val result = provider.query(model, request, ctx)
        val ok = result.isRight
        println(s"[harness] ${label.padTo(70, ' ')} ok=$ok (expected-route=$expectedRoute)")
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
      val expectedRewrites = QueryMix.count(_._4)
      val expectedRefusals = QueryMix.size - expectedRewrites
      println()
      println(s"  EXPECTED (from the query mix): rewrites=$expectedRewrites refusals=$expectedRefusals")
      // Verdict gates (review R1 MEDIUM): totals AND per-reason
      // distribution. A totals-only check could pass while the
      // per-reason breakdown silently drifts (e.g. all refusals
      // landing on the wrong reason), which is the diagnostic drift
      // the harness exists to catch.
      val totalsMatch = snap.rewrites == expectedRewrites && snap.refusals == expectedRefusals
      val expectedReasons = QueryMix.filterNot(_._4).size
      val reasonMatch =
        snap.refusalsByReason.size == 1 &&
        snap.refusalsByReason.head._2 == expectedReasons.toLong
      val routeMatch = totalsMatch && reasonMatch
      println(s"  REASON MATCH: $reasonMatch (expected 1 reason class with $expectedReasons refusals; " +
        s"got ${snap.refusalsByReason.map(r => s"${r._1}=${r._2}").mkString(", ")})")
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
