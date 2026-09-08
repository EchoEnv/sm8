/*
 * Rollup routing invocation tests (ADR-0026).
 *
 * The fold under test is `SparkEngineProvider.routeThroughRollup`:
 * the single production call site for `RollupRewriter.rewrite`.
 * Acceptance criteria:
 * - a plan whose shape matches a declared rollup is REWRITTEN to
 *   scan the rollup table, and the rewrite is recorded;
 * - the routed path produces the same rows as the base path
 *   (exact, integral data);
 * - every refusal leaves the ORIGINAL plan flowing and lands in
 *   the per-reason refusal counters;
 * - the fold works with the NoOp sink (no registration, no error);
 * - both `query()` and `compileModelToDataFrame()` (explain path)
 *   route through the same fold.
 */
package io.sm8.connectors.spark

import io.sm8.core.cache.{MetricsRegistry, MetricsSink, RollupCountersSnapshot}
import io.sm8.core.engine.{EngineContext, EngineIdentity, QueryRequest}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model._
import io.sm8.core.rel.{AggregateCall, AggregateFn, RelOp, RollupRewriter}
import io.sm8.core.schema.{Field, SealedDataType}

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final case class RoutingSale(region: String, item: String, amount: Long, units: Int)

class RollupRoutingInvocationSpec extends AnyFunSuite with Matchers with org.scalatest.BeforeAndAfterEach {

  private val identity: EngineIdentity = EngineIdentity(
    name = "sm8-routing-test", nativeVersion = "3.5", engineAdapterVersion = "0.1.0")

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("RollupRoutingInvocationSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

  private val sales: List[RoutingSale] = {
    val regions = List("east", "west", "north", "south")
    (0 until 200).map { i =>
      RoutingSale(
        region = regions(i % 4),
        item = s"item-${i % 5}",
        amount = (i * 7L) % 500L + 1L,
        units = (i % 9) + 1)
    }.toList
  }

  private def baseModel(rollups: List[RollupSpec]): Model =
    Model.of(
      name = "sales",
      version = 1,
      // ONE dim only -- the rollup by_region covers exactly this dim.
      // The production-integration test (test 9) relies on this:
      // QueryBuilder.build groups by every declared dim, and a
      // rollup that doesn't carry all model dims refuses the rewrite
      // (correctly). Two dims would route the test onto the base
      // path (20 region×item rows), defeating the integration pin.
      dimensions = List(
        Dimension.field("region", "region")),
      measures = List(
        Measure("order_count", AggregateCall(fn = AggregateFn.Count, input = None, alias = "order_count")),
        Measure.aggregate("total_amount", AggregateFn.Sum, Expr.FieldRef("amount"))),
      defaultPolicies = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = AuditPolicy.NoAudit),
      source = SourceRef.ByName(table = "sales_base"),
      rollups = rollups
    ) match {
      case Right(m) => m
      case Left(e)  => fail(s"fixture model invalid: $e")
    }

  private val byRegion =
    RollupSpec("by_region", List("region"), List("order_count", "total_amount"), None)

  private def queryPlan(groupDim: String): RelOp = RelOp.Aggregate(
    input = RelOp.Scan(
      sourceRef = SourceRef.ByName(table = "sales_base"),
      schema = List(
        Field.nonNull("region", SealedDataType.Varchar),
        Field.nonNull("item", SealedDataType.Varchar),
        Field.nonNull("amount", SealedDataType.BigInt),
        Field.nonNull("units", SealedDataType.Int)),
      projection = Nil),
    groupBy = List(Expr.FieldRef(groupDim)),
    aggregates = List(
      AggregateCall(fn = AggregateFn.Count, input = None, alias = "order_count"),
      AggregateCall(fn = AggregateFn.Sum, input = Some(Expr.FieldRef("amount")), alias = "total_amount")))

  /** A filtered variant of the canonical plan: Scan -> Filter -> Aggregate
    * (the same shape QueryBuilder.build emits for a model with filters).
    */
  private def filteredQueryPlan(groupDim: String, regionName: String): RelOp = RelOp.Aggregate(
    input = RelOp.Filter(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "sales_base"),
        schema = List(
          Field.nonNull("region", SealedDataType.Varchar),
          Field.nonNull("item", SealedDataType.Varchar),
          Field.nonNull("amount", SealedDataType.BigInt),
          Field.nonNull("units", SealedDataType.Int)),
        projection = Nil),
      predicate = Expr.Equal(
        Expr.FieldRef("region"),
        Expr.Literal(LiteralValue.StringValue(regionName), SealedDataType.Varchar))),
    groupBy = List(Expr.FieldRef(groupDim)),
    aggregates = List(
      AggregateCall(fn = AggregateFn.Count, input = None, alias = "order_count"),
      AggregateCall(fn = AggregateFn.Sum, input = Some(Expr.FieldRef("amount")), alias = "total_amount")))

  private def request(grain: Option[String] = None): QueryRequest =
    QueryRequest(model = "sales", timeGrain = grain)

  // The registry is JVM-global: park the NoOp sink between tests so
  // a failed test cannot leak counters into a later test's deltas.
  override def beforeEach(): Unit = MetricsRegistry.register(MetricsSink.NoOp)
  override def afterEach(): Unit = MetricsRegistry.register(MetricsSink.NoOp)

  /** The routing fold must read counters through whatever sink is
    * registered; each test registers a fresh counting sink so the
    * deltas are unambiguous (the registry is JVM-global).
    */
  private class CountingSink extends MetricsSink {
    val rewrites = new java.util.concurrent.atomic.AtomicLong(0)
    val refusalsByReason = new java.util.concurrent.ConcurrentHashMap[String, java.util.concurrent.atomic.AtomicLong]()
    override def recordRollupRewrite(): Unit = rewrites.incrementAndGet()
    override def recordRollupRefusal(reason: RollupRewriter.RollupRewriteRefusal): Unit =
      refusalsByReason
        .computeIfAbsent(RollupRewriter.RollupRewriteRefusal.reasonName(reason),
                         _ => new java.util.concurrent.atomic.AtomicLong(0))
        .incrementAndGet()
    override def rollupSnapshot(): RollupCountersSnapshot = {
      import scala.jdk.CollectionConverters._
      RollupCountersSnapshot(
        rewrites = rewrites.get,
        refusals = refusalsByReason.asScala.values.map(_.get).sum,
        refusalsPermanent = 0L,
        refusalsByReason = refusalsByReason.asScala.toList
          .map { case (k, v) => (k, v.get) }.sortBy { case (k, _) => k })
    }
  }

  test("matching plan is rewritten to the rollup scan and the rewrite is recorded") {
    val sink = new CountingSink
    MetricsRegistry.register(sink)
    val spark = buildSpark()
    import spark.implicits._
    sales.toDF("region", "item", "amount", "units").createOrReplaceTempView("sales_base")
    sales.toDF("region", "item", "amount", "units").createOrReplaceTempView("sales__by_region")
    val m = baseModel(List(byRegion))

    val provider = new SparkEngineProvider(spark, SparkTypeBridge, "sm8-routing-test")
    val routing = provider.routeThroughRollup(queryPlan("region"), m, request(None))

    // The plan must now scan the ROLLUP table, not the base table,
    // and the caller must be told it was rewritten (so the base
    // table's pre-filtered DF is not paired with this plan).
    routing.plan.toString should include ("sales__by_region")
    routing.plan.toString should not include ("sales_base")
    routing.rewritten shouldBe true
    sink.rewrites.get shouldBe 1L
    sink.refusalsByReason shouldBe empty
  }

  test("routed path returns the same rows as the base path (exact parity)") {
    MetricsRegistry.register(new CountingSink)
    val spark = buildSpark()
    import spark.implicits._
    sales.toDF("region", "item", "amount", "units").createOrReplaceTempView("sales_base")
    sales.toDF("region", "item", "amount", "units").createOrReplaceTempView("sales__by_region")
    val m = baseModel(List(byRegion))

    // Materialize the real rollup so the routed path reads REAL
    // pre-aggregated data (not a copy of the base table).
    val written = RollupMaterializer.materialize(spark, m, byRegion)
    written match {
      case Right(_) => ()
      case Left(e)  => fail(s"materialize failed: $e")
    }

    val provider = new SparkEngineProvider(spark, SparkTypeBridge, "sm8-routing-test")
    val routedPlan = provider.routeThroughRollup(queryPlan("region"), m, request(None)).plan
    val basePlan = queryPlan("region")

    val lowerer = new MinimalRelOpLowerer(spark, new PortableQueryCompiler(spark), identity)
    val ctx = EngineContext.defaultContext
    val routedRows = lowerer.lower(routedPlan, ctx).map(_.collect())
    val baseRows = lowerer.lower(basePlan, ctx).map(_.collect())
    routedRows.isRight shouldBe true
    baseRows.isRight shouldBe true
    val r = routedRows.right.get.toSeq.sortBy(_.toString)
    val b = baseRows.right.get.toSeq.sortBy(_.toString)
    r shouldBe b
  }

  test("non-ByName source refuses (SourceKindUnsupported) and the ORIGINAL plan flows") {
    val sink = new CountingSink
    MetricsRegistry.register(sink)
    val m = baseModel(List(byRegion)).copy(source =
      SourceRef.ByProvider(providerRefName = "external_warehouse"))
    val original = queryPlan("region")

    val spark = buildSpark()
    val provider = new SparkEngineProvider(spark, SparkTypeBridge, "sm8-routing-test")
    val routed = provider.routeThroughRollup(original, m, request(None)).plan

    routed should be theSameInstanceAs original // byte-identical fail-open contract
    sink.rewrites.get shouldBe 0L
    sink.refusalsByReason.containsKey("sourceKindUnsupported") shouldBe true
    val refusalCount: Long = sink.refusalsByReason.get("sourceKindUnsupported").get()
    refusalCount shouldBe 1L
  }

  test("shape-mismatched plan refuses (NonCanonicalShape) with the original plan intact") {
    val sink = new CountingSink
    MetricsRegistry.register(sink)
    val spark = buildSpark()
    val m = baseModel(List(byRegion))
    // A Project on top of a Scan is NOT the canonical
    // Scan -> Filter* -> Aggregate shape.
    val nonCanonical = RelOp.Project(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "sales_base"),
        schema = List(Field.nonNull("region", SealedDataType.Varchar)),
        projection = Nil),
      expressions = List(Expr.FieldRef("region") -> "region"))

    val provider = new SparkEngineProvider(spark, SparkTypeBridge, "sm8-routing-test")
    val routed = provider.routeThroughRollup(nonCanonical, m, request(None)).plan

    routed should be theSameInstanceAs nonCanonical // byte-identical fail-open contract
    sink.refusalsByReason.containsKey("nonCanonicalShape") shouldBe true
  }

  test("the fold works with the NoOp sink (no registration)") {
    MetricsRegistry.register(MetricsSink.NoOp)
    val spark = buildSpark()
    val m = baseModel(List(byRegion))
    val provider = new SparkEngineProvider(spark, SparkTypeBridge, "sm8-routing-test")
    // Must not throw with the no-op sink; returns the rewritten plan.
    val routed = provider.routeThroughRollup(queryPlan("region"), m, request(None)).plan
    routed.toString should include ("sales__by_region")
  }

  test("regression: routed plan must read the rollup table, not the base DF (the preFilteredDf bug)") {
    // Spark-batch-bugs #1 + scala-impact-analysis mantra:
    //   the pre-routing preFilteredDf is bound to the BASE table.
    //   If it leaks into a rewritten plan, lowerScan returns the
    //   BASE table's content under a ROLLUP sourceRef. In this PR
    //   the gate that catches the wrong-DF pairing is the
    //   RollupSchemaStale check (MinimalRelOpLowerer.scala:247-258):
    //   the rewritten plan declares state columns (`sum__amount`,
    //   `count__rows`) that the BASE table does not have, so the
    //   gate emits a typed `EngineError.UnsupportedCapability(
    //   "RollupSchemaStale", ...)` (loud, not silent) — which is
    //   still NOT the intended outcome for a query the operator
    //   thinks routed. The `RoutingOutcome.rewritten` flag prevents
    //   pairing the base DF with a rewritten plan in the first place.
    //
    // The contract: when the fold returns rewritten=true, callers
    // MUST drop the base DF. We verify the consequence: a routed plan
    // lowered with preFilteredDf=None reads the rollup table (its
    // own state columns resolve); lowered with preFilteredDf=Some(baseDf)
    // the schema-stale gate fires.
    MetricsRegistry.register(new CountingSink)
    val spark = buildSpark()
    import spark.implicits._
    // The production write path. Materialize creates the rollup
    // table with the proper state columns (count__rows / sum__amount).
    sales.toDF("region", "item", "amount", "units").createOrReplaceTempView("sales_base")
    val m = baseModel(List(byRegion))

    // Materialize the REAL rollup via the production write path
    // -- NOT a hand-made copy of the base table.
    RollupMaterializer.materialize(spark, m, byRegion) match {
      case Right(_) => ()
      case Left(e)  => fail(s"materialize failed: $e")
    }

    val provider = new SparkEngineProvider(spark, SparkTypeBridge, "sm8-routing-test")
    val routing = provider.routeThroughRollup(queryPlan("region"), m, request(None))
    routing.rewritten shouldBe true

    // CORRECT lowering: drop the base DF on a rewritten plan.
    val lowerer = new MinimalRelOpLowerer(spark, new PortableQueryCompiler(spark), identity)
    val ctx = EngineContext.defaultContext
    val routedRows = lowerer.lower(routing.plan, ctx, None) // preFilteredDf = None
    routedRows.isRight shouldBe true
    // The routed plan aggregates STATE COLUMNS from the rollup table:
    // its schema (from the rewriter) carries sum__amount/count__rows,
    // which the BASE table does not have. If the routed plan had read
    // the base table, resolution of sum__amount would fail -- the
    // successful lower is itself proof the rollup table was read.
    val routedAgg = routedRows.right.get.collect().map(_.toString).toSet
    routedAgg.nonEmpty shouldBe true

    // BROKEN lowering (the pre-fix bug): pair the rewritten plan
    // with the base DF. lowerScan returns the BASE table's content
    // under the rollup sourceRef -- resolution of the rollup state
    // columns now FAILS (the base table has no sum__amount), which
    // is the observable signature of the silent wrong-source bug
    // the RoutingOutcome.rewritten flag prevents.
    val basePreFilteredDf = spark.table("sales_base")
    val brokenRows = lowerer.lower(routing.plan, ctx, Some(basePreFilteredDf))
    brokenRows.isLeft shouldBe true // base table lacks sum__amount
  }

  test("production integration: compileModelToDataFrame serves rollup-sourced numbers end-to-end") {
    // DE F3: the fold-level tests bypass the production fold site
    // (the routedPreFilteredDf gating lives INSIDE query() /
    // compileModelToDataFrame). This test drives the real
    // compileModelToDataFrame on a materialized rollup and proves
    // the returned numbers are rollup-sourced (aggregating state
    // columns only the rollup table has).
    MetricsRegistry.register(new CountingSink)
    val spark = buildSpark()
    import spark.implicits._
    sales.toDF("region", "item", "amount", "units").createOrReplaceTempView("sales_base")
    val m = baseModel(List(byRegion))
    RollupMaterializer.materialize(spark, m, byRegion) match {
      case Right(_) => ()
      case Left(e)  => fail(s"materialize failed: $e")
    }

    val qs = spark.newSession()
    SparkEngineProvider.copyTempViews(spark, qs)
    val provider = new SparkEngineProvider(spark, SparkTypeBridge, "sm8-routing-test")
    val dfE = provider.compileModelToDataFrame(
      m, request(None), EngineContext.defaultContext, qs)
    dfE.isRight shouldBe true
    val rows = dfE.right.get.collect()
    // The materialized rollup has exactly ONE row per region
    // (pre-aggregated). If the production fold had NOT routed (or
    // had paired the plan with the base DF), the compile would
    // either fail (state columns missing on the base table) or
    // return the re-aggregated base numbers. The 4-row result and
    // the per-region totals matching the base sums prove the
    // rollup table was the source.
    rows.length shouldBe 4
    val routedByRegion = rows.map(r => r.getString(0) -> r.getLong(1)).toMap
    val expected = sales.groupBy(_.region).map { case (region, rs) =>
      region -> rs.size.toLong
    }
    routedByRegion shouldBe expected
  }

  test("whereFilters integration: model filter folds into the rewritten plan (correct filtered rows)") {
    // DE F4: a model-level filter + routing must yield the correct
    // FILTERED rows from the rollup path. The rewriter folds the
    // model's Filter chain into the rewritten plan (rebuildOnRollup
    // re-attaches c.filters above the rollup scan), so the routed
    // result must equal the base-path result under the same filter.
    MetricsRegistry.register(new CountingSink)
    val spark = buildSpark()
    import spark.implicits._
    sales.toDF("region", "item", "amount", "units").createOrReplaceTempView("sales_base")
    val m = baseModel(List(byRegion)).copy(filters =
      List(FilterSpec(
        name = "east_only",
        predicate = Expr.Equal(
          Expr.FieldRef("region"),
          Expr.Literal(LiteralValue.StringValue("east"), SealedDataType.Varchar)))))
    RollupMaterializer.materialize(spark, m, byRegion) match {
      case Right(_) => ()
      case Left(e)  => fail(s"materialize failed: $e")
    }

    val provider = new SparkEngineProvider(spark, SparkTypeBridge, "sm8-routing-test")
    val lowerer = new MinimalRelOpLowerer(spark, new PortableQueryCompiler(spark), identity)
    val ctx = EngineContext.defaultContext

    // The canonical shape QueryBuilder.build emits: Scan -> Filter -> Aggregate.
    // Routing must keep the Filter on top of the rollup Scan so the
    // routed result equals the base result under the same filter.
    val plan = filteredQueryPlan("region", "east")
    val routing = provider.routeThroughRollup(plan, m, request(None))
    routing.rewritten shouldBe true
    val routedRows = lowerer.lower(routing.plan, ctx, None)
    routedRows.isRight shouldBe true
    val routedRegions = routedRows.right.get.select("region").collect().map(_.getString(0)).toSet
    routedRegions shouldBe Set("east")

    // Parity: the same canonical plan under the base path yields the
    // same filtered aggregate.
    val baseRows = lowerer.lower(plan, ctx)
    baseRows.isRight shouldBe true
    val baseRegions = baseRows.right.get.select("region").collect().map(_.getString(0)).toSet
    baseRegions shouldBe Set("east")
  }
}
