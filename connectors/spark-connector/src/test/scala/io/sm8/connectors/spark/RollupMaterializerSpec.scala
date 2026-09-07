/*
 * SM8 Spark Connector — RollupMaterializerSpec (Ticket 5 of
 * docs/wayfinder/2026-09-06-pre-aggregation.md).
 *
 * Acceptance criteria under test (wayfinder Ticket 5):
 * 1. One rollup END-TO-END on Spark: materialize -> rewrite a
 *    query plan -> lower BOTH paths -> collect -> compare.
 * 2. Rollup-path ≡ base-path on a KNOWN INTEGRAL dataset:
 *    Sum/Count/Min/Max exact (integral data per the AC — float
 *    partial sums are non-associative).
 * 3. Avg via sum+count: asserted at the STATE level (the rollup
 *    carries sum__x; dividing by count__rows re-derives the mean
 *    exactly for integral data). Routing refuses Avg until state
 *    columns are first-class (Ticket 4 v1 contract).
 * 4. Speedup documented: wall-clock of base vs rollup path over a
 *    synthetic dataset (small local set: the number is recorded in
 *    the output, not asserted as a threshold — local[1] timing is
 *    not production-representative).
 * 5. Spark closure serialization audited on the write path (user
 *    directive 2026-09-06): the materializer uses ONLY built-in
 *    Column aggregations — this spec verifies the write completes
 *    with the default serialization (nothing custom to ship) and
 *    asserts the physical plan contains no BatchEvalPython/UDF
 *    nodes.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, EngineIdentity, ResolvedSource}
import io.sm8.core.expr.Expr
import io.sm8.core.model._
import io.sm8.core.rel.{AggregateCall, AggregateFn, RelOp, RollupRewriter}
import io.sm8.core.schema.{Field, SealedDataType}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** File-scope fixture row (Spark encoder requirement: not an inner
  * class of the spec). ALL-INTEGRAL fields per the wayfinder AC:
  * float partial sums are non-associative, so equality assertions
  * use integral data. */
final case class Sale(region: String, item: String, amount: Long, units: Int)

/** File-scope NULL-semantics fixture row: boxed nullable amount. */
final case class SealedNullRow(region: String, amount: java.lang.Long)

class RollupMaterializerSpec extends AnyFunSuite with Matchers {

  private val identity: EngineIdentity = EngineIdentity(
    name = "sm8-rollup-mat-test", nativeVersion = "3.5", engineAdapterVersion = "0.1.0")

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("RollupMaterializerSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

  // -- fixture: KNOWN INTEGRAL dataset (per the AC: float partial
  // sums are non-associative; integral data keeps exact equality) --
  // Sale lives at file scope: Spark encoders can't capture inner
  // classes of the spec (outer-scope access error).

  private val sales: List[Sale] = {
    // 4 regions x deterministic amounts; all Long/Int (integral).
    val regions = List("east", "west", "north", "south")
    (0 until 400).map { i =>
      Sale(
        region = regions(i % 4),
        item = s"item-${i % 7}",
        amount = (i * 13L) % 1000L + 1L, // deterministic, no floats
        units = (i % 11) + 1)
    }.toList
  }

  private def baseModel(rollups: List[RollupSpec]): Model =
    Model.of(
      name = "sales",
      version = 1,
      dimensions = List(
        Dimension.field("region", "region"),
        Dimension.field("item", "item")),
      measures = List(
        Measure("order_count", AggregateCall(fn = AggregateFn.Count, input = None, alias = "order_count")),
        Measure.aggregate("total_amount", AggregateFn.Sum, Expr.FieldRef("amount")),
        Measure.aggregate("min_amount", AggregateFn.Min, Expr.FieldRef("amount")),
        Measure.aggregate("max_amount", AggregateFn.Max, Expr.FieldRef("amount"))),
      defaultPolicies = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = AuditPolicy.NoAudit),
      source = SourceRef.ByName(table = "sales_base"),
      rollups = rollups
    ).right.get

  private val byRegion =
    RollupSpec("by_region", List("region"), List("order_count", "total_amount", "min_amount", "max_amount"), None)

  private def ctx: EngineContext = EngineContext.defaultContext

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
      AggregateCall(fn = AggregateFn.Sum, input = Some(Expr.FieldRef("amount")), alias = "total_amount"),
      AggregateCall(fn = AggregateFn.Min, input = Some(Expr.FieldRef("amount")), alias = "min_amount"),
      AggregateCall(fn = AggregateFn.Max, input = Some(Expr.FieldRef("amount")), alias = "max_amount")))

  // ===== 1+2: end-to-end parity on integral data =====

  test("end-to-end: materialize by_region, rewrite plan, rollup-path == base-path (exact)") {
    val spark = buildSpark()
    import spark.implicits._
    val df = sales.toDF("region", "item", "amount", "units")
    df.createOrReplaceTempView("sales_base")

    val m = baseModel(List(byRegion))

    // Materialize (the WRITE path — closure audit target).
    val written = RollupMaterializer.materialize(spark, m, byRegion)
    written.isRight shouldBe true
    written.right.get shouldBe "sales__by_region"

    // The rollup view must resolve through the normal SourceResolver.
    val resolved = new SparkSourceResolver(spark).resolve(
      SourceRef.ByName(table = "sales__by_region"), identity)
    resolved.isRight shouldBe true

    // Rewrite the query plan (Ticket 4 rewriter; routes to the rollup).
    val rewritten = RollupRewriter.rewrite(queryPlan("region"), m, None)
    rewritten shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]

    // Lower BOTH paths through the SAME compile step.
    val lowerer = new MinimalRelOpLowerer(spark, new PortableQueryCompiler(spark), identity)
    val baseResult = lowerer.lower(queryPlan("region"), ctx)
    val rollupResult = lowerer.lower(
      rewritten.asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten].plan, ctx)

    (baseResult, rollupResult) match {
      case (Right(bdf), Left(e)) => fail(s"rollup path failed: $e")
      case (Left(e), _)          => fail(s"base path failed: $e")
      case (Right(bdf), Right(rdf)) =>
        val base = bdf.collect().map(r => (r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))).sortBy(_._1).toList
        val roll = rdf.collect().map(r => (r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))).sortBy(_._1).toList
        base shouldBe roll
        // Sanity: 4 regions, known counts.
        base.map(_._1) shouldBe List("east", "north", "south", "west")
        base.foreach { case (_, cnt, _, _, _) => cnt shouldBe 100L }
    }
  }

  test("regression: re-grouping a COARSER rollup to another group-by (region from carrier-style) stays exact") {
    val spark = buildSpark()
    import spark.implicits._
    sales.toDF("region", "item", "amount", "units").createOrReplaceTempView("sales_base")
    // Rollup at [region, item] grain; query groups by region only
    // (coarser re-aggregation — exact for Additive).
    val fine = RollupSpec("by_region_item", List("region", "item"),
      List("order_count", "total_amount", "min_amount", "max_amount"), None)
    val m = baseModel(List(fine))
    RollupMaterializer.materialize(spark, m, fine).isRight shouldBe true

    val rewritten = RollupRewriter.rewrite(queryPlan("region"), m, None)
    rewritten shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]

    val lowerer = new MinimalRelOpLowerer(spark, new PortableQueryCompiler(spark), identity)
    val baseResult = lowerer.lower(queryPlan("region"), ctx)
    val rollupResult = lowerer.lower(
      rewritten.asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten].plan, ctx)
    (baseResult, rollupResult) match {
      case (Right(bdf), Right(rdf)) =>
        val base = bdf.collect().map(r => (r.getString(0), r.getLong(1), r.getLong(2))).sortBy(_._1).toList
        val roll = rdf.collect().map(r => (r.getString(0), r.getLong(1), r.getLong(2))).sortBy(_._1).toList
        base shouldBe roll
      case (Left(e), _) => fail(s"failed: $e")
      case _            => fail("rollup path failed")
    }
  }

  test("regression: n=1 single-observation group — Count/Sum/Min/Max exact (the edge from the map)") {
    val spark = buildSpark()
    import spark.implicits._
    // One group with exactly ONE row.
    List(Sale("solo", "item-0", 42L, 1)).toDF("region", "item", "amount", "units")
      .createOrReplaceTempView("sales_base")
    val m = baseModel(List(byRegion))
    RollupMaterializer.materialize(spark, m, byRegion).isRight shouldBe true
    val rewritten = RollupRewriter.rewrite(queryPlan("region"), m, None)
    rewritten shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]

    val lowerer = new MinimalRelOpLowerer(spark, new PortableQueryCompiler(spark), identity)
    val base = lowerer.lower(queryPlan("region"), ctx)
    val roll = lowerer.lower(
      rewritten.asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten].plan, ctx)
    (base, roll) match {
      case (Right(b), Right(r)) =>
        val bRow = b.collect().head
        val rRow = r.collect().head
        rRow.getLong(1) shouldBe bRow.getLong(1) // count = 1
        rRow.getLong(2) shouldBe bRow.getLong(2) // sum = 42
        rRow.getLong(3) shouldBe bRow.getLong(3) // min = 42
        rRow.getLong(4) shouldBe bRow.getLong(4) // max = 42
      case _ => fail("both paths must succeed")
    }
  }

  // ===== 3: Avg via sum+count (state level) =====

  test("Avg: rollup carries sum__amount; dividing by count__rows re-derives the mean exactly") {
    val spark = buildSpark()
    import spark.implicits._
    sales.toDF("region", "item", "amount", "units").createOrReplaceTempView("sales_base")
    val m = baseModel(List(byRegion))
    RollupMaterializer.materialize(spark, m, byRegion).isRight shouldBe true
    val roll = spark.table("sales__by_region")
      .select("region", "count__rows", "sum__amount")
      .collect().map(r => (r.getString(0), r.getLong(1), r.get(2).toString.toDouble))
      .sortBy(_._1).toList
    val expected = sales.groupBy(_.region).map { case (r, rows) =>
      (r, rows.size.toLong, rows.map(_.amount).sum.toDouble): (String, Long, Double)
    }.toList.sortBy(_._1)
    roll shouldBe expected
    // Mean re-derivation for integral data: the rollup state
    // (sum__amount / count__rows) must equal the directly-computed
    // per-region mean.
    val meanFromState = roll.map { case (r, c, s) => r -> (s / c.toDouble) }.toMap
    val meanDirect = sales.groupBy(_.region).map { case (r, rows) =>
      r -> (rows.map(_.amount).sum.toDouble / rows.size)
    }
    meanFromState shouldBe meanDirect
  }

  // ===== NULL semantics (DE review F5): all-NULL group parity =====

  test("NULL semantics: all-NULL group yields NULL sum on BOTH paths; count parity holds") {
    val spark = buildSpark()
    // Java boxed Long for a nullable column; one region all-NULL.
    val rows: List[SealedNullRow] = List(
      SealedNullRow("normal", 10L), SealedNullRow("normal", 20L),
      SealedNullRow("empty", null))
    import spark.implicits._
    rows.toDF("region", "amount").createOrReplaceTempView("sales_base")
    // amount is now nullable Long — rebuild the model/schema accordingly.
    val m = Model.of(
      name = "sales", version = 1,
      dimensions = List(Dimension.field("region", "region")),
      measures = List(
        Measure("order_count", AggregateCall(fn = AggregateFn.Count, input = None, alias = "order_count")),
        Measure.aggregate("total_amount", AggregateFn.Sum, Expr.FieldRef("amount"))),
      source = SourceRef.ByName(table = "sales_base"),
      rollups = List(RollupSpec("by_region", List("region"), List("order_count", "total_amount"), None))
    ).right.get
    RollupMaterializer.materialize(spark, m, m.rollups.head).isRight shouldBe true

    val rewritten = RollupRewriter.rewrite(
      RelOp.Aggregate(
        input = RelOp.Scan(
          sourceRef = SourceRef.ByName(table = "sales_base"),
          schema = List(
            Field.nonNull("region", SealedDataType.Varchar),
            Field.nullable("amount", SealedDataType.BigInt)),
          projection = Nil),
        groupBy = List(Expr.FieldRef("region")),
        aggregates = List(
          AggregateCall(fn = AggregateFn.Count, input = None, alias = "order_count"),
          AggregateCall(fn = AggregateFn.Sum, input = Some(Expr.FieldRef("amount")), alias = "total_amount"))),
      m, None)
    rewritten shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]

    val lowerer = new MinimalRelOpLowerer(spark, new PortableQueryCompiler(spark), identity)
    val ctx = EngineContext.defaultContext
    val base = lowerer.lower(
      RelOp.Aggregate(
        input = RelOp.Scan(
          sourceRef = SourceRef.ByName(table = "sales_base"),
          schema = List(
            Field.nonNull("region", SealedDataType.Varchar),
            Field.nullable("amount", SealedDataType.BigInt)),
          projection = Nil),
        groupBy = List(Expr.FieldRef("region")),
        aggregates = List(
          AggregateCall(fn = AggregateFn.Count, input = None, alias = "order_count"),
          AggregateCall(fn = AggregateFn.Sum, input = Some(Expr.FieldRef("amount")), alias = "total_amount"))),
      ctx)
    val roll = lowerer.lower(
      rewritten.asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten].plan, ctx)
    (base, roll) match {
      case (Right(b), Right(r)) =>
        val baseRows = b.collect().map(r => (r.getString(0), r.getLong(1), Option(r.get(2)))).sortBy(_._1).toList
        val rollRows = r.collect().map(r => (r.getString(0), r.getLong(1), Option(r.get(2)))).sortBy(_._1).toList
        // Exact parity INCLUDING the all-NULL group's NULL sum.
        baseRows shouldBe rollRows
        // Explicit: the all-NULL group has NULL sum on both paths,
        // and count counts ROWS (2 vs 1), not non-null amounts.
        baseRows.find(_._1 == "empty").map(_._3) shouldBe Some(None)
        rollRows.find(_._1 == "empty").map(_._3) shouldBe Some(None)
        baseRows.find(_._1 == "normal").map(_._2) shouldBe Some(2L)
        rollRows.find(_._1 == "normal").map(_._2) shouldBe Some(2L)
      case _ => fail("both paths must succeed")
    }
  }

  // ===== 4: speedup documented =====

  test("speedup documented: base vs rollup wall-clock on the synthetic set (recorded, not asserted)") {
    val spark = buildSpark()
    import spark.implicits._
    // A larger synthetic set for a measurable (if modest) local delta.
    val big: List[Sale] = (0 until 200000).map { i =>
      val regions = List("east", "west", "north", "south")
      Sale(regions(i % 4), s"item-${i % 7}", (i * 13L) % 1000L + 1L, (i % 11) + 1)
    }.toList
    big.toDF("region", "item", "amount", "units").createOrReplaceTempView("sales_base")
    val m = baseModel(List(byRegion))
    RollupMaterializer.materialize(spark, m, byRegion).isRight shouldBe true
    val rewritten = RollupRewriter.rewrite(queryPlan("region"), m, None)
      .asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten]
    val lowerer = new MinimalRelOpLowerer(spark, new PortableQueryCompiler(spark), identity)

    /** Wall-clock a thunk (nanoseconds) — test-local timing helper.
      *
      * @param f the thunk to time (run once)
      * @return elapsed nanoseconds
      */
    def timed(f: () => Unit): Long = { val t0 = System.nanoTime(); f(); System.nanoTime() - t0 }
    val plan = queryPlan("region")
    // Warm both plans once (JIT + catalog cache), then measure.
    lowerer.lower(plan, ctx).map(_.collect())
    lowerer.lower(rewritten.plan, ctx).map(_.collect())
    val baseNs = timed(() => lowerer.lower(plan, ctx).map(_.collect()))
    val rollNs = timed(() => lowerer.lower(rewritten.plan, ctx).map(_.collect()))
    // RECORD, don't assert: local[1] on a 200k-row synthetic set is
    // not production-representative; the number goes to the log for
    // the runbook.
    println(f"[ticket5-speedup] base=${baseNs / 1000000L}ms rollup=${rollNs / 1000000L}ms " +
      f"rows=200000 groups=4 (local[1]; production speedup measured at the warehouse)")
    succeed
  }

  // ===== 5: closure audit + typed refusals =====

  test("closure audit: materialized plan contains no UDF / python evaluation nodes") {
    val spark = buildSpark()
    import spark.implicits._
    sales.toDF("region", "item", "amount", "units").createOrReplaceTempView("sales_base")
    val m = baseModel(List(byRegion))
    RollupMaterializer.materialize(spark, m, byRegion).isRight shouldBe true
    val planString = spark.table("sales__by_region").queryExecution.executedPlan.toString
    planString should not include "BatchEvalPython"
    planString should not include "pythonUDF"
    planString.toLowerCase should not include "udf"
  }

  test("typed refusals: timeGrain, Algebraic, COUNT(expr), non-ByName, missing base") {
    val spark = buildSpark()
    import spark.implicits._
    sales.toDF("region", "item", "amount", "units").createOrReplaceTempView("sales_base")

    // timeGrain set -> refuse (finer-than-grain carry-item)
    RollupMaterializer.validateSpec(baseModel(Nil),
      RollupSpec("grainy", List("region"), List("order_count"), Some("day"))).isLeft shouldBe true

    // Algebraic measure -> refuse
    val mAvg = Model.of(
      name = "sales", version = 1,
      dimensions = List(Dimension.field("region", "region")),
      measures = List(Measure.aggregate("avg_amount", AggregateFn.Avg, Expr.FieldRef("amount"))),
      source = SourceRef.ByName(table = "sales_base"),
      rollups = List(RollupSpec("by_region_avg", List("region"), List("avg_amount"), None))
    ).right.get
    // Algebraic (Avg) NOW SUPPORTED via (n, sum, sumSq) partial columns —
    // validateSpec accepts it (the Ticket 6 contract pass flipped this
    // refusal). isLeft assertion removed.

    // COUNT(expr) -> refuse
    val mCnt = Model.of(
      name = "sales", version = 1,
      dimensions = List(Dimension.field("region", "region")),
      measures = List(Measure.aggregate("nonnull", AggregateFn.Count, Expr.FieldRef("amount"))),
      source = SourceRef.ByName(table = "sales_base"),
      rollups = List(RollupSpec("by_region_nn", List("region"), List("nonnull"), None))
    ).right.get
    RollupMaterializer.validateSpec(mCnt, mCnt.rollups.head).isLeft shouldBe true

    // non-ByName base -> refuse at readBase
    val byPathModel = Model.of(
      name = "sales", version = 1,
      dimensions = List(Dimension.field("region", "region")),
      measures = List(Measure("order_count", AggregateCall(fn = AggregateFn.Count, input = None, alias = "order_count"))),
      source = SourceRef.ByPath(format = "parquet", path = "/data/sales"),
      rollups = List(RollupSpec("by_region", List("region"), List("order_count"), None))
    ).right.get
    RollupMaterializer.readBase(spark, byPathModel).isLeft shouldBe true

    // missing base table -> typed error
    val missing = Model.of(
      name = "ghost", version = 1,
      dimensions = List(Dimension.field("region", "region")),
      measures = List(Measure("order_count", AggregateCall(fn = AggregateFn.Count, input = None, alias = "order_count"))),
      source = SourceRef.ByName(table = "no_such_table_xyz"),
      rollups = List(RollupSpec("by_region", List("region"), List("order_count"), None))
    ).right.get
    RollupMaterializer.materialize(spark, missing, missing.rollups.head).isLeft shouldBe true
  }
}

// ===== schema-parity pin (arch review A3): connector stateColumns vs
// core rollupSchema must agree on names — Ticket 6's Algebraic columns
// make drift likely; this test fails loudly if either side changes. =====

class RollupSchemaParitySpec extends AnyFunSuite with Matchers {
  import io.sm8.core.rel.RollupRewriter

  private val dims = List(Dimension.field("region", "region"))
  private val meas = List(
    Measure("order_count", AggregateCall(fn = AggregateFn.Count, input = None, alias = "order_count")),
    Measure.aggregate("total_amount", AggregateFn.Sum, Expr.FieldRef("amount")),
    Measure.aggregate("min_amount", AggregateFn.Min, Expr.FieldRef("amount")),
    Measure.aggregate("max_amount", AggregateFn.Max, Expr.FieldRef("amount")))

  test("connector stateColumns names == core rollupSchema names (drift pin)") {
    val model = Model.of(
      name = "sales", version = 1,
      dimensions = dims, measures = meas,
      source = SourceRef.ByName(table = "sales_base"),
      rollups = List(RollupSpec("by_region", List("region"),
        List("order_count", "total_amount", "min_amount", "max_amount"), None))
    ).right.get
    val spec = model.rollups.head
    val coreNames = RollupRewriter.rollupSchema(spec, model).map(_.name).sorted
    // Connector side: materialize against an empty in-memory table —
    // buildRollupDf's column NAMES come from stateColumns.
    val spark = SparkSession.builder()
      .master("local[1]").appName("RollupSchemaParitySpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    import spark.implicits._
    List(Sale("east", "i", 1L, 1)).toDF("region", "item", "amount", "units")
      .createOrReplaceTempView("sales_base")
    val dfE = RollupMaterializer.buildRollupDf(spark.table("sales_base"), model, spec)
    val df = dfE.right.get
    val connectorNames = df.columns.toList.sorted
    coreNames shouldBe connectorNames
  }
}

// ===== Algebraic state wiring (Ticket 6 contract pass) =====

class RollupMaterializerAlgebraicSpec extends AnyFunSuite with Matchers {

  import io.sm8.core.expr.Expr
  import io.sm8.core.rel.{AggregateCall, AggregateFn}
  import io.sm8.core.model.{Measure, RollupSpec}
  import io.sm8.core.schema.{Field, SealedDataType}

  // Self-contained fixture (the RollupMaterializerSpec class's fixtures
  // are private; mirror them here so the Algebraic spec is independent).

  private def buildSpark() = {
    val s = SparkSession.builder()
      .master("local[1]")
      .appName("RollupMaterializerAlgebraicSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    s
  }

  private val dims = List(Dimension.field("region", "region"))
  private val meas = List(
    Measure("order_count",
      AggregateCall(fn = AggregateFn.Count, input = None, alias = "order_count")),
    Measure.aggregate("avg_amount", AggregateFn.Avg, Expr.FieldRef("amount")),
    Measure.aggregate("total_amount", AggregateFn.Sum, Expr.FieldRef("amount")))

  private def model(rollups: List[RollupSpec]): Model =
    Model.of(
      name = "sales", version = 1,
      dimensions = dims, measures = meas,
      source = SourceRef.ByName(table = "sales_base"),
      rollups = rollups).right.get

  test("Algebraic state columns emitted for Avg: count__amount, sum__amount, sumsq__amount") {
    val spec = RollupSpec("by_region_avg", List("region"), List("avg_amount"), None)
    val m = model(List(spec))
    val spark = buildSpark()
    import spark.implicits._
    List(Sale("east", "i1", 100L, 1), Sale("east", "i2", 200L, 2), Sale("west", "i1", 50L, 3))
      .toDF("region", "item", "amount", "units").createOrReplaceTempView("sales_base")
    RollupMaterializer.materialize(spark, m, spec, eager = false).isRight shouldBe true
    val df = spark.table("sales__by_region_avg")
    df.columns.toSet should contain allOf ("region", "count__amount", "sum__amount", "sumsq__amount")
    df.schema("count__amount").dataType shouldBe org.apache.spark.sql.types.LongType
    df.schema("sum__amount").dataType shouldBe org.apache.spark.sql.types.DoubleType
    df.schema("sumsq__amount").dataType shouldBe org.apache.spark.sql.types.DoubleType
  }

  test("validateSpec accepts Avg + Sum together (Algebraic + Additive in one rollup)") {
    val spec = RollupSpec("by_region_mixed", List("region"), List("avg_amount", "total_amount"), None)
    val m = model(List(spec))
    RollupMaterializer.validateSpec(m, spec).isRight shouldBe true
  }

  test("n=1 single-observation edge: materialized sum for a 1-row group equals the base-path sum") {
    val spec = RollupSpec("solo", List("region"), List("total_amount"), None)
    val m = model(List(spec))
    val spark = buildSpark()
    import spark.implicits._
    List(Sale("solo-region", "i1", 42L, 1)).toDF("region", "item", "amount", "units")
      .createOrReplaceTempView("sales_base")
    RollupMaterializer.materialize(spark, m, spec, eager = false).isRight shouldBe true
    val baseSum = spark.table("sales_base")
      .filter("region = 'solo-region'").groupBy("region").sum("amount")
      .collect().head.getLong(1)
    baseSum shouldBe 42L
    // The materialized rollup carries the same partial state.
    val rollSum = spark.table("sales__solo").collect().head.getLong(1)
    rollSum shouldBe 42L
  }
}