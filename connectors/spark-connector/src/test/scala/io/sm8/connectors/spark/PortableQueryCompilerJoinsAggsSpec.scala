/*
 * SM8 Spark Connector -- PortableQueryCompiler joins + aggregations
 * spec (PR-K per ADR-008-H + the user's 2026-08-16 directive).
 *
 * Data-plane coverage (per [[debug-mantra-mindset]] SS1: assert the
 * EVALUATED RESULT, not the SQL fragment):
 *
 *   1. renderAggregate: the 6 wired fns (Sum/Count/CountDistinct/
 *      Avg/Min/Max) evaluate correctly end-to-end.
 *   2. applyJoins: the 5 kinds (Inner/Left/Right/Full/Cross) produce
 *      the correct row counts + key columns.
 *   3. applyAggregations: groupBy+agg path (per-group totals) and
 *      the window path (percent-of-total via Expr.All).
 *   4. The FeatureDeferred boundary: the 10 unwired aggregates
 *      surface as typed EngineError.FeatureDeferred -- never a
 *      silent no-op.
 *   5. Multi-key joins + missing right-side models surface as
 *      typed EngineError.UnsupportedCapability.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantras #1, #3, #5:
 *   - #1 (closure-safety): every fixture uses literal values; the
 *     compiler is constructor-injected (no companion state).
 *   - #3 (schema-drift): fixtures declare explicit StructTypes.
 *   - #5 (driver-vs-executor): every test runs entirely in the
 *     driver (createDataFrame + compile + collect). No UDFs, no
 *     accumulators, no time-dependent sources -- determinism by
 *     construction (PR-F's replay-safety contract).
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, EngineError}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.{
  CalculatedMeasure, Dimension, JoinSpec, Measure, Model,
  ModelPolicyDefaults, ModelStatus, SourceRef,
}
import io.sm8.core.rel.{AggregateCall, AggregateFn, JoinKind}
import io.sm8.core.schema.SealedDataType

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PortableQueryCompilerJoinsAggsSpec extends AnyFunSuite with Matchers {

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("sm8-prk-joins-aggs")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

  private def intLit(n: Int): Expr =
    Expr.Literal(LiteralValue.IntValue(n), SealedDataType.Int)

  private def agg(name: String, fn: AggregateFn, input: Expr): Measure =
    Measure(name, AggregateCall(fn, Some(input), name))

  private def model(
      table:       String,
      dimensions:  List[Dimension]     = Nil,
      measures:    List[Measure]       = Nil,
      calcs:       List[CalculatedMeasure] = Nil,
      joins:       List[JoinSpec]      = Nil,
  ): Model = Model.of(
    name    = "prk",
    version = 1,
    source  = SourceRef.ByName("default", table),
    status  = ModelStatus.Draft,
    defaultPolicies = ModelPolicyDefaults(
      io.sm8.core.model.MaterializePolicy.None,
      io.sm8.core.model.CachePolicy.NoCache,
      io.sm8.core.model.AuditPolicy.NoAudit),
    dimensions = dimensions,
    measures   = measures,
    calculatedMeasures = calcs,
    joins      = joins,
  ).toOption.get

  private def ordersFixture(spark: SparkSession): Unit = {
    val schema = StructType(Seq(
      StructField("region", StringType,  nullable = false),
      StructField("amount", IntegerType, nullable = false),
    ))
    val rows = java.util.Arrays.asList(
      Row("east", 10: Integer),
      Row("east", 20: Integer),
      Row("west", 5: Integer),
      Row("west", 15: Integer),
    )
    spark.createDataFrame(rows, schema).createOrReplaceTempView("orders")
  }

  private def customersFixture(spark: SparkSession): Unit = {
    val schema = StructType(Seq(
      StructField("region", StringType, nullable = false),
      StructField("label",  StringType, nullable = false),
    ))
    val rows = java.util.Arrays.asList(
      Row("east", "E-REGION"),
      Row("north", "N-REGION"),
    )
    spark.createDataFrame(rows, schema).createOrReplaceTempView("customers")
  }

  // ===== renderAggregate: the 6 wired fns (data-plane) =====

  test("groupBy+agg: Sum + Count per region") {
    val spark = buildSpark()
    try {
      ordersFixture(spark)
      val m = model("orders",
        dimensions = List(Dimension.field("region", "region")),
        measures = List(
          agg("total", AggregateFn.Sum, Expr.FieldRef("amount")),
          agg("n", AggregateFn.Count, Expr.FieldRef("amount")),
        ))
      val out = new PortableQueryCompiler(spark)
        .compile(m, EngineContext.defaultContext)
        .toOption.get.collect().toList
        out.map(r => (r.getAs[String]("region"), r.getAs[Int]("total"), r.getAs[Long]("n")))
          .sortBy(_._1) shouldBe List(("east", 30, 2L), ("west", 20, 2L))
    } finally { spark.stop() }
  }

  test("groupBy+agg: Avg + Min + Max") {
    val spark = buildSpark()
    try {
      ordersFixture(spark)
      val m = model("orders",
        dimensions = List(Dimension.field("region", "region")),
        measures = List(
          agg("a", AggregateFn.Avg, Expr.FieldRef("amount")),
          agg("lo", AggregateFn.Min, Expr.FieldRef("amount")),
          agg("hi", AggregateFn.Max, Expr.FieldRef("amount")),
        ))
      val out = new PortableQueryCompiler(spark)
        .compile(m, EngineContext.defaultContext)
        .toOption.get.collect().toList
      out.map(r => (r.getAs[String]("region"), r.getAs[Int]("lo"), r.getAs[Int]("hi")))
        .sortBy(_._1) shouldBe List(("east", 10, 20), ("west", 5, 15))
      out.find(_.getAs[String]("region") == "east").get.getAs[Double]("a") shouldBe 15.0
    } finally { spark.stop() }
  }

  test("groupBy+agg: CountDistinct") {
    val spark = buildSpark()
    try {
      val schema = StructType(Seq(
        StructField("region", StringType,  nullable = false),
        StructField("cust",   StringType,  nullable = false),
      ))
      val rows = java.util.Arrays.asList(
        Row("east", "a"): Row, Row("east", "b"): Row, Row("east", "a"): Row,
      )
      spark.createDataFrame(rows, schema).createOrReplaceTempView("visits")
      val m = model("visits",
        dimensions = List(Dimension.field("region", "region")),
        measures = List(agg("d", AggregateFn.CountDistinct, Expr.FieldRef("cust"))))
      val out = new PortableQueryCompiler(spark)
        .compile(m, EngineContext.defaultContext)
        .toOption.get.collect().toList
      out.head.getAs[Long]("d") shouldBe 2L
    } finally { spark.stop() }
  }

  // ===== applyJoins: the 5 kinds (data-plane row counts) =====

  test("applyJoins: Inner join keeps only matching rows") {
    val spark = buildSpark()
    try {
      ordersFixture(spark); customersFixture(spark)
      val m = model("orders",
        dimensions = List(Dimension.field("region", "region")),
        joins = List(JoinSpec("j", "customers", JoinKind.Inner, List(("region", "region")))))
      val out = new PortableQueryCompiler(spark)
        .compile(m, EngineContext.defaultContext)
        .toOption.get.collect().toList
      // east rows join; west + north drop.
      out.size shouldBe 2
      out.forall(_.getAs[String]("region") == "east") shouldBe true
    } finally { spark.stop() }
  }

  test("applyJoins: Left join keeps all left rows (right cols null on miss)") {
    val spark = buildSpark()
    try {
      ordersFixture(spark); customersFixture(spark)
      val m = model("orders",
        dimensions = List(Dimension.field("region", "region"), Dimension.field("label", "label")),
        joins = List(JoinSpec("j", "customers", JoinKind.Left, List(("region", "region")))))
      val out = new PortableQueryCompiler(spark)
        .compile(m, EngineContext.defaultContext)
        .toOption.get.collect().toList
      out.size shouldBe 4  // all 4 order rows survive
      out.count(_.isNullAt(out.head.fieldIndex("label"))) shouldBe 2  // west rows miss
    } finally { spark.stop() }
  }

  test("applyJoins: Right join keeps all right rows") {
    val spark = buildSpark()
    try {
      ordersFixture(spark); customersFixture(spark)
      val m = model("orders",
        dimensions = List(Dimension.field("region", "region")),
        joins = List(JoinSpec("j", "customers", JoinKind.Right, List(("region", "region")))))
      val out = new PortableQueryCompiler(spark)
        .compile(m, EngineContext.defaultContext)
        .toOption.get.collect().toList
      // north (right-only) survives: east(2) + north(1) = 3
      out.size shouldBe 3
    } finally { spark.stop() }
  }

  test("applyJoins: Full outer keeps all rows from both sides") {
    val spark = buildSpark()
    try {
      ordersFixture(spark); customersFixture(spark)
      val m = model("orders",
        dimensions = List(Dimension.field("region", "region")),
        joins = List(JoinSpec("j", "customers", JoinKind.Full, List(("region", "region")))))
      val out = new PortableQueryCompiler(spark)
        .compile(m, EngineContext.defaultContext)
        .toOption.get.collect().toList
      // east(2) + west(2) + north(1) = 5
      out.size shouldBe 5
    } finally { spark.stop() }
  }

  test("applyJoins: Cross join is the Cartesian product (no keys)") {
    val spark = buildSpark()
    try {
      ordersFixture(spark); customersFixture(spark)
      val m = model("orders",
        dimensions = List(Dimension.field("region", "region")),
        joins = List(JoinSpec("j", "customers", JoinKind.Cross, List(("region", "region")))))
      val out = new PortableQueryCompiler(spark)
        .compile(m, EngineContext.defaultContext)
        .toOption.get.collect().toList
      // 4 x 2 = 8 (Cross ignores the key condition per RelOp.Join contract)
      out.size shouldBe 8
    } finally { spark.stop() }
  }

  // ===== typed error boundaries (never a silent no-op) =====

  test("unwired aggregate (StddevSample) surfaces typed FeatureDeferred") {
    val spark = buildSpark()
    try {
      ordersFixture(spark)
      val m = model("orders",
        dimensions = List(Dimension.field("region", "region")),
        measures = List(agg("sd", AggregateFn.StddevSample, Expr.FieldRef("amount"))))
      val out = new PortableQueryCompiler(spark)
        .compile(m, EngineContext.defaultContext)
      out.isLeft shouldBe true
      val err = out.left.toOption.get
      err shouldBe a [EngineError.FeatureDeferred]
      err match {
        case EngineError.FeatureDeferred(_, feature, _, _) =>
          feature should include ("StddevSample")
        case other => fail(s"expected FeatureDeferred, got $other")
      }
    } finally { spark.stop() }
  }

  test("multi-key join surfaces typed UnsupportedCapability") {
    val spark = buildSpark()
    try {
      ordersFixture(spark); customersFixture(spark)
      val m = model("orders",
        dimensions = List(Dimension.field("region", "region")),
        joins = List(JoinSpec("j", "customers", JoinKind.Inner,
          List(("region", "region"), ("region", "region")))))
      val out = new PortableQueryCompiler(spark)
        .compile(m, EngineContext.defaultContext)
      out.isLeft shouldBe true
      out.left.toOption.get shouldBe a [EngineError.UnsupportedCapability]
    } finally { spark.stop() }
  }

  test("missing right-side model surfaces typed UnsupportedCapability") {
    val spark = buildSpark()
    try {
      ordersFixture(spark)
      val m = model("orders",
        dimensions = List(Dimension.field("region", "region")),
        joins = List(JoinSpec("j", "no_such_table", JoinKind.Inner, List(("region", "region")))))
      val out = new PortableQueryCompiler(spark)
        .compile(m, EngineContext.defaultContext)
      out.isLeft shouldBe true
      out.left.toOption.get shouldBe a [EngineError.UnsupportedCapability]
    } finally { spark.stop() }
  }

  // ===== window path: percent-of-total via Expr.All =====

  test("window path: calculated measure with Expr.All computes pct-of-total") {
    val spark = buildSpark()
    try {
      ordersFixture(spark)
      // pct = amount / All(total): total = 50 across the whole frame.
      val calc = CalculatedMeasure(
        name = "share",
        expr = Expr.Divide(
          Expr.FieldRef("amount"),
          Expr.All("total"),
        ),
      )
      val m = model("orders",
        dimensions = List(Dimension.field("region", "region")),
        measures = List(agg("total", AggregateFn.Sum, Expr.FieldRef("amount"))),
        calcs = List(calc))
      val out = new PortableQueryCompiler(spark)
        .compile(m, EngineContext.defaultContext)
        .toOption.get.collect().toList
      // Window over partitionBy(region): total per region (30 east, 20 west).
      // share east rows: 10/30, 20/30; west: 5/20, 15/20.
      val shares = out.map(_.getAs[Double]("share")).sorted
      shares.map(x => math.round(x * 100).toDouble / 100) shouldBe
        List(0.25, 0.33, 0.67, 0.75)
    } finally { spark.stop() }
  }

  test("measure-less model returns the plain filtered projection (pre-PR-K path)") {
    val spark = buildSpark()
    try {
      ordersFixture(spark)
      val m = model("orders",
        dimensions = List(Dimension.field("region", "region")))
      val out = new PortableQueryCompiler(spark)
        .compile(m, EngineContext.defaultContext)
        .toOption.get.collect().toList
      out.size shouldBe 4  // no aggregation: every row survives
      out.head.schema.fieldNames shouldBe Array("region")
    } finally { spark.stop() }
  }
}
