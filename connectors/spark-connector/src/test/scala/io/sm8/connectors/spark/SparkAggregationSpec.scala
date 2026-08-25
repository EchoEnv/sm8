/*
 * SM8 Spark Connector -- SparkAggregationSpec (PR-19, ADR-008-R §PR-19).
 *
 * Test categories per ADR-008-R §"Decision":
 *   1. Typed aggregateMeasures (5 tests -- Sum, Count, Avg, CountDistinct, Min+Max)
 *   2. Typed having (3 tests -- GT, EQ, LE)
 *   3. Typed orderBy + partitionBy (2 tests)
 *   4. No-op (1 test -- legacy 19 callers)
 *
 * Per `debug-mantra` SS1: every test asserts the EVALUATED
 * RESULT (collect on the resulting DataFrame).
 *
 * Per `scala-spark-batch-bugs-mindset` mantras SS1, SS3, SS5:
 *   - SS1 (closure-safety): every fixture uses literal values; the
 *     compiler is created per-test (no companion state, no ThreadLocals).
 *   - SS3 (schema-drift): fixtures declare explicit StructTypes.
 *   - SS5 (driver-vs-executor): every test runs entirely in the driver
 *     (createDataFrame + compile + collect). No UDFs, no accumulators.
 *
 * Per `scala-bug-hunting-mindset` SS1 (trust compiler, not runtime):
 * the typed-witness -> QueryRequest cast is the PR-16 documented
 * pattern (`asInstanceOf[Seq[Foo[Nothing]]]` at the variance
 * boundary). The cast is SAFE because the phantom `[D]` is captured
 * at the witness construction site (object level).
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, QueryRequest}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.TypedDimension
import io.sm8.core.engine.{EngineContext, EngineError, QueryRequest}
import io.sm8.core.rel.{AggregateFn, ComparisonOp, Having, PartitionBy, TypedAggregateCall}
import io.sm8.core.schema.SealedDataType

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkAggregationSpec extends AnyFunSuite with Matchers {

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("SparkAggregationSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

  private val schema = StructType(Seq(
    StructField("region",  StringType,  nullable = false),
    StructField("amount",  DoubleType,  nullable = false),
    StructField("id",      LongType,    nullable = false),
  ))

  private def fixtureRows: Seq[Row] = Seq(
    Row("east", 100.0, 1L),
    Row("east", 200.0, 2L),
    Row("east", 300.0, 3L),
    Row("west",  50.0, 4L),
    Row("west", 150.0, 5L),
    Row("west",  75.0, 6L),
  )

  // === Phantom-typed witnesses (object level, per PR-16 closure-safety contract) ===

  sealed trait PatientCount
  sealed trait Region
  sealed trait Amount
  sealed trait Id

  private object Refs {
    val region:           TypedDimension[Region]  = TypedDimension.of[Region]("region")
    val amount:           TypedDimension[Amount]  = TypedDimension.of[Amount]("amount")
    val id:               TypedDimension[Id]      = TypedDimension.of[Id]("id")
    val count:            TypedAggregateCall[PatientCount] = TypedAggregateCall.count[PatientCount]("count")
    val sumAmount:        TypedAggregateCall[PatientCount] = TypedAggregateCall.sum[PatientCount]("sum_amount", "amount")
    val avgAmount:        TypedAggregateCall[PatientCount] = TypedAggregateCall.avg[PatientCount]("avg_amount", "amount")
    val countDistinctId:  TypedAggregateCall[PatientCount] = TypedAggregateCall.countDistinct[PatientCount]("distinct_ids", "id")
    val minAmount:        TypedAggregateCall[PatientCount] = TypedAggregateCall.min[PatientCount]("min_amount", "amount")
    val maxAmount:        TypedAggregateCall[PatientCount] = TypedAggregateCall.max[PatientCount]("max_amount", "amount")
  }

  // === Variance-coercion helpers (PR-16 documented pattern) ===

  // The QueryRequest fields are typed as `Seq[Foo[Nothing]]`
  // (variance-safety for the wire DTO). Per
  // `scala-bug-hunting-mindset` SS1, the typed-witness `Seq` is
  // explicitly erased via `asInstanceOf` at the variance boundary.
  // SAFE: the phantom `[D]` is captured at construction (Refs above).

  private def wrapMeasures(
      measures: TypedAggregateCall[PatientCount]*
  ): Seq[TypedAggregateCall[Nothing]] =
    measures.toIndexedSeq.asInstanceOf[Seq[TypedAggregateCall[Nothing]]]

  private def wrapDimensions(
      dims: TypedDimension[_]*
  ): Seq[TypedDimension[Nothing]] =
    dims.toIndexedSeq.asInstanceOf[Seq[TypedDimension[Nothing]]]

  private def wrapHavings(
      havs: Having[_]*
  ): Seq[Having[Nothing]] =
    havs.toIndexedSeq.asInstanceOf[Seq[Having[Nothing]]]

  private def wrapPartitions(
      parts: PartitionBy[_]*
  ): Seq[PartitionBy[Nothing]] =
    parts.toIndexedSeq.asInstanceOf[Seq[PartitionBy[Nothing]]]

  private def fixtureDF(spark: SparkSession): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(fixtureRows), schema)

  // === Test categories (per ADR-008-R §"Decision") ===

  // Category 1: Typed aggregateMeasures (5 tests)

  test("aggregateMeasures: Sum grouped by region") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model             = "test",
        dimensions        = Seq("region"),
        aggregateMeasures = wrapMeasures(Refs.sumAmount),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      val got = r.select("region", "sum_amount").collect()
        .map(row => (row.getString(0), row.getDouble(1))).toSet
      got shouldBe Set(("east", 600.0), ("west", 275.0))
    } finally spark.stop()
  }

  test("aggregateMeasures: Count grouped by region") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model             = "test",
        dimensions        = Seq("region"),
        aggregateMeasures = wrapMeasures(Refs.count),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      val got = r.select("region", "count").collect()
        .map(row => (row.getString(0), row.getLong(1))).toSet
      got shouldBe Set(("east", 3L), ("west", 3L))
    } finally spark.stop()
  }

  test("aggregateMeasures: Avg grouped by region") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model             = "test",
        dimensions        = Seq("region"),
        aggregateMeasures = wrapMeasures(Refs.avgAmount),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      val got = r.select("region", "avg_amount").collect()
        .map(row => (row.getString(0), row.getDouble(1))).toMap
      got("east") shouldBe 200.0 +- 0.001
      got("west") shouldBe 91.66666666666667 +- 0.001
    } finally spark.stop()
  }

  test("aggregateMeasures: CountDistinct on id column") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model             = "test",
        dimensions        = Seq("region"),
        aggregateMeasures = wrapMeasures(Refs.countDistinctId),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      val got = r.select("region", "distinct_ids").collect()
        .map(row => (row.getString(0), row.getLong(1))).toSet
      got shouldBe Set(("east", 3L), ("west", 3L))
    } finally spark.stop()
  }

  test("aggregateMeasures: Min + Max grouped by region") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model             = "test",
        dimensions        = Seq("region"),
        aggregateMeasures = wrapMeasures(Refs.minAmount, Refs.maxAmount),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      val got = r.select("region", "min_amount", "max_amount").collect()
        .map(row => (row.getString(0), row.getDouble(1), row.getDouble(2))).toSet
      got shouldBe Set(("east", 100.0, 300.0), ("west", 50.0, 150.0))
    } finally spark.stop()
  }

  // Category 2: Typed having (3 tests)

  test("having: ComparisonOp.GE (amount >= 100) lowers to >= without MatchError (P1-SM8-01)") {
    // P1-SM8-01 / B1: `ComparisonOp.GE` was the missing 6th arm of
    // havingColumn — a `Having` with GE threw `MatchError` at runtime.
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model      = "test",
        dimensions = Seq("region"),
        having     = wrapHavings(Having[Amount](Refs.amount, ComparisonOp.GE,
          Expr.Literal(LiteralValue.DoubleValue(100.0), SealedDataType.Double))),
      )
      // Before the fix this threw MatchError; now it returns a valid
      // DataFrame plan containing `>=`.
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      val got = r.select("region", "amount").collect()
        .map(row => (row.getString(0), row.getDouble(1))).toSet
      // amount >= 100 → 100, 200, 300 (east) + 150 (west)
      got shouldBe Set(
        ("east", 100.0), ("east", 200.0), ("east", 300.0), ("west", 150.0)
      )
    } finally spark.stop()
  }

  test("having: ComparisonOp.EQ (id == 3)") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model      = "test",
        dimensions = Seq("region"),
        having     = wrapHavings(Having[Id](Refs.id, ComparisonOp.EQ,
          Expr.Literal(LiteralValue.LongValue(3L), SealedDataType.BigInt))),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      val got = r.select("region", "id").collect()
        .map(row => (row.getString(0), row.getLong(1))).toSet
      got shouldBe Set(("east", 3L))
    } finally spark.stop()
  }

  test("having: ComparisonOp.LE (amount <= 100)") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model      = "test",
        dimensions = Seq("region"),
        having     = wrapHavings(Having[Amount](Refs.amount, ComparisonOp.LE,
          Expr.Literal(LiteralValue.DoubleValue(100.0), SealedDataType.Double))),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      val got = r.select("region", "amount").collect()
        .map(row => (row.getString(0), row.getDouble(1))).toSet
      got shouldBe Set(("east", 100.0), ("west", 50.0), ("west", 75.0))
    } finally spark.stop()
  }

  // Category 3: Typed orderBy + partitionBy (2 tests)

  test("orderBy: ascending by amount") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model      = "test",
        dimensions = Seq("region"),
        orderBy    = wrapDimensions(Refs.amount),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      val amounts = r.select("amount").collect().map(_.getDouble(0)).toList
      amounts shouldBe Seq(50.0, 75.0, 100.0, 150.0, 200.0, 300.0)
    } finally spark.stop()
  }

  test("partitionBy: hint applied (does not break result row count)") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model        = "test",
        dimensions   = Seq("region"),
        partitionBy  = wrapPartitions(PartitionBy[Region](Refs.region)),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      result.toOption.get.count() shouldBe 6L
    } finally spark.stop()
  }

  // Category 4: No-op (legacy 19 callers -- zero behavior change)

  test("no-op: empty typed fields returns input unchanged") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(model = "test", dimensions = Seq("region"))
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      r.count() shouldBe 6L
      r.schema.fields.map(_.name).toSet shouldBe Set("region", "amount", "id")
    } finally spark.stop()
  }
  // -- PR-133 / ADR-008-X: lowering-layer input-required fix regression tests --

  // -- Test 1: Count with input = None lowers to count(lit(1)) (COUNT(*) shape) --
  test("aggregateToColumn: Count with input = None lowers to count(lit(1)) and produces 2 grouped rows") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model    = "test",
        dimensions  = Seq("region"),
        aggregateMeasures = Seq(TypedAggregateCall.of[Nothing](
          name = "encounter_count",
          fn   = AggregateFn.Count,
          input = None  // <-- the COUNT(*) shape
        )),
        whereFilters = Nil,
        having    = Nil,
        partitionBy = Nil,
        orderBy   = Nil,
        window    = Nil,
        limit     = None,
        sortDirections = Nil
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val rows = result.toOption.get.select("region", "encounter_count").collect()
        .map(r => (r.getString(0), r.getLong(1))).toSet
      rows shouldBe Set(("east", 3L), ("west", 3L))
    } finally spark.stop()
  }

  // -- Test 2: Sum with input = None fails loud with typed error --
  test("aggregateToColumn: Sum with input = None fails loud with EngineError.UnsupportedCapability") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model    = "test",
        dimensions  = Seq("region"),
        aggregateMeasures = Seq(TypedAggregateCall.of[Nothing](
          name = "total",
          fn   = AggregateFn.Sum,
          input = None  // <-- misconfiguration
        )),
        whereFilters = Nil,
        having    = Nil,
        partitionBy = Nil,
        orderBy   = Nil,
        window    = Nil,
        limit     = None,
        sortDirections = Nil
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isLeft shouldBe true
      val err = result.left.toOption.get
      err shouldBe a [EngineError.UnsupportedCapability]
      err.message should include ("measures[total]")
      err.message should include ("Sum")
    } finally spark.stop()
  }

  // -- Test 3: CountDistinct with input = None fails loud with typed error --
  test("aggregateToColumn: CountDistinct with input = None fails loud with typed error") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model    = "test",
        dimensions  = Seq("region"),
        aggregateMeasures = Seq(TypedAggregateCall.of[Nothing](
          name = "unique",
          fn   = AggregateFn.CountDistinct,
          input = None
        )),
        whereFilters = Nil,
        having    = Nil,
        partitionBy = Nil,
        orderBy   = Nil,
        window    = Nil,
        limit     = None,
        sortDirections = Nil
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isLeft shouldBe true
      val err = result.left.toOption.get
      err shouldBe a [EngineError.UnsupportedCapability]
      err.message should include ("CountDistinct")
    } finally spark.stop()
  }

  // -- Test 4: Avg/Min/Max with input = None all fail loud --
  test("aggregateToColumn: Avg/Min/Max with input = None all fail loud (mirror Sum)") {
    for (fn <- Seq(AggregateFn.Avg, AggregateFn.Min, AggregateFn.Max)) {
      val spark = buildSpark()
      try {
        val df = fixtureDF(spark)
        val req = QueryRequest(
          model    = "test",
          dimensions  = Seq("region"),
          aggregateMeasures = Seq(TypedAggregateCall.of[Nothing](
            name = s"bad_${fn}",
            fn   = fn,
            input = None
          )),
          whereFilters = Nil,
          having    = Nil,
          partitionBy = Nil,
          orderBy   = Nil,
          window    = Nil,
          limit     = None,
          sortDirections = Nil
        )
        val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
        result.isLeft shouldBe true
        result.left.toOption.get.message should include (fn.toString)
      } finally spark.stop()
    }
  }
}
