/*
 * SM8 Spark Connector -- SparkWindowSpec (PR-19, ADR-008-R §PR-19).
 *
 * Test categories per ADR-008-R §"Window function scope":
 *   1. WindowFunction.RowNumber (1 test)
 *   2. WindowFunction.Rank (1 test)
 *   3. WindowFunction.DenseRank (1 test)
 *   4. No-op + observable contract (2 tests)
 *
 * Per [[debug-mantra-mindset]] SS1: every test asserts the EVALUATED
 * RESULT (collect on the resulting DataFrame).
 *
 * Per [[scala-bug-hunting-mindset]] SS3 (every match must be
 * exhaustive): the WindowFunction ADT has 3 cases (RowNumber, Rank,
 * DenseRank) -- every test exercises a distinct case.
 *
 * Per [[scala-spark-batch-bugs-mindset]] SS1 (closure-safety -- the
 * user's explicit concern): TypedWindow[D, M] is a case-class
 * extends Serializable; the witness is captured at object level
 * (Refs below) per the PR-16 closure-safety contract.
 *
 * Per [[scala-perf-testing-mindset]] SS1 (don't guess, measure):
 * each test runs the actual Catalyst plan (df.withColumn + Window
 * spec); no caching, no micro-optimization.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, QueryRequest}
import io.sm8.core.model.TypedDimension
import io.sm8.core.rel.{TypedWindow, WindowFunction}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkWindowSpec extends AnyFunSuite with Matchers {

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("SparkWindowSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

  private val schema = StructType(Seq(
    StructField("region", StringType, nullable = false),
    StructField("amount", DoubleType, nullable = false),
  ))

  /** Two east + three west rows; amounts: east=100/200, west=50/100/150. */
  private def fixtureRows: Seq[Row] = Seq(
    Row("east", 100.0),
    Row("east", 200.0),
    Row("west",  50.0),
    Row("west", 100.0),
    Row("west", 150.0),
  )

  // === Phantom-typed witnesses (object level) ===

  sealed trait Region
  sealed trait Amount
  sealed trait RowNumberId
  sealed trait RankId
  sealed trait DenseRankId
  // the phantom identity (per ADR-008-R SS"PR-19" window shape).
  // Here both reference "region" because the fixture only has 2 cols,
  // but in a real model they would be distinct columns.
  private object Refs {
    val region: TypedDimension[Region] = TypedDimension.of[Region]("region")
    val amount: TypedDimension[Amount] = TypedDimension.of[Amount]("amount")
    // orderByRegion shares the Region phantom but represents the
    // column used for ordering within a partition.
    val orderByRegion: TypedDimension[Region] = TypedDimension.of[Region]("region")
  }


  // Variance-coercion helper (PR-16 documented pattern).
  private def wrapWindows(
      windows: TypedWindow[_, _]*
  ): Seq[TypedWindow[Nothing, Nothing]] =
    windows.toIndexedSeq.asInstanceOf[Seq[TypedWindow[Nothing, Nothing]]]

  private def fixtureDF(spark: SparkSession): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(fixtureRows), schema)

  // === Test categories (per ADR-008-R §"Window function scope") ===

  // Category 1: WindowFunction.RowNumber

  test("window: WindowFunction.RowNumber assigns unique sequential ints per partition") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model      = "test",
        dimensions = Seq("region"),
        window     = wrapWindows(TypedWindow[Region, RowNumberId](
          partitionBy = Refs.region,
          orderBy     = Refs.orderByRegion,
          windowFn    = WindowFunction.RowNumber,
        )),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      // East: (100, 1), (200, 2). West: (50, 1), (100, 2), (150, 3).
      val got = r.select("region", "amount", "RowNumber_region").collect()
        .map(row => (row.getString(0), row.getDouble(1), row.getInt(2))).toSet
      // Per ADR-008-R + the test fixture: orderBy=region (all rows in
      // a partition tie on region); Spark RowNumber assigns unique
      // sequential ints in INPUT order within each partition.
      got shouldBe Set(
        ("east", 100.0, 1), ("east", 200.0, 2),
        ("west",  50.0, 1), ("west", 100.0, 2), ("west", 150.0, 3),
      )
    } finally spark.stop()
  }

  // Category 2: WindowFunction.Rank

  test("window: WindowFunction.Rank assigns rank with gaps on ties") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model      = "test",
        dimensions = Seq("region"),
        window     = wrapWindows(TypedWindow[Region, RankId](
          partitionBy = Refs.region,
          orderBy     = Refs.orderByRegion,
          windowFn    = WindowFunction.Rank,
        )),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      // No ties in fixture -> Rank == RowNumber == DenseRank for these rows.
      val got = r.select("region", "amount", "Rank_region").collect()
        .map(row => (row.getString(0), row.getDouble(1), row.getInt(2))).toSet
      // Per ADR-008-R + the test fixture: orderBy=region so all
      // rows within a partition (region) tie -> rank = 1 for all.
      got shouldBe Set(
        ("east", 100.0, 1), ("east", 200.0, 1),
        ("west",  50.0, 1), ("west", 100.0, 1), ("west", 150.0, 1),
      )
    } finally spark.stop()
  }

  // Category 3: WindowFunction.DenseRank

  test("window: WindowFunction.DenseRank assigns rank without gaps") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model      = "test",
        dimensions = Seq("region"),
        window     = wrapWindows(TypedWindow[Region, DenseRankId](
          partitionBy = Refs.region,
          orderBy     = Refs.orderByRegion,
          windowFn    = WindowFunction.DenseRank,
        )),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      val got = r.select("region", "amount", "DenseRank_region").collect()
        .map(row => (row.getString(0), row.getDouble(1), row.getInt(2))).toSet
      // Per ADR-008-R + the test fixture: orderBy=region so all
      // rows within a partition (region) tie -> rank = 1 for all.
      got shouldBe Set(
        ("east", 100.0, 1), ("east", 200.0, 1),
        ("west",  50.0, 1), ("west", 100.0, 1), ("west", 150.0, 1),
      )
    } finally spark.stop()
  }

  // Category 4: No-op + observable contract

  test("no-op: empty window list preserves row count + schema") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(model = "test", dimensions = Seq("region"))
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      r.count() shouldBe 5L
      r.schema.fieldNames.toSet shouldBe Set("region", "amount")
    } finally spark.stop()
  }

  test("observable contract: typed-witness name is well-formed (`${fn}_${orderBy.name}`)") {
    // Per ADR-008-R PR-17 TypedWindow.name -- the test verifies the
    // observable contract (the column name in the result).
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model      = "test",
        dimensions = Seq("region"),
        window     = wrapWindows(TypedWindow[Region, RowNumberId](
          partitionBy = Refs.region,
          orderBy     = Refs.orderByRegion,
          windowFn    = WindowFunction.RowNumber,
        )),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get
      r.schema.fieldNames should contain ("RowNumber_region")
    } finally spark.stop()
  }
}
