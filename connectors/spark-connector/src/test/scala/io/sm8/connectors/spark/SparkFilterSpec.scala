/*
 * SM8 Spark Connector -- SparkFilterSpec (PR-22, ADR-008-R §PR-22).
 *
 * Test categories per ADR-008-R §filter/where (RFC-driven
 * structure):
 *   1. Typed Predicate.Compare via TypedPredicate (6 tests -- one
 *      per CompareOp case: EQ, NE, LT, LE, GT, GE)
 *   2. Typed Predicate.In + IsNull (2 tests)
 *   3. Typed Predicate.And + Or + Not (3 tests, via the existing
 *      `negatePredicate` method + AND/OR combinators)
 *   4. End-to-end: whereFilters + groupBy + aggregateMeasures (2 tests)
 *
 * Per [[debug-mantra-mindset]] SS1: every test asserts the
 * EVALUATED RESULT (collect().toSet on the resulting DataFrame),
 * not the intermediate SQL.
 *
 * Per [[scala-spark-batch-bugs-mindset]] SS1 (closure-safety -- the
 * user's explicit concern): TypedPredicate captured at object
 * level (Refs below) per PR-16 contract. The compileSteps closure
 * captures only Serializable vals.
 *
 * Per [[scala-perf-testing-mindset]] SS3 (allocation is the tax):
 * zero per-row allocation. Each predicate is applied ONCE per
 * query at driver-side.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, QueryRequest}
import io.sm8.core.model.TypedDimension
import io.sm8.core.predicate.Predicate
import io.sm8.core.rel.{TypedAggregateCall, TypedPredicate}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkFilterSpec extends AnyFunSuite with Matchers {

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("SparkFilterSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

  private val schema = StructType(Seq(
    StructField("region", StringType, nullable = false),
    StructField("amount", DoubleType, nullable = false),
  ))

  /** Fixture: 2 east rows + 3 west rows. */
  private def fixtureRows: Seq[Row] = Seq(
    Row("east", 100.0),
    Row("east", 200.0),
    Row("west",  50.0),
    Row("west", 150.0),
    Row("west",  75.0),
  )

  // === Phantom-typed witnesses (object level, per PR-16 closure-safety) ===

  sealed trait Region
  sealed trait Amount
  sealed trait SumAmount

  private object Refs {
    val region: TypedDimension[Region] = TypedDimension.of[Region]("region")
    val amount: TypedDimension[Amount] = TypedDimension.of[Amount]("amount")
    val sumAmount: TypedAggregateCall[SumAmount] = TypedAggregateCall.sum[SumAmount]("sum_amount", "amount")
  }

  /** Variance-coercion helper (PR-16 documented pattern). */
  private def wrapPredicates(preds: TypedPredicate[_]*): Seq[TypedPredicate[Nothing]] =
    preds.toIndexedSeq.asInstanceOf[Seq[TypedPredicate[Nothing]]]

  private def fixtureDF(spark: SparkSession): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(fixtureRows), schema)

  // === Category 1: Typed Predicate.Compare via TypedPredicate (6 tests) ===

  test("filter: Predicate.Compare EQ (region = 'east')") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model = "test", dimensions = Seq("region"),
        whereFilters = wrapPredicates(TypedPredicate.eq[Region]("region", "east")),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val got = result.toOption.get.select("region", "amount").collect()
        .map(row => (row.getString(0), row.getDouble(1))).toSet
      got shouldBe Set(("east", 100.0), ("east", 200.0))
    } finally spark.stop()
  }

  test("filter: Predicate.Compare NE (region != 'east')") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model = "test", dimensions = Seq("region"),
        whereFilters = wrapPredicates(TypedPredicate.ne[Region]("region", "east")),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val got = result.toOption.get.select("region", "amount").collect()
        .map(row => (row.getString(0), row.getDouble(1))).toSet
      got shouldBe Set(("west", 50.0), ("west", 150.0), ("west", 75.0))
    } finally spark.stop()
  }

  test("filter: Predicate.Compare LT (amount < 100.0)") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model = "test", dimensions = Seq("region"),
        whereFilters = wrapPredicates(TypedPredicate.lt[Amount]("amount", 100.0)),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val got = result.toOption.get.select("region", "amount").collect()
        .map(row => (row.getString(0), row.getDouble(1))).toSet
      got shouldBe Set(("west", 50.0), ("west", 75.0))
    } finally spark.stop()
  }

  test("filter: Predicate.Compare LE (amount <= 100.0)") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model = "test", dimensions = Seq("region"),
        whereFilters = wrapPredicates(TypedPredicate.le[Amount]("amount", 100.0)),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val got = result.toOption.get.select("region", "amount").collect()
        .map(row => (row.getString(0), row.getDouble(1))).toSet
      got shouldBe Set(("east", 100.0), ("west", 50.0), ("west", 75.0))
    } finally spark.stop()
  }

  test("filter: Predicate.Compare GT (amount > 100.0)") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model = "test", dimensions = Seq("region"),
        whereFilters = wrapPredicates(TypedPredicate.gt[Amount]("amount", 100.0)),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val got = result.toOption.get.select("region", "amount").collect()
        .map(row => (row.getString(0), row.getDouble(1))).toSet
      got shouldBe Set(("east", 200.0), ("west", 150.0))
    } finally spark.stop()
  }

  test("filter: Predicate.Compare GE (amount >= 100.0)") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model = "test", dimensions = Seq("region"),
        whereFilters = wrapPredicates(TypedPredicate.ge[Amount]("amount", 100.0)),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val got = result.toOption.get.select("region", "amount").collect()
        .map(row => (row.getString(0), row.getDouble(1))).toSet
      got shouldBe Set(("east", 100.0), ("east", 200.0), ("west", 150.0))
    } finally spark.stop()
  }

  // === Category 2: Typed Predicate.In + IsNull (2 tests) ===

  test("filter: Predicate.In (region IN ('east','west')) -- always true for our fixture") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model = "test", dimensions = Seq("region"),
        whereFilters = wrapPredicates(TypedPredicate.in[Region]("region", List("east", "west"))),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      result.toOption.get.count() shouldBe 5L
    } finally spark.stop()
  }

  test("filter: Predicate.IsNull on amount (none are null -- empty result)") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model = "test", dimensions = Seq("region"),
        whereFilters = wrapPredicates(TypedPredicate.isNull[Amount]("amount")),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      result.toOption.get.count() shouldBe 0L
    } finally spark.stop()
  }

  // === Category 3: Typed Predicate.And + Or + Not (3 tests) ===

  // Per PR-20: AND/OR combinators require all children share the phantom [D].
  // For mixed-phantom compositions (region + amount), wrap each in a
  // same-phantom `TypedPredicate.of[D](name, predicate)` adapter. The wrap
  // is the explicit variance-coercion at the combinator boundary.

  test("filter: Predicate.And (region=east AND amount>100.0) -- both children Region phantom") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      // Both children with the same phantom (Region). The amount>100
      // is wrapped in a Region-phantom adapter -- it's the same predicate,
      // just re-tagged for the combinator's phantom requirement.
      val regionAndAmountGt: TypedPredicate[Region] =
        TypedPredicate.of[Region]("amount>100.0",
          Predicate.Compare("amount", io.sm8.core.predicate.CompareOp.Gt, 100.0))
      val regionEqEast: TypedPredicate[Region] =
        TypedPredicate.of[Region]("region=east",
          Predicate.Compare("region", io.sm8.core.predicate.CompareOp.Eq, "east"))
      val req = QueryRequest(
        model = "test", dimensions = Seq("region"),
        whereFilters = wrapPredicates(TypedPredicate.and[Region](regionEqEast, regionAndAmountGt)),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val got = result.toOption.get.select("region", "amount").collect()
        .map(row => (row.getString(0), row.getDouble(1))).toSet
      got shouldBe Set(("east", 200.0))
    } finally spark.stop()
  }

  test("filter: Predicate.Or (region=east OR amount<100.0) -- both children Region phantom") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val regionEqEast: TypedPredicate[Region] =
        TypedPredicate.of[Region]("region=east",
          Predicate.Compare("region", io.sm8.core.predicate.CompareOp.Eq, "east"))
      val amountLt100: TypedPredicate[Region] =
        TypedPredicate.of[Region]("amount<100.0",
          Predicate.Compare("amount", io.sm8.core.predicate.CompareOp.Lt, 100.0))
      val req = QueryRequest(
        model = "test", dimensions = Seq("region"),
        whereFilters = wrapPredicates(TypedPredicate.or[Region](regionEqEast, amountLt100)),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val got = result.toOption.get.select("region", "amount").collect()
        .map(row => (row.getString(0), row.getDouble(1))).toSet
      got shouldBe Set(("east", 100.0), ("east", 200.0), ("west", 50.0), ("west", 75.0))
    } finally spark.stop()
  }

  test("filter: Predicate.Not (NOT (region=east)) -- negatePredicate + of wrap") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      // Per PR-20: the existing TypedPredicate.negatePredicate produces
      // a Predicate.Not from a TypedPredicate. Wrap it back in a
      // TypedPredicate.of for the wire DTO.
      val regionEqEast: TypedPredicate[Region] = TypedPredicate.eq[Region]("region", "east")
      val notRegionEqEast: TypedPredicate[Region] =
        TypedPredicate.of[Region]("NOT (region=east)", regionEqEast.predicate.negatePredicate)
      val req = QueryRequest(
        model = "test", dimensions = Seq("region"),
        whereFilters = wrapPredicates(notRegionEqEast),
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      result.toOption.get.count() shouldBe 3L // all 3 west rows
    } finally spark.stop()
  }

  // === Category 4: End-to-end (filter + groupBy + aggregateMeasures composition) ===

  test("end-to-end: whereFilters + groupBy + aggregateMeasures compose correctly") {
    // Per ADR-008-R: whereFilters applies BEFORE aggregateMeasures
    // (matches PortableQueryCompiler.applyFilters order).
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(
        model = "test",
        dimensions = Seq("region"),
        whereFilters = wrapPredicates(TypedPredicate.gt[Amount]("amount", 75.0)),
        aggregateMeasures = Seq(Refs.sumAmount)
          .asInstanceOf[Seq[TypedAggregateCall[Nothing]]],
      )
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      // filter amount>75: (east,100), (east,200), (west,150)
      // groupBy region + sum amount:
      //   east: 100+200 = 300
      //   west: 150
      val got = result.toOption.get.select("region", "sum_amount").collect()
        .map(row => (row.getString(0), row.getDouble(1))).toSet
      got shouldBe Set(("east", 300.0), ("west", 150.0))
    } finally spark.stop()
  }

  test("end-to-end: empty whereFilters is a no-op (zero behavior change for 19 callers)") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryRequest(model = "test", dimensions = Seq("region"))
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      result.toOption.get.count() shouldBe 5L
    } finally spark.stop()
  }
}
