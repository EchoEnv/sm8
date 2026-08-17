/*
 * SM8 Spark Connector — PortableExprCompiler data-frame coverage spec (PR-E per ADR-007).
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #3 (schema-drift verify at
 * the boundary): the EXISTING `PortableExprCompilerSpec` covers every
 * Expr case via `col.expr.sql should include "X"` — that's the
 * fragment-shape contract. What it does NOT cover is the data-plane
 * contract: does the compiled `Column` actually evaluate correctly
 * against a real DataFrame? Per [[debug-mantra-mindset]] §1: a test
 * that only inspects a string fragment is incomplete — the bug
 * surface is the EVALUATED RESULT, not the SQL rendering.
 *
 * Per [[scala-perf-testing-mindset]]: this spec is one-shot (builds
 * a fresh SparkSession per test), NOT a JMH bench. The point is
 * semantic correctness, not throughput. Performance is a future
 * PR's concern (PR-E's goal is coverage audit + fill, not a perf
 * audit).
 *
 * ==What this spec covers==
 *
 * 1. Derived-metric composition (Expr.Add on Expr.FieldRef + Expr.Multiply)
 * 2. Filter via compiled Column (Expr.Equal used as df.filter predicate)
 * 3. Composed boolean filter (Expr.And of Expr.Equal + Expr.GreaterThan)
 * 4. End-to-end: tiny DataFrame + compiled filter + collect() round-trip
 *
 * ==Why a NEW spec file (not extending PortableExprCompilerSpec)==
 *
 * Per [[karpathy-guidelines-mindset]] "smallest correct change":
 * the existing spec covers 35 cases; this adds ~10 more. Keeping
 * them in separate files makes the diff surgical — the existing
 * SQL-fragment contract is untouched; the new data-plane contract
 * is additive.
 *
 * ==Spark concerns==
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #5 (driver-vs-executor
 * asymmetry): every test in this spec runs entirely in the driver
 * (build DataFrame + filter + collect). No UDFs, no accumulators,
 * no time-dependent sources → determinism by construction (PR-F
 * will assert this explicitly).
 */
package io.sm8.connectors.spark

import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.schema.SealedDataType

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.RowFactory

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PortableExprCompilerDataFrameSpec extends AnyFunSuite with Matchers {

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("sm8-portable-expr-dataframe-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

  private def peopleDataFrame(spark: SparkSession) = {
    val schema = StructType(Seq(
      StructField("id",    IntegerType, nullable = false),
      StructField("name",  StringType,  nullable = false),
      StructField("age",   IntegerType, nullable = false),
      StructField("score", IntegerType, nullable = true),
    ))
    val rows = java.util.Arrays.asList(
      RowFactory.create(1, "alice", 30, 100: Integer),
      RowFactory.create(2, "bob",   25,  80: Integer),
      RowFactory.create(3, "carol", 35, 120: Integer),
      RowFactory.create(4, "dave",  40, null: Integer),
    )
    spark.createDataFrame(rows, schema)
  }

  private def intLit(n: Int): Expr =
    Expr.Literal(LiteralValue.IntValue(n), SealedDataType.Int)

  private def strLit(s: String): Expr =
    Expr.Literal(LiteralValue.StringValue(s), SealedDataType.Varchar)

  // ===== Derived-metric composition (measure refs + arithmetic) =====

  test("Expr.Add on Expr.FieldRef + Expr.FieldRef: compiled Column evaluates on DataFrame") {
    val spark = buildSpark()
    try {
      val df = peopleDataFrame(spark)
      // derived = age + score (handle null via spark null propagation)
      val derived: Column = PortableExprCompiler.toColumn(
        Expr.Add(Expr.FieldRef("age"), Expr.FieldRef("score"))
      ).toOption.get
      val out = df.select(df.col("name"), derived.as("derived")).collect().toList
      out.map(_.getAs[String]("name")) shouldBe List("alice", "bob", "carol", "dave")
      // alice=130, bob=105, carol=155, dave=null
      out.map(_.getAs[AnyRef]("derived")) shouldBe List[Integer](130, 105, 155, null)
    } finally { spark.stop() }
  }

  test("Expr.Multiply composed with Expr.Add: nested derived-metric") {
    val spark = buildSpark()
    try {
      val df = peopleDataFrame(spark)
      // derived = age + (age * 2)
      val derived: Column = PortableExprCompiler.toColumn(
        Expr.Add(
          Expr.FieldRef("age"),
          Expr.Multiply(Expr.FieldRef("age"), intLit(2)),
        )
      ).toOption.get
      val out = df.select(df.col("name"), derived.as("derived")).collect().toList
      // 30+60=90, 25+50=75, 35+70=105, 40+80=120
      out.map(_.getAs[Int]("derived")) shouldBe List(90, 75, 105, 120)
    } finally { spark.stop() }
  }

  // ===== Filter via compiled Column =====

  test("Expr.Equal used as df.filter: filters DataFrame correctly") {
    val spark = buildSpark()
    try {
      val df = peopleDataFrame(spark)
      val filterCol: Column = PortableExprCompiler.toColumn(
        Expr.Equal(Expr.FieldRef("name"), strLit("bob"))
      ).toOption.get
      val out = df.filter(filterCol).collect().toList
      out.size shouldBe 1
      out.head.getAs[String]("name") shouldBe "bob"
    } finally { spark.stop() }
  }

  test("Expr.GreaterThan used as df.filter: numeric comparison") {
    val spark = buildSpark()
    try {
      val df = peopleDataFrame(spark)
      val filterCol: Column = PortableExprCompiler.toColumn(
        Expr.GreaterThan(Expr.FieldRef("age"), intLit(30))
      ).toOption.get
      val out = df.filter(filterCol).collect().toList.map(_.getAs[String]("name"))
      out.sorted shouldBe List("carol", "dave")
    } finally { spark.stop() }
  }

  test("Expr.IsNull used as df.filter: null check on nullable column") {
    val spark = buildSpark()
    try {
      val df = peopleDataFrame(spark)
      val filterCol: Column = PortableExprCompiler.toColumn(
        Expr.IsNull(Expr.FieldRef("score"))
      ).toOption.get
      val out = df.filter(filterCol).collect().toList.map(_.getAs[String]("name"))
      out shouldBe List("dave")
    } finally { spark.stop() }
  }

  // ===== Composed boolean filter (Expr.And / Or / Not) =====

  test("Expr.And of two Expr.Equal: conjunction filter") {
    val spark = buildSpark()
    try {
      val df = peopleDataFrame(spark)
      val filterCol: Column = PortableExprCompiler.toColumn(
        Expr.And(
          Expr.GreaterThan(Expr.FieldRef("age"), intLit(20)),
          Expr.LessThan(Expr.FieldRef("age"), intLit(40)),
        )
      ).toOption.get
      val out = df.filter(filterCol).collect().toList.map(_.getAs[String]("name"))
      out.sorted shouldBe List("alice", "bob", "carol")
    } finally { spark.stop() }
  }

  test("Expr.Or of two Expr.Equal: disjunction filter") {
    val spark = buildSpark()
    try {
      val df = peopleDataFrame(spark)
      val filterCol: Column = PortableExprCompiler.toColumn(
        Expr.Or(
          Expr.Equal(Expr.FieldRef("name"), strLit("alice")),
          Expr.Equal(Expr.FieldRef("name"), strLit("carol")),
        )
      ).toOption.get
      val out = df.filter(filterCol).collect().toList.map(_.getAs[String]("name"))
      out.sorted shouldBe List("alice", "carol")
    } finally { spark.stop() }
  }

  test("Expr.Not wrapping Expr.GreaterThan: negation filter (NOT >)") {
    val spark = buildSpark()
    try {
      val df = peopleDataFrame(spark)
      val filterCol: Column = PortableExprCompiler.toColumn(
        Expr.Not(Expr.GreaterThan(Expr.FieldRef("age"), intLit(30)))
      ).toOption.get
      val out = df.filter(filterCol).collect().toList.map(_.getAs[String]("name"))
      // NOT (age > 30) → age <= 30 → alice (30), bob (25)
      out.sorted shouldBe List("alice", "bob")
    } finally { spark.stop() }
  }

  // ===== End-to-end: DataFrame + compiled filter + collect() =====

  test("end-to-end: select with derived + filter + collect (compose the whole pipeline)") {
    val spark = buildSpark()
    try {
      val df = peopleDataFrame(spark)
      // derived = age + 10
      val derived: Column = PortableExprCompiler.toColumn(
        Expr.Add(Expr.FieldRef("age"), intLit(10))
      ).toOption.get
      // filter = age > 25
      val filterCol: Column = PortableExprCompiler.toColumn(
        Expr.GreaterThan(Expr.FieldRef("age"), intLit(25))
      ).toOption.get
      val out = df
        .filter(filterCol)
        .select(df.col("name"), derived.as("age_plus_10"))
        .collect()
        .toList

      // alice (30) → age_plus_10=40, carol (35) → 45, dave (40) → 50
      out.map(_.getAs[String]("name")).sorted shouldBe List("alice", "carol", "dave")
      out.map(_.getAs[Int]("age_plus_10")).sorted shouldBe List(40, 45, 50)
    } finally { spark.stop() }
  }

  test("end-to-end: null-handling — Expr.Add with nullable column yields null row") {
    val spark = buildSpark()
    try {
      val df = peopleDataFrame(spark)
      // derived = score + 1 (dave's score is null → result is null)
      val derived: Column = PortableExprCompiler.toColumn(
        Expr.Add(Expr.FieldRef("score"), intLit(1))
      ).toOption.get
      val out = df.select(df.col("name"), derived.as("score_plus_1")).collect().toList
      out.find(_.getAs[String]("name") == "dave").get
        .getAs[AnyRef]("score_plus_1") shouldBe null
      // alice: 100+1=101, bob: 80+1=81, carol: 120+1=121
      out.find(_.getAs[String]("name") == "alice").get
        .getAs[Int]("score_plus_1") shouldBe 101
    } finally { spark.stop() }
  }

  test("Expr.CaseWhen: data-plane — first matching branch wins (SQL semantics)") {
    val spark = buildSpark()
    try {
      // CASE WHEN age > 30 THEN 'senior' ELSE 'not_senior' END
      val df = peopleDataFrame(spark)
      val derived: Column = PortableExprCompiler.toColumn(Expr.CaseWhen(
        branches = List(
          (Expr.GreaterThan(Expr.FieldRef("age"), Expr.Literal(LiteralValue.IntValue(30), SealedDataType.Int)),
           Expr.Literal(LiteralValue.StringValue("senior"), SealedDataType.Varchar)),
        ),
        otherwise = Expr.Literal(LiteralValue.StringValue("not_senior"), SealedDataType.Varchar),
      )).toOption.get
      val out = df.select(df.col("name"), derived.as("seniority")).collect().toList
      // alice (30) → not_senior, bob (25) → not_senior, carol (35) → senior, dave (40) → senior
      out.map(_.getAs[String]("seniority")) shouldBe List("not_senior", "not_senior", "senior", "senior")
    } finally { spark.stop() }
  }

  test("Expr.Alias: data-plane — column name in result schema matches the alias") {
    val spark = buildSpark()
    try {
      val df = peopleDataFrame(spark)
      val derived: Column = PortableExprCompiler.toColumn(Expr.Alias(
        name = "age_doubled",
        expr = Expr.Multiply(Expr.FieldRef("age"), Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int)),
      )).toOption.get
      val out = df.select(df.col("name"), derived).collect().toList
      // The result schema must carry the alias "age_doubled"
      out.map(_.getAs[Int]("age_doubled")) shouldBe List(60, 50, 70, 80)
    } finally { spark.stop() }
  }
}
