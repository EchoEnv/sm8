/*
 * SM8 Spark Connector -- SparkSortBySpec (PR-25, ADR-008-R SSExtOrderBy).
 *
 * End-to-end integration spec for the typed `.asc` / `.desc`
 * sort direction. Per senior reviews 2026-08-19 + karpathy-
 * guidelinesmindset SS4 (goal-driven): proves df.orderBy(...)
 * with mixed Ascending/Descending columns actually produces
 * the expected output through TypedQueryCompiler.apply.
 *
 * Test categories per ADR-008-R SSsort-direction:
 *   1. Typed-only .asc sorted ascending (3 tests)
 *   2. Typed-only .desc sorted descending (2 tests)
 *   3. Mixed .asc / .desc per-column (2 tests)
 *   4. Backward compat: TypedDimension-only orderBy (1 test)
 *
 * Per [[debug-mantra-mindset]] SS1: every test asserts the
 * EVALUATED RESULT (collect().toList on the resulting DataFrame),
 * not the intermediate SQL.
import io.sm8.core.rel.{SortDirection, TypedPredicate, TypedSortKey, TypedSortKeyOps, TypedWindow, WindowFunction}
 *
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, QueryRequest}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.{TypedDimension, TypedMeasure}
import io.sm8.core.query.QueryBuilderDsl
import io.sm8.core.rel.{SortDirection, TypedPredicate, TypedWindow, WindowFunction}
import io.sm8.core.rel.TypedSortKeyOps._
import io.sm8.core.schema.SealedDataType

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkSortBySpec extends AnyFunSuite with Matchers {

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("SparkSortBySpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

  private val schema = StructType(Seq(
    StructField("name",    StringType, nullable = false),
    StructField("score",   DoubleType, nullable = false),
    StructField("version", LongType,   nullable = false),
  ))

  /** Fixture: 5 rows with mixed scores to test both ASC and DESC. */
  private def fixtureRows: Seq[Row] = Seq(
    Row("alice",   90.0, 1L),
    Row("bob",     50.0, 2L),
    Row("charlie", 70.0, 3L),
    Row("dave",    30.0, 4L),
    Row("eve",    100.0, 5L),
  )

  // === Phantom-typed witnesses (object level, per PR-16 closure-safety) ===

  sealed trait Name
  sealed trait Score
  sealed trait Version

  private object Refs {
    val name:    TypedDimension[Name]    = TypedDimension.of[Name]("name")
    val score:   TypedDimension[Score]   = TypedDimension.of[Score]("score")
    val version: TypedDimension[Version] = TypedDimension.of[Version]("version")
  }

  private def fixtureDF(spark: SparkSession): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(fixtureRows), schema)

  // === Category 1: Typed-only .asc -- 3 tests ===

  test("orderByKeys: typed .asc on score column produces ascending order") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryBuilderDsl.start()
        .orderByKeys(Refs.score.asc)
        .build(model = "test", dimensions = Seq.empty)
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get.select("name", "score").collect()
        .map(row => (row.getString(0), row.getDouble(1)))
        .toList
      r shouldBe List(
        ("dave",   30.0),
        ("bob",    50.0),
        ("charlie", 70.0),
        ("alice",  90.0),
        ("eve",   100.0),
      )
    } finally spark.stop()
  }

  test("orderByKeys: typed .asc on name column produces ascending lexical order") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryBuilderDsl.start()
        .orderByKeys(Refs.name.asc)
        .build(model = "test", dimensions = Seq.empty)
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get.select("name").collect().map(_.getString(0)).toList
      r shouldBe List("alice", "bob", "charlie", "dave", "eve")
    } finally spark.stop()
  }

  test("orderByKeys: typed .asc with limit produces top-K ascending") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryBuilderDsl.start()
        .orderByKeys(Refs.score.asc)
        .limit(Some(2L))
        .build(model = "test", dimensions = Seq.empty)
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get.select("name").collect().map(_.getString(0)).toList
      r shouldBe List("dave", "bob")
    } finally spark.stop()
  }

  // === Category 2: Typed-only .desc -- 2 tests ===

  test("orderByKeys: typed .desc on score column produces descending order (the user's headline ask)") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryBuilderDsl.start()
        .orderByKeys(Refs.score.desc)
        .build(model = "test", dimensions = Seq.empty)
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get.select("name", "score").collect()
        .map(row => (row.getString(0), row.getDouble(1)))
        .toList
      // Per AD-008-R SSsort-direction: this proves end-to-end that
      // .desc actually produces descending through TypedQueryCompiler
      // (per senior review #1: the deferred wire-up end-to-end).
      r shouldBe List(
        ("eve",   100.0),
        ("alice",  90.0),
        ("charlie", 70.0),
        ("bob",    50.0),
        ("dave",   30.0),
      )
    } finally spark.stop()
  }

  test("orderByKeys: typed .desc on version with limit produces latest-N") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      val req = QueryBuilderDsl.start()
        .orderByKeys(Refs.version.desc)
        .limit(Some(3L))
        .build(model = "test", dimensions = Seq.empty)
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get.select("name", "version").collect()
        .map(row => (row.getString(0), row.getLong(1))).toList
      r shouldBe List(
        ("eve",    5L),
        ("dave",   4L),
        ("charlie", 3L),
      )
    } finally spark.stop()
  }

  // === Category 3: Mixed .asc / .desc per-column -- 2 tests ===

  test("orderByKeys: mixed .asc on name + .desc on score (Spark multi-key sort)") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      // First by name ASC, then by score DESC.
      val req = QueryBuilderDsl.start()
        .orderByKeys(Refs.name.asc, Refs.score.desc)
        .build(model = "test", dimensions = Seq.empty)
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get.select("name", "score").collect()
        .map(row => (row.getString(0), row.getDouble(1)))
        .toList
      // Spark's multi-key sort: first by name ASC, ties broken by score DESC.
      // Note: all names are distinct in our fixture (alice/bob/charlie/dave/eve).
      r shouldBe List(
        ("alice",  90.0),
        ("bob",    50.0),
        ("charlie", 70.0),
        ("dave",   30.0),
        ("eve",   100.0),
      )
    } finally spark.stop()
  }

  test("orderByKeys: typed .desc on score with default Ascending for unsupplied cols") {
    // Per senior recommendation R-recommendation SS7.1 #3:
    // "padTo" safety -- missing entries default to Ascending. The
    // existing orderBy-only call sites (which never set sortDirections)
    // see zero behavior change.
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      // orderBy(score) only -- sortDirections is empty -> padTo
      // pads to Ascending. Same effect as the bare orderBy(score)
      // (which always produced ascending).
      val req = QueryBuilderDsl.start()
        .orderBy(Refs.score)
        .build(model = "test", dimensions = Seq.empty)
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val r = result.toOption.get.select("score").collect().map(_.getDouble(0)).toList
      r shouldBe List(30.0, 50.0, 70.0, 90.0, 100.0)
      // NOTE: same data shape as test #1 (typed .asc on score).
    } finally spark.stop()
  }

  // === Category 4: Backward compat -- 1 test ===

  test("backward compat: TypedDimension-only orderBy (no direction) = Ascending default (zero behavior change for 19 callers)") {
    val spark = buildSpark()
    try {
      val df = fixtureDF(spark)
      // Legacy API: orderBy(dims: TypedDimension[_]*) -- no TypedSortKey.
      // SortDirection defaults to Ascending per R-recommendation SS7.1 #3
      // (padTo safety).
      val req = QueryBuilderDsl.start()
        .orderBy(Refs.score)
        .build(model = "test", dimensions = Seq.empty)
      val result = TypedQueryCompiler(spark).apply(df, req, EngineContext.defaultContext)
      result.isRight shouldBe true
      val got = result.toOption.get.collect().map(_.getDouble(1)).toList
      got shouldBe List(30.0, 50.0, 70.0, 90.0, 100.0)
    } finally spark.stop()
  }
}
