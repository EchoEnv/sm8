/*
 * SM8 Spark Connector -- CompileRelOpSpec (PR-32, ADR-008-R SSR3
 * broader fix to `compileRelOp`).
 *
 * Per the user's 2026-08-20 directive ("go start PR-32, ensure
 * follow ALL skills we have in memory, especially spark serialization
 * concern and executor performance and RFC for categories code
 * structure"): the PR-32 work lifts the `ModelValidator` work-around
 * from `SparkEngineProvider.compileModelToDataFrame` into the
 * canonical `compileRelOp(model, relOp, ctx, scan, preFilteredDf)`
 * entry point. Every direct caller of `compileRelOp` now benefits
 * from schema validation -- not just callers that go through the
 * PR-27 helper.
 *
 * Per [[scala-app-design-mindset]] SS3.1 (Protocols before
 * Implementations) + RFC SS3 (layer ownership): the validator is
 * at sm8-core (the protocol); the spark-connector consumes it
 * inside the canonical entry point. The wire-DTO `Model` flows
 * through unchanged.
 *
 * Per [[scala-impact-analysis-mindset]] SS2 (binary
 * compatibility): the new overload is ADDITIVE. The existing
 * `compileRelOp(relOp, ctx)` (2-arg) and `compileRelOp(relOp, ctx,
 * preFilteredDf)` (3-arg) signatures are preserved. This spec
 * exercises ONLY the new 5-arg overload; the existing overloads
 * are covered by MinimalRelOpLowererSpec + FilterPushdownSpec.
 *
 * Per [[scala-bug-hunting-mindset]] SS1 (trust compiler): the
 * validator's `Either[ModelValidationError, Unit]` return type
 * forces the caller to handle the validation result at compile
 * time. No silent null. No runtime `UNRESOLVED_COLUMN`.
 *
 * Per [[scala-spark-batch-bugs-mindset]] SS1 (closure-safety
 * -- the user's explicit priority): the validation runs in the
 * driver (no Spark closure capture). The pre-filtered DF is built
 * driver-side by `resolveWithPushdown`.
 *
 * Per [[debug-mantra-mindset]] SS1: every test asserts on the
 * EVALUATED RESULT (Left/Right + DataFrame count), not the
 * intermediate SQL.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, EngineError, EngineIdentity, ResolvedSource}
import io.sm8.core.expr.Expr
import io.sm8.core.model._
import io.sm8.core.query.QueryBuilder
import io.sm8.core.rel.AggregateCall
import io.sm8.core.rel.AggregateFn
import io.sm8.core.schema.{Field, SealedDataType}

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CompileRelOpSpec extends AnyFunSuite with Matchers {

  // Per the existing pattern in SparkFilterSpec / FilterPushdownSpec.
  private val identity: EngineIdentity = EngineIdentity(
    name = "sm8-compileRelOp-test", nativeVersion = "3.5.8", engineAdapterVersion = "0.1.0",
  )

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("CompileRelOpSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

  // === Fixtures =========================================================

  // A 4-field ResolvedSource.Scan: id, name, region, amount.
  private val peopleScan: ResolvedSource.Scan = ResolvedSource.Scan(
    source = SourceRef.ByName(table = "people"),
    schema = List(
      Field("id",     SealedDataType.Int,     nullable = false),
      Field("name",   SealedDataType.Varchar, nullable = false),
      Field("region", SealedDataType.Varchar, nullable = false),
      Field("amount", SealedDataType.Int,     nullable = false),
    ),
  )

  // A valid Model referencing real fields. Built via Model.of (the
  // public boundary) so the pure-model validation has already passed.
  private def validModel: Model =
    Model.of(
      name       = "ok",
      version    = 1,
      source     = SourceRef.ByName(table = "people"),
      dimensions = List(
        Dimension.field("id", "id"),
        Dimension.field("region", "region"),
      ),
      measures = List(
        Measure(
          "total",
          AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "total"),
        ),
      ),
    ).toOption.get

  // A Model that references a NON-EXISTENT field ("bogus_column").
  // The pure-model validation (`validate`) passes (the field name is
  // a valid String) but the schema validation (`validateAgainstSchema`)
  // fails because "bogus_column" is not in `peopleScan.schema`.
  //
  // Per [[karpathy-bug-hunting-mindset]] SS1 (trust compiler): we
  // construct this Model via Model.of to get past the pure-model
  // gate, then expect the schema validator to catch it.
  private def modelWithBadField: Model = {
    val out = Model.of(
      name       = "bogus",
      version    = 1,
      source     = SourceRef.ByName(table = "people"),
      dimensions = List(
        Dimension.field("id", "id"),
        // Bogus dimension: references a field NOT in the schema.
        Dimension.field("region", "bogus_column"),
      ),
    )
    out.toOption.get
  }

  // === Test 1: canonical overload validates -- bad Model -> typed Left ==

  test("compileRelOp: canonical 5-arg overload validates against schema -- bad Model returns Left(UnsupportedCapability)") {
    // Per PR-32's headline ask: the canonical `compileRelOp(model,
    // relOp, ctx, scan, preFilteredDf)` overload runs the
    // `ModelValidator.validateAgainstSchema` check BEFORE the
    // lowerer fires. A Model referencing a field not in the
    // resolved scan schema must surface as a typed `Left` at
    // compile time (not a runtime `UNRESOLVED_COLUMN` after the
    // df is materialized).
    val spark = buildSpark()
    try {
      // Register a temp view so `QueryBuilder.build` -> `resolver.resolve`
      // can succeed (the resolver needs a spark table to point at).
      spark.sql(
        "SELECT * FROM VALUES " +
        "(1, 'alice', 'east', 100), " +
        "(2, 'bob',   'east', 200), " +
        "(3, 'carol', 'west', 50) " +
        "AS t(id, name, region, amount)"
      ).createTempView("people")

      val resolver = new SparkSourceResolver(spark)
      val model = modelWithBadField  // references "bogus_column"
      // Build a relOp from the (bad) model. The QueryBuilder.build
      // itself doesn't validate against the schema -- it lowers
      // the model to a portable RelOp tree. The validation is the
      // responsibility of the canonical compileRelOp overload.
      val relOpE = QueryBuilder.build(model, resolver, identity)
      relOpE.isRight shouldBe true  // build itself succeeds
      val relOp = relOpE.toOption.get

      // The canonical compileRelOp overload must surface the
      // schema mismatch as a typed Left.
      val pc = new PortableQueryCompiler(spark)
      val result = pc.compileRelOp(model, relOp, EngineContext.defaultContext,
        peopleScan, Some(spark.emptyDataFrame))
      result.isLeft shouldBe true
      val err = result.left.toOption.get
      // Per PR-32 architect + data-eng review (NIT): the
      // capability string is implementation-leakage (a future
      // rename would break the test). The structural check below
      // is what matters: the typed error shape + the bogus
      // field name (the behaviour-revealing detail).
      err shouldBe a [EngineError.UnsupportedCapability]
      err.toString should include ("bogus_column")
    } finally spark.stop()
  }

  // === Test 2: canonical overload succeeds -- valid Model -> Right(df) ==

  test("compileRelOp: canonical 5-arg overload succeeds for a valid Model -- returns Right(DataFrame)") {
    // Per [[karpathy-bug-hunting-mindset]] SS1 (trust compiler): the
    // valid-Model case must NOT surface a typed Left. The validator
    // passes; the lowerer succeeds; the canonical entry point
    // returns Right(df).
    val spark = buildSpark()
    try {
      // Register a temp view with the schema matching the fixture.
      spark.sql(
        "SELECT * FROM VALUES " +
        "(1, 'alice', 'east', 100), " +
        "(2, 'bob',   'east', 200), " +
        "(3, 'carol', 'west', 50) " +
        "AS t(id, name, region, amount)"
      ).createTempView("people")

      val resolver = new SparkSourceResolver(spark)
      val model = validModel
      val relOpE = QueryBuilder.build(model, resolver, identity)
      relOpE.isRight shouldBe true
      val relOp = relOpE.toOption.get

      val pc = new PortableQueryCompiler(spark)
      val result = pc.compileRelOp(model, relOp, EngineContext.defaultContext,
        peopleScan, None)
      result.isRight shouldBe true
      // Per PR-32 data-engineer review (NIT): pin the row count
      // to the 3-row temp view fixture. The Left/Right split proves
      // the validation surface; the row-count check proves the
      // canonical overload actually compiled the relOp against
      // the resolved source's schema.
      result.toOption.get.count() shouldBe 3L
    } finally spark.stop()
  }
}
