/*
 * SM8 Core -- ModelValidator spec (PR-M2 per ADR-008-L Appendix GAP 2).
 *
 * Per the debug-mantra falsification: the public API for building a
 * Model is `Model.of` (and the `ModelBuilder` chain). We exercise the
 * validator THROUGH this public boundary -- which has two benefits:
 *   1. proves the validator works
 *   2. proves Model.of calls the validator (the PR-M2 wiring)
 *
 * Schema-level validation requires a ResolvedSource.Scan, which is
 * only available post-source-resolution. The `validateAgainstSchema`
 * surface is exercised directly (no public-API dependency there).
 *
 * Per [[debug-mantra-mindset]] SS1: errors accumulate -- never silent
 * partial-validation.
 */
package io.sm8.core.model

import io.sm8.core.engine.ResolvedSource
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.rel.{AggregateCall, AggregateFn}
import io.sm8.core.schema.{Field, SealedDataType}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ModelValidatorSpec extends AnyFunSuite with Matchers {

  // -- Valid fixtures via the public boundary (Model.of / ModelBuilder) --

  private val peopleScan: ResolvedSource.Scan = ResolvedSource.Scan(
    source = io.sm8.core.model.SourceRef.ByName("default", "people"),
    schema = List(
      Field("id",    SealedDataType.Int,    nullable = false),
      Field("name",  SealedDataType.Varchar, nullable = false),
      Field("region", SealedDataType.Varchar, nullable = false),
      Field("amount", SealedDataType.Int,    nullable = false),
    ),
  )

  private def validModel: Model =
    Model.of(
      name       = "ok",
      version    = 1,
      source     = io.sm8.core.model.SourceRef.ByName("default", "people"),
      dimensions = List(
        io.sm8.core.model.Dimension("id", "id"),
        io.sm8.core.model.Dimension("region", "region"),
      ),
      measures = List(
        io.sm8.core.model.Measure(
          "total",
          AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "total"))),
    ).toOption.get

  // ===== validate (pure model-level) -- exercised through Model.of =====

  test("validate: a valid Model.of passes") {
    ModelValidator.validate(validModel) shouldBe Right(())
  }

  test("validate: duplicate DIMENSION name -- Model.of returns Left(SchemaValidation)") {
    // Per ADR-008-L Appendix GAP 2: PR-M2 wired Model.of to call
    // ModelValidator.validate. The validator aggregates ALL errors.
    // Asserting the typed Left at the public boundary verifies BOTH
    // the validator AND its integration with Model.of.
    val out = Model.of(
      name       = "dups",
      version    = 1,
      source     = io.sm8.core.model.SourceRef.ByName("default", "people"),
      dimensions = List(
        io.sm8.core.model.Dimension("id", "id"),
        io.sm8.core.model.Dimension("id", "region"),  // dup name
      ),
    )
    out.isLeft shouldBe true
    val err = out.left.toOption.get
    err shouldBe a [ModelValidationError.SchemaValidation]
    err.asInstanceOf[ModelValidationError.SchemaValidation].messages shouldBe
      List("duplicate dimension name 'id'")
  }

  test("validate: duplicate MEASURE name fails loud") {
    val out = Model.of(
      name    = "dups",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName("default", "people"),
      measures = List(
        io.sm8.core.model.Measure("total", AggregateCall(AggregateFn.Sum,  Some(Expr.FieldRef("amount")), "total")),
        io.sm8.core.model.Measure("total", AggregateCall(AggregateFn.Avg,  Some(Expr.FieldRef("amount")), "total")),
      ),
    )
    out.left.toOption.get shouldBe a [ModelValidationError.SchemaValidation]
  }

  test("validate: duplicate CALCULATED_MEASURE name fails loud") {
    val out = Model.of(
      name    = "dups",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName("default", "people"),
      calculatedMeasures = List(
        io.sm8.core.model.CalculatedMeasure("x", Expr.FieldRef("amount")),
        io.sm8.core.model.CalculatedMeasure("x", Expr.FieldRef("amount")),
      ),
    )
    out.left.toOption.get shouldBe a [ModelValidationError.SchemaValidation]
  }

  test("validate: duplicate FILTER name fails loud") {
    val out = Model.of(
      name    = "dups",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName("default", "people"),
      filters = List(
        io.sm8.core.model.FilterSpec("f", Expr.Equal(
          Expr.FieldRef("id"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))),
        io.sm8.core.model.FilterSpec("f", Expr.Equal(
          Expr.FieldRef("region"), Expr.Literal(LiteralValue.StringValue("east"), SealedDataType.Varchar))),
      ),
    )
    out.left.toOption.get shouldBe a [ModelValidationError.SchemaValidation]
  }

  test("validate: duplicate JOIN name fails loud") {
    val out = Model.of(
      name    = "dups",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName("default", "people"),
      joins = List(
        io.sm8.core.model.JoinSpec("j", "customers",
          io.sm8.core.rel.JoinKind.Inner, List(("region", "region"))),
        io.sm8.core.model.JoinSpec("j", "products",
          io.sm8.core.rel.JoinKind.Inner, List(("id", "id"))),
      ),
    )
    out.left.toOption.get shouldBe a [ModelValidationError.SchemaValidation]
  }

  test("validate: name uniqueness is WITHIN each kind (dim 'age' + measure 'age' is fine)") {
    val out = Model.of(
      name    = "ok",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName("default", "people"),
      dimensions = List(io.sm8.core.model.Dimension("age", "amount")),
      measures   = List(io.sm8.core.model.Measure(
        "age", AggregateCall(AggregateFn.Count, Some(Expr.FieldRef("amount")), "age"))),
    )
    out.isRight shouldBe true
  }

  // ===== validateAgainstSchema (schema-level) -- exercised directly =====

  test("validateAgainstSchema: all fields present passes") {
    val m = Model.of(
      name    = "ok",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName("default", "people"),
      dimensions = List(
        io.sm8.core.model.Dimension("id", "id"),
        io.sm8.core.model.Dimension("region", "region"),
      ),
      measures = List(io.sm8.core.model.Measure(
        "total", AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "total"))),
      filters = List(io.sm8.core.model.FilterSpec(
        "f", Expr.Equal(Expr.FieldRef("id"),
          Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)))),
    ).toOption.get

    ModelValidator.validateAgainstSchema(m, peopleScan) shouldBe Right(())
  }

  test("validateAgainstSchema: unknown dimension expr field fails loud") {
    val m = Model.of(
      name    = "ok",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName("default", "people"),
      dimensions = List(io.sm8.core.model.Dimension("d", "ghost_field")),
    ).toOption.get
    val msgs = ModelValidator.validateAgainstSchema(m, peopleScan).left.toOption.get
      .asInstanceOf[ModelValidationError.SchemaValidation].messages
    msgs.head should include ("ghost_field")
  }

  test("validateAgainstSchema: unknown measure input field fails loud") {
    val m = Model.of(
      name    = "ok",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName("default", "people"),
      measures = List(io.sm8.core.model.Measure(
        "total", AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("ghost_field")), "total"))),
    ).toOption.get
    ModelValidator.validateAgainstSchema(m, peopleScan).isLeft shouldBe true
  }

  test("validateAgainstSchema: unknown calc field fails loud") {
    val m = Model.of(
      name    = "ok",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName("default", "people"),
      calculatedMeasures = List(io.sm8.core.model.CalculatedMeasure("c", Expr.FieldRef("ghost"))),
    ).toOption.get
    ModelValidator.validateAgainstSchema(m, peopleScan).isLeft shouldBe true
  }

  test("validateAgainstSchema: filter predicate referencing unknown field fails loud") {
    val m = Model.of(
      name    = "ok",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName("default", "people"),
      filters = List(io.sm8.core.model.FilterSpec("f", Expr.FieldRef("ghost"))),
    ).toOption.get
    ModelValidator.validateAgainstSchema(m, peopleScan).isLeft shouldBe true
  }

  test("validateAgainstSchema: join left-key referencing unknown field fails loud") {
    val m = Model.of(
      name    = "ok",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName("default", "people"),
      joins = List(io.sm8.core.model.JoinSpec("j", "x", io.sm8.core.rel.JoinKind.Inner, List(("ghost", "id")))),
    ).toOption.get
    val msgs = ModelValidator.validateAgainstSchema(m, peopleScan).left.toOption.get
      .asInstanceOf[ModelValidationError.SchemaValidation].messages
    msgs.head should include ("ghost")
  }

  test("validateAgainstSchema: errors aggregate (multiple unknown fields)") {
    val m = Model.of(
      name    = "ok",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName("default", "people"),
      dimensions = List(io.sm8.core.model.Dimension("a", "ghost1")),
      filters    = List(io.sm8.core.model.FilterSpec("f", Expr.FieldRef("ghost2"))),
    ).toOption.get
    val msgs = ModelValidator.validateAgainstSchema(m, peopleScan).left.toOption.get
      .asInstanceOf[ModelValidationError.SchemaValidation].messages
    msgs should have size 2
    msgs.exists(_.contains("ghost1")) shouldBe true
    msgs.exists(_.contains("ghost2")) shouldBe true
  }

  test("validateAgainstSchema: walker covers CASE WHEN + Alias (PR-I grammar)") {
    val m = Model.of(
      name    = "ok",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName("default", "people"),
      calculatedMeasures = List(io.sm8.core.model.CalculatedMeasure("band",
        Expr.Alias("band", Expr.CaseWhen(
          branches = List((Expr.GreaterThan(
            Expr.FieldRef("amount"),
            Expr.Literal(LiteralValue.IntValue(100), SealedDataType.Int)),
            Expr.Literal(LiteralValue.StringValue("high"), SealedDataType.Varchar))),
          otherwise = Expr.Literal(LiteralValue.StringValue("low"), SealedDataType.Varchar))))),
    ).toOption.get
    ModelValidator.validateAgainstSchema(m, peopleScan) shouldBe Right(())
  }

  test("validateAgainstSchema: walker ignores MeasureRef / All (engine-known references)") {
    val m = Model.of(
      name    = "ok",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName("default", "people"),
      calculatedMeasures = List(io.sm8.core.model.CalculatedMeasure("share",
        Expr.Divide(Expr.FieldRef("amount"), Expr.All("total")))),
    ).toOption.get
    // amount exists; total is engine-known -- no spurious errors.
    ModelValidator.validateAgainstSchema(m, peopleScan) shouldBe Right(())
  }
}
