/*
 * SM8 Core — RollupSpec tests (Ticket #3 of
 * docs/wayfinder/2026-09-06-pre-aggregation.md; design in
 * docs/adr/0022-pre-aggregation-sm8-native.md).
 *
 * Acceptance criteria under test:
 * 1. Round-trip: YAML -> Model.rollups (typed RollupSpec) — and the
 *    programmatic path (Model.of / ModelBuilder) agree.
 * 2. Validation errors for unknown refs (same discipline as
 *    calculatedMeasures: all errors collected, never silent).
 * 3. Existing models unchanged: `rollups` defaults to Nil.
 *
 * The validator is exercised THROUGH the public boundary
 * (`Model.of`), so the tests also prove the wiring
 * (same pattern as ModelValidatorSpec).
 */
package io.sm8.core.model

import io.sm8.core.manifest.ModelLoader
import io.sm8.core.manifest.ManifestError

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RollupSpecSpec extends AnyFunSuite with Matchers {

  private val base =
    """name: flights
      |version: 1
      |source:
      |  byName:
      |    table: flights_raw
      |dimensions:
      |  - name: carrier
      |    expr: carrier
      |  - name: dest_region
      |    expr: dest_region
      |measures:
      |  - name: rows
      |    expr: count(*)
      |  - name: total_fare
      |    expr: sum(fare)
      |""".stripMargin

  // ===== round-trip: YAML -> Model.rollups =====

  test("YAML rollups block parses to typed RollupSpec list (round-trip)") {
    val yaml = base +
      """rollups:
        |  - name: by_carrier_day
        |    dimensions: [carrier]
        |    measures: [rows, total_fare]
        |    time_grain: day
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    val model = out.right.get
    model.rollups shouldBe List(
      RollupSpec(
        name = "by_carrier_day",
        dimensions = List("carrier"),
        measures = List("rows", "total_fare"),
        timeGrain = Some("day")))
  }

  test("rollup without time_grain parses with timeGrain=None; camelCase alias accepted") {
    val yaml = base +
      """rollups:
        |  - name: by_route
        |    dimensions: [carrier, dest_region]
        |    measures: [rows]
        |    timeGrain: hour
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    out.right.get.rollups shouldBe List(
      RollupSpec("by_route", List("carrier", "dest_region"), List("rows"), Some("hour")))
  }

  test("absent rollups block -> Model.rollups == Nil (existing models unchanged)") {
    val out = ModelLoader.fromString(base)
    out.isRight shouldBe true
    out.right.get.rollups shouldBe Nil
  }

  test("rollup entry missing 'name' fails loud as typed ManifestError") {
    val yaml = base +
      """rollups:
        |  - dimensions: [carrier]
        |    measures: [rows]
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a[ManifestError.ParseFailure]
    out.left.get.toString should include("missing 'name'")
  }

  test("rollup entry with missing name key on SECOND entry still fails loud (fold propagates)") {
    val yaml = base +
      """rollups:
        |  - name: good
        |    dimensions: [carrier]
        |    measures: [rows]
        |  - dimensions: [dest_region]
        |    measures: [total_fare]
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a[ManifestError.ParseFailure]
  }

  // ===== programmatic path: Model.of + ModelBuilder =====

  test("Model.of accepts valid rollup refs and carries them on the Model") {
    val model = Model.of(
      name = "flights",
      version = 1,
      dimensions = List(Dimension.field("carrier", "carrier")),
      measures = List(
        Measure.aggregate("rows", io.sm8.core.rel.AggregateFn.Count, io.sm8.core.expr.Expr.FieldRef("x"))),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = List(RollupSpec("by_carrier", List("carrier"), List("rows"), Some("day")))
    )
    model.isRight shouldBe true
    model.right.get.rollups.map(_.name) shouldBe List("by_carrier")
  }

  test("unknown dimension ref fails loud via Model.of (SchemaValidation, message names the ref)") {
    val model = Model.of(
      name = "flights",
      version = 1,
      dimensions = List(Dimension.field("carrier", "carrier")),
      measures = List(
        Measure.aggregate("rows", io.sm8.core.rel.AggregateFn.Count, io.sm8.core.expr.Expr.FieldRef("x"))),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = List(RollupSpec("bad_dim", List("nope"), List("rows"), None))
    )
    model.isLeft shouldBe true
    val err = model.left.get
    err shouldBe a[ModelValidationError.SchemaValidation]
    err.message should include("rollups[bad_dim] references unknown dimension 'nope'")
  }

  test("unknown measure ref fails loud via Model.of") {
    val model = Model.of(
      name = "flights",
      version = 1,
      dimensions = List(Dimension.field("carrier", "carrier")),
      measures = List(
        Measure.aggregate("rows", io.sm8.core.rel.AggregateFn.Count, io.sm8.core.expr.Expr.FieldRef("x"))),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = List(RollupSpec("bad_meas", List("carrier"), List("nope"), None))
    )
    model.isLeft shouldBe true
    model.left.get.message should include("rollups[bad_meas] references unknown measure 'nope'")
  }

  test("duplicate rollup names fail loud (same uniqueness discipline as other kinds)") {
    val model = Model.of(
      name = "flights",
      version = 1,
      dimensions = List(Dimension.field("carrier", "carrier")),
      measures = List(
        Measure.aggregate("rows", io.sm8.core.rel.AggregateFn.Count, io.sm8.core.expr.Expr.FieldRef("x"))),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = List(
        RollupSpec("dup", List("carrier"), List("rows"), None),
        RollupSpec("dup", List("carrier"), List("rows"), None))
    )
    model.isLeft shouldBe true
    model.left.get.message should include("duplicate rollup name 'dup'")
  }

  test("all validation errors accumulate in ONE SchemaValidation (never silent partial)") {
    val model = Model.of(
      name = "flights",
      version = 1,
      dimensions = List(Dimension.field("carrier", "carrier")),
      measures = List(
        Measure.aggregate("rows", io.sm8.core.rel.AggregateFn.Count, io.sm8.core.expr.Expr.FieldRef("x"))),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = List(
        RollupSpec("bad", List("nope-dim"), List("nope-meas"), None))
    )
    model.isLeft shouldBe true
    val msg = model.left.get.message
    msg should include("unknown dimension 'nope-dim'")
    msg should include("unknown measure 'nope-meas'")
  }

  // ===== review-round guards (dual-review findings, applied) =====

  test("rollup referencing a CALCULATED measure fails with the distinct base-measures message") {
    val model = Model.of(
      name = "flights",
      version = 1,
      dimensions = List(Dimension.field("carrier", "carrier")),
      measures = List(
        Measure.aggregate("rows", io.sm8.core.rel.AggregateFn.Count, io.sm8.core.expr.Expr.FieldRef("x"))),
      calculatedMeasures = List(CalculatedMeasure("fare_per_row", io.sm8.core.expr.Expr.FieldRef("total_fare"))),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = List(RollupSpec("uses_calc", List("carrier"), List("fare_per_row"), None))
    )
    model.isLeft shouldBe true
    val msg = model.left.get.message
    msg should include ("references calculated measure 'fare_per_row'")
    msg should include ("base measures")
    msg should not include "unknown measure"
  }

  test("YAML scalar ref value is accepted as a 1-list (author convenience)") {
    val yaml = base +
      "rollups:\n" +
      "  - name: scalar_ref\n" +
      "    dimensions: carrier\n" +
      "    measures: rows\n"
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    out.right.get.rollups shouldBe List(
      RollupSpec("scalar_ref", List("carrier"), List("rows"), None))
  }

  test("YAML non-list, non-string ref value fails loud (never coerced to Nil)") {
    val yaml = base +
      "rollups:\n" +
      "  - name: bad_ref\n" +
      "    dimensions: 42\n" +
      "    measures: [rows]\n"
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a[ManifestError.ParseFailure]
    out.left.get.toString should include ("dimensions must be a list")
  }

  test("rollup with BOTH dimensions and measures empty fails loud (degenerate no-op guard)") {
    val yaml = base +
      "rollups:\n" +
      "  - name: empty_rollup\n"
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a[ManifestError.ParseFailure]
    out.left.get.toString should include ("must not both be empty")
  }

  test("both time_grain and timeGrain specified fails loud (never silently pick one)") {
    val yaml = base +
      "rollups:\n" +
      "  - name: double_grain\n" +
      "    dimensions: [carrier]\n" +
      "    measures: [rows]\n" +
      "    time_grain: day\n" +
      "    timeGrain: hour\n"
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a[ManifestError.ParseFailure]
    out.left.get.toString should include ("at most one of time_grain / timeGrain")
  }

  // ===== review-driven hardening (dual review round 1) =====

  test("non-map entry in the rollups list fails loud instead of being silently dropped") {
    val yaml = base +
      """
        |rollups:
        |  - name: good
        |    dimensions: [carrier]
        |    measures: [rows]
        |  - "oops-a-scalar"
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.get shouldBe a[ManifestError.ParseFailure]
  }

  test("ModelBuilder.withRollup / withRollups round-trips to Model.rollups") {
    val built = ModelBuilder()
      .withName("flights").withVersion(1)
      .withSource(SourceRef.ByName(table = "flights_raw"))
      .withDimension("carrier", io.sm8.core.expr.Expr.FieldRef("carrier"))
      .withMeasureAgg("rows", io.sm8.core.rel.AggregateFn.Count, io.sm8.core.expr.Expr.FieldRef("x"))
      .withRollups(List(RollupSpec("by_carrier", List("carrier"), List("rows"), None)))
      .build
    built.isRight shouldBe true
    built.right.get.rollups.map(_.name) shouldBe List("by_carrier")
  }
}
