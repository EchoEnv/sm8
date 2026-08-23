/*
 * SM8 Core -- ModelLoader PR-M1 extension spec (ADR-008-L Appendix
 * GAP 4: YAML parsing for joins + calculated_measures).
 *
 * Per [[debug-mantra-mindset]] SS1: each test asserts the loaded
 * Model shape (typed JoinSpec / CalculatedMeasure), or the typed
 * ManifestError on invalid input -- never silent.
 */
package io.sm8.core.manifest

import io.sm8.core.expr.Expr
import io.sm8.core.model.{JoinSpec, SourceRef}
import io.sm8.core.rel.JoinKind

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ModelLoaderM1Spec extends AnyFunSuite with Matchers {

  private val base =
    """name: m1
      |version: 1
      |source:
      |  byName:
      |    table: orders
      |""".stripMargin

  // ===== joins =====

  test("joins: single inner join with one key pair parses to JoinSpec") {
    val yaml = base +
      """joins:
        |  - name: j1
        |    rightModel: customers
        |    kind: inner
        |    keys:
        |      - [region, region]
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    out.toOption.get.joins shouldBe List(
      JoinSpec("j1", "customers", JoinKind.Inner, List(("region", "region"))))
  }

  test("joins: kind is case-insensitive and 'outer' aliases 'full'") {
    val yaml = base +
      """joins:
        |  - name: j1
        |    rightModel: c
        |    kind: OUTER
        |    keys:
        |      - [a, b]
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.toOption.get.joins.head.kind shouldBe JoinKind.Full
  }

  test("joins: multi-key pairs are preserved (PR-K rejects at compile; loader does not)"){
    val yaml = base +
      """joins:
        |  - name: j1
        |    rightModel: c
        |    kind: inner
        |    keys:
        |      - [a, b]
        |      - [x, y]
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.toOption.get.joins.head.keys shouldBe List(("a", "b"), ("x", "y"))
  }

  test("joins: unknown kind is a typed ParseFailure (never silent)") {
    val yaml = base +
      """joins:
        |  - name: j1
        |    rightModel: c
        |    kind: diagonal
        |    keys:
        |      - [a, b]
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.toOption.get shouldBe a [ManifestError.ParseFailure]
    out.left.toOption.get.toString should include ("diagonal")
  }

  test("joins: estimated_rows parses to JoinSpec.estimatedRows") {
    val yaml = base +
      """joins:
        |  - name: j1
        |    rightModel: c
        |    kind: inner
        |    estimated_rows: 5000
        |    keys:
        |      - [a, b]
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isRight shouldBe true
    out.toOption.get.joins.head.estimatedRows shouldBe Some(5000L)
  }

  test("joins: absent estimated_rows yields None (backward compat)") {
    val yaml = base +
      """joins:
        |  - name: j1
        |    rightModel: c
        |    kind: inner
        |    keys:
        |      - [a, b]
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.toOption.get.joins.head.estimatedRows shouldBe None
  }

  test("joins: negative estimated_rows is a typed ParseFailure (never silent)") {
    val yaml = base +
      """joins:
        |  - name: j1
        |    rightModel: c
        |    kind: inner
        |    estimated_rows: -5
        |    keys:
        |      - [a, b]
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.toOption.get.toString should include ("estimated_rows")
  }

  test("joins: non-numeric estimated_rows is a typed ParseFailure (never silent)") {
    val yaml = base +
      """joins:
        |  - name: j1
        |    rightModel: c
        |    kind: inner
        |    estimated_rows: "many"
        |    keys:
        |      - [a, b]
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.toOption.get.toString should include ("estimated_rows")
  }

  test("joins: absent block yields Nil (backward compat)") {
    val out = ModelLoader.fromString(base)
    out.toOption.get.joins shouldBe Nil
  }

  // ===== calculated_measures =====

  test("calculated_measures: arithmetic expr parses to CalculatedMeasure") {
    val yaml = base +
      """calculated_measures:
        |  - name: double_total
        |    expr: total * 2
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    val calcs = out.toOption.get.calculatedMeasures
    calcs shouldBe List(io.sm8.core.model.CalculatedMeasure(
      "double_total",
      Expr.Multiply(
        Expr.FieldRef("total"),
        Expr.Literal(io.sm8.core.expr.LiteralValue.IntValue(2),
                     io.sm8.core.schema.SealedDataType.Int))))
  }

  test("calculated_measures: pct-of-total via all() (GAP 1 grammar + GAP 4 loader)") {
    val yaml = base +
      """calculated_measures:
        |  - name: share
        |    expr: amount / all(total)
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.toOption.get.calculatedMeasures.head.expr shouldBe
      Expr.Divide(Expr.FieldRef("amount"), Expr.All("total"))
  }

  test("calculated_measures: CASE WHEN + AS alias end-to-end through the loader") {
    val yaml = base +
      """calculated_measures:
        |  - name: band
        |    expr: CASE WHEN amount > 100 THEN 'high' ELSE 'low' END AS band
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.toOption.get.calculatedMeasures.head.expr shouldBe a [Expr.Alias]
  }

  test("calculated_measures: unparsable expr is a typed ParseFailure (never silent)") {
    val yaml = base +
      """calculated_measures:
        |  - name: bad
        |    expr: CASE WHEN
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    val err = out.left.toOption.get
    err shouldBe a [ManifestError.ParseFailure]
    err.toString should include ("bad")
  }

  test("calculated_measures: missing expr field is a typed ParseFailure") {
    val yaml = base +
      """calculated_measures:
        |  - name: noexpr
        |""".stripMargin
    val out = ModelLoader.fromString(yaml)
    out.isLeft shouldBe true
    out.left.toOption.get shouldBe a [ManifestError.ParseFailure]
  }

  test("calculated_measures: absent block yields Nil (backward compat)") {
    val out = ModelLoader.fromString(base)
    out.toOption.get.calculatedMeasures shouldBe Nil
  }
}
