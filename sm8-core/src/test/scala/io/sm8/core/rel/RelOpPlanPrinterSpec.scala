/*
 * SM8 Core -- RelOpPlanPrinter spec (PR-N1).
 *
 * Per [[debug-mantra-mindset]] §1: assert substring content (per
 * node presence + per-node args). The output is a multi-line indented
 * string; tests assert on substrings + structural order.
 */
package io.sm8.core.rel

import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.{CalculatedMeasure, Dimension, FilterSpec, JoinSpec, Measure, Model, ModelPolicyDefaults, ModelStatus, SourceRef}
import io.sm8.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RelOpPlanPrinterSpec extends AnyFunSuite with Matchers {

  private def strLit(s: String) = Expr.Literal(LiteralValue.StringValue(s), SealedDataType.Varchar)
  private def intLit(n: Int)    = Expr.Literal(LiteralValue.IntValue(n), SealedDataType.Int)

  // ===== top-level dispatch =====

  test("print: returns empty for null input") {
    RelOpPlanPrinter.print(null) shouldBe ""
  }

  test("print: renders a single Scan with byName source") {
    val plan = RelOp.Scan(
      sourceRef = SourceRef.ByName(table = "people"),
      schema     = Nil,
      projection = Nil,
    )
    val s = RelOpPlanPrinter.print(plan)
    s should include ("Scan(")
    s should include ("table=people")
    s should not include "name="
  }

  test("print: renders a Scan with byPath source") {
    val plan = RelOp.Scan(
      sourceRef = SourceRef.ByPath(format = "parquet", path = "/data/x", options = Map.empty),
      schema     = Nil,
      projection = Nil,
    )
    RelOpPlanPrinter.print(plan) should include ("format=parquet")
    RelOpPlanPrinter.print(plan) should include ("path=/data/x")
  }

  test("print: renders a Filter wrapping a Scan") {
    val plan = RelOp.Filter(
      input     = RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil),
      predicate = Expr.GreaterThan(Expr.FieldRef("amount"), intLit(100)),
    )
    val s = RelOpPlanPrinter.print(plan)
    s should include ("Filter(")
    s should include ("amount")
    s should include ("Scan(")
    s should include ("table=t")
    // Filter comes before Scan in the printed plan
    s.indexOf("Filter") should be < s.indexOf("Scan")
  }

  test("print: renders a Project with aliases") {
    val plan = RelOp.Project(
      input = RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil),
      expressions = List(
        (Expr.FieldRef("a"), "alpha"),
        (Expr.FieldRef("b"), "beta"),
      ),
    )
    val s = RelOpPlanPrinter.print(plan)
    s should include ("Project(")
    s should include ("AS alpha")
    s should include ("AS beta")
    s should include ("FieldRef(a)")
  }

  test("print: renders an Aggregate with Sum + CountDistinct") {
    val plan = RelOp.Aggregate(
      input      = RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil),
      groupBy    = List(Expr.FieldRef("region")),
      aggregates = List(
        AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "total", false, Nil),
        AggregateCall(AggregateFn.CountDistinct, Some(Expr.FieldRef("user")), "users", false, Nil),
      ),
    )
    val s = RelOpPlanPrinter.print(plan)
    s should include ("Aggregate(")
    s should include ("Sum(")
    s should include ("CountDistinct(")
    s should include ("AS total")
    s should include ("AS users")
    s should include ("region")
  }

  test("print: renders all 5 JoinKind cases") {
    for (kind <- Seq(JoinKind.Inner, JoinKind.Left, JoinKind.Right, JoinKind.Full, JoinKind.Cross)) {
      val plan = RelOp.Join(
        left = RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil),
        right = RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil),
        kind = kind,
        condition = Expr.Equal(Expr.FieldRef("id"), Expr.FieldRef("id")),
      )
      val s = RelOpPlanPrinter.print(plan)
      s should include ("Join(")
      s should include (kind.toString)
    }
  }

  test("print: renders Sort with ASC + DESC + nulls FIRST + nulls LAST") {
    val plan = RelOp.Sort(
      input = RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil),
      keys = List(
        SortKey(Expr.FieldRef("a"), SortDirection.Ascending,  NullOrdering.First),
        SortKey(Expr.FieldRef("b"), SortDirection.Descending, NullOrdering.Last),
      ),
    )
    val s = RelOpPlanPrinter.print(plan)
    s should include ("Sort(")
    s should include ("ASC")
    s should include ("DESC")
    s should include ("NULLS FIRST")
    s should include ("NULLS LAST")
  }

  test("print: renders Limit with explicit count and offset") {
    val plan = RelOp.Limit(
      input = RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil),
      count = 10L,
      offset = 5L,
    )
    val s = RelOpPlanPrinter.print(plan)
    s should include ("Limit(")
    s should include ("count=10")
    s should include ("offset=5")
  }

  test("print: renders CaseWhen with branches + otherwise") {
    val plan = RelOp.Project(
      input = RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil),
      expressions = List(
        (Expr.Alias("band",
          Expr.CaseWhen(
            branches  = List((Expr.GreaterThan(Expr.FieldRef("amount"), intLit(100)), strLit("high"))),
            otherwise = strLit("low"),
          )), "band"),
      ),
    )
    val s = RelOpPlanPrinter.print(plan)
    s should include ("CASE")
    s should include ("WHEN")
    s should include ("THEN")
    s should include ("ELSE")
    s should include ("high")
    s should include ("low")
  }

  test("print: indentation reflects depth (Filter > Scan)") {
    val plan = RelOp.Filter(
      input = RelOp.Filter(
        input = RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil),
        predicate = Expr.Equal(Expr.FieldRef("x"), intLit(1)),
      ),
      predicate = Expr.Equal(Expr.FieldRef("y"), intLit(2)),
    )
    val s = RelOpPlanPrinter.print(plan)
    s should include ("Filter((FieldRef(y) = Literal(IntValue(2),Int)))")
    s should include ("  Filter((FieldRef(x) = Literal(IntValue(1),Int)))")
    s should include ("    Scan(table=t)")
    val outerFilterPos = s.indexOf("Filter(")
    val innerFilterPos = s.indexOf("  Filter(")
    val scanPos        = s.indexOf("    Scan(")
    (outerFilterPos < innerFilterPos) shouldBe true
    (innerFilterPos < scanPos) shouldBe true
  }

  test("print: end-to-end nested pipeline (Limit -> Sort -> Project -> Filter -> Scan)") {
    val plan = RelOp.Limit(
      input = RelOp.Sort(
        input = RelOp.Project(
          input = RelOp.Filter(
            input = RelOp.Scan(SourceRef.ByName(table = "people"), Nil, Nil),
            predicate = Expr.GreaterThan(Expr.FieldRef("age"), intLit(0)),
          ),
          expressions = List((Expr.FieldRef("name"), "n")),
        ),
        keys = List(SortKey(Expr.FieldRef("n"), SortDirection.Ascending, NullOrdering.First)),
      ),
      count = 10L,
      offset = 0L,
    )
    val s = RelOpPlanPrinter.print(plan)
    s should include ("Limit(")
    s should include ("Sort(")
    s should include ("Project(")
    s should include ("Filter(")
    s should include ("Scan(")
    // The order (outermost first) matches the tree structure
    (List("Limit", "Sort", "Project", "Filter", "Scan").map(s.indexOf).sorted == List("Limit", "Sort", "Project", "Filter", "Scan").map(s.indexOf)) shouldBe true
  }

  // ===== PR-O4d (ADR-008-O): RelOp.Scan.resolution rendered as a short tag =====

  test("print: Scan with resolution = Some(Scan) renders resolution=Scan") {
    val plan = RelOp.Scan(
      sourceRef  = SourceRef.ByName(table = "people"),
      schema     = Nil,
      projection = Nil,
      resolution = Some(io.sm8.core.engine.ResolvedSource.Scan(
        source = SourceRef.ByName(table = "people"),
        schema = Nil,
      )),
    )
    val s = RelOpPlanPrinter.print(plan)
    s should include ("Scan(")
    s should include ("table=people")
    s should include ("resolution=Scan")
  }

  test("print: Scan with resolution = None omits the resolution tag") {
    val plan = RelOp.Scan(
      sourceRef  = SourceRef.ByName(table = "events"),
      schema     = Nil,
      projection = Nil,
      // resolution = None (the new default)
    )
    val s = RelOpPlanPrinter.print(plan)
    s should include ("table=events")
    s should not include "resolution="
  }

  test("print: Scan with resolution = NotFound surfaces the failure tag") {
    val plan = RelOp.Scan(
      sourceRef  = SourceRef.ByName(table = "ghost"),
      schema     = Nil,
      projection = Nil,
      resolution = Some(io.sm8.core.engine.ResolvedSource.NotFound(
        source = SourceRef.ByName(table = "ghost"),
        reason = "table not in catalog",
      )),
    )
    val s = RelOpPlanPrinter.print(plan)
    s should include ("resolution=NotFound")
  }

}
