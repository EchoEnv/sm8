/*
 * SM8 Core -- QueryBuilder conformance spec (PR-L per ADR-008-L).
 *
 * Per RFC SS12: assert the Model -> RelOp lowering is correct
 * (no IO beyond the SourceResolver call, every RelOp case is
 * covered, cycle detection raises typed errors, join fan-out
 * produces a Join tree).
 *
 * Per [[scala-data-driven-refactor-mindset]] SS1 (data-only tests):
 * the spec inspects the resulting RelOp tree's structure --
 * the cases, the field references, the join kind, etc.
 *
 * Per [[scala-error-handling-mindset]]: typed errors are tested
 * via Left(...) not exceptions.
 */
package io.sm8.core.query

import io.sm8.core.engine.{
  EngineError, EngineIdentity, ResolvedSource, SourceResolver,
}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.{
  CalculatedMeasure, Dimension, FilterSpec, JoinSpec, Measure,
  Model, ModelPolicyDefaults, ModelStatus, SourceRef,
}
import io.sm8.core.rel.{AggregateCall, AggregateFn, JoinKind, RelOp, SortKey}
import io.sm8.core.schema.{Field, SealedDataType}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class QueryBuilderSpec extends AnyFunSuite with Matchers {

  /** Test fixture: an in-memory SourceResolver that resolves every
    * SourceRef.ByName to a Scan with a fixed 2-field schema.
    * The `rightModels` map supports model-by-name resolution for
    * join tests.
    */
  private case class FakeResolver(
      rightModels: Map[String, List[Field]] = Map.empty,
      primaryFields: List[Field] = List(
        Field("id",   SealedDataType.Int,    nullable = false),
        Field("name", SealedDataType.Varchar, nullable = false),
      ),
      failAs: Option[ResolvedSource] = None,
  ) extends SourceResolver {
    override def resolve(
        source:   SourceRef,
        identity: EngineIdentity,
    ): Either[EngineError, ResolvedSource] = failAs match {
      case Some(f) => Right(f)
      case None    => Right(ResolvedSource.Scan(source, primaryFields))
    }
    override def resolveModel(
        name:     String,
        identity: EngineIdentity,
    ): Either[EngineError, SourceRef] =
      rightModels.get(name) match {
        case Some(_) => Right(SourceRef.ByName(table = name))
        case None    => Right(SourceRef.ByName(table = name))
      }
  }

  private val identity: EngineIdentity = EngineIdentity(
    name = "test", nativeVersion = "embedded", engineAdapterVersion = "0.1.0",
  )


  /** Helper: peel the Limit + Sort(empty keys) envelope so tests
    * can reach the Project / Aggregate / Filter / Scan / Join nodes
    * directly. Sort with empty keys is a no-op (PR-L decision:
    * portable sort defers to engine adapter), so it peels too. */
  private def unwrapSortLimit(plan: RelOp): RelOp = plan match {
    case _: RelOp.Limit            => unwrapSortLimit(plan.asInstanceOf[RelOp.Limit].input)
    case s: RelOp.Sort if s.keys.isEmpty => unwrapSortLimit(s.input)
    case _ => plan
  }

  private def model(
      table:   String,
      dimensions: List[Dimension]              = Nil,
      measures:   List[Measure]                = Nil,
      calcs:      List[CalculatedMeasure]      = Nil,
      joins:      List[JoinSpec]               = Nil,
      filters:    List[FilterSpec]             = Nil,
  ): Model = Model.of(
    name    = "qb-test",
    version = 1,
    source  = SourceRef.ByName(table = table),
    status  = ModelStatus.Draft,
    defaultPolicies = ModelPolicyDefaults(
      io.sm8.core.model.MaterializePolicy.None,
      io.sm8.core.model.CachePolicy.NoCache,
      io.sm8.core.model.AuditPolicy.NoAudit),
    dimensions = dimensions,
    measures   = measures,
    calculatedMeasures = calcs,
    joins      = joins,
    filters    = filters,
  ).toOption.get

  private def agg(name: String, fn: AggregateFn, field: String): Measure =
    Measure(name, AggregateCall(fn, Some(Expr.FieldRef(field)), name))

  // ===== single-source Model -> Scan =====

  test("Model with no dims/measures/filters/joins -> Limit -> Sort -> Project -> Scan") {
    val m = model("t")
    val out = QueryBuilder.build(m, FakeResolver(), identity).toOption.get
    val s = unwrapSortLimit(out).asInstanceOf[RelOp.Project].input.asInstanceOf[RelOp.Scan]
    s.sourceRef shouldBe SourceRef.ByName(table = "t")
    s.schema shouldBe List(
      Field("id",   SealedDataType.Int,    nullable = false),
      Field("name", SealedDataType.Varchar, nullable = false),
    )
  }

  // ===== dimensions + measures -> Aggregate + Project =====

  test("Model with dims+measures -> Scan -> Aggregate -> Project") {
    val m = model("t",
      dimensions = List(Dimension.field("region", "region")),
      measures   = List(agg("total", AggregateFn.Sum, "amount")),
    )
    val plan = QueryBuilder.build(m, FakeResolver(), identity).toOption.get
    // Scan -> Aggregate -> Project -> Sort -> Limit
    val project = unwrapSortLimit(plan).asInstanceOf[RelOp.Project]
    val aggNode = project.input.asInstanceOf[RelOp.Aggregate]
    val scan    = aggNode.input.asInstanceOf[RelOp.Scan]
    scan shouldBe a [RelOp.Scan]
    aggNode.groupBy shouldBe List(Expr.FieldRef("region"))
    aggNode.aggregates shouldBe List(AggregateCall(
      AggregateFn.Sum, Some(Expr.FieldRef("amount")), "total"))
    project.expressions shouldBe List(
      (Expr.FieldRef("region"), "region"),
      (Expr.FieldRef("total"),  "total"),
    )
  }

  // ===== calculated measures appear as Aliased columns =====

  test("CalculatedMeasure appears as Expr.Alias(name, expr) in the Project") {
    val m = model("t",
      dimensions = List(Dimension.field("region", "region")),
      measures   = List(agg("total", AggregateFn.Sum, "amount")),
      calcs      = List(CalculatedMeasure(
        name = "share",
        expr = Expr.Divide(Expr.FieldRef("amount"), Expr.All("total")),
      )),
    )
    val plan = QueryBuilder.build(m, FakeResolver(), identity).toOption.get
    val project = unwrapSortLimit(plan).asInstanceOf[RelOp.Project]
    project.expressions.collect {
      case (Expr.Alias(name, _), alias) => (name, alias)
    } shouldBe List(("share", "share"))
  }

  // ===== filters -> Filter chain =====

  test("Model filters -> RelOp.Filter chain (foldLeft order)") {
    val m = model("t",
      filters = List(
        FilterSpec("f1", Expr.GreaterThan(Expr.FieldRef("amount"), Expr.Literal(LiteralValue.IntValue(100), SealedDataType.Int))),
        FilterSpec("f2", Expr.LessThan(Expr.FieldRef("amount"),    Expr.Literal(LiteralValue.IntValue(500), SealedDataType.Int))),
      ),
    )
    val plan = QueryBuilder.build(m, FakeResolver(), identity).toOption.get
    val outer = unwrapSortLimit(plan).asInstanceOf[RelOp.Project]
    // Sort(empty keys) sits ABOVE Project in the tree; the filter
    // chain sits BELOW Project. So Project.input is the OUTER filter.
    val f2    = outer.input.asInstanceOf[RelOp.Filter]
    val f1    = f2.input.asInstanceOf[RelOp.Filter]
    f1.predicate shouldBe a [Expr.GreaterThan]
    f2.predicate shouldBe a [Expr.LessThan]
  }

  // ===== joins fold the Scan nodes =====

  test("Model with one Join -> Scan_1 -> Join -> Scan_2 -> ...") {
    val m = model("orders",
      dimensions = List(Dimension.field("region", "region")),
      joins      = List(JoinSpec("j", "customers", JoinKind.Inner, List(("region", "region")))),
    )
    val plan = QueryBuilder.build(m, FakeResolver(primaryFields = List(
      Field("region", SealedDataType.Varchar, nullable = false),
      Field("amount", SealedDataType.Int,     nullable = false),
    )), identity).toOption.get
    // Sort(empty keys) sits ABOVE Project in the tree; the Join
    // chain sits BELOW Project. So Project.input is the Join.
    val project = unwrapSortLimit(plan).asInstanceOf[RelOp.Project]
    val join    = project.input.asInstanceOf[RelOp.Join]
    join.kind shouldBe JoinKind.Inner
    join.condition shouldBe Expr.Equal(Expr.FieldRef("region"), Expr.FieldRef("region"))
    // Both sides are Scans
    join.left shouldBe a [RelOp.Scan]
    join.right shouldBe a [RelOp.Scan]
  }

  test("Multiple joins fold left-to-right") {
    val m = model("a",
      joins = List(
        JoinSpec("j1", "b", JoinKind.Inner, List(("x", "x"))),
        JoinSpec("j2", "c", JoinKind.Left,  List(("y", "y"))),
      ),
    )
    val plan = QueryBuilder.build(m, FakeResolver(), identity).toOption.get
    val project = unwrapSortLimit(plan).asInstanceOf[RelOp.Project]
    val outerJoin = project.input.asInstanceOf[RelOp.Join]
    outerJoin.kind shouldBe JoinKind.Left   // last-registered join is outermost
    val innerJoin = outerJoin.left.asInstanceOf[RelOp.Join]
    innerJoin.kind shouldBe JoinKind.Inner
  }

  // ===== typed error boundaries =====

  test("source NotFound -> typed FeatureDeferred") {
    val nf = ResolvedSource.NotFound(
      SourceRef.ByName(table = "missing"), reason = "table not in catalog")
    val out = QueryBuilder.build(model("missing"), FakeResolver(failAs = Some(nf)), identity)
    out.isLeft shouldBe true
    out.left.toOption.get shouldBe a [EngineError.FeatureDeferred]
  }

  test("calculated-measure cycle -> typed UnsupportedCapability") {
    // a -> b -> a (mutual recursion through measure names)
    val m = model("t",
      calcs = List(
        CalculatedMeasure(
          name = "a",
          expr = Expr.Divide(Expr.FieldRef("b"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
        ),
        CalculatedMeasure(
          name = "b",
          expr = Expr.Divide(Expr.FieldRef("a"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
        ),
      ),
    )
    val out = QueryBuilder.build(m, FakeResolver(), identity)
    out.isLeft shouldBe true
    val err = out.left.toOption.get
    err shouldBe a [EngineError.UnsupportedCapability]
    err match {
      case EngineError.UnsupportedCapability(_, capability, message) =>
        capability shouldBe "CalculatedMeasure.cycle"
        message should include ("a")
        message should include ("b")
      case other => fail(s"expected UnsupportedCapability, got $other")
    }
  }

  test("self-cycle (a -> a) -> typed UnsupportedCapability") {
    val m = model("t",
      calcs = List(
        CalculatedMeasure(
          name = "a",
          expr = Expr.Divide(Expr.FieldRef("a"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
        ),
      ),
    )
    val out = QueryBuilder.build(m, FakeResolver(), identity)
    out.isLeft shouldBe true
    out.left.toOption.get shouldBe a [EngineError.UnsupportedCapability]
  }

  // ===== Resolver failure tagged with the model name =====

  test("FeatureDeferred error is tagged with the model name (diagnostics)") {
    val nf = ResolvedSource.NotFound(
      SourceRef.ByName(table = "missing"), reason = "nope")
    val out = QueryBuilder.build(model("missing"), FakeResolver(failAs = Some(nf)), identity)
    val err = out.left.toOption.get.asInstanceOf[EngineError.FeatureDeferred]
    err.feature should include ("qb-test")
    err.feature should include ("query-builder")
  }
}
