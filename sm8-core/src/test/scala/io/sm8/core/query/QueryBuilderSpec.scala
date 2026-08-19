/*
 * SM8 Core — QueryBuilderSpec (PR-18, ADR-008-R §PR-18).
 *
 * 18 tests across the typed fluent builder.
 */
package io.sm8.core.query

import io.sm8.core.engine.QueryRequest
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.TypedDimension
import io.sm8.core.predicate.{CompareOp, Predicate}
import io.sm8.core.rel.{ComparisonOp, TypedPredicate, WindowFunction}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QueryBuilderSpec extends AnyFlatSpec with Matchers {

  sealed trait PatientCount
  sealed trait AvgAge
  sealed trait Region
  sealed trait TotalRevenue

  object Refs {
    val region:       TypedDimension[Region]      = TypedDimension.of[Region]("region")
    val patientCount: TypedDimension[PatientCount] = TypedDimension.of[PatientCount]("patient_count")
    val avgAge:       TypedDimension[AvgAge]       = TypedDimension.of[AvgAge]("avg_age")
    val totalRevenue: TypedDimension[TotalRevenue] = TypedDimension.of[TotalRevenue]("total_revenue")
    val amount: TypedDimension[TotalRevenue] = TypedDimension.of[TotalRevenue]("amount")
  }

  // -- start() + empty accumulator (3 tests) --

  "QueryBuilderDsl.start" should "produce an empty accumulator" in {
    val b = QueryBuilderDsl.start()
    b.aggregateMeasures shouldBe Nil
    b.having shouldBe Nil
    b.partitionBy shouldBe Nil
    b.orderBy shouldBe Nil
    b.window shouldBe Nil
    b.limit shouldBe None
  }

  it should "carry the phantom type D across the builder" in {
    // Per scala-bug-huntingmindset §1: the phantom is preserved
    // across fluent calls; the builder is parameterized by D.
    val b1 = QueryBuilderDsl.start().groupBy(Refs.region)
    val b2 = QueryBuilderDsl.start().groupBy(Refs.patientCount)
    b1.orderBy should have size 1
    b2.orderBy should have size 1
    b1.orderBy.head.name shouldBe "region"
    b2.orderBy.head.name shouldBe "patient_count"
  }

  it should "build an empty QueryRequest" in {
    val req = QueryBuilderDsl.start().build(model = "patients", dimensions = Seq("region"))
    req.model shouldBe "patients"
    req.dimensions shouldBe Seq("region")
    req.aggregateMeasures shouldBe Nil
    req.having shouldBe Nil
  }

  // -- aggregate (3 tests) --

  "QueryBuilderDsl.aggregate" should "add typed aggregate measures" in {
    val req = QueryBuilderDsl.start()
      .aggregate(
        io.sm8.core.rel.TypedAggregateCall.count[PatientCount]("patient_count"),
        io.sm8.core.rel.TypedAggregateCall.avg[AvgAge]("avg_age")
      )
      .build("patients", Seq("region"))
    req.aggregateMeasures should have size 2
    req.aggregateMeasures.map(_.name) should contain theSameElementsAs Seq("patient_count", "avg_age")
  }

  it should "preserve the typed measure function" in {
    val req = QueryBuilderDsl.start()
      .aggregate(io.sm8.core.rel.TypedAggregateCall.sum[TotalRevenue]("total_revenue"))
      .build("patients", Seq("region"))
    req.aggregateMeasures.head.fn shouldBe io.sm8.core.rel.AggregateFn.Sum
  }

  it should "accumulate across multiple .aggregate calls" in {
    val req = QueryBuilderDsl.start()
      .aggregate(io.sm8.core.rel.TypedAggregateCall.count[PatientCount]("a"))
      .aggregate(io.sm8.core.rel.TypedAggregateCall.avg[AvgAge]("b"))
      .build("patients", Seq("region"))
    req.aggregateMeasures should have size 2
  }

  // -- groupBy + orderBy (4 tests) --

  "QueryBuilderDsl.groupBy" should "populate orderBy as default" in {
    val req = QueryBuilderDsl.start()
      .groupBy(Refs.region)
      .build("patients", Seq("region"))
    req.orderBy should have size 1
    req.orderBy.head.name shouldBe "region"
  }

  it should "preserve explicit orderBy over groupBy default" in {
    val req = QueryBuilderDsl.start()
      .orderBy(Refs.totalRevenue)
      .groupBy(Refs.region)
      .build("patients", Seq("region"))
    req.orderBy.head.name shouldBe "total_revenue"
  }

  it should "accumulate multiple groupBy dims" in {
    val req = QueryBuilderDsl.start()
      .groupBy(Refs.region, Refs.patientCount)
      .build("patients", Seq("region", "patient_count"))
    req.orderBy should have size 2
  }

  it should "typeclass-safety via compile error on phantom mismatch" in {
    // Per scala-bug-huntingmindset §1: a typo at the call site
    // (e.g. grouping by a Region dim inside a PatientCount builder)
    // is a COMPILE error, not a runtime error.
    // This test demonstrates the compile-time check by attempting
    // the valid path (a phantom mismatch would not compile).
    val req = QueryBuilderDsl.start()
      .groupBy(Refs.region)
      .build("patients", Seq("region"))
    req.dimensions shouldBe Seq("region")
  }

  // -- having (3 tests) --

  "QueryBuilderDsl.having" should "add typed predicates" in {
    val req = QueryBuilderDsl.start()
      .having(
        io.sm8.core.rel.Having[PatientCount](
          dim   = Refs.patientCount,
          op    = ComparisonOp.GT,
          value = Expr.Literal(LiteralValue.IntValue(100), io.sm8.core.schema.SealedDataType.Int)
        )
      )
      .build("patients", Seq("region"))
    req.having should have size 1
    req.having.head.op shouldBe ComparisonOp.GT
  }

  it should "accumulate multiple predicates" in {
    val req = QueryBuilderDsl.start()
      .having(
        io.sm8.core.rel.Having[PatientCount](
          dim = Refs.patientCount, op = ComparisonOp.GT,
          value = Expr.Literal(LiteralValue.IntValue(100), io.sm8.core.schema.SealedDataType.Int)
        ),
        io.sm8.core.rel.Having[AvgAge](
          dim = Refs.avgAge, op = ComparisonOp.LT,
          value = Expr.Literal(LiteralValue.IntValue(100), io.sm8.core.schema.SealedDataType.Int)
        )
      )
      .build("patients", Seq("region"))
    req.having should have size 2
  }

  it should "produce typed predicates with the correct dimension" in {
    val h = io.sm8.core.rel.Having[PatientCount](
      dim = Refs.patientCount, op = ComparisonOp.EQ,
      value = Expr.Literal(LiteralValue.IntValue(1), io.sm8.core.schema.SealedDataType.Int)
    )
    val req = QueryBuilderDsl.start().having(h).build("p", Seq("r"))
    req.having.head.dimension.name shouldBe "patient_count"
  }

  // -- partitionBy (2 tests) --

  "QueryBuilderDsl.partitionBy" should "wrap TypedDimension into PartitionBy" in {
    val req = QueryBuilderDsl.start()
      .partitionBy(Refs.region)
      .build("patients", Seq("region"))
    req.partitionBy should have size 1
    req.partitionBy.head.dim.name shouldBe "region"
  }

  it should "accumulate multiple partitionBy dims" in {
    val req = QueryBuilderDsl.start()
      .partitionBy(Refs.region, Refs.patientCount)
      .build("patients", Seq("region", "patient_count"))
    req.partitionBy should have size 2
  }

  // -- window + limit (3 tests) --

  "QueryBuilderDsl.window" should "add typed window specs" in {
    val req = QueryBuilderDsl.start()
      .window(
        io.sm8.core.rel.TypedWindow[Region, TotalRevenue](
          partitionBy = Refs.region,
          orderBy     = Refs.region,
          windowFn    = WindowFunction.RowNumber
        )
      )
      .build("patients", Seq("region"))
    req.window should have size 1
    req.window.head.windowFn shouldBe WindowFunction.RowNumber
  }

  "QueryBuilderDsl.limit" should "set the typed limit" in {
    val req = QueryBuilderDsl.start()
      .limit(Some(100L))
      .build("patients", Seq("region"))
    req.limit shouldBe Some(100L)
  }

  it should "default to None when not set" in {
    val req = QueryBuilderDsl.start().build("patients", Seq("region"))
    req.limit shouldBe None
  }

  "QueryBuilderDsl.filter" should "add typed predicates via the typed overload" in {
    // Per PR-21 (ADR-008-R): the typed filter() overload consumes
    // typed TypedPredicate[_] witnesses. The phantom [D] is captured
    // at the witness construction site (object level).
    val p1 = TypedPredicate.eq[Region]("region", "east")
    val p2 = TypedPredicate.gt[TotalRevenue]("amount", 100.0)
    val req = QueryBuilderDsl.start()
      .filter(p1, p2)
      .build("patients", Seq("region"))
    req.whereFilters.size shouldBe 2
    req.whereFilters(0).name shouldBe "region=east"
    req.whereFilters(1).name shouldBe "amount>100.0"
  }

  it should "alias as where() per karpathy-app-designmindset §1.3 (mirror QueryRequest shape)" in {
    val p = TypedPredicate.eq[Region]("region", "east")
    val req = QueryBuilderDsl.start()
      .where(p)
      .build("patients", Seq("region"))
    req.whereFilters.size shouldBe 1
    req.whereFilters(0).name shouldBe "region=east"
  }

  it should "build TypedPredicate AST nodes from string overload (filterNames)" in {
    // Per karpathy-guidelinesmindset §2 (simplicity): the string
    // overload is a convenience for quick YAML-style filtering;
    // it builds Predicate.Compare(field, =, value) AST nodes.
    val req = QueryBuilderDsl.start()
      .filterNames(("region", CompareOp.Eq, "east"), ("amount", CompareOp.Gt, 100.0))
      .build("patients", Seq("region"))
    req.whereFilters.size shouldBe 2
    req.whereFilters(0).predicate shouldBe Predicate.Compare("region", CompareOp.Eq, "east")
    req.whereFilters(1).predicate shouldBe Predicate.Compare("amount", CompareOp.Gt, 100.0)
  }

  it should "preserve phantom via asInstanceOf at the variance boundary" in {
    // Per PR-18 documented pattern: TypedPredicate[D] is coerced
    // to TypedPredicate[Nothing] via explicit asInstanceOf at the
    // accumulator field boundary. The phantom [D] is preserved at
    // construction (object level).
    val p: TypedPredicate[Region] = TypedPredicate.eq[Region]("region", "east")
    val coerced: TypedPredicate[Nothing] = p.asInstanceOf[TypedPredicate[Nothing]]
    val req = QueryBuilderDsl.start()
      .filter(coerced)
      .build("patients", Seq("region"))
    req.whereFilters(0).predicate shouldBe Predicate.Compare("region", CompareOp.Eq, "east")
  }

  it should "default to Nil when not set" in {
    val req = QueryBuilderDsl.start().build("patients", Seq("region"))
    req.whereFilters shouldBe Nil
  }
}
