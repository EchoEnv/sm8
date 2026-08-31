/*
 * SM8 Core — QueryPartsSpec (PR-17, ADR-008-R).
 *
 * Per ADR-008-R §"PR-17 Core types" scope: 15 shape + Serializable
 * tests across TypedAggregateCall + Having + PartitionBy +
 * ComparisonOp + WindowFunction + TypedWindow. The closure-safety
 * spec (3 tests) is in `TypedAggregateCallClosureSafetySpec.scala`.
 */
package io.sm8.core.rel

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.TypedDimension

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QueryPartsSpec extends AnyFlatSpec with Matchers {

  sealed trait PatientCount
  sealed trait AvgAge
  sealed trait Region
  sealed trait TotalRevenue

  object Refs {
    val region:       TypedDimension[Region]      = TypedDimension.of[Region]("region")
    val patientCount: TypedDimension[PatientCount] = TypedDimension.of[PatientCount]("patient_count")
    val totalRevenue: TypedDimension[TotalRevenue] = TypedDimension.of[TotalRevenue]("total_revenue")
  }

  // -- TypedAggregateCall (5 tests) --

  "TypedAggregateCall.count" should "carry AggregateFn.Count with empty input" in {
    val m = TypedAggregateCall.count[PatientCount]("patient_count")
    m.name shouldBe "patient_count"
    m.fn shouldBe AggregateFn.Count
    m.input shouldBe None
    m.distinct shouldBe false
    m.arguments shouldBe Nil
  }

  it should "project via toAggregateCall to the underlying AggregateCall" in {
    val m = TypedAggregateCall.sum[TotalRevenue]("total", "amount")
    val base = m.toAggregateCall
    base.fn shouldBe AggregateFn.Sum
    base.input shouldBe Some(Expr.FieldRef("amount"))
    base.alias shouldBe "total"  // TypedAggregateCall.name propagates as alias
  }

  it should "carry sum aggregate with field ref input" in {
    val m = TypedAggregateCall.sum[TotalRevenue]("total_revenue", "amount")
    m.fn shouldBe AggregateFn.Sum
    m.input shouldBe Some(Expr.FieldRef("amount"))
  }

  it should "carry countDistinct with distinct flag" in {
    val m = TypedAggregateCall.countDistinct[PatientCount]("unique_patients", "patient_id")
    m.fn shouldBe AggregateFn.CountDistinct
    m.distinct shouldBe true
    m.input shouldBe Some(Expr.FieldRef("patient_id"))
  }

  it should "carry distinct literal arguments" in {
    val m = TypedAggregateCall.of[AvgAge](
      name      = "p95_age",
      fn        = AggregateFn.Avg,
      input     = Some(Expr.FieldRef("age")),
      arguments = List(LiteralValue.IntValue(95))
    )
    m.arguments shouldBe List(LiteralValue.IntValue(95))
  }

  // -- Having (3 tests) --

  "Having" should "carry dimension + op + value typed phantom" in {
    val h = Having[PatientCount](
      dim   = Refs.patientCount,
      op    = ComparisonOp.GT,
      value = Expr.Literal(LiteralValue.IntValue(100), io.sm8.core.schema.SealedDataType.Int)
    )
    h.dimension shouldBe Refs.patientCount
    h.op shouldBe ComparisonOp.GT
    h.value shouldBe Expr.Literal(LiteralValue.IntValue(100), io.sm8.core.schema.SealedDataType.Int)
  }

  it should "be Serializable (ObjectOutputStream round-trip)" in {
    val h = Having[PatientCount](Refs.patientCount, ComparisonOp.GE,
      Expr.Literal(LiteralValue.IntValue(50), io.sm8.core.schema.SealedDataType.Int))
    val baos = new ByteArrayOutputStream(256)
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(h); oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val r = ois.readObject().asInstanceOf[Having[PatientCount]]
    r.dimension.name shouldBe "patient_count"
    r.op shouldBe ComparisonOp.GE
  }

  it should "preserve the phantom dimension type tag" in {
    val h = Having[PatientCount](Refs.patientCount, ComparisonOp.EQ,
      Expr.Literal(LiteralValue.IntValue(1), io.sm8.core.schema.SealedDataType.Int))
    // Per scala-bug-huntingmindset §1: a probe function `[T] => T`
    // proves the recovered type's phantom identity.
    val probe: Having[PatientCount] => Unit = { _ => () }
    probe(h)
  }

  // -- PartitionBy (2 tests) --

  "PartitionBy" should "carry the typed dimension" in {
    val p = PartitionBy[Region](Refs.region)
    p.dim shouldBe Refs.region
  }

  it should "be Serializable" in {
    val p = PartitionBy[Region](Refs.region)
    val baos = new ByteArrayOutputStream(256)
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(p); oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val r = ois.readObject().asInstanceOf[PartitionBy[Region]]
    r.dim.name shouldBe "region"
  }

  // -- ComparisonOp (2 tests) --

  "ComparisonOp" should "have 6 cases (EQ/NE/LT/LE/GT/GE)" in {
    ComparisonOp.EQ shouldBe a [ComparisonOp]
    ComparisonOp.NE shouldBe a [ComparisonOp]
    ComparisonOp.LT shouldBe a [ComparisonOp]
    ComparisonOp.LE shouldBe a [ComparisonOp]
    ComparisonOp.GT shouldBe a [ComparisonOp]
    ComparisonOp.GE shouldBe a [ComparisonOp]
  }

  it should "be Serializable (per [[scala-spark-batch-bugs-mindset]] §1 closure-safety)" in {
    val baos = new ByteArrayOutputStream(64)
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(ComparisonOp.GT); oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val r = ois.readObject().asInstanceOf[ComparisonOp]
    r shouldBe ComparisonOp.GT
  }

  // -- WindowFunction (2 tests) --

  "WindowFunction" should "have 3 cases (RowNumber, Rank, DenseRank)" in {
    WindowFunction.RowNumber shouldBe a [WindowFunction]
    WindowFunction.Rank shouldBe a [WindowFunction]
    WindowFunction.DenseRank shouldBe a [WindowFunction]
  }

  it should "produce the expected wire-stable name" in {
    // Per ADR-008-R: WindowFunction carries the wire-stable name;
    // the engine adapter pattern-matches on it.
    WindowFunction.RowNumber.toString shouldBe "RowNumber"
    WindowFunction.Rank.toString shouldBe "Rank"
    WindowFunction.DenseRank.toString shouldBe "DenseRank"
  }

  // -- TypedWindow (2 tests) --
  //
  // Per ADR-008-R §"TypedWindow" contract: partitionBy + orderBy share
  // the phantom type [D] (both reference the SAME partition column in
  // practice — e.g. rank within each region by total_revenue, where the
  // partition column is `region` for both).

  "TypedWindow" should "carry partition + order + window function (same phantom D)" in {
    val w = TypedWindow[Region, TotalRevenue](
      partitionBy = Refs.region,
      orderBy     = Refs.region,
      windowFn    = WindowFunction.RowNumber
    )
    w.partitionBy shouldBe Refs.region
    w.orderBy shouldBe Refs.region
    w.windowFn shouldBe WindowFunction.RowNumber
    w.name shouldBe "RowNumber_region"
  }

  it should "be Serializable (per the spark serialization concern)" in {
    val w = TypedWindow[Region, TotalRevenue](Refs.region, Refs.region, WindowFunction.Rank)
    val baos = new ByteArrayOutputStream(256)
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(w); oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val r = ois.readObject().asInstanceOf[TypedWindow[Region, TotalRevenue]]
    r.partitionBy.name shouldBe "region"
    r.orderBy.name shouldBe "region"
    r.windowFn shouldBe WindowFunction.Rank
  }
}
