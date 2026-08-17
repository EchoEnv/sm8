/*
 * SM8 Spark Connector -- MinimalRelOpLowerer spec (PR-N2: multi-key joins).
 *
 * The minimalRelOpLowerer is constructed here WITHOUT a SparkSession
 * (spark = null). Only the public + private static analysis
 * functions that don't need a real Spark are exercised; for any
 * test that needs SparkSession, defer to the existing
 * PortableQueryCompilerSpec / EngineSmokeSpec.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineContext, EngineIdentity, SourceResolver}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.SourceRef
import io.sm8.core.rel.{AggregateCall, AggregateFn, JoinKind, NullOrdering, RelOp, SortDirection, SortKey}
import io.sm8.core.schema.SealedDataType

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MinimalRelOpLowererSpec extends AnyFunSuite with Matchers {

  private def lowerer =
    new MinimalRelOpLowerer(
      spark    = null,
      pc       = null,
      identity = EngineIdentity("spark-3.5", "3.5", "0.1.0"),
    )

  // ===== PR-N2: multi-key join extraction =====

  test("extractJoinKeys: single Equal(FlatFieldRef, FieldRef) returns 1 pair") {
    val cond = Expr.Equal(Expr.FieldRef("a"), Expr.FieldRef("b"))
    val r = lowerer.extractJoinKeys(cond)
    r shouldBe List(("a", "b"))
  }

  test("extractJoinKeys: And(Equal, Equal) returns 2 pairs (commutative order)") {
    val cond = Expr.And(
      Expr.Equal(Expr.FieldRef("a"), Expr.FieldRef("b")),
      Expr.Equal(Expr.FieldRef("c"), Expr.FieldRef("d")),
    )
    val r = lowerer.extractJoinKeys(cond)
    r shouldBe List(("a", "b"), ("c", "d"))
  }

  test("extractJoinKeys: nested And(Equal, And(Equal, Equal)) returns 3 pairs (flattened)") {
    val cond = Expr.And(
      Expr.Equal(Expr.FieldRef("a"), Expr.FieldRef("b")),
      Expr.And(
        Expr.Equal(Expr.FieldRef("c"), Expr.FieldRef("d")),
        Expr.Equal(Expr.FieldRef("e"), Expr.FieldRef("f")),
      ),
    )
    val r = lowerer.extractJoinKeys(cond)
    r shouldBe List(("a", "b"), ("c", "d"), ("e", "f"))
  }

  test("extractJoinKeys: Equal with a literal on either side returns Nil") {
    val cond = Expr.Equal(Expr.FieldRef("a"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))
    lowerer.extractJoinKeys(cond) shouldBe Nil
  }

  test("extractJoinKeys: GreaterThan returns Nil (only Equal is recognised)") {
    val cond = Expr.GreaterThan(Expr.FieldRef("a"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))
    lowerer.extractJoinKeys(cond) shouldBe Nil
  }

  test("extractJoinKeys: mixed And(Equal, GreaterThan) returns 1 pair (only the Equal branch)") {
    val cond = Expr.And(
      Expr.Equal(Expr.FieldRef("a"), Expr.FieldRef("b")),
      Expr.GreaterThan(Expr.FieldRef("c"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
    )
    val r = lowerer.extractJoinKeys(cond)
    r shouldBe List(("a", "b"))
  }

  // ===== PR-N3: direct Aggregate lowering (typed-error contract) =====
  //
  // The happy-path (real SparkSession) lowering is covered by the
  // existing PortableQueryCompilerSpec's AggregateOnRealTableSpec.
  // Here we only assert the typed-error contract: non-FieldRef
  // groupBy keys return Left(UnsupportedCapability), NEVER a thrown
  // exception. We use a real SparkSession + createDataFrame so the
  // lower(Scan) call succeeds and the typed-error gate fires.

  test("lowerAggregate: groupBy containing a Literal returns Left(UnsupportedCapability)") {
    // PR-N3: only FieldRef / MeasureRef groupBy keys are accepted.
    // Anything else is a typed error -- never a Spark runtime crash.
    val spark = SparkSession.builder().master("local[1]").appName("tAgg").getOrCreate()
    try {
      val lowererWithSpark = new MinimalRelOpLowerer(spark, null, EngineIdentity("spark-3.5", "3.5", "0.1.0"))
      // Register a temporary table so lower(Scan) succeeds.
      val schema = org.apache.spark.sql.types.StructType(Array(
        org.apache.spark.sql.types.StructField("a", org.apache.spark.sql.types.IntegerType),
      ))
      val rows = Array(org.apache.spark.sql.RowFactory.create(1: java.lang.Integer))
      spark.createDataFrame(java.util.Arrays.asList(rows: _*), schema).createOrReplaceTempView("t")
      val agg = RelOp.Aggregate(
        input      = RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil),
        groupBy    = List(Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),  // not a FieldRef
        aggregates = List(AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("a")), "sum_a", false, Nil)),
      )
      val out = lowererWithSpark.lowerAggregate(agg, EngineContext.defaultContext)
      out.isLeft shouldBe true
      out.left.toOption.get shouldBe a [io.sm8.core.engine.EngineError.UnsupportedCapability]
      out.left.toOption.get.asInstanceOf[io.sm8.core.engine.EngineError.UnsupportedCapability].message should include ("only FieldRef/MeasureRef")
    } finally {
      spark.stop()
    }
  }

  // ===== PR-N4: direct Join lowering (typed-error contract) =====

  test("lowerJoin: Join.left is not a Scan returns Left(UnsupportedCapability)") {
    val join = RelOp.Join(
      left      = RelOp.Project(RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil), Nil),  // not a Scan
      right     = RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil),
      kind      = io.sm8.core.rel.JoinKind.Inner,
      condition = Expr.Equal(Expr.FieldRef("a"), Expr.FieldRef("a")),
    )
    val out = lowerer.lowerJoin(join, EngineContext.defaultContext)
    out.isLeft shouldBe true
    val err = out.left.toOption.get.asInstanceOf[io.sm8.core.engine.EngineError.UnsupportedCapability]
    err.message should include ("Join.left must be a Scan")
  }

  test("lowerJoin: Join.right is not a Scan returns Left(UnsupportedCapability)") {
    val join = RelOp.Join(
      left      = RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil),
      right     = RelOp.Project(RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil), Nil),  // not a Scan
      kind      = io.sm8.core.rel.JoinKind.Inner,
      condition = Expr.Equal(Expr.FieldRef("a"), Expr.FieldRef("a")),
    )
    val out = lowerer.lowerJoin(join, EngineContext.defaultContext)
    out.isLeft shouldBe true
    val err = out.left.toOption.get.asInstanceOf[io.sm8.core.engine.EngineError.UnsupportedCapability]
    err.message should include ("Join.right must be a Scan")
  }

  test("lowerJoin: condition with no extractable keys returns Left(UnsupportedCapability)") {
    val join = RelOp.Join(
      left      = RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil),
      right     = RelOp.Scan(SourceRef.ByName(table = "t"), Nil, Nil),
      kind      = io.sm8.core.rel.JoinKind.Inner,
      condition = Expr.GreaterThan(Expr.FieldRef("a"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),  // not Equal
    )
    val out = lowerer.lowerJoin(join, EngineContext.defaultContext)
    out.isLeft shouldBe true
    val err = out.left.toOption.get.asInstanceOf[io.sm8.core.engine.EngineError.UnsupportedCapability]
    err.message should include ("at least one Expr.Equal")
  }
}