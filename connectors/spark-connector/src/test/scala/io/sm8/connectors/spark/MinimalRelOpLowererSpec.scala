/*
 * SM8 Spark Connector -- MinimalRelOpLowerer spec (PR-N2: multi-key joins).
 *
 * The minimalRelOpLowerer is constructed here WITHOUT a SparkSession
 * (spark = null). Only the public + private static analysis
 * functions that don't need a real Spark are exercised; for any
 * test that needs SparkSession, defer to the in-tree
 * SparkSession-bearing specs (Spark*, PortableQueryCompiler*,
 * PortableExprCompiler*, CompileRelOpSpec, FilterPushdownSpec,
 * TypedQueryCompilerPushdownSpec).
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
  // ===== PR-O1e (ADR-008-O, P0-3): column pruning via scan.projection =====

  test("lowerScan: applies projection when scan.projection is non-empty") {
    // Per [[scala-spark-batch-bugs-mindset]] mantra #6
    // (partition-pruning + projection-pushdown): a non-empty
    // scan.projection MUST select only those columns. Without
    // this, every query reads all columns of every partition --
    // fatal at scale for wide tables.
    val spark = SparkSession.builder().master("local[1]").appName("tPrune").getOrCreate()
    try {
      val schema = new org.apache.spark.sql.types.StructType(Array(
        org.apache.spark.sql.types.StructField("a", org.apache.spark.sql.types.IntegerType),
        org.apache.spark.sql.types.StructField("b", org.apache.spark.sql.types.IntegerType),
        org.apache.spark.sql.types.StructField("c", org.apache.spark.sql.types.IntegerType),
      ))
      val rows = Array(
        org.apache.spark.sql.RowFactory.create(1: java.lang.Integer, 2: java.lang.Integer, 3: java.lang.Integer),
      )
      spark.createDataFrame(java.util.Arrays.asList(rows: _*), schema).createOrReplaceTempView("tPrune")

      val scan = RelOp.Scan(
        sourceRef  = SourceRef.ByName(table = "tPrune"),
        schema     = Nil,
        projection = List(Expr.FieldRef("a"), Expr.FieldRef("c")),  // skip b
      )
      val lowererWithSpark = new MinimalRelOpLowerer(spark, null, EngineIdentity("spark-3.5", "3.5", "0.1.0"))
      val out = lowererWithSpark.lowerScan(scan)
      out.isRight shouldBe true
      val pruned = out.toOption.get
      pruned.columns.toSet shouldBe Set("a", "c")
    } finally {
      spark.stop()
    }
  }

  test("lowerScan: empty projection returns the full DataFrame (no select)") {
    val spark = SparkSession.builder().master("local[1]").appName("tNoPrune").getOrCreate()
    try {
      val schema = new org.apache.spark.sql.types.StructType(Array(
        org.apache.spark.sql.types.StructField("a", org.apache.spark.sql.types.IntegerType),
        org.apache.spark.sql.types.StructField("b", org.apache.spark.sql.types.IntegerType),
      ))
      val rows = Array(
        org.apache.spark.sql.RowFactory.create(1: java.lang.Integer, 2: java.lang.Integer),
      )
      spark.createDataFrame(java.util.Arrays.asList(rows: _*), schema).createOrReplaceTempView("tNoPrune")

      val scan = RelOp.Scan(
        sourceRef  = SourceRef.ByName(table = "tNoPrune"),
        schema     = Nil,
        projection = Nil,
      )
      val lowererWithSpark = new MinimalRelOpLowerer(spark, null, EngineIdentity("spark-3.5", "3.5", "0.1.0"))
      val out = lowererWithSpark.lowerScan(scan)
      out.isRight shouldBe true
      val df = out.toOption.get
      df.columns.toSet shouldBe Set("a", "b")
    } finally {
      spark.stop()
    }
  }

  test("lowerJoin: broadcastRightBelowBytes set joins small right side cleanly (IR path)") {
    // PR-O2 (ADR-008-O, P0-4): the IR path (compileRelOp -> lowerJoin)
    // must honor `ctx.joinHints.broadcastRightBelowBytes` the same way
    // the legacy applyJoins path does. Covers the broadcast decision
    // (size probe -> broadcast(rightDf) wrap -> join) end to end: a
    // 1-row right table under a 1MB threshold joins without error and
    // produces the correct rows. Plan-shape assertions are deliberately
    // avoided -- AQE re-planning differs between Spark 3.5.x and 4.1.x.
    val spark = SparkSession.builder().master("local[1]").appName("tJoinBcast").getOrCreate()
    try {
      val leftSchema = new org.apache.spark.sql.types.StructType(Array(
        org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType),
        org.apache.spark.sql.types.StructField("name", org.apache.spark.sql.types.StringType),
      ))
      val leftRow = Array(
        org.apache.spark.sql.RowFactory.create(1: java.lang.Integer, "alpha": String),
      )
      spark.createDataFrame(java.util.Arrays.asList(leftRow: _*), leftSchema).createOrReplaceTempView("tJoinBcastL")

      val rightSchema = new org.apache.spark.sql.types.StructType(Array(
        org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType),
        org.apache.spark.sql.types.StructField("label", org.apache.spark.sql.types.StringType),
      ))
      val rightRow = Array(
        org.apache.spark.sql.RowFactory.create(1: java.lang.Integer, "one": String),
      )
      spark.createDataFrame(java.util.Arrays.asList(rightRow: _*), rightSchema).createOrReplaceTempView("tJoinBcastR")

      val join = RelOp.Join(
        left      = RelOp.Scan(SourceRef.ByName(table = "tJoinBcastL"), Nil, Nil),
        right     = RelOp.Scan(SourceRef.ByName(table = "tJoinBcastR"), Nil, Nil),
        kind      = io.sm8.core.rel.JoinKind.Inner,
        condition = Expr.Equal(Expr.FieldRef("id"), Expr.FieldRef("id")),
      )
      val ctx = EngineContext.defaultContext.copy(
        joinHints = io.sm8.core.engine.JoinHints(broadcastRightBelowBytes = Some(1048576L)),
      )
      val lowererWithSpark = new MinimalRelOpLowerer(spark, null, EngineIdentity("spark-3.5", "3.5", "0.1.0"))
      val out = lowererWithSpark.lowerJoin(join, ctx)
      out.isRight shouldBe true
      val rows = out.toOption.get.collect()
      rows should have size 1
      rows(0).getAs[String]("name") shouldBe "alpha"
      rows(0).getAs[String]("label") shouldBe "one"
    } finally {
      spark.stop()
    }
  }

}
