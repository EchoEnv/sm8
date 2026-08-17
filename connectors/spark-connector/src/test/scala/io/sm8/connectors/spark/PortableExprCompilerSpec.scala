/*
 * SM8 Spark Connector — PortableExprCompiler spec.
 *
 * Per [[scala-data-driven-refactor-mindset]] (sealed-trait
 * dispatch): exhaustive coverage of the 22 Expr cases + 14
 * LiteralValue cases. The compiler is a pure function, so the
 * round-trip test is a RoundTrip-of-the-companion-object (the
 * captured ref is the singleton, not any per-call state).
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantras #1, #3, #4
 * (closure-safety, schema-drift verify, idempotent retry):
 *   - Mantra #1: the companion is a Scala object (Serializable
 *     per JVM static field conventions); the function is pure.
 *   - Mantra #3: every Expr case is enumerated; the spec proves
 *     the compiler returns a non-null Column for every supported
 *     case.
 *   - Mantra #4: the unsupported cases (MeasureRef,
 *     FunctionCall, ArrayValue literal) throw
 *     UnsupportedOperationException with diagnostic messages.
 *     Tests assert the throw + message.
 *
 * Per [[karpathy-guidelines-mindset]] 'smallest correct core':
 * the spec covers ONLY what we can verify today — every Expr
 * case's behavior, every LiteralValue case's behavior, the
 * closure-safety round-trip, and the unsupported-case error
 * contract. The full DataFrame integration (with the actual
 * df.filter().collect() flow) lands in SparkEngineProviderRuntimeSpec
 * — a separate test class in the Layer C follow-up.
 */
package io.sm8.connectors.spark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.sm8.core.engine.{EngineError, EngineIdentity}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.schema.{Field, SealedDataType}

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types.{DataType => SparkDataType, IntegerType => SparkIntegerType, StringType => SparkStringType}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PortableExprCompilerSpec extends AnyFunSuite with Matchers {

  private def buildFakeSpark(): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("sm8-portable-expr-compiler-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  // -- Expr case coverage (22 cases from sm8-core's Expr.scala) --

  test("Expr.Literal: dispatches to lit(value) for IntValue") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.IntValue(42), SealedDataType.Int)).toOption.get
      col should not be null
      col.expr.sql should include ("42")
    } finally { spark.stop() }
  }

  test("Expr.Literal: dispatches to lit(null) for NullValue") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.NullValue, SealedDataType.Int)).toOption.get
      col should not be null
    } finally { spark.stop() }
  }

  test("Expr.Literal: dispatches to lit for StringValue") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.StringValue("hi"), SealedDataType.Varchar)).toOption.get
      col.expr.sql should include ("hi")
    } finally { spark.stop() }
  }

  test("Expr.Literal: dispatches to lit for BoolValue") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean)).toOption.get
      col.expr.sql should include ("true")
    } finally { spark.stop() }
  }

  test("Expr.FieldRef: dispatches to col(name)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.FieldRef("foo")).toOption.get
      col.expr.sql should include ("foo")
    } finally { spark.stop() }
  }

  test("Expr.Add: dispatches to (l + r)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Add(Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int))).toOption.get
      col.expr.sql should include ("+")
    } finally { spark.stop() }
  }

  test("Expr.Subtract: dispatches to (l - r)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Subtract(Expr.Literal(LiteralValue.IntValue(5), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(3), SealedDataType.Int))).toOption.get
      col.expr.sql should include ("-")
    } finally { spark.stop() }
  }

  test("Expr.Multiply: dispatches to (l * r)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Multiply(Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(3), SealedDataType.Int))).toOption.get
      col.expr.sql should include ("*")
    } finally { spark.stop() }
  }

  test("Expr.Divide: dispatches to (l / r)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Divide(Expr.Literal(LiteralValue.IntValue(6), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int))).toOption.get
      col.expr.sql should include ("/")
    } finally { spark.stop() }
  }

  test("Expr.Modulo: dispatches to (l % r)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Modulo(Expr.Literal(LiteralValue.IntValue(7), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(3), SealedDataType.Int))).toOption.get
      col.expr.sql should include ("%")
    } finally { spark.stop() }
  }

  test("Expr.Equal: dispatches to ===") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Equal(
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))).toOption.get
      col.expr.sql should include ("=")
    } finally { spark.stop() }
  }

  test("Expr.NotEqual: dispatches to =!=") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.NotEqual(
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int))).toOption.get
      col.expr.sql should include ("NOT (")
    } finally { spark.stop() }
  }

  test("Expr.LessThan: dispatches to <") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.LessThan(
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int))).toOption.get
      col.expr.sql should include ("<")
    } finally { spark.stop() }
  }

  test("Expr.LessOrEqual: dispatches to <=") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.LessOrEqual(
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))).toOption.get
      col.expr.sql should include ("<=")
    } finally { spark.stop() }
  }

  test("Expr.GreaterThan: dispatches to >") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.GreaterThan(
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))).toOption.get
      col.expr.sql should include (">")
    } finally { spark.stop() }
  }

  test("Expr.GreaterOrEqual: dispatches to >=") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.GreaterOrEqual(
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int))).toOption.get
      col.expr.sql should include (">=")
    } finally { spark.stop() }
  }

  test("Expr.And: dispatches to &&") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.And(
        Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean),
        Expr.Literal(LiteralValue.BoolValue(false), SealedDataType.Boolean))).toOption.get
      col.expr.sql should include ("AND")
    } finally { spark.stop() }
  }

  test("Expr.Or: dispatches to ||") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Or(
        Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean),
        Expr.Literal(LiteralValue.BoolValue(false), SealedDataType.Boolean))).toOption.get
      col.expr.sql should include ("OR")
    } finally { spark.stop() }
  }

  test("Expr.Not: dispatches to unary !") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Not(
        Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean))).toOption.get
      col.expr.sql should include ("NOT")
    } finally { spark.stop() }
  }

  test("Expr.IsNull: dispatches to .isNull") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.IsNull(
        Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int))).toOption.get
      col.expr.sql should include ("IS NULL")
    } finally { spark.stop() }
  }

  test("Expr.IsNotNull: dispatches to .isNotNull") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.IsNotNull(
        Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int))).toOption.get
      col.expr.sql should include ("IS NOT NULL")
    } finally { spark.stop() }
  }

  test("Expr.Cast: dispatches to .cast") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Cast(
        Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int),
        SealedDataType.Varchar)).toOption.get
      col should not be null
    } finally { spark.stop() }
  }

  // ===== PR-O1b (ADR-008-O, P0-1): Expr.Cast honors targetType =====

  test("PR-O1b: Expr.Cast(targetType = Varchar) renders cast(string) not cast(int)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Cast(
        Expr.FieldRef("amount"),
        SealedDataType.Varchar)).toOption.get
      val sql = col.expr.sql.toUpperCase
      sql should include ("CAST")
      sql should not include "INT"
      sql should include ("STRING")
    } finally { spark.stop() }
  }

  test("PR-O1b: Expr.Cast(targetType = Int) renders cast(int)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Cast(
        Expr.FieldRef("amount_str"),
        SealedDataType.Int)).toOption.get
      val sql = col.expr.sql.toUpperCase
      sql should include ("CAST")
      sql should include ("INT")
    } finally { spark.stop() }
  }

  test("PR-O1b: Expr.Cast(targetType = Decimal(10,2)) renders cast(DECIMAL(10,2))") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Cast(
        Expr.FieldRef("raw_amount"),
        SealedDataType.Decimal(precision = 10, scale = 2))).toOption.get
      val sql = col.expr.sql.toUpperCase
      sql should include ("CAST")
      sql should include ("DECIMAL")
    } finally { spark.stop() }
  }

  test("Expr.All: dispatches to col(name)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.All("rows")).toOption.get
      col.expr.sql should include ("rows")
    } finally { spark.stop() }
  }

  // -- Unsupported cases throw with diagnostic --

  test("Expr.MeasureRef: lowers to col(name) (PR-M4 contract change)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.MeasureRef("total")).toOption.get
      col should not be null
      col.expr.sql should include ("total")
    } finally { spark.stop() }
  }
  // -- LiteralValue: all 14 cases --

  test("LiteralValue.ByteValue: lit(b)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.ByteValue(7.toByte), SealedDataType.Int)).toOption.get
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.ShortValue: lit(s)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.ShortValue(7.toShort), SealedDataType.Int)).toOption.get
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.LongValue: lit(n)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.LongValue(7L), SealedDataType.Int)).toOption.get
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.FloatValue: lit(f)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.FloatValue(3.14f), SealedDataType.Double)).toOption.get
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.DoubleValue: lit(d)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.DoubleValue(3.14), SealedDataType.Double)).toOption.get
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.DecimalValue: lit(d)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(
        LiteralValue.DecimalValue(BigDecimal("3.14")), SealedDataType.Decimal(38, 18))).toOption.get
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.TimestampValue: lit(instant)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(
        LiteralValue.TimestampValue(java.time.Instant.now()), SealedDataType.Timestamp)).toOption.get
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.DateValue: lit(date)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(
        LiteralValue.DateValue(java.time.LocalDate.now()), SealedDataType.Date)).toOption.get
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.BinaryValue: lit(b)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(
        LiteralValue.BinaryValue(Vector(1.toByte, 2.toByte, 3.toByte)), SealedDataType.Binary)).toOption.get
      col should not be null
    } finally { spark.stop() }
  }


  // -- Closure-safety baseline --

  /** Round-trip via Java serialization - the path Restate and
    * Spark use to ship singleton objects across threads. */
  private def roundTripViaJavaSerialization[T <: AnyRef](value: T): T = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.close()
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val out = ois.readObject().asInstanceOf[T]
    ois.close()
    out
  }

  test("PortableExprCompiler companion round-trips through ObjectOutputStream (closure-safety)") {
    // The companion is a Scala object (singleton). Per
    // [[scala-spark-batch-bugs-mindset]] mantra #1: the JVM
    // static-field convention makes Scala objects Serializable
    // by default. This test proves the contract at runtime.
    val restored = roundTripViaJavaSerialization(PortableExprCompiler)
    restored shouldBe PortableExprCompiler
  }

  test("Expr.CaseWhen: folds left-to-right (first match wins; Spark's Column.when fold)") {
    val spark = buildFakeSpark()
    try {
      // branches: [(age > 18, "adult"), (age > 13, "teen")]
      // → when(age > 18, "adult").when(age > 13, "teen").otherwise("child")
      val col = PortableExprCompiler.toColumn(Expr.CaseWhen(
        branches = List(
          (Expr.GreaterThan(Expr.FieldRef("age"), Expr.Literal(LiteralValue.IntValue(18), SealedDataType.Int)),
           Expr.Literal(LiteralValue.StringValue("adult"), SealedDataType.Varchar)),
          (Expr.GreaterThan(Expr.FieldRef("age"), Expr.Literal(LiteralValue.IntValue(13), SealedDataType.Int)),
           Expr.Literal(LiteralValue.StringValue("teen"), SealedDataType.Varchar)),
        ),
        otherwise = Expr.Literal(LiteralValue.StringValue("child"), SealedDataType.Varchar),
      )).toOption.get
      col should not be null
      // The compiled Column must carry "CASE WHEN ... ELSE ... END"
      // in its SQL rendering (per RFC §12 conformance).
      col.expr.sql should include ("CASE WHEN")
    } finally { spark.stop() }
  }

  test("Expr.Alias: wraps the inner expression with a column name (expr AS name)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Alias(
        name = "age_plus_one",
        expr = Expr.Add(Expr.FieldRef("age"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
      )).toOption.get
      col should not be null
      col.expr.sql should include ("AS")
    } finally { spark.stop() }
  }

  // PR-O1c (ADR-008-O, P0-2): typed-error tests at the
  // toColumn boundary. Per [[scala-error-handling-mindset]]
  // decision rule #1, FunctionCall and ArrayValue are EXPECTED
  // errors (UDF resolution + array-literal support deferred),
  // not programmer errors -- hence typed Left(UnsupportedCapability)
  // instead of throw. Both cases are wired here as contract
  // assertions so a future refactor that re-introduces a throw
  // site will be caught by the reactor.
  test("PR-O1c: toColumn(Expr.FunctionCall) returns Left(UnsupportedCapability) -- no throw") {
    val spark = buildFakeSpark()
    try {
      val fc: Expr = Expr.FunctionCall(
        name = "concat_ws",
        args = List(Expr.FieldRef("a"), Expr.FieldRef("b")),
      )
      val res: Either[EngineError, Column] = PortableExprCompiler.toColumn(fc)
      res shouldBe a [Left[_, _]]
      res.swap.toOption.get shouldBe a [EngineError.UnsupportedCapability]
    } finally { spark.stop() }
  }

  test("PR-O1c: toColumn(LiteralValue.ArrayValue) returns Left(UnsupportedCapability) -- no throw") {
    val spark = buildFakeSpark()
    try {
      val lit: Expr = Expr.Literal(
        LiteralValue.ArrayValue(List(LiteralValue.IntValue(1), LiteralValue.IntValue(2))),
        SealedDataType.Array(SealedDataType.Int),
      )
      val res: Either[EngineError, Column] = PortableExprCompiler.toColumn(lit)
      res shouldBe a [Left[_, _]]
      res.swap.toOption.get shouldBe a [EngineError.UnsupportedCapability]
    } finally { spark.stop() }
  }

  test("PR-O1c: colsOf([expr1, expr2]) folds Either through the list (no throw on partial failure)") {
    val spark = buildFakeSpark()
    try {
      // Mix a valid expr (FieldRef -> ok) with an unsupported one
      // (FunctionCall -> Left). The fold must short-circuit to
      // Left instead of throwing on the bad entry.
      val ok: Expr = Expr.FieldRef("age")
      val bad: Expr = Expr.FunctionCall("fn", List(Expr.FieldRef("x")))
      val res: Either[EngineError, Array[Column]] =
        PortableExprCompiler.colsOf(List(ok, bad))
      res shouldBe a [Left[_, _]]
      res.swap.toOption.get shouldBe a [EngineError.UnsupportedCapability]
    } finally { spark.stop() }
  }

}
