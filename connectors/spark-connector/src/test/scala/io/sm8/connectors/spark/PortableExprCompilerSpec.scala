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
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.IntValue(42), SealedDataType.Int))
      col should not be null
      col.expr.sql should include ("42")
    } finally { spark.stop() }
  }

  test("Expr.Literal: dispatches to lit(null) for NullValue") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.NullValue, SealedDataType.Int))
      col should not be null
    } finally { spark.stop() }
  }

  test("Expr.Literal: dispatches to lit for StringValue") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.StringValue("hi"), SealedDataType.Varchar))
      col.expr.sql should include ("hi")
    } finally { spark.stop() }
  }

  test("Expr.Literal: dispatches to lit for BoolValue") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean))
      col.expr.sql should include ("true")
    } finally { spark.stop() }
  }

  test("Expr.FieldRef: dispatches to col(name)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.FieldRef("foo"))
      col.expr.sql should include ("foo")
    } finally { spark.stop() }
  }

  test("Expr.Add: dispatches to (l + r)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Add(Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int)))
      col.expr.sql should include ("+")
    } finally { spark.stop() }
  }

  test("Expr.Subtract: dispatches to (l - r)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Subtract(Expr.Literal(LiteralValue.IntValue(5), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(3), SealedDataType.Int)))
      col.expr.sql should include ("-")
    } finally { spark.stop() }
  }

  test("Expr.Multiply: dispatches to (l * r)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Multiply(Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(3), SealedDataType.Int)))
      col.expr.sql should include ("*")
    } finally { spark.stop() }
  }

  test("Expr.Divide: dispatches to (l / r)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Divide(Expr.Literal(LiteralValue.IntValue(6), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int)))
      col.expr.sql should include ("/")
    } finally { spark.stop() }
  }

  test("Expr.Modulo: dispatches to (l % r)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Modulo(Expr.Literal(LiteralValue.IntValue(7), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(3), SealedDataType.Int)))
      col.expr.sql should include ("%")
    } finally { spark.stop() }
  }

  test("Expr.Equal: dispatches to ===") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Equal(
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)))
      col.expr.sql should include ("=")
    } finally { spark.stop() }
  }

  test("Expr.NotEqual: dispatches to =!=") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.NotEqual(
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int)))
      col.expr.sql should include ("NOT (")
    } finally { spark.stop() }
  }

  test("Expr.LessThan: dispatches to <") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.LessThan(
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int)))
      col.expr.sql should include ("<")
    } finally { spark.stop() }
  }

  test("Expr.LessOrEqual: dispatches to <=") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.LessOrEqual(
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)))
      col.expr.sql should include ("<=")
    } finally { spark.stop() }
  }

  test("Expr.GreaterThan: dispatches to >") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.GreaterThan(
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)))
      col.expr.sql should include (">")
    } finally { spark.stop() }
  }

  test("Expr.GreaterOrEqual: dispatches to >=") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.GreaterOrEqual(
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int)))
      col.expr.sql should include (">=")
    } finally { spark.stop() }
  }

  test("Expr.And: dispatches to &&") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.And(
        Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean),
        Expr.Literal(LiteralValue.BoolValue(false), SealedDataType.Boolean)))
      col.expr.sql should include ("AND")
    } finally { spark.stop() }
  }

  test("Expr.Or: dispatches to ||") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Or(
        Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean),
        Expr.Literal(LiteralValue.BoolValue(false), SealedDataType.Boolean)))
      col.expr.sql should include ("OR")
    } finally { spark.stop() }
  }

  test("Expr.Not: dispatches to unary !") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Not(
        Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean)))
      col.expr.sql should include ("NOT")
    } finally { spark.stop() }
  }

  test("Expr.IsNull: dispatches to .isNull") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.IsNull(
        Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int)))
      col.expr.sql should include ("IS NULL")
    } finally { spark.stop() }
  }

  test("Expr.IsNotNull: dispatches to .isNotNull") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.IsNotNull(
        Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int)))
      col.expr.sql should include ("IS NOT NULL")
    } finally { spark.stop() }
  }

  test("Expr.Cast: dispatches to .cast") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Cast(
        Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int),
        SealedDataType.Varchar))
      col should not be null
    } finally { spark.stop() }
  }

  test("Expr.All: dispatches to col(name)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.All("rows"))
      col.expr.sql should include ("rows")
    } finally { spark.stop() }
  }

  // -- Unsupported cases throw with diagnostic --

  test("Expr.MeasureRef: throws UnsupportedOperationException (subquery deferred)") {
    val spark = buildFakeSpark()
    try {
      val ex = intercept[UnsupportedOperationException] {
        PortableExprCompiler.toColumn(Expr.MeasureRef("foo"))
      }
      ex.getMessage should include ("MeasureRef")
    } finally { spark.stop() }
  }

  test("Expr.FunctionCall: throws UnsupportedOperationException (UDF deferred)") {
    val spark = buildFakeSpark()
    try {
      val ex = intercept[UnsupportedOperationException] {
        PortableExprCompiler.toColumn(Expr.FunctionCall("myUdf", Nil))
      }
      ex.getMessage should include ("myUdf")
    } finally { spark.stop() }
  }

  // -- LiteralValue: all 14 cases --

  test("LiteralValue.ByteValue: lit(b)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.ByteValue(7.toByte), SealedDataType.Int))
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.ShortValue: lit(s)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.ShortValue(7.toShort), SealedDataType.Int))
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.LongValue: lit(n)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.LongValue(7L), SealedDataType.Int))
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.FloatValue: lit(f)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.FloatValue(3.14f), SealedDataType.Double))
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.DoubleValue: lit(d)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(LiteralValue.DoubleValue(3.14), SealedDataType.Double))
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.DecimalValue: lit(d)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(
        LiteralValue.DecimalValue(BigDecimal("3.14")), SealedDataType.Decimal(38, 18)))
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.TimestampValue: lit(instant)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(
        LiteralValue.TimestampValue(java.time.Instant.now()), SealedDataType.Timestamp))
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.DateValue: lit(date)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(
        LiteralValue.DateValue(java.time.LocalDate.now()), SealedDataType.Date))
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.BinaryValue: lit(b)") {
    val spark = buildFakeSpark()
    try {
      val col = PortableExprCompiler.toColumn(Expr.Literal(
        LiteralValue.BinaryValue(Vector(1.toByte, 2.toByte, 3.toByte)), SealedDataType.Binary))
      col should not be null
    } finally { spark.stop() }
  }

  test("LiteralValue.ArrayValue: throws UnsupportedOperationException (deferred)") {
    val spark = buildFakeSpark()
    try {
      val ex = intercept[UnsupportedOperationException] {
        PortableExprCompiler.toColumn(Expr.Literal(
          LiteralValue.ArrayValue(List(LiteralValue.IntValue(1), LiteralValue.IntValue(2))),
          SealedDataType.Array(SealedDataType.Int)))
      }
      ex.getMessage should include ("ArrayValue")
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
}
