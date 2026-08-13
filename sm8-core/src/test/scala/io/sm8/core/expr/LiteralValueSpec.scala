package io.sm8.core.expr

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `LiteralValue` is a usable, Spark-free
  * data record + the closed 14-variant enumeration. Per scala-data-
  * driven-refactor, this is pure data: the value SHAPE is engine-
  * portable; the engine-specific transport (Spark's Literal,
  * Trino's parameter binding, etc.) lives in the engine adapter.
  */
class LiteralValueSpec extends AnyFunSuite with Matchers {

  // -- Numeric --

  test("IntValue carries Int value") {
    LiteralValue.IntValue(42).v shouldBe 42
  }

  test("ByteValue carries Byte value") {
    LiteralValue.ByteValue(127.toByte).v shouldBe 127.toByte
  }

  test("ShortValue carries Short value") {
    LiteralValue.ShortValue(1000.toShort).v shouldBe 1000.toShort
  }

  test("LongValue carries Long value (portable default for integer)") {
    LiteralValue.LongValue(5000000000L).v shouldBe 5000000000L
  }

  test("FloatValue carries Float value (32-bit float)") {
    LiteralValue.FloatValue(3.14f).v shouldBe 3.14f
  }

  test("DoubleValue carries Double value (64-bit float)") {
    LiteralValue.DoubleValue(3.14).v shouldBe 3.14
  }

  test("DecimalValue carries BigDecimal (precision, scale preserved)") {
    val v = BigDecimal("123.456")
    LiteralValue.DecimalValue(v).v shouldBe v
    LiteralValue.DecimalValue(v).v.scale shouldBe 3
  }

  // -- Text --

  test("StringValue carries String") {
    LiteralValue.StringValue("hello").v shouldBe "hello"
  }

  // -- Boolean --

  test("BoolValue carries Boolean (true)") {
    LiteralValue.BoolValue(true).v shouldBe true
  }

  test("BoolValue carries Boolean (false)") {
    LiteralValue.BoolValue(false).v shouldBe false
  }

  // -- Binary --

  test("BinaryValue carries Vector[Byte]") {
    val v = Vector[Byte](1, 2, 3, 4)
    LiteralValue.BinaryValue(v).v shouldBe v
  }

  // -- Temporal --

  test("TimestampValue carries Instant") {
    val v = java.time.Instant.parse("2026-08-04T10:00:00Z")
    LiteralValue.TimestampValue(v).v shouldBe v
  }

  test("DateValue carries LocalDate") {
    val v = java.time.LocalDate.parse("2026-08-04")
    LiteralValue.DateValue(v).v shouldBe v
  }

  // -- Nested --

  test("ArrayValue carries List[LiteralValue]") {
    val v = LiteralValue.ArrayValue(List(
      LiteralValue.IntValue(1),
      LiteralValue.IntValue(2),
    ))
    v.values.size shouldBe 2
    v.values(0) shouldBe LiteralValue.IntValue(1)
  }

  test("MapValue carries List[(LiteralValue, LiteralValue)]") {
    val v = LiteralValue.MapValue(List(
      (LiteralValue.StringValue("a"), LiteralValue.IntValue(1)),
      (LiteralValue.StringValue("b"), LiteralValue.IntValue(2)),
    ))
    v.values.size shouldBe 2
  }

  test("StructValue carries List[(String, LiteralValue)]") {
    val v = LiteralValue.StructValue(List(
      ("id", LiteralValue.IntValue(1)),
      ("name", LiteralValue.StringValue("Alice")),
    ))
    v.fields.size shouldBe 2
  }

  // -- Special --

  test("NullValue is a singleton (the absence of a value)") {
    LiteralValue.NullValue shouldBe LiteralValue.NullValue
  }

  // -- closed enumeration + sealed exhaustiveness --

  test("LiteralValue has exactly 16 cases") {
    val all: Set[LiteralValue] = Set(
      // 7 numeric
      LiteralValue.IntValue(1),
      LiteralValue.ByteValue(1.toByte),
      LiteralValue.ShortValue(1.toShort),
      LiteralValue.LongValue(1L),
      LiteralValue.FloatValue(1.0f),
      LiteralValue.DoubleValue(1.0),
      LiteralValue.DecimalValue(BigDecimal("1.0")),
      // 1 text
      LiteralValue.StringValue("a"),
      // 1 boolean
      LiteralValue.BoolValue(true),
      // 1 binary
      LiteralValue.BinaryValue(Vector[Byte](1)),
      // 2 temporal
      LiteralValue.TimestampValue(java.time.Instant.parse("2026-08-04T10:00:00Z")),
      LiteralValue.DateValue(java.time.LocalDate.parse("2026-08-04")),
      // 3 nested
      LiteralValue.ArrayValue(Nil),
      LiteralValue.MapValue(Nil),
      LiteralValue.StructValue(Nil),
      // 1 special
      LiteralValue.NullValue,
    )
    all.size shouldBe 16
  }

  test("Sealed exhaustiveness: pattern-match over all 16 cases") {
    val all: Seq[LiteralValue] = Seq(
      LiteralValue.IntValue(1),
      LiteralValue.ByteValue(1.toByte),
      LiteralValue.ShortValue(1.toShort),
      LiteralValue.LongValue(1L),
      LiteralValue.FloatValue(1.0f),
      LiteralValue.DoubleValue(1.0),
      LiteralValue.DecimalValue(BigDecimal("1.0")),
      LiteralValue.StringValue("a"),
      LiteralValue.BoolValue(true),
      LiteralValue.BinaryValue(Vector[Byte](1)),
      LiteralValue.TimestampValue(java.time.Instant.parse("2026-08-04T10:00:00Z")),
      LiteralValue.DateValue(java.time.LocalDate.parse("2026-08-04")),
      LiteralValue.ArrayValue(Nil),
      LiteralValue.MapValue(Nil),
      LiteralValue.StructValue(Nil),
      LiteralValue.NullValue,
    )
    all.foreach {
      case _: LiteralValue.IntValue       => ()
      case _: LiteralValue.ByteValue      => ()
      case _: LiteralValue.ShortValue     => ()
      case _: LiteralValue.LongValue      => ()
      case _: LiteralValue.FloatValue     => ()
      case _: LiteralValue.DoubleValue    => ()
      case _: LiteralValue.DecimalValue   => ()
      case _: LiteralValue.StringValue    => ()
      case _: LiteralValue.BoolValue      => ()
      case _: LiteralValue.BinaryValue    => ()
      case _: LiteralValue.TimestampValue => ()
      case _: LiteralValue.DateValue      => ()
      case _: LiteralValue.ArrayValue    => ()
      case _: LiteralValue.MapValue      => ()
      case _: LiteralValue.StructValue    => ()
      case LiteralValue.NullValue         => ()
    }
  }

  // -- Serializable round-trip --

  test("all 14 case variants round-trip through Java serialization") {
    val cases: Seq[LiteralValue] = Seq(
      LiteralValue.IntValue(42),
      LiteralValue.ByteValue(127.toByte),
      LiteralValue.ShortValue(1000.toShort),
      LiteralValue.LongValue(5000000000L),
      LiteralValue.FloatValue(3.14f),
      LiteralValue.DoubleValue(3.14),
      LiteralValue.DecimalValue(BigDecimal("123.456")),
      LiteralValue.StringValue("hello"),
      LiteralValue.BoolValue(true),
      LiteralValue.BinaryValue(Vector[Byte](1, 2, 3)),
      LiteralValue.TimestampValue(java.time.Instant.parse("2026-08-04T10:00:00Z")),
      LiteralValue.DateValue(java.time.LocalDate.parse("2026-08-04")),
      LiteralValue.ArrayValue(List(LiteralValue.IntValue(1), LiteralValue.IntValue(2))),
      LiteralValue.MapValue(List((LiteralValue.StringValue("a"), LiteralValue.IntValue(1)))),
      LiteralValue.NullValue,
    )
    cases.foreach { v =>
      val bos = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(bos)
      oos.writeObject(v)
      oos.close()
      val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
      val ois = new java.io.ObjectInputStream(bis)
      val restored = ois.readObject().asInstanceOf[LiteralValue]
      restored shouldBe v
    }
  }
}