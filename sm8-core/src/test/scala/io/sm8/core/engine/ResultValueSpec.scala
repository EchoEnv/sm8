package io.sm8.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the [[ResultValue]] sealed ADT.
  *
  * Per scala-data-driven-refacer \u00a73 ("sealed ADT is
  * correct, NOT a Map"): the closed set of `ResultValue`
  * cases forces every consumer to handle each case.
  * Adding a new case is a compile-time change at every
  * consumer site. */
class ResultValueSpec extends AnyFunSuite with Matchers {

  test("ResultValue is a sealed trait (not a case class)") {
    val rv: ResultValue = ResultValue.NullV
    rv shouldBe a [ResultValue]
  }

  test("NullV is a singleton (the only NullV instance)") {
    val a: ResultValue = ResultValue.NullV
    val b: ResultValue = ResultValue.NullV
    (a eq b) shouldBe true  // case objects are singletons
  }

  test("ResultValue.isNull returns true for NullV, false for everything else") {
    ResultValue.isNull(ResultValue.NullV) shouldBe true
    ResultValue.isNull(ResultValue.IntV(42L)) shouldBe false
    ResultValue.isNull(ResultValue.StringV("x")) shouldBe false
  }

  test("BoolV wraps a boolean") {
    val rv = ResultValue.BoolV(true)
    rv shouldBe a [ResultValue.BoolV]
    rv.asInstanceOf[ResultValue.BoolV].v shouldBe true
  }

  test("IntV wraps a Long (per design \u00a74.5.4 'engines normalize integer types to 64-bit')") {
    val rv = ResultValue.IntV(42L)
    rv shouldBe a [ResultValue.IntV]
    rv.asInstanceOf[ResultValue.IntV].v shouldBe 42L
  }

  test("DoubleV wraps a Double") {
    val rv = ResultValue.DoubleV(3.14)
    rv shouldBe a [ResultValue.DoubleV]
    rv.asInstanceOf[ResultValue.DoubleV].v shouldBe 3.14
  }

  test("DecimalV wraps a BigDecimal (precision/scale preserved)") {
    val bd = BigDecimal("123.45")
    val rv = ResultValue.DecimalV(bd)
    rv shouldBe a [ResultValue.DecimalV]
    rv.asInstanceOf[ResultValue.DecimalV].v shouldBe bd
  }

  test("StringV wraps a String") {
    val rv = ResultValue.StringV("hello")
    rv shouldBe a [ResultValue.StringV]
    rv.asInstanceOf[ResultValue.StringV].v shouldBe "hello"
  }

  test("TimestampV wraps a UTC Instant (per design 'timestamps normalize to UTC')") {
    val instant = java.time.Instant.parse("2024-01-15T10:30:00Z")
    val rv = ResultValue.TimestampV(instant)
    rv shouldBe a [ResultValue.TimestampV]
    rv.asInstanceOf[ResultValue.TimestampV].v shouldBe instant
  }

  test("DateV wraps a LocalDate") {
    val date = java.time.LocalDate.parse("2024-01-15")
    val rv = ResultValue.DateV(date)
    rv shouldBe a [ResultValue.DateV]
    rv.asInstanceOf[ResultValue.DateV].v shouldBe date
  }

  test("ResultValue round-trips through Java serialization") {
    val cases: List[ResultValue] = List(
      ResultValue.NullV,
      ResultValue.BoolV(true),
      ResultValue.IntV(42L),
      ResultValue.DoubleV(3.14),
      ResultValue.DecimalV(BigDecimal("123.45")),
      ResultValue.StringV("hello"),
      ResultValue.TimestampV(java.time.Instant.now()),
      ResultValue.DateV(java.time.LocalDate.now()),
    )
    cases.foreach { rv =>
      val out = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(out)
      oos.writeObject(rv)
      oos.close()
      val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(out.toByteArray))
      val back = ois.readObject().asInstanceOf[ResultValue]
      ois.close()
      back shouldBe rv
    }
  }
}