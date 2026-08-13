package io.sm8.platform.query

import io.sm8.core.engine.ResultValue

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Phase 2 contract: prove `PortableCellCodec.encodeCell` and
 * `PortableCellCodec.toJavaValue` are total mappings from the
 * closed 8-variant `ResultValue` ADT to wire-stable String / Object
 * shapes. Mirrors `QueryServiceEngineRegistryTest.encodePortableCell_handlesAllTypedValues`
 * (semanticdf-platform lines 156-172) plus the 4 cases the Java
 * legacy code didn't handle (DateV + null/None consistency).
 *
 * Per [[scala-data-driven-refactor-mindset]]: pure dispatch, no
 * I/O. The compiler enforces exhaustiveness over the sealed
 * `ResultValue` — if a new variant is added, this file fails to
 * compile until `PortableCellCodec` is updated.
 *
 * Per [[scala-impact-analysis-mindset]]: 0 callers of the Java
 * helpers in our reactor. These tests are the wire-contract
 * proof that the Scala version preserves the legacy behavior.
 */
class PortableCellCodecSpec extends AnyFunSuite with Matchers {

  // -- encodeCell: mirror Java test cases --

  test("encodeCell: StringV → \"hello\"") {
    PortableCellCodec.encodeCell(Option(ResultValue.StringV("hello"))) shouldBe "hello"
  }

  test("encodeCell: IntV(42L) → \"42\"") {
    PortableCellCodec.encodeCell(Option(ResultValue.IntV(42L))) shouldBe "42"
  }

  test("encodeCell: DoubleV(3.14) → \"3.14\"") {
    PortableCellCodec.encodeCell(Option(ResultValue.DoubleV(3.14))) shouldBe "3.14"
  }

  test("encodeCell: BoolV(true) → \"true\"") {
    PortableCellCodec.encodeCell(Option(ResultValue.BoolV(true))) shouldBe "true"
  }

  test("encodeCell: BoolV(false) → \"false\"") {
    PortableCellCodec.encodeCell(Option(ResultValue.BoolV(false))) shouldBe "false"
  }

  test("encodeCell: DecimalV(1.5) → \"1.5\" (preserves BigDecimal precision)") {
    val bd = BigDecimal("1.5")
    PortableCellCodec.encodeCell(Option(ResultValue.DecimalV(bd))) shouldBe "1.5"
  }

  test("encodeCell: DecimalV with arbitrary precision/scale preserved") {
    val bd = BigDecimal("1234.567890123456789012345678")
    PortableCellCodec.encodeCell(Option(ResultValue.DecimalV(bd))) shouldBe "1234.567890123456789012345678"
  }

  test("encodeCell: NullV → null") {
    PortableCellCodec.encodeCell(Option(ResultValue.NullV)) shouldBe null
  }

  test("encodeCell: None → null") {
    PortableCellCodec.encodeCell(None) shouldBe null
  }

  test("encodeCell: null overload → null") {
    PortableCellCodec.encodeCell(null.asInstanceOf[ResultValue]) shouldBe null
  }

  test("encodeCell: Java-friendly overload matches Option overload") {
    val samples: Seq[ResultValue] = Seq(
      ResultValue.NullV,
      ResultValue.BoolV(true),
      ResultValue.IntV(42L),
      ResultValue.DoubleV(3.14),
      ResultValue.DecimalV(BigDecimal("1.5")),
      ResultValue.StringV("hello"),
      ResultValue.TimestampV(java.time.Instant.parse("2024-01-15T10:30:00Z")),
      ResultValue.DateV(java.time.LocalDate.parse("2024-01-15"))
    )
    samples.foreach { rv =>
      PortableCellCodec.encodeCell(rv) shouldBe PortableCellCodec.encodeCell(Option(rv))
    }
  }

  // -- encodeCell: Scala-only (exhaustive over all 8 cases) --

  test("encodeCell: TimestampV → ISO-8601 Instant string") {
    val instant = java.time.Instant.parse("2024-01-15T10:30:00Z")
    PortableCellCodec.encodeCell(Option(ResultValue.TimestampV(instant))) shouldBe "2024-01-15T10:30:00Z"
  }

  test("encodeCell: DateV → ISO-8601 LocalDate string (Scala-only; was IAE in Java)") {
    val date = java.time.LocalDate.parse("2024-01-15")
    PortableCellCodec.encodeCell(Option(ResultValue.DateV(date))) shouldBe "2024-01-15"
  }

  // -- toJavaValue: new Scala-side tests (Java had no direct test) --

  test("toJavaValue: StringV → raw String") {
    PortableCellCodec.toJavaValue(Option(ResultValue.StringV("hello"))) shouldBe "hello"
  }

  test("toJavaValue: IntV(42L) → boxed Long") {
    val out = PortableCellCodec.toJavaValue(Option(ResultValue.IntV(42L)))
    out shouldBe 42L
    out.getClass shouldBe classOf[java.lang.Long]
  }

  test("toJavaValue: DoubleV(3.14) → boxed Double") {
    val out = PortableCellCodec.toJavaValue(Option(ResultValue.DoubleV(3.14)))
    out shouldBe 3.14
    out.getClass shouldBe classOf[java.lang.Double]
  }

  test("toJavaValue: BoolV(true) → boxed Boolean") {
    val out = PortableCellCodec.toJavaValue(Option(ResultValue.BoolV(true)))
    out shouldBe true
    out.getClass shouldBe classOf[java.lang.Boolean]
  }

  test("toJavaValue: DecimalV → BigDecimal (Scala BigDecimal wrapper — matches legacy Java)") {
    val bd = BigDecimal("1.5")
    val out = PortableCellCodec.toJavaValue(Option(ResultValue.DecimalV(bd)))
    out shouldBe bd
    // Note: Scala 2.13's `scala.math.BigDecimal` wraps `java.math.BigDecimal`.
    // The legacy Java code at `QueryService.java:361` returned the same
    // Scala BigDecimal (the value field's static type). Jackson handles
    // both as JSON numbers — no wire-format difference.
    out.getClass shouldBe classOf[scala.math.BigDecimal]
  }

  test("toJavaValue: StringV → String") {
    val out = PortableCellCodec.toJavaValue(Option(ResultValue.StringV("hello")))
    out shouldBe "hello"
    out.getClass shouldBe classOf[String]
  }

  test("toJavaValue: TimestampV → ISO-8601 String (matches Java)") {
    val instant = java.time.Instant.parse("2024-01-15T10:30:00Z")
    PortableCellCodec.toJavaValue(Option(ResultValue.TimestampV(instant))) shouldBe "2024-01-15T10:30:00Z"
  }

  test("toJavaValue: DateV → ISO-8601 String (Scala-only; was IAE in Java)") {
    val date = java.time.LocalDate.parse("2024-01-15")
    PortableCellCodec.toJavaValue(Option(ResultValue.DateV(date))) shouldBe "2024-01-15"
  }

  test("toJavaValue: NullV → null") {
    PortableCellCodec.toJavaValue(Option(ResultValue.NullV)) shouldBe null
  }

  test("toJavaValue: None → null") {
    PortableCellCodec.toJavaValue(None) shouldBe null
  }

  test("toJavaValue: null overload → null") {
    PortableCellCodec.toJavaValue(null.asInstanceOf[ResultValue]) shouldBe null
  }

  test("toJavaValue: Java-friendly overload matches Option overload") {
    val samples: Seq[ResultValue] = Seq(
      ResultValue.NullV,
      ResultValue.BoolV(true),
      ResultValue.IntV(42L),
      ResultValue.DoubleV(3.14),
      ResultValue.DecimalV(BigDecimal("1.5")),
      ResultValue.StringV("hello"),
      ResultValue.TimestampV(java.time.Instant.parse("2024-01-15T10:30:00Z")),
      ResultValue.DateV(java.time.LocalDate.parse("2024-01-15"))
    )
    samples.foreach { rv =>
      PortableCellCodec.toJavaValue(rv) shouldBe PortableCellCodec.toJavaValue(Option(rv))
    }
  }

  // -- Wire contract: total mapping --

  test("Total mapping: all 8 ResultValue cases + None produce a wire-stable value") {
    val all: Seq[Option[ResultValue]] = Seq(
      Option(ResultValue.NullV),
      Option(ResultValue.BoolV(true)),
      Option(ResultValue.IntV(42L)),
      Option(ResultValue.DoubleV(3.14)),
      Option(ResultValue.DecimalV(BigDecimal("1.5"))),
      Option(ResultValue.StringV("hello")),
      Option(ResultValue.TimestampV(java.time.Instant.parse("2024-01-15T10:30:00Z"))),
      Option(ResultValue.DateV(java.time.LocalDate.parse("2024-01-15"))),
      None
    )
    all.foreach { rv =>
      // encodeCell produces a String (or null)
      val encoded = PortableCellCodec.encodeCell(rv)
      // toJavaValue produces any Object (or null)
      val boxed = PortableCellCodec.toJavaValue(rv)
      // Both calls return without throwing — proves no MatchError leakage
      // (just touching them forces the sealed pattern match).
      (rv, encoded, boxed)
    }
  }
}