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
 * `decodeCell` (added in PR-C4a) is the inverse of `encodeCell`
 * — string-matching on the 9 `RestateCachedRow.T_*` tag constants.
 * Not exhaustive in the same sense (the tag set is open — adding
 * a new tag is a forward-compat break, not a compile-time change),
 * so the unknown-tag branch throws `IllegalArgumentException`.
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

  test("Total mapping: all 9 ResultValue cases + None produce a wire-stable value") {
    val all: Seq[Option[ResultValue]] = Seq(
      Option(ResultValue.NullV),
      Option(ResultValue.BoolV(true)),
      Option(ResultValue.IntV(42L)),
      Option(ResultValue.DoubleV(3.14)),
      Option(ResultValue.DecimalV(BigDecimal("1.5"))),
      Option(ResultValue.StringV("hello")),
      Option(ResultValue.TimestampV(java.time.Instant.parse("2024-01-15T10:30:00Z"))),
      Option(ResultValue.DateV(java.time.LocalDate.parse("2024-01-15"))),
      Option(ResultValue.BinaryV("hello".getBytes("UTF-8"))),
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

  // -- decodeCell: inverse of encodeCell (PR-C4a) --

  test("decodeCell: T_NULL → null regardless of encoded value") {
    PortableCellCodec.decodeCell(RestateCachedRow.T_NULL, "x")      shouldBe null
    PortableCellCodec.decodeCell(RestateCachedRow.T_NULL, null)     shouldBe null
    PortableCellCodec.decodeCell(RestateCachedRow.T_NULL, "")       shouldBe null
  }

  test("decodeCell: encoded=null → null regardless of tag") {
    PortableCellCodec.decodeCell(RestateCachedRow.T_STRING,  null) shouldBe null
    PortableCellCodec.decodeCell(RestateCachedRow.T_LONG,    null) shouldBe null
    PortableCellCodec.decodeCell(RestateCachedRow.T_DOUBLE,  null) shouldBe null
    PortableCellCodec.decodeCell(RestateCachedRow.T_DECIMAL, null) shouldBe null
    PortableCellCodec.decodeCell(RestateCachedRow.T_BOOLEAN, null) shouldBe null
  }

  test("decodeCell: T_STRING → raw String") {
    PortableCellCodec.decodeCell(RestateCachedRow.T_STRING, "hello") shouldBe "hello"
  }

  test("decodeCell: T_LONG → boxed Long") {
    val out = PortableCellCodec.decodeCell(RestateCachedRow.T_LONG, "42")
    out shouldBe 42L
    out.getClass shouldBe classOf[java.lang.Long]
  }

  test("decodeCell: T_DOUBLE → boxed Double") {
    val out = PortableCellCodec.decodeCell(RestateCachedRow.T_DOUBLE, "3.14")
    out shouldBe 3.14
    out.getClass shouldBe classOf[java.lang.Double]
  }

  test("decodeCell: T_DECIMAL → java.math.BigDecimal (precision preserved)") {
    val out = PortableCellCodec.decodeCell(RestateCachedRow.T_DECIMAL, "1234.567890123456789012345678")
    out shouldBe new java.math.BigDecimal("1234.567890123456789012345678")
    out.getClass shouldBe classOf[java.math.BigDecimal]
  }

  test("decodeCell: T_BOOLEAN → boxed Boolean") {
    val outTrue = PortableCellCodec.decodeCell(RestateCachedRow.T_BOOLEAN, "true")
    outTrue shouldBe true
    outTrue.getClass shouldBe classOf[java.lang.Boolean]

    val outFalse = PortableCellCodec.decodeCell(RestateCachedRow.T_BOOLEAN, "false")
    outFalse shouldBe false
  }

  test("decodeCell: T_TIMESTAMP → java.sql.Timestamp (UTC Instant preserved)") {
    val expected = java.sql.Timestamp.from(java.time.Instant.parse("2024-01-15T10:30:00Z"))
    val out = PortableCellCodec.decodeCell(RestateCachedRow.T_TIMESTAMP, "2024-01-15T10:30:00Z")
    out shouldBe a [java.sql.Timestamp]
    out shouldBe expected
  }

  test("decodeCell: T_DATE → java.sql.Date at UTC midnight (timezone-independent)") {
    // Per the legacy Java code: `Date.getTime()` must be JVM-timezone-
    // independent. Building from `LocalDate.atStartOfDay(UTC).toEpochMilli`
    // gives the UTC midnight epoch millis, which IS timezone-independent.
    val expectedMillis = java.time.LocalDate.parse("2024-01-15")
      .atStartOfDay(java.time.ZoneOffset.UTC)
      .toInstant()
      .toEpochMilli()
    val out = PortableCellCodec.decodeCell(RestateCachedRow.T_DATE, "2024-01-15")
    out shouldBe a [java.sql.Date]
    out.asInstanceOf[java.sql.Date].getTime shouldBe expectedMillis
  }

  test("decodeCell: T_BINARY → byte[] (Base64 decoded)") {
    val bytes = "hello".getBytes("UTF-8")
    val encoded = java.util.Base64.getEncoder.encodeToString(bytes)
    val out = PortableCellCodec.decodeCell(RestateCachedRow.T_BINARY, encoded)
    out shouldBe a [Array[Byte]]
    out shouldBe bytes
  }

  test("decodeCell: unknown tag → IllegalArgumentException (forward-compat break)") {
    an [IllegalArgumentException] should be thrownBy {
      PortableCellCodec.decodeCell("unknown_tag", "x")
    }
  }

  // -- Round-trip: encodeCell → decodeCell --

  test("Round-trip: encodeCell then decodeCell produces the same typed value") {
    // For each type, we encode a ResultValue, then decode the
    // string back using the corresponding RestateCachedRow tag.
    // This proves the wire format is consistent in both directions.
    val samples: Seq[(ResultValue, String)] = Seq(
      ResultValue.BoolV(true)                                                -> RestateCachedRow.T_BOOLEAN,
      ResultValue.IntV(42L)                                                   -> RestateCachedRow.T_LONG,
      ResultValue.DoubleV(3.14)                                               -> RestateCachedRow.T_DOUBLE,
      ResultValue.DecimalV(BigDecimal("123.45"))                              -> RestateCachedRow.T_DECIMAL,
      ResultValue.StringV("hello")                                            -> RestateCachedRow.T_STRING,
      ResultValue.TimestampV(java.time.Instant.parse("2024-01-15T10:30:00Z")) -> RestateCachedRow.T_TIMESTAMP,
      ResultValue.DateV(java.time.LocalDate.parse("2024-01-15"))              -> RestateCachedRow.T_DATE
    )
    samples.foreach { case (rv, tag) =>
      val encoded = PortableCellCodec.encodeCell(Option(rv))
      val decoded = PortableCellCodec.decodeCell(tag, encoded)
      // The decode direction returns the Java type (not ResultValue).
      // For booleans, longs, doubles, strings — the value matches.
      // For timestamps, dates, decimals — the Java type matches what
      // the Java legacy code would return.
      decodeExpectations(rv, decoded)
    }
  }

  /** Helper for the round-trip test: asserts the decoded value matches
    * what the legacy Java code would return for the same encoded form. */
  private def decodeExpectations(rv: ResultValue, decoded: Object): Unit = rv match {
    case ResultValue.BoolV(b)        => decoded shouldBe java.lang.Boolean.valueOf(b)
    case ResultValue.IntV(n)         => decoded shouldBe java.lang.Long.valueOf(n)
    case ResultValue.DoubleV(d)      => decoded shouldBe java.lang.Double.valueOf(d)
    case ResultValue.DecimalV(bd)    => decoded shouldBe new java.math.BigDecimal(bd.toString)
    case ResultValue.StringV(s)      => decoded shouldBe s
    case ResultValue.TimestampV(i)   => decoded shouldBe java.sql.Timestamp.from(i)
    case ResultValue.DateV(d)        =>
      val expectedMillis = d.atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli()
      decoded shouldBe a [java.sql.Date]
      decoded.asInstanceOf[java.sql.Date].getTime shouldBe expectedMillis
    case other                      => fail(s"unexpected ResultValue: $other")
  }
}