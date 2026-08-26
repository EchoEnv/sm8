package io.sm8.core.cache

import io.sm8.core.engine.{EngineError, PortableQueryResult, ResultRow, ResultSchema, ResultValue}
import io.sm8.core.schema.{Field, SealedDataType}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


/**
 * Tests for `CachedRowDecoder.fromRestateCachedRow` — the row-level
 * decoder for the engine-portable cached-row wire format.
 *
 * Per [[scala-data-driven-refactor-mindset]]: pure dispatch, no
 * I/O. The decoder walks the `RestateCachedRow.rows` List, applying
 * `PortableCellCodec.decodeCell` (PR-C4a) to each cell.
 *
 * Per [[scala-impact-analysis-mindset]]: 0 callers of the Java
 * `QueryService.fromRestateCachedRow` in our reactor. These tests
 * are the wire-contract proof.
 */
class CachedRowDecoderSpec extends AnyFunSuite with Matchers {

  // -- Empty / boundary cases --

  test("fromRestateCachedRow: empty rows → empty list") {
    val row = RestateCachedRow(
      fieldNames = List("a", "b"),
      fieldTypes = List(RestateCachedRow.T_STRING, RestateCachedRow.T_LONG),
      rows       = Nil
    )
    CachedRowDecoder.fromRestateCachedRow(row) shouldBe Nil
  }

  test("fromRestateCachedRow: null row entry → empty list (matches Java's RowFactory.create(Object[0]))") {
    // The legacy Java code converts null cells to empty Rows via
    // `RowFactory.create(new Object[0])`. The Scala version preserves
    // this: null cells → List.empty[Object].
    val row = RestateCachedRow(
      fieldNames = List("a", "b"),
      fieldTypes = List(RestateCachedRow.T_STRING, RestateCachedRow.T_LONG),
      rows       = List(null, Array("x", "1"))
    )
    val decoded = CachedRowDecoder.fromRestateCachedRow(row)
    decoded should have size 2
    decoded(0) shouldBe List.empty[Object]
    decoded(1) shouldBe List("x", 1L)
  }

  // -- Single-row, all-tag-type coverage --

  test("fromRestateCachedRow: 2 cols, 2 rows → 2 decoded rows") {
    val row = RestateCachedRow(
      fieldNames = List("carrier", "rows"),
      fieldTypes = List(RestateCachedRow.T_STRING, RestateCachedRow.T_LONG),
      rows       = List(Array("AA", "100"), Array("BB", "200"))
    )
    val decoded = CachedRowDecoder.fromRestateCachedRow(row)
    decoded shouldBe List(
      List("AA", 100L),
      List("BB", 200L)
    )
  }

  test("fromRestateCachedRow: 1 row with all 8 supported types (excluding T_DATE wire-spec): " +
       "String/Long/Double/Decimal/Boolean/Timestamp/Binary/NullV") {
    val instant = java.time.Instant.parse("2024-01-15T10:30:00Z")
    val bytes = "hello".getBytes("UTF-8")
    val row = RestateCachedRow(
      fieldNames = List("s", "l", "d", "dec", "b", "ts", "bin", "n"),
      fieldTypes = List(
        RestateCachedRow.T_STRING,
        RestateCachedRow.T_LONG,
        RestateCachedRow.T_DOUBLE,
        RestateCachedRow.T_DECIMAL,
        RestateCachedRow.T_BOOLEAN,
        RestateCachedRow.T_TIMESTAMP,
        RestateCachedRow.T_BINARY,
        RestateCachedRow.T_NULL
      ),
      rows = List(Array(
        "hello", "42", "3.14", "1234.56", "true",
        "2024-01-15T10:30:00Z",
        java.util.Base64.getEncoder.encodeToString(bytes),
        null
      ))
    )
    val decoded = CachedRowDecoder.fromRestateCachedRow(row)
    decoded should have size 1
    val cells = decoded(0)
    cells(0) shouldBe "hello"
    cells(1) shouldBe 42L
    cells(2) shouldBe 3.14
    cells(3) shouldBe new java.math.BigDecimal("1234.56")
    cells(4) shouldBe true
    cells(5) shouldBe java.sql.Timestamp.from(instant)
    cells(6) shouldBe bytes
    cells(7) shouldBe null
  }

  test("fromRestateCachedRow: T_DATE → java.sql.Date at UTC midnight (timezone-independent)") {
    val expectedMillis = java.time.LocalDate.parse("2024-01-15")
      .atStartOfDay(java.time.ZoneOffset.UTC)
      .toInstant()
      .toEpochMilli()
    val row = RestateCachedRow(
      fieldNames = List("d"),
      fieldTypes = List(RestateCachedRow.T_DATE),
      rows       = List(Array("2024-01-15"))
    )
    val decoded = CachedRowDecoder.fromRestateCachedRow(row)
    decoded(0)(0) shouldBe a [java.sql.Date]
    decoded(0)(0).asInstanceOf[java.sql.Date].getTime shouldBe expectedMillis
  }

  // -- Mixed types in one row --

  test("fromRestateCachedRow: 1 row with mixed types (String + Long + Decimal + Boolean)") {
    val row = RestateCachedRow(
      fieldNames = List("name", "count", "price", "active"),
      fieldTypes = List(
        RestateCachedRow.T_STRING,
        RestateCachedRow.T_LONG,
        RestateCachedRow.T_DECIMAL,
        RestateCachedRow.T_BOOLEAN
      ),
      rows = List(Array("widget", "100", "9.99", "true"))
    )
    val decoded = CachedRowDecoder.fromRestateCachedRow(row)
    decoded(0) shouldBe List("widget", 100L, new java.math.BigDecimal("9.99"), true)
  }

  // -- Round-trip: encodeCell + fromRestateCachedRow preserves data --

  test("Round-trip: encodeCell → fromRestateCachedRow preserves the typed row") {
    // Encode a few ResultValues, then run them through fromRestateCachedRow.
    val cell1 = PortableCellCodec.encodeCell(Option(io.sm8.core.engine.ResultValue.StringV("hello")))
    val cell2 = PortableCellCodec.encodeCell(Option(io.sm8.core.engine.ResultValue.IntV(42L)))
    val cell3 = PortableCellCodec.encodeCell(Option(io.sm8.core.engine.ResultValue.BoolV(true)))

    val row = RestateCachedRow(
      fieldNames = List("a", "b", "c"),
      fieldTypes = List(
        RestateCachedRow.T_STRING,
        RestateCachedRow.T_LONG,
        RestateCachedRow.T_BOOLEAN
      ),
      rows = List(Array(cell1, cell2, cell3))
    )

    val decoded = CachedRowDecoder.fromRestateCachedRow(row)
    decoded(0) shouldBe List("hello", 42L, true)
  }

  test("Round-trip: full workflow — encode Cell → build RestateCachedRow → decode back") {
    // This is the realistic flow: a portal writes a cached row from a
    // PortableQueryResult, then reads it back. The Scala version supports
    // this in 2 steps (encodeCell + fromRestateCachedRow).
    val encoded = List(
      PortableCellCodec.encodeCell(Option(io.sm8.core.engine.ResultValue.StringV("Alice"))),
      PortableCellCodec.encodeCell(Option(io.sm8.core.engine.ResultValue.IntV(30L))),
      PortableCellCodec.encodeCell(Option(io.sm8.core.engine.ResultValue.StringV("NYC")))
    )

    val row = RestateCachedRow(
      fieldNames = List("name", "age", "city"),
      fieldTypes = List(
        RestateCachedRow.T_STRING,
        RestateCachedRow.T_LONG,
        RestateCachedRow.T_STRING
      ),
      rows = List(encoded.toArray)
    )

    val decoded = CachedRowDecoder.fromRestateCachedRow(row)
    decoded(0) shouldBe List("Alice", 30L, "NYC")
    // The fieldNames are preserved on the row itself (not in the
    // decoded rows list) — they're the source-of-truth for column identity.
    row.fieldNames shouldBe List("name", "age", "city")
  }

  // -- Wire contract guarantees --

  test("fromRestateCachedRow: never throws (no MatchError leak as long as RestateCachedRow was constructed valid)") {
    // The decoder walks the rows. The RestateCachedRow's smart
    // constructor guarantees fieldNames.size == fieldTypes.size and
    // each row has the right number of cells. So the decoder is total
    // over valid RestateCachedRow inputs.
    val row = RestateCachedRow(
      fieldNames = List("a", "b", "c"),
      fieldTypes = List(
        RestateCachedRow.T_STRING,
        RestateCachedRow.T_LONG,
        RestateCachedRow.T_BOOLEAN
      ),
      rows = List(
        Array("x", "1", "true"),
        Array("y", "2", "false"),
        Array("z", "3", "true"),
        null,
        Array("a", "4", "false")
      )
    )
    noException should be thrownBy CachedRowDecoder.fromRestateCachedRow(row)
  }
  // -- ADR-008-Z v1.1: encoder row-length validation returns typed-Left --

  test("toRestateCachedRowFromPortable: row-length mismatch returns Left(IncompatibleExprShape)") {
    // 4-field schema, but the row has 3 cells. The encoder must return
    // a typed-Left at the journal boundary (was: IllegalArgumentException
    // at the case-class apply site).
    val portable = PortableQueryResult(
      schema = ResultSchema(List(
        Field.nonNull("col1", SealedDataType.Varchar),
        Field.nonNull("col2", SealedDataType.Int),
        Field.nonNull("col3", SealedDataType.Varchar),
        Field.nonNull("col4", SealedDataType.Int)
      )),
      rows = Vector(ResultRow(
        values = List(
          ResultValue.StringV("a"),
          ResultValue.IntV(1L),
          ResultValue.StringV("b")
        ),
        schema = ResultSchema(Nil)
      ))
    )
    val out = CachedRowDecoder.toRestateCachedRowFromPortable(portable)
    out.left.get shouldBe a [EngineError.IncompatibleExprShape]
    out.left.get.message should include ("row 0 has 3 cells, expected 4")
  }

  test("toRestateCachedRowFromPortable: well-formed PortableQueryResult returns Right(RestateCachedRow)") {
    val portable = PortableQueryResult(
      schema = ResultSchema(List(
        Field.nonNull("name", SealedDataType.Varchar),
        Field.nonNull("age", SealedDataType.Int)
      )),
      rows = Vector(ResultRow(
        values = List(
          ResultValue.StringV("Alice"),
          ResultValue.IntV(30L)
        ),
        schema = ResultSchema(Nil)
      ))
    )
    val out = CachedRowDecoder.toRestateCachedRowFromPortable(portable)
    out.isRight shouldBe true
    out.right.get.fieldNames shouldBe List("name", "age")
    out.right.get.fieldTypes shouldBe List(RestateCachedRow.T_STRING, RestateCachedRow.T_LONG)
  }

  test("round-trip: toRestateCachedRowFromPortable then fromRestateCachedRowAsPortable preserves truncated (ADR-009-e)") {
    // ADR-009-e criterion #4: the cache journal must not turn a
    // truncated result into a complete one. encodeC + decode must
    // carry `truncated` verbatim in BOTH directions and BOTH values.
    def roundTrip(truncated: Boolean): PortableQueryResult = {
      val portable = PortableQueryResult(
        schema = ResultSchema(List(Field.nonNull("name", SealedDataType.Varchar))),
        rows   = Vector(ResultRow(
          values = List(ResultValue.StringV("Alice")),
          schema = ResultSchema(Nil)
        )),
        truncated = truncated
      )
      val encoded = CachedRowDecoder.toRestateCachedRowFromPortable(portable)
      encoded shouldBe a [scala.util.Right[_, _]]
      CachedRowDecoder.fromRestateCachedRowAsPortable(encoded.right.get)
    }
    roundTrip(truncated = true).truncated  shouldBe true
    roundTrip(truncated = false).truncated shouldBe false
  }
}
