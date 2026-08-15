package io.sm8.platform.query.cache

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.sm8.platform.query.QueryResult

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

  // -- toQueryResultFromJournaled (PR-C4c) --

  test("toQueryResultFromJournaled: 2 rows, 2 cols → QueryResult with decoded rows") {
    val row = RestateCachedRow(
      fieldNames = List("carrier", "rows"),
      fieldTypes = List(RestateCachedRow.T_STRING, RestateCachedRow.T_LONG),
      rows       = List(Array("AA", "100"), Array("BB", "200"))
    )
    val result = CachedRowDecoder.toQueryResultFromJournaled("flights", row)
    result.model shouldBe "flights"
    result.measures shouldBe List("carrier", "rows")
    result.rows shouldBe List(List("AA", 100L), List("BB", 200L))
    result.rowCount shouldBe 2L
    result.truncated shouldBe false
  }

  test("toQueryResultFromJournaled: null modelName → \"unknown\"") {
    val row = RestateCachedRow(
      fieldNames = List("a"),
      fieldTypes = List(RestateCachedRow.T_STRING),
      rows       = List(Array("x"))
    )
    val result = CachedRowDecoder.toQueryResultFromJournaled(null, row)
    result.model shouldBe "unknown"
  }

  test("toQueryResultFromJournaled: empty rows → QueryResult with empty rows list") {
    val row = RestateCachedRow(
      fieldNames = List("a", "b"),
      fieldTypes = List(RestateCachedRow.T_STRING, RestateCachedRow.T_LONG),
      rows       = Nil
    )
    val result = CachedRowDecoder.toQueryResultFromJournaled("m", row)
    result.rows shouldBe Nil
    result.rowCount shouldBe 0L
    result.truncated shouldBe false
  }

  test("toQueryResultFromJournaled: null row entries → empty list entries (matches fromRestateCachedRow; legacy Java NPE'd)") {
    // The legacy Java code NPE'd on null row entries (line 634:
    // `cells.length` without null check). The Scala version handles
    // null gracefully — consistent with `fromRestateCachedRow`.
    val row = RestateCachedRow(
      fieldNames = List("a", "b"),
      fieldTypes = List(RestateCachedRow.T_STRING, RestateCachedRow.T_LONG),
      rows       = List(null, Array("x", "1"))
    )
    val result = CachedRowDecoder.toQueryResultFromJournaled("m", row)
    result.rows should have size 2
    result.rows(0) shouldBe List.empty[Object]
    result.rows(1) shouldBe List("x", 1L)
  }

  test("toQueryResultFromJournaled: T_DATE → java.sql.Date at UTC midnight") {
    val expectedMillis = java.time.LocalDate.parse("2024-01-15")
      .atStartOfDay(java.time.ZoneOffset.UTC)
      .toInstant()
      .toEpochMilli()
    val row = RestateCachedRow(
      fieldNames = List("d"),
      fieldTypes = List(RestateCachedRow.T_DATE),
      rows       = List(Array("2024-01-15"))
    )
    val result = CachedRowDecoder.toQueryResultFromJournaled("m", row)
    result.rows(0)(0) shouldBe a [java.sql.Date]
    result.rows(0)(0).asInstanceOf[java.sql.Date].getTime shouldBe expectedMillis
  }

  test("toQueryResultFromJournaled: truncation flag = rows.size >= maxRows") {
    val row = RestateCachedRow(
      fieldNames = List("a"),
      fieldTypes = List(RestateCachedRow.T_STRING),
      rows       = (1 to 10).map(i => Array(s"row$i")).toList
    )
    // Default maxRows=100_000. 10 rows < 100_000, so truncated=false.
    CachedRowDecoder.toQueryResultFromJournaled("m", row).truncated shouldBe false
    // maxRows=10. 10 rows >= 10, so truncated=true.
    CachedRowDecoder.toQueryResultFromJournaled("m", row, maxRows = 10).truncated shouldBe true
    // maxRows=11. 10 rows < 11, so truncated=false.
    CachedRowDecoder.toQueryResultFromJournaled("m", row, maxRows = 11).truncated shouldBe false
  }

  test("toQueryResultFromJournaled: rowCount = rows.size.toLong") {
    val row = RestateCachedRow(
      fieldNames = List("a"),
      fieldTypes = List(RestateCachedRow.T_STRING),
      rows       = List(Array("x"), Array("y"), Array("z"))
    )
    val result = CachedRowDecoder.toQueryResultFromJournaled("m", row)
    result.rowCount shouldBe 3L
    result.rows.size shouldBe 3
  }

  test("toQueryResultFromJournaled: round-trip with encodeCell + fromRestateCachedRow") {
    // Encode cells, build a RestateCachedRow, decode via toQueryResultFromJournaled.
    val encoded = List(
      PortableCellCodec.encodeCell(Option(io.sm8.core.engine.ResultValue.StringV("Alice"))),
      PortableCellCodec.encodeCell(Option(io.sm8.core.engine.ResultValue.IntV(30L)))
    )
    val row = RestateCachedRow(
      fieldNames = List("name", "age"),
      fieldTypes = List(RestateCachedRow.T_STRING, RestateCachedRow.T_LONG),
      rows       = List(encoded.toArray)
    )
    val result = CachedRowDecoder.toQueryResultFromJournaled("users", row)
    result.model shouldBe "users"
    result.measures shouldBe List("name", "age")
    result.rows shouldBe List(List("Alice", 30L))
    result.rowCount shouldBe 1L
  }

  test("QueryResult: wire contract — same field names + types as the legacy Java record") {
    // The legacy Java record:
    //   public record QueryResult(
    //       String model, List<String> measures, List<List<Object>> rows,
    //       boolean truncated, long rowCount)
    val q = QueryResult(
      "m",
      List("a", "b"),
      List(List[Object]("x", java.lang.Long.valueOf(1L))),
      false,
      1L
    )
    q.model shouldBe "m"
    q.measures shouldBe List("a", "b")
    q.rows shouldBe List(List[Object]("x", java.lang.Long.valueOf(1L)))
    q.truncated shouldBe false
    q.rowCount shouldBe 1L
  }
}