/*
 * SM8 Platform — QueryResult.fromJournaled spec (cache-rehome Phase 1).
 *
 * Relocated from CachedRowDecoderSpec's toQueryResultFromJournaled
 * section: the conversion is platform-side (produces the platform
 * wire DTO), so its contract tests live beside the type.
 */
package io.sm8.platform.query

import io.sm8.core.cache.{CachedRowDecoder, PortableCellCodec, RestateCachedRow}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class QueryResultJournalSpec extends AnyFunSuite with Matchers {

  // -- toQueryResultFromJournaled (PR-C4c) --

  test("toQueryResultFromJournaled: 2 rows, 2 cols → QueryResult with decoded rows") {
    val row = RestateCachedRow(
      fieldNames = List("carrier", "rows"),
      fieldTypes = List(RestateCachedRow.T_STRING, RestateCachedRow.T_LONG),
      rows       = List(Array("AA", "100"), Array("BB", "200"))
    )
    val result = QueryResult.fromJournaled("flights", row)
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
    val result = QueryResult.fromJournaled(null, row)
    result.model shouldBe "unknown"
  }

  test("toQueryResultFromJournaled: empty rows → QueryResult with empty rows list") {
    val row = RestateCachedRow(
      fieldNames = List("a", "b"),
      fieldTypes = List(RestateCachedRow.T_STRING, RestateCachedRow.T_LONG),
      rows       = Nil
    )
    val result = QueryResult.fromJournaled("m", row)
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
    val result = QueryResult.fromJournaled("m", row)
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
    val result = QueryResult.fromJournaled("m", row)
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
    QueryResult.fromJournaled("m", row).truncated shouldBe false
    // maxRows=10. 10 rows >= 10, so truncated=true.
    QueryResult.fromJournaled("m", row, maxRows = 10).truncated shouldBe true
    // maxRows=11. 10 rows < 11, so truncated=false.
    QueryResult.fromJournaled("m", row, maxRows = 11).truncated shouldBe false
  }

  test("toQueryResultFromJournaled: rowCount = rows.size.toLong") {
    val row = RestateCachedRow(
      fieldNames = List("a"),
      fieldTypes = List(RestateCachedRow.T_STRING),
      rows       = List(Array("x"), Array("y"), Array("z"))
    )
    val result = QueryResult.fromJournaled("m", row)
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
    val result = QueryResult.fromJournaled("users", row)
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
