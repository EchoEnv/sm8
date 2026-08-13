package io.sm8.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.sm8.core.schema.{Field, SealedDataType}

/** Tests for the portable result ADTs (`ResultRow`,
  * `PortableQueryResult`, `ResultEncoder`, `ResultError`).
  *
  * Per the design \u00a74.5.4: "ResultSchema and ResultRow are
  * case classes because conformance tests compare them with ==". */
class PortableQueryResultSpec extends AnyFunSuite with Matchers {

  // -- ResultRow --

  test("ResultRow is well-formed iff values.size == schema.fields.size") {
    val schema = ResultSchema(List(
      Field("a", SealedDataType.Int, nullable = true),
      Field("b", SealedDataType.Varchar, nullable = true),
    ))
    val goodRow = ResultRow(List(ResultValue.IntV(1L), ResultValue.StringV("x")), schema)
    goodRow.isWellFormed shouldBe true

    val badRow = ResultRow(List(ResultValue.IntV(1L)), schema)
    badRow.isWellFormed shouldBe false
  }

  test("ResultRow.get(name) returns the value at the matching field index") {
    val schema = ResultSchema(List(
      Field("a", SealedDataType.Int, nullable = true),
      Field("b", SealedDataType.Varchar, nullable = true),
    ))
    val row = ResultRow(List(ResultValue.IntV(42L), ResultValue.StringV("hi")), schema)
    row.get("a") shouldBe Some(ResultValue.IntV(42L))
    row.get("b") shouldBe Some(ResultValue.StringV("hi"))
    row.get("c") shouldBe None
  }

  test("ResultRow equality includes the schema reference (per design conformance)") {
    val schema1 = ResultSchema(List(Field("a", SealedDataType.Int, nullable = true)))
    val schema2 = ResultSchema(List(Field("a", SealedDataType.Varchar, nullable = true)))
    val row1 = ResultRow(List(ResultValue.IntV(1L)), schema1)
    val row2 = ResultRow(List(ResultValue.IntV(1L)), schema1)
    val row3 = ResultRow(List(ResultValue.IntV(1L)), schema2)
    (row1 == row2) shouldBe true   // same schema + same values
    (row1 == row3) shouldBe false  // different schema
  }

  // -- PortableQueryResult --

  test("PortableQueryResult.empty is the canonical zero-row result") {
    PortableQueryResult.empty.rowCount shouldBe 0
    PortableQueryResult.empty.isEmpty shouldBe true
    PortableQueryResult.empty.isWellFormed shouldBe true
  }

  test("PortableQueryResult.isWellFormed iff every row is well-formed") {
    val schema = ResultSchema(List(Field("a", SealedDataType.Int, nullable = true)))
    val goodRow = ResultRow(List(ResultValue.IntV(1L)), schema)
    val badRow  = ResultRow(List(ResultValue.IntV(1L), ResultValue.IntV(2L)), schema)
    val allGood = PortableQueryResult(schema, Vector(goodRow, goodRow))
    allGood.isWellFormed shouldBe true
    val mixed   = PortableQueryResult(schema, Vector(goodRow, badRow))
    mixed.isWellFormed shouldBe false
  }

  test("PortableQueryResult round-trips through Java serialization (per design \u00a71.3)") {
    val schema = ResultSchema(List(Field("a", SealedDataType.Int, nullable = true)))
    val rows = Vector(
      ResultRow(List(ResultValue.IntV(1L)), schema),
      ResultRow(List(ResultValue.IntV(2L)), schema),
    )
    val pqr = PortableQueryResult(schema, rows, Map("source" -> "test"))
    val out = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(out)
    oos.writeObject(pqr)
    oos.close()
    val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(out.toByteArray))
    val back = ois.readObject().asInstanceOf[PortableQueryResult]
    ois.close()
    back shouldBe pqr
  }

  // -- ResultError --
  // Removed from this spec — `ResultError` is nested inside
  // `ResultEncoder.scala` (a different PR's scope). Tests for it
  // land when `ResultEncoder` is moved (PR-C0c-extension).
}