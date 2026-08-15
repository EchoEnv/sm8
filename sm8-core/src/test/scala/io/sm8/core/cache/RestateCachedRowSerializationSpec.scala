package io.sm8.core.cache

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Wire-contract preservation tests for `RestateCachedRow`.
 *
 * Per [[scala-spark-batch-bugs-mindset]] (write correctness, schema
 * stability): every Spark task that captures a `RestateCachedRow`
 * in a closure must deserialize it back to the same data on the
 * executor. Proves Java serialization round-trip — the default
 * for both Spark's closure cleaner AND Restate SDK journals.
 *
 * Per [[scala-jvm-safety-mindset]]: case-class equality on
 * `List[Array[String]]` uses array reference identity, so the
 * "round-trip equal" assertion uses content equality on the
 * array fields. The wire contract is content-equality (each
 * cell is a String), not reference-equality.
 *
 * Per [[scala-impact-analysis-mindset]]: if these tests ever
 * break, the wire contract has drifted and downstream consumers
 * (Spark closures, Restate journals) will silently corrupt
 * cached results.
 */
class RestateCachedRowSerializationSpec extends AnyFunSuite with Matchers {

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

  /** Convert a `Array[String]` to a `Seq` for content-equality assertions.
    * Required because `Array[String].equals` uses reference identity,
    * not content equality. The wire contract is content-equality. */
  private def rowContent(arr: Array[String]): Seq[String] =
    if (arr == null) null else arr.toSeq

  test("RestateCachedRow: 2 cols, 2 rows round-trips via Java serialization") {
    val original = RestateCachedRow(
      fieldNames = List("carrier", "rows"),
      fieldTypes = List(RestateCachedRow.T_STRING, RestateCachedRow.T_LONG),
      rows       = List(Array("AA", "100"), Array("BB", "200"))
    )
    val restored = roundTripViaJavaSerialization(original)

    restored.fieldNames shouldBe List("carrier", "rows")
    restored.fieldTypes shouldBe List(RestateCachedRow.T_STRING, RestateCachedRow.T_LONG)
    restored.rows should have size 2
    rowContent(restored.rows(0)) shouldBe Seq("AA", "100")
    rowContent(restored.rows(1)) shouldBe Seq("BB", "200")
  }

  test("RestateCachedRow: empty lists round-trip") {
    val original = RestateCachedRow(Nil, Nil, Nil)
    val restored = roundTripViaJavaSerialization(original)
    restored.fieldNames shouldBe Nil
    restored.fieldTypes shouldBe Nil
    restored.rows shouldBe Nil
  }

  test("RestateCachedRow: null row entry survives Java serialization (wire contract)") {
    // Java record allows null row entries (line 96: `if (row != null && ...)`)
    val original = RestateCachedRow(
      fieldNames = List("a", "b"),
      fieldTypes = List(RestateCachedRow.T_STRING, RestateCachedRow.T_STRING),
      rows       = List(null, Array("x", "y"))
    )
    val restored = roundTripViaJavaSerialization(original)
    restored.rows should have size 2
    restored.rows(0) shouldBe null
    rowContent(restored.rows(1)) shouldBe Seq("x", "y")
  }

  test("RestateCachedRow: all 9 T_* tag values survive round-trip") {
    val allTags = List(
      RestateCachedRow.T_NULL,
      RestateCachedRow.T_STRING,
      RestateCachedRow.T_LONG,
      RestateCachedRow.T_DOUBLE,
      RestateCachedRow.T_DECIMAL,
      RestateCachedRow.T_BOOLEAN,
      RestateCachedRow.T_TIMESTAMP,
      RestateCachedRow.T_DATE,
      RestateCachedRow.T_BINARY
    )
    val original = RestateCachedRow(
      fieldNames = List.fill(9)("col"),
      fieldTypes = allTags,
      rows       = List(Array.fill(9)("v"))
    )
    val restored = roundTripViaJavaSerialization(original)
    restored.fieldTypes shouldBe allTags
  }

  test("RestateCachedRow: Spark closure pattern — capture + execute preserves content") {
    // Simulates a Spark closure that captures a RestateCachedRow.
    // Spark serializes via ObjectOutputStream on the driver, then
    // deserializes on the executor via ObjectInputStream. This is
    // exactly what `roundTripViaJavaSerialization` does.
    val driverSide = RestateCachedRow(
      fieldNames = List("x"),
      fieldTypes = List(RestateCachedRow.T_DECIMAL),
      rows       = List(Array("1234.5678"))
    )
    // "Ship to executor"
    val executorSide = roundTripViaJavaSerialization(driverSide)
    // Executor reads the data:
    rowContent(executorSide.rows(0)) shouldBe Seq("1234.5678")
    executorSide.fieldTypes(0) shouldBe RestateCachedRow.T_DECIMAL
  }
}