/*
 * SM8 Spark Connector — SparkTypeBridge spec.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #3 (schema drift):
 * verify, don't assume. The bridge is the boundary between
 * Spark's `DataType` ADT and our portable `SealedDataType` ADT.
 * Every Spark type that maps to a `SealedDataType` case is
 * asserted here. Fallbacks (Json) are asserted for nested + unknown
 * types.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1 (closure-safety):
 * the companion object is `extends java.io.Serializable` so the
 * `EngineProvider` that captures it round-trips through
 * `ObjectOutputStream`. The round-trip test at the bottom proves
 * this at runtime.
 *
 * Per [[karpathy-guidelines-mindset]] "smallest correct core":
 * 9 + 4 + 1 = 14 tests in 1 file. The bridge is the testable part
 * — the real runtime (Layer C) lands in a follow-up PR.
 */
package io.sm8.connectors.spark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.sm8.core.schema.SealedDataType

import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  BooleanType,
  DataType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  MapType,
  StringType,
  StructType,
  TimestampType
}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkTypeBridgeSpec extends AnyFunSuite with Matchers {

  // -- Primitives: direct one-to-one mapping --

  test("StringType → SealedDataType.Varchar") {
    SparkTypeBridge.sparkTypeToSealedDataType(StringType) shouldBe SealedDataType.Varchar
  }

  test("IntegerType → SealedDataType.Int") {
    SparkTypeBridge.sparkTypeToSealedDataType(IntegerType) shouldBe SealedDataType.Int
  }

  test("LongType → SealedDataType.Int (portable has no Long; widen semantics)") {
    SparkTypeBridge.sparkTypeToSealedDataType(LongType) shouldBe SealedDataType.Int
  }

  test("FloatType → SealedDataType.Double") {
    SparkTypeBridge.sparkTypeToSealedDataType(FloatType) shouldBe SealedDataType.Double
  }

  test("DoubleType → SealedDataType.Double") {
    SparkTypeBridge.sparkTypeToSealedDataType(DoubleType) shouldBe SealedDataType.Double
  }

  test("BooleanType → SealedDataType.Boolean") {
    SparkTypeBridge.sparkTypeToSealedDataType(BooleanType) shouldBe SealedDataType.Boolean
  }

  test("TimestampType → SealedDataType.Timestamp") {
    SparkTypeBridge.sparkTypeToSealedDataType(TimestampType) shouldBe SealedDataType.Timestamp
  }

  test("DateType → SealedDataType.Date") {
    SparkTypeBridge.sparkTypeToSealedDataType(DateType) shouldBe SealedDataType.Date
  }

  test("DecimalType → SealedDataType.Decimal(38, 18)") {
    SparkTypeBridge.sparkTypeToSealedDataType(DecimalType(38, 18)) shouldBe SealedDataType.Decimal(38, 18)
  }

  // -- Nested types: fall back to Json --

  test("ArrayType → SealedDataType.Json (nested types deferred to Layer C)") {
    SparkTypeBridge.sparkTypeToSealedDataType(
      ArrayType(StringType)
    ) shouldBe SealedDataType.Json
  }

  test("MapType → SealedDataType.Json (nested types deferred to Layer C)") {
    SparkTypeBridge.sparkTypeToSealedDataType(
      MapType(StringType, IntegerType)
    ) shouldBe SealedDataType.Json
  }

  test("StructType → SealedDataType.Json (nested types deferred to Layer C)") {
    SparkTypeBridge.sparkTypeToSealedDataType(
      StructType(Nil)
    ) shouldBe SealedDataType.Json
  }

  test("BinaryType → SealedDataType.Json (binary values encode as base64; Layer C handles)") {
    SparkTypeBridge.sparkTypeToSealedDataType(BinaryType) shouldBe SealedDataType.Json
  }

  // -- Closure-safety baseline --

  /** Per [[scala-spark-batch-bugs-mindset]] mantra #1: round-trip
    * the bridge + the resulting `SealedDataType` through
    * ObjectOutputStream. Proves the captured state is
    * Serializable (the contract PR #36 enforces). */
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

  test("SparkTypeBridge companion + every SealedDataType return shape round-trips through ObjectOutputStream") {
    // The bridge returns `SealedDataType` values; the bridge itself
    // is `extends java.io.Serializable`. Round-tripping both proves
    // the closure-safety contract PR #36 set up.
    val sampleTypes: Seq[DataType] = Seq(
      StringType, IntegerType, LongType, FloatType, DoubleType,
      BooleanType, TimestampType, DateType, DecimalType(38, 18),
      ArrayType(StringType), MapType(StringType, IntegerType),
      StructType(Nil), BinaryType
    )

    for (dt <- sampleTypes) {
      val mapped: SealedDataType = SparkTypeBridge.sparkTypeToSealedDataType(dt)
      val restored: SealedDataType = roundTripViaJavaSerialization(mapped)
      withClue(s"round-trip failed for Spark $dt → mapped $mapped: ") {
        restored shouldBe mapped
      }
    }

    // The companion itself (the bridge object) round-trips too —
    // it's the thing the `EngineProvider` captures.
    val bridgeRestored = roundTripViaJavaSerialization(SparkTypeBridge)
    bridgeRestored shouldBe SparkTypeBridge
  }

  // ===== PR-O1a (ADR-008-O): sealedDataTypeToSparkType round-trip =====

  test("SparkTypeBridge.sealedDataTypeToSparkType: Varchar -> StringType") {
    SparkTypeBridge.sealedDataTypeToSparkType(io.sm8.core.schema.SealedDataType.Varchar) shouldBe
      org.apache.spark.sql.types.StringType
  }

  test("SparkTypeBridge.sealedDataTypeToSparkType: Int -> IntegerType") {
    SparkTypeBridge.sealedDataTypeToSparkType(io.sm8.core.schema.SealedDataType.Int) shouldBe
      org.apache.spark.sql.types.IntegerType
  }

  test("SparkTypeBridge.sealedDataTypeToSparkType: Decimal(p,s) -> DecimalType(p,s)") {
    val dec = io.sm8.core.schema.SealedDataType.Decimal(precision = 10, scale = 2)
    SparkTypeBridge.sealedDataTypeToSparkType(dec) shouldBe
      org.apache.spark.sql.types.DecimalType(10, 2)
  }

  test("SparkTypeBridge.sealedDataTypeToSparkType: roundtrip sparkTypeToSealedDataType-then-back") {
    // The inverse is deterministic for primitives + Decimal; the
    // 'one-way' direction (sparkTypeToSealedDataType collapses
    // Array/Map/Row to Json) is documented in the inverse method.
    val samples = List(
      org.apache.spark.sql.types.StringType,
      org.apache.spark.sql.types.IntegerType,

      org.apache.spark.sql.types.DoubleType,
      org.apache.spark.sql.types.BooleanType,
      org.apache.spark.sql.types.TimestampType,
      org.apache.spark.sql.types.DateType,
      org.apache.spark.sql.types.DecimalType(38, 18),

    )
    samples.foreach { t =>
      val sdt = SparkTypeBridge.sparkTypeToSealedDataType(t)
      val back = SparkTypeBridge.sealedDataTypeToSparkType(sdt)
      withClue(s"roundtrip for $t: ") {
        back shouldBe t
      }
    }
  }

}
