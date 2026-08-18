/*
 * SM8 Core — TypedMeasure test (PR-16, ADR-008-Q §PR-16).
 *
 * Per ADR-008-Q §PR-16 scope: 8 tests covering:
 *   - 6 specialized factories (count, sum, avg, min, max, countDistinct)
 *   - default fieldName overload
 *   - Serializable round-trip
 *   - phantom tag preservation
 */
package io.sm8.core.model

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.sm8.core.rel.AggregateFn

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TypedMeasureSpec extends AnyFlatSpec with Matchers {

  // -- Phantom-tag carriers --
  sealed trait PatientCount
  sealed trait AvgAge

  // -- 6 specialized factories (sum, avg, min, max, count, countDistinct) --

  "TypedMeasure.count" should "carry AggregateFn.Count with fieldName = '*'" in {
    val m = TypedMeasure.count[PatientCount]("patient_count")
    m.name shouldBe "patient_count"
    m.aggregateFn shouldBe AggregateFn.Count
    m.fieldName shouldBe "*"
  }

  it should "build a sum measure with explicit + default fieldName" in {
    val explicit = TypedMeasure.sum[PatientCount]("total_amount", "amount")
    explicit.aggregateFn shouldBe AggregateFn.Sum
    explicit.fieldName shouldBe "amount"

    val defaulted = TypedMeasure.sum[PatientCount]("total_amount")
    defaulted.fieldName shouldBe "amount"
    defaulted.aggregateFn shouldBe AggregateFn.Sum
  }

  it should "build an avg measure" in {
    val m = TypedMeasure.avg[AvgAge]("average_age")
    m.aggregateFn shouldBe AggregateFn.Avg
    m.fieldName shouldBe "value"
  }

  it should "build a min measure" in {
    val m = TypedMeasure.min[PatientCount]("min_age")
    m.aggregateFn shouldBe AggregateFn.Min
    m.fieldName shouldBe "value"
  }

  it should "build a max measure" in {
    val m = TypedMeasure.max[PatientCount]("max_age")
    m.aggregateFn shouldBe AggregateFn.Max
    m.fieldName shouldBe "value"
  }

  it should "build a countDistinct measure with default fieldName = 'id'" in {
    val m = TypedMeasure.countDistinct[PatientCount]("unique_patient_count")
    m.aggregateFn shouldBe AggregateFn.CountDistinct
    m.fieldName shouldBe "id"
  }

  // -- Serializable (Spark closure-safety + Restate forward-looking) --

  "TypedMeasure witness" should "survive ObjectOutputStream round-trip" in {
    val m = TypedMeasure.sum[PatientCount]("total_amount", "amount")
    val baos = new ByteArrayOutputStream(256)
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(m)
    oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val recovered = ois.readObject().asInstanceOf[TypedMeasure[PatientCount]]
    recovered.name shouldBe "total_amount"
    recovered.aggregateFn shouldBe AggregateFn.Sum
    recovered.fieldName shouldBe "amount"
  }

  it should "preserve the phantom type tag after ObjectOutputStream round-trip" in {
    val m1 = TypedMeasure.count[PatientCount]("patient_count")
    val m2 = TypedMeasure.avg[AvgAge]("average_age")

    val baos = new ByteArrayOutputStream(256)
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(m1)
    oos.writeObject(m2)
    oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))

    val r1 = ois.readObject().asInstanceOf[TypedMeasure[PatientCount]]
    val r2 = ois.readObject().asInstanceOf[TypedMeasure[AvgAge]]

    // Per scala-bug-huntingmindset §1: a probe function `[T] => T`
    // proves the recovered type's phantom identity at compile time.
    val probe1: TypedMeasure[PatientCount] => Unit = { w => w.name shouldBe "patient_count" }
    val probe2: TypedMeasure[AvgAge]       => Unit = { w => w.name shouldBe "average_age" }
    probe1(r1)
    probe2(r2)
  }
}
