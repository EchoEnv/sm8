/*
 * SM8 Core -- TypedMeasureBridgeSpec (PR-26, ADR-008-R SSMeasureBridge).
 *
 * Per [[karpathy-guidelines-mindset]] SS4 (goal-driven execution):
 * prove the typed-witness to un-typed Measure bridge produces the
 * correct Measure(name, expr: AggregateCall) shape for each of the
 * 6 sealed ADT cases of AggregateFn.
 *
 * Per [[scala-bug-hunting-mindset]] SS3 (every match must be
 * exhaustive): one test per AggregateFn (Count / Sum / Avg / Min /
 * Max / CountDistinct) + closure-safety round-trip.
 *
 * Per [[scala-spark-batch-bugs-mindset]] SS1 (closure-safety -- the
 * user's explicit concern): every witness is a case-class extends
 * Serializable + the bridge is a pure function. Safe to use in
 * Spark UDF closure contexts.
 */
package io.sm8.core.model

import io.sm8.core.expr.Expr
import io.sm8.core.rel.{AggregateCall, AggregateFn}
import io.sm8.core.model.TypedMeasureBridge._

import java.io._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TypedMeasureBridgeSpec extends AnyFunSuite with Matchers {

  sealed trait PatientCount
  sealed trait AvgAmount

  // === Category 1-5: One test per AggregateFn case (5 cases) ===

  test("bridge: Count -> AggregateCall(no input, alias=name)") {
    val typed: TypedMeasure[PatientCount] = TypedMeasure.count[PatientCount]("pc")
    val m: Measure = typed.toMeasure
    m.name shouldBe "pc"
    m.expr.fn shouldBe AggregateFn.Count
    m.expr.input shouldBe None
    m.expr.alias shouldBe "pc"
    m.expr.distinct shouldBe false
  }

  test("bridge: Sum -> AggregateCall(input=Expr.FieldRef, alias=name)") {
    val typed: TypedMeasure[PatientCount] =
      TypedMeasure.sum[PatientCount]("sum_amt", "amount")
    val m: Measure = typed.toMeasure
    m.name shouldBe "sum_amt"
    m.expr.fn shouldBe AggregateFn.Sum
    m.expr.input shouldBe Some(Expr.FieldRef("amount"))
    m.expr.alias shouldBe "sum_amt"
  }

  test("bridge: Avg -> AggregateCall(input=Expr.FieldRef, alias=name)") {
    val typed: TypedMeasure[AvgAmount] =
      TypedMeasure.avg[AvgAmount]("avg_amt", "amount")
    val m: Measure = typed.toMeasure
    m.name shouldBe "avg_amt"
    m.expr.fn shouldBe AggregateFn.Avg
    m.expr.input shouldBe Some(Expr.FieldRef("amount"))
    m.expr.alias shouldBe "avg_amt"
  }

  test("bridge: Min -> AggregateCall(input=Expr.FieldRef, alias=name)") {
    val typed: TypedMeasure[PatientCount] =
      TypedMeasure.min[PatientCount]("min_amt", "amount")
    val m: Measure = typed.toMeasure
    m.name shouldBe "min_amt"
    m.expr.fn shouldBe AggregateFn.Min
    m.expr.input shouldBe Some(Expr.FieldRef("amount"))
  }

  test("bridge: Max -> AggregateCall(input=Expr.FieldRef, alias=name)") {
    val typed: TypedMeasure[PatientCount] =
      TypedMeasure.max[PatientCount]("max_amt", "amount")
    val m: Measure = typed.toMeasure
    m.name shouldBe "max_amt"
    m.expr.fn shouldBe AggregateFn.Max
    m.expr.input shouldBe Some(Expr.FieldRef("amount"))
  }

  // === Category 6: CountDistinct (the special-case per PR-17 spec) ===

  test("bridge: CountDistinct -> AggregateCall(input + distinct=true)") {
    val typed: TypedMeasure[PatientCount] =
      TypedMeasure.countDistinct[PatientCount]("distinct_ids", "id")
    val m: Measure = typed.toMeasure
    m.name shouldBe "distinct_ids"
    m.expr.fn shouldBe AggregateFn.CountDistinct
    m.expr.input shouldBe Some(Expr.FieldRef("id"))
    m.expr.distinct shouldBe true
  }

  // === Category 7: Closure-safety round-trip (per PR-16 pattern) ===

  test("closure-safety: bridge + TypedMeasure survives ObjectOutputStream round-trip") {
    val typed: TypedMeasure[PatientCount] =
      TypedMeasure.sum[PatientCount]("rt_amt", "amount")
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(typed)
    oos.close()
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val deser = ois.readObject().asInstanceOf[TypedMeasure[PatientCount]]
    deser.name shouldBe "rt_amt"
    deser.aggregateFn shouldBe AggregateFn.Sum
    deser.fieldName shouldBe "amount"
    val m = deser.toMeasure
    m.expr.input shouldBe Some(Expr.FieldRef("amount"))
  }

  test("bridge: pure-function derivation (no captured state)") {
    val typed: TypedMeasure[PatientCount] = TypedMeasure.count[PatientCount]("pure_test")
    typed.isInstanceOf[Serializable] shouldBe true
    val m: Measure = typed.toMeasure
    m.name shouldBe "pure_test"
    m.expr.fn shouldBe AggregateFn.Count
    m.expr.input shouldBe None
  }
}
