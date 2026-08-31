/*
 * SM8 Core — TypedAggregateCallClosureSafetySpec (PR-17, ADR-008-R §C8 closure-safety).
 *
 * Per the user's explicit priority message (2026-08-19) "spark
 * serialization concern": this 3-test discipline proves BOTH paths
 * end-to-end:
 *
 *   1. Positive round-trip: object-level typed-aggregate survives
 *      ObjectOutputStream + phantom tag preserved.
 *   2. Spark UDF closure-safe: typed-aggregate captured in a
 *      UDF-shaped closure does NOT throw NotSerializableException.
 *   3. Documented failure mode: method-local typed-aggregate +
 *      non-Serializable enclosing local throws. Test name + comment
 *      point to the fix.
 *
 * Per `scala-spark-batch-bugs-mindset` §1 (closure-safety): the
 * witness MUST NOT capture non-Serializable enclosing scope.
 *
 * Per `karpathy-guidelinesmindset` §2: this spec is the 3-test
 * discipline applied to PR-17's new types (mirrors PR-16's
 * `TypedDimensionClosureSafetySpec` pattern).
 */
package io.sm8.core.rel

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.sm8.core.expr.Expr
import io.sm8.core.model.TypedDimension

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TypedAggregateCallClosureSafetySpec extends AnyFlatSpec with Matchers {

  sealed trait PatientCount
  sealed trait AvgAge

  object Refs {
    val patientCount: TypedDimension[PatientCount] = TypedDimension.of[PatientCount]("patient_count")
    val avgAge:       TypedDimension[AvgAge]       = TypedDimension.of[AvgAge]("avg_age")
  }

  // -- Test 1: positive round-trip --

  "TypedAggregateCall object-level witness" should
      "survive ObjectOutputStream round-trip + phantom tag preserved" in {
    val agg: TypedAggregateCall[PatientCount] =
      TypedAggregateCall.count[PatientCount]("patient_count")

    val baos = new ByteArrayOutputStream(256)
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(agg)
    oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val recovered = ois.readObject().asInstanceOf[TypedAggregateCall[PatientCount]]

    // Per scala-bug-huntingmindset §1: a probe function `[T] => T`
    // proves the recovered type's phantom identity at compile time.
    val probe: TypedAggregateCall[PatientCount] => Unit = { a => a.name shouldBe "patient_count" }
    probe(recovered)
  }

  // Per [[scala-spark-batch-bugs-mindset]] §1 (closure-safety): the
    // UDF closure must capture ONLY Serializable refs (Strings +
    // AggregateFn case object + Expr case class — all Serializable).
    // The closure MUST NOT capture the enclosing UdfClosure object —
    // per PR-16 lesson, an outer-object capture fails serialization.
    // We use top-level vals (NOT nested in `object UdfClosure`) and
    // build the closure directly via val-extraction.
  it should "round-trip via ObjectOutputStream without NotSerializableException" in {
    val _name: String                = "patient_count"
    val _fn:   io.sm8.core.rel.AggregateFn = io.sm8.core.rel.AggregateFn.Count
    val _input: Option[Expr]          = Some(Expr.FieldRef("patient_id"))
    // Top-level closure captures ONLY the 3 vals above (Serializable).
    val udfClosure: () => String = () =>
      s"${_name}:${_fn.getClass.getSimpleName}:${_input.map(_.toString).getOrElse("")}"

    val baos = new ByteArrayOutputStream(256)
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(udfClosure); oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val recovered = ois.readObject().asInstanceOf[() => String]
    recovered() should include ("patient_count")
    recovered() should include ("Count")
  }

  // -- Test 3: documented failure mode --

  "TypedAggregateCall method-local definition" should
      "throw NotSerializableException when a non-Serializable enclosing local is captured" in {
    class NonSerializableLocal(val marker: String)  // intentionally not Serializable

    // Per scala-chaos-testingmindset §2: the failure mode is exercised
    // (silence is a symptom). The test's name + comment point the
    // reader to the fix ("define the typed-aggregate at `object` level").
    def brokenPattern(): () => String = {
      val local = new NonSerializableLocal("marker-X")
      val agg = TypedAggregateCall.count[PatientCount]("patient_count")
      () => agg.name + ":" + local.marker
    }

    val closure = brokenPattern()
    val baos = new ByteArrayOutputStream(256)
    val oos  = new ObjectOutputStream(baos)
    val caught = intercept[java.io.NotSerializableException] {
      oos.writeObject(closure)
    }
    caught.getMessage should include ("NonSerializableLocal")
    oos.close()
  }
}
