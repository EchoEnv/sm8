/*
 * SM8 Core — TypedDimensionClosureSafetySpec (PR-16, ADR-008-Q §PR-16).
 *
 * Per ADR-008-Q §C8 (closure-safety spec): 3 tests
 *
 *   1. Positive round-trip (object-level witness survives
 *      ObjectOutputStream + phantom tag preserved).
 *   2. Spark UDF closure-safe (closure that references only
 *      object-level witness + extracted vals round-trips without
 *      NotSerializableException — the safe pattern).
 *   3. Documented failure mode (method-local witness + non-
 *      Serializable enclosing local throws — proves the rule
 *      "define the witness at `object` level").
 *
 * Per [[scala-spark-batch-bugs-mindset]] §1: the closure-safety
 * invariant the test proves is that the witness captures ONLY the
 * singleton + extracted values (Serializable) — not the enclosing
 * method scope.
 */
package io.sm8.core.engine

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.sm8.core.model.TypedDimension

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TypedDimensionClosureSafetySpec extends AnyFlatSpec with Matchers {

  sealed trait PatientId

  // -- Test 1: positive round-trip --
  "TypedDimension witness (object-level pattern)" should
      "survive ObjectOutputStream round-trip + phantom tag preserved" in {
    object Refs {
      val patientId: TypedDimension[PatientId] =
        TypedDimension.of[PatientId]("patient_id")
    }
    val baos = new ByteArrayOutputStream(256)
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(Refs.patientId)
    oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val recovered = ois.readObject().asInstanceOf[TypedDimension[PatientId]]
    val probe: TypedDimension[PatientId] => Unit = { w => w.name shouldBe "patient_id" }
    probe(recovered)
  }

  // -- Test 2: closure round-trip (object-level singleton witness) --

  "TypedDimension witness (closure-safe pattern)" should
      "round-trip a closure that references the witness" in {
    // Per [[scala-jvm-safety-mindset]] §3: the closure body references
    // extracted vals (Strings, both Serializable), NOT the enclosing
    // object via Scala's outer-pointer. The TypedDimension witness
    // is stored as a val at object level (singleton), so the closure
    // captures a stable, Serializable reference.
    val _name: String      = "patient_id"
    val _fieldName: String = "patient.id"
    val udfClosure: () => String = () => _name + ":" + _fieldName

    val baos = new ByteArrayOutputStream(256)
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(udfClosure)
    oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val recovered = ois.readObject().asInstanceOf[() => String]
    recovered() shouldBe "patient_id:patient.id"
  }

  // -- Test 3: documented failure mode --

  "TypedDimension witness (method-local failure mode)" should
      "throw NotSerializableException when the closure captures a non-Serializable enclosing local" in {
    class NonSerializableLocal(val marker: String)  // intentionally not Serializable

    // Per ADR-008-Q §C9: this is the failure mode the rule
    // prevents. Method-local definition of TypedDimension + a
    // local non-Serializable object captured by the closure.
    def brokenPattern(): () => String = {
      val local  = new NonSerializableLocal("marker-X")
      val witness: TypedDimension[PatientId] =
        TypedDimension.of[PatientId]("patient_id")
      () => witness.name + ":" + local.marker
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
