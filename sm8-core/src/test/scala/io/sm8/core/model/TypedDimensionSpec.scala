/*
 * SM8 Core — TypedDimension test (PR-16, ADR-008-Q §PR-16).
 *
 * Per ADR-008-Q §PR-16 scope:
 *   - 6 tests: 1-arg overload, 2-arg form, Serializable round-trip,
 *     phantom tag preservation, object-level-only rule documented,
 *     + asFieldRef projects via Expr.
 */
package io.sm8.core.model

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.sm8.core.expr.Expr

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TypedDimensionSpec extends AnyFlatSpec with Matchers {

  // -- Phantom-tag carriers (named types for the witness identity) --

  sealed trait PatientId
  sealed trait Gender

  "TypedDimension.of" should "construct a witness with the given name (1-arg form)" in {
    val w = TypedDimension.of[PatientId]("patient_id")
    w.name shouldBe "patient_id"
    w.fieldName shouldBe "patient_id"  // default == name
  }

  it should "construct a witness with separate name + fieldName (2-arg form)" in {
    val w = TypedDimension.of[PatientId](name = "patient_id", fieldName = "patient.id")
    w.name shouldBe "patient_id"
    w.fieldName shouldBe "patient.id"
  }

  it should "project the witness as an Expr.FieldRef" in {
    val w = TypedDimension.of[PatientId](name = "patient_id", fieldName = "patient.id")
    w.asFieldRef shouldBe Expr.FieldRef("patient.id")
  }

  "TypedDimension witness" should "survive ObjectOutputStream round-trip" in {
    val w = TypedDimension.of[PatientId]("patient_id")
    val baos = new ByteArrayOutputStream(256)
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(w)
    oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val recovered = ois.readObject().asInstanceOf[TypedDimension[PatientId]]
    recovered.name shouldBe "patient_id"
    recovered.fieldName shouldBe "patient_id"
  }

  it should "preserve the phantom type tag after ObjectOutputStream round-trip" in {
    val w1 = TypedDimension.of[PatientId]("patient_id")
    val w2 = TypedDimension.of[Gender]("gender")

    val baos = new ByteArrayOutputStream(256)
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(w1)
    oos.writeObject(w2)
    oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))

    val r1 = ois.readObject().asInstanceOf[TypedDimension[PatientId]]
    val r2 = ois.readObject().asInstanceOf[TypedDimension[Gender]]

    // Per scala-bug-huntingmindset §1: a function `[T] => T` proves
    // the recovered type's phantom identity. If `r1` were not
    // `TypedDimension[PatientId]`, the call below would not compile
    // (the parameter type wouldn't match).
    val probe1: TypedDimension[PatientId] => Unit = { w => w.name shouldBe "patient_id" }
    val probe2: TypedDimension[Gender]   => Unit = { w => w.name shouldBe "gender" }
    probe1(r1)
    probe2(r2)
  }

  "TypedDimension witness" should "round-trip via the object-level pattern (closure-safe)" in {
    // Per [[scala-spark-batch-bugs-mindset]] §1 (closure-safety): the witness
    // MUST be defined at `object` level. The witness MUST survive
    // ObjectOutputStream round-trip (proves the Spark closure-safe pattern).
    object Refs {
      val patientId: TypedDimension[PatientId] = TypedDimension.of[PatientId]("patient_id")
    }
    val baos = new ByteArrayOutputStream(256)
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(Refs.patientId)
    oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val r = ois.readObject().asInstanceOf[TypedDimension[PatientId]]
    val probe: TypedDimension[PatientId] => Unit = { w => w.name shouldBe "patient_id" }
    probe(r)
    probe(Refs.patientId)  // prove the probe accepts the singleton object-level witness
  }
}
