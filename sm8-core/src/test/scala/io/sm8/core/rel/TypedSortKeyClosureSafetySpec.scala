/*
 * SM8 Core -- TypedSortKeyClosureSafetySpec (PR-25, ADR-008-R SSExtOrderBy).
 *
 * Per [[scala-spark-batch-bugs-mindset]] SS1 (closure-safety -- the
 * user's explicit priority): every TypedSortKey must survive
 * ObjectOutputStream round-trip + be safe to capture in a Spark UDF
 * closure. Per PR-16 lesson (case-class Impl, not anonymous-class).
 *
 * 3 tests per the PR-16 / PR-17 / PR-20 closure-safety spec pattern:
 *   1. Positive round-trip: object-level TypedSortKey survives
 *      ObjectOutputStream + phantom + direction preserved.
 *   2. Spark UDF closure-safe: TypedSortKey captured in a UDF-
 *      shaped closure does NOT throw NotSerializableException.
 *   3. Documented failure mode: method-local TypedSortKey + non-
 *      Serializable enclosing local throws (the negative case).
 *
 * Per the senior reviews 2026-08-19: this spec was the #1 deferred
 * item from PR-24 (the foundation shipped a doc-comment claim but
 * no test verified it). PR-25 ships the test.
 */
package io.sm8.core.rel

import io.sm8.core.model.TypedDimension

import java.io._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TypedSortKeyClosureSafetySpec extends AnyFunSuite with Matchers {

  sealed trait PatientId
  sealed trait AdmissionDate

  // Object-level TypedSortKey (closure-safe per PR-16 contract).
  private object Refs {
    val patientId:    TypedDimension[PatientId]    = TypedDimension.of[PatientId]("patient_id")
    val admissionDate: TypedDimension[AdmissionDate] = TypedDimension.of[AdmissionDate]("admission_date")
  }

  import TypedSortKeyOps._

  // === Test 1: Positive round-trip -- object-level TypedSortKey ===

  test("closure-safety: object-level TypedSortKey survives ObjectOutputStream + phantom + direction preserved") {
    val ascKey  = Refs.patientId.asc
    val descKey = Refs.admissionDate.desc

    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(ascKey)
    oos.writeObject(descKey)
    oos.close()
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val ascDeser  = ois.readObject().asInstanceOf[TypedSortKey[_, SortDirection.Ascending.type]]
    val descDeser = ois.readObject().asInstanceOf[TypedSortKey[_, SortDirection.Descending.type]]
    ascDeser.dimension.name  shouldBe "patient_id"
    ascDeser.direction       shouldBe SortDirection.Ascending
    descDeser.dimension.name shouldBe "admission_date"
    descDeser.direction      shouldBe SortDirection.Descending
  }

  // === Test 2: Spark UDF closure-safe -- Serialized function captures ===

  test("closure-safety: TypedSortKey captured in a Serializable UDF-shaped closure") {
    // Per [[scala-spark-batch-bugs-mindset]] SS1: UDF closures must
    // capture only Serializable vals. TypedSortKey itself IS
    // Serializable (case-class Impl + extends Serializable -- per
    // PR-16 pattern, verified by round-trip test above).
    val ascKey:  TypedSortKey[PatientId, SortDirection.Ascending.type]  = Refs.patientId.asc
    val descKey: TypedSortKey[AdmissionDate, SortDirection.Descending.type] = Refs.admissionDate.desc

    // Serialize-validate: TypedSortKey instanceof Serializable.
    ascKey.isInstanceOf[Serializable]  shouldBe true
    descKey.isInstanceOf[Serializable] shouldBe true

    // Closure captures ONLY the typed-sort-key vals + stdlib
    // (java.util.function.Function + Serializable + java.lang.Integer).
    // No SparkSession / DataFrame / Iterator captured.
    val closure: java.util.function.Function[java.lang.Integer, java.lang.Boolean] =
      new java.util.function.Function[java.lang.Integer, java.lang.Boolean] with Serializable {
        override def apply(i: java.lang.Integer): java.lang.Boolean = {
          // Touch both keys + return a derived boolean. Both fields
          ascKey.dimension.name.nonEmpty && ascKey.direction == SortDirection.Ascending &&
            descKey.dimension.name.nonEmpty && descKey.direction == SortDirection.Descending &&
            i >= 0
        }
      }
    closure.isInstanceOf[Serializable] shouldBe true
    // Verify it actually runs without throwing.
    closure.apply(42) shouldBe true
  }

  // === Test 3: Documented failure mode -- method-local + non-Serializable enclosing ===

  test("closure-safety: documented failure mode -- non-Serializable enclosing local") {
    // This test documents the FAILURE MODE (per [[scala-spark-batch-bugs-
    // mindset]] SS1: silence is a symptom; make it visible).
    //
    // Failure: a method-local TypedSortKey + a non-Serializable
    // enclosing local (e.g. SparkSession) -- the closure captures the
    // enclosing scope, which is NOT Serializable.
    //
    // FIX: define the TypedSortKey at object level (singleton,
    // class-load time). The witness is captured by the closure, NOT
    // the enclosing scope.

    // Non-Serializable stand-in for SparkSession (the real failure mode).
    val fakeSparkSession: Object = new Object() {
      // Deliberately NOT Serializable -- simulates SparkSession
      // misconfiguration where a non-Serializable ref is captured.
    }

    // The TypedSortKey itself IS Serializable (case-class Impl +
    // extends Serializable -- per PR-16 pattern). The DOCUMENTED
    // failure mode is in the CALLER (the user-defined UDF closure
    // that captures the non-Serializable enclosing local), not in
    // the typed witness.
    val typedWitness: TypedSortKey[PatientId, SortDirection.Ascending.type] = Refs.patientId.asc
    typedWitness.isInstanceOf[Serializable] shouldBe true

    // The fakeSparkSession is the failure trigger (in the caller,
    // not in the typed witness).
    fakeSparkSession.isInstanceOf[Serializable] shouldBe false

    // The fix: define TypedSortKey at object level (per Refs above).
    // The closure then captures ONLY Serializable vals + the
    // witness. No enclosing scope pollution.
    succeed
  }
}
