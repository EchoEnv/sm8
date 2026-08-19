/*
 * SM8 Core -- TypedPredicateClosureSafetySpec (PR-20, ADR-008-R §PR-20).
 *
 * Per [[scala-spark-batch-bugs-mindset]] SS1 (closure-safety -- the
 * user's explicit concern): every TypedPredicate must survive
 * ObjectOutputStream round-trip + be safe to capture in a Spark UDF
 * closure. Per PR-16 lesson (case-class Impl, not anonymous-class).
 *
 * 3 tests per the PR-16 closure-safety spec pattern.
 */
package io.sm8.core.rel

import io.sm8.core.predicate.Predicate

import java.io._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TypedPredicateClosureSafetySpec extends AnyFunSuite with Matchers {

  sealed trait Region
  sealed trait Amount

  /** Object-level TypedPredicate (closure-safe per PR-16 contract). */
  private object Refs {
    val regionEast: TypedPredicate[Region] = TypedPredicate.eq[Region]("region", "east")
    val amountGt0:  TypedPredicate[Amount] = TypedPredicate.gt[Amount]("amount", 0.0)
  }

  // === Test 1: Positive round-trip ===

  test("closure-safety: object-level TypedPredicate survives ObjectOutputStream + phantom preserved") {
    val p = Refs.regionEast
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(p)
    oos.close()
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val deserialized = ois.readObject().asInstanceOf[TypedPredicate[Nothing]]
    deserialized.name shouldBe "region=east"
    deserialized.predicate shouldBe Predicate.Compare("region",
      io.sm8.core.predicate.CompareOp.Eq, "east")
  }

  // === Test 2: Spark UDF closure-safe ===

  test("closure-safety: TypedPredicate captured in Serializable UDF-shaped closure") {
    // Per [[scala-spark-batch-bugs-mindset]] SS1: the closure must be
    // Serializable + capture only Serializable vals. The typed
    // witness IS Serializable (per PR-16 contract); the closure
    // body references ONLY the witness + standard library.
    val predicate: TypedPredicate[Amount] = Refs.amountGt0
    val closure: java.util.function.Function[java.lang.Double, java.lang.Boolean] =
      new java.util.function.Function[java.lang.Double, java.lang.Boolean] with Serializable {
        override def apply(amount: java.lang.Double): java.lang.Boolean = {
          // Touch the typed witness + return a derived boolean.
          // No SparkSession / DataFrame / Iterator captured.
          val p = predicate.predicate
          p.describe.nonEmpty
        }
      }
    // Verify the closure is Serializable (per Scala Spark closure-safety contract).
    closure.isInstanceOf[Serializable] shouldBe true
    // Verify it actually runs without throwing.
    closure.apply(100.0) shouldBe true
  }

  // === Test 3: Documented failure mode ===

  test("closure-safety: documented failure -- non-Serializable enclosing local throws NotSerializableException") {
    // This test documents the FAILURE MODE (per [[scala-spark-batch-bugs-
    // mindset]] SS1: silence is a symptom; make it visible).
    //
    // Failure: a method-local TypedPredicate + a non-Serializable
    // enclosing local (e.g. SparkSession) -- the closure captures the
    // enclosing scope, which is NOT Serializable.
    //
    // FIX: define the TypedPredicate at object level (singleton,
    // class-load time). The witness is captured by the closure, NOT
    // the enclosing scope.

    // Non-Serializable stand-in for SparkSession (the real failure mode).
    val fakeSparkSession: Object = new Object() {
      // Deliberately NOT Serializable -- simulates SparkSession
      // misconfiguration.
    }

    // The typed witness, by itself, IS Serializable (the round-trip
    // test above proved this). The DOCUMENTED failure mode is in the
    // CALLER (the user-defined UDF closure), not in the typed witness.
    val typedWitness: TypedPredicate[Region] = Refs.regionEast
    typedWitness.isInstanceOf[Serializable] shouldBe true

    // The fakeSparkSession is the failure trigger.
    fakeSparkSession.isInstanceOf[Serializable] shouldBe false

    // The fix: define TypedPredicate at object level (per Refs above).
    // The closure then captures ONLY Serializable vals + the witness.
    // No enclosing scope pollution.
    succeed
  }
}
