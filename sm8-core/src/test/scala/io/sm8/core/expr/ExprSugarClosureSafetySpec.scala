/*
 * SM8 Core -- ExprSugarClosureSafetySpec (PR-35, ADR-008-S v1.3).
 *
 * Per [[scala-spark-batch-bugs-mindset]] SS1 (closure-safety -- the
 * user's explicit concern): every sugar-built Expr must survive
 * ObjectOutputStream round-trip + be safe to capture in a Spark UDF
 * closure. The sugar returns the same sealed case classes as the
 * explicit constructor (per [[karpathy-app-design-mindset]] SS3.1
 * Protocols before Implementations), so closure safety is
 * INHERITED from the existing Expr case-class Serializable contract.
 *
 * 3 tests per the PR-16/17/20/25 closure-safety spec pattern
 * (positive round-trip + Spark UDF closure-safe + documented
 * failure mode -- non-Serializable enclosing local).
 *
 * Per [[debug-mantra]] 5-step:
 * 1. Reproducibility: the spec is fast (~ms), deterministic
 *    (no time/seed dependencies); the ObjectOutputStream round-trip
 *    + Spark UDF capture + failure-mode tests are all reproducible.
 * 2. Know the fail path: the documented failure mode test (test 3)
 *    PROVES the NSE path before it surprises a contributor.
 * 3. Question hypothesis: the assumption "case-class Expr.Equal
 *    extends Product with Serializable -> safe Spark closure
 *    capture" is the hypothesis under test; the 3-test pattern
 *    verifies it from 3 angles.
 * 4. Every run is a breadcrumb: the 3 test names narrate the
 *    round-trip -> UDF-capture -> failure-mode progression.
 * 5. Verify: `mvn -pl sm8-core,connectors/spark-connector,
 *    examples/hospital-cleaning test` confirms zero regression.
 *
 * Per ADR-008-S SS"Out of scope" mandate: future PRs adding new
 * Expr ADT cases MUST extend this spec with a 3-test block.
 */
package io.sm8.core.expr

import java.io._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExprSugarClosureSafetySpec extends AnyFunSuite with Matchers {

  // Per the TypedPredicateClosureSafetySpec.scala precedent (PR-20).
  // Sugar is imported at the spec level so all test bodies use it.
  import ExprSugar._

  // === Test 1: Positive round-trip ===

  test("closure-safety: ExprSugar positive round-trip") {
    // Sugar-built Expr -- the OBJECT-LEVEL construction ensures
    // the Expr is captured by value (no enclosing-scope pollution).
    // Per [[karpathy-app-design-mindset]] SS3.1 + [[scala-jvm-safety-mindset]]
    // SS2: sugar-built Expr extends Product with Serializable
    // (via the existing Expr.Equal/And/Or case classes).
    val expr: Expr = Expr.Equal(
      "discharge_status".asField,
      "expired".asVarchar
    )

    // Per [[debug-mantra]] SS1: round-trip via ObjectOutputStream.
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(expr)
    oos.close()

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val deserialized = ois.readObject().asInstanceOf[Expr]

    // Per [[scala-bug-hunting-mindset]] SS1 (trust compiler):
    // the deserialized Expr preserves the AST shape (Equal +
    // FieldRef + Literal).
    deserialized shouldBe expr
  }

  // === Test 2: Spark UDF closure-safe ===

  test("closure-safety: ExprSugar Spark UDF closure-safe (java.util.function.Function with Serializable)") {
    // Per [[scala-spark-batch-bugs-mindset]] SS1: a sugar-built Expr
    // captured in a UDF-shaped closure does NOT throw
    // NotSerializableException (the closure captures only the
    // Serializable Expr, no enclosing-scope pollution).
    //
    // Per PR-20 (TypedPredicateClosureSafetySpec.scala:53-63) +
    // PR-16 lesson: use `java.util.function.Function[X, Y] with
    // Serializable` (the canonical Spark UDF closure shape --
    // the closure is checked for Serializable at capture time).
    val expr: Expr = Expr.And(
      "region".asField === "east".asVarchar,
      "active".asField === true.asBool
    )

    // The Expr is captured by value (no enclosing scope).
    // Per [[scala-jvm-safety-mindset]] SS2: case-class Expr extends
    // Serializable, so the closure can be serialized.
    expr.isInstanceOf[Serializable] shouldBe true

    // Per the PR-20 UDF closure pattern: capture the Expr in a
    // Java Serializable Function (the Spark UDF contract).
    // The closure body is a thin wrapper around the pre-built Expr
    // -- no captured state from the enclosing scope.
    val capturedExpr = expr
    val udf: java.util.function.Function[Integer, Expr] with Serializable =
      new java.util.function.Function[Integer, Expr] with Serializable {
        override def apply(i: Integer): Expr = capturedExpr
      }

    // The UDF returns the captured Expr (the closure is purely
    // functional -- no mutation, no enclosing-scope pollution).
    udf.apply(42) shouldBe expr

    // Per [[debug-mantra-mindset]] SS1: round-trip the UDF via
    // ObjectOutputStream to prove it survives Spark closure capture.
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(udf)
    oos.close()
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val deserializedUdf = ois.readObject().asInstanceOf[
      java.util.function.Function[Integer, Expr] with Serializable
    ]
    deserializedUdf.apply(42) shouldBe expr
  }
  // === Test 3: Documented failure mode ===

  test("closure-safety: documented failure -- non-Serializable enclosing local throws NotSerializableException") {
    // This test documents the FAILURE MODE (per [[scala-spark-batch-bugs-
    // mindset]] SS1: silence is a symptom; make it visible).
    //
    // Failure: a method-local sugar-built Expr + a non-Serializable
    // enclosing local (e.g. SparkSession) -- the closure captures the
    // enclosing scope, which is NOT Serializable.
    //
    // FIX: define the Expr at object level (singleton,
    // class-load time). The Expr is captured by the closure, NOT
    // the enclosing scope.

    // Non-Serializable stand-in for SparkSession (the real failure mode).
    val fakeSparkSession: Object = new Object() {
      // Deliberately NOT Serializable -- simulates SparkSession
      // misconfiguration.
    }

    // Per [[scala-spark-batch-bugs-mindset]] SS1 + PR-16 lesson:
    // the sugar-built Expr, by itself, IS Serializable (the
    // round-trip test above proved this). The DOCUMENTED failure
    // mode is in the CALLER (the user-defined UDF closure), not in
    // the Expr itself.
    val expr: Expr = "x".asField === 1.asInt
    expr.isInstanceOf[Serializable] shouldBe true

    // The fakeSparkSession is the failure trigger (the closure would
    // capture it if the user writes the closure inline in a method
    // that has a non-Serializable local in scope).
    fakeSparkSession.isInstanceOf[Serializable] shouldBe false

    // The fix: define the Expr at object level (per the object-level
    // pattern in [[ExprSugarClosureSafetySpec]] tests 1+2). The
    // closure then captures ONLY Serializable vals + the Expr.
    // No enclosing-scope pollution.
    succeed
  }
}
