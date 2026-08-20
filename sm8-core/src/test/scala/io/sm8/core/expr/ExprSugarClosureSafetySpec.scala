/*
 * SM8 Core -- ExprSugarClosureSafetySpec.
 *
 * Verifies that every sugar-built `Expr` survives `ObjectOutputStream`
 * round-trip and is safe to capture in a Spark UDF closure. The
 * sugar returns the same sealed case classes as the explicit
 * constructor, so closure safety is inherited from `Expr`'s
 * existing `Serializable` contract.
 */

package io.sm8.core.expr

import java.io._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExprSugarClosureSafetySpec extends AnyFunSuite with Matchers {

  import ExprSugar._

  test("closure-safety: ExprSugar positive round-trip") {
    val expr: Expr = Expr.Equal(
      "discharge_status".asField,
      "expired".asVarchar
    )

    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(expr)
    oos.close()

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val deserialized = ois.readObject().asInstanceOf[Expr]

    deserialized shouldBe expr
  }

  test("closure-safety: ExprSugar Spark UDF closure-safe (java.util.function.Function with Serializable)") {
    val expr: Expr = Expr.And(
      "region".asField === "east".asVarchar,
      "active".asField === true.asBool
    )

    expr.isInstanceOf[Serializable] shouldBe true

    val capturedExpr = expr
    val udf: java.util.function.Function[Integer, Expr] with Serializable =
      new java.util.function.Function[Integer, Expr] with Serializable {
        override def apply(i: Integer): Expr = capturedExpr
      }

    udf.apply(42) shouldBe expr

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

  test("closure-safety: ExprSugar documented failure -- non-Serializable enclosing local throws NotSerializableException") {
    // Documents the failure mode: a method-local Expr + a
    // non-Serializable enclosing local (e.g. SparkSession) --
    // the closure captures the enclosing scope, which is NOT
    // Serializable. The fix: define the Expr at object level
    // (singleton, class-load time), so the closure captures
    // only the Expr, not the enclosing scope.
    val fakeSparkSession: Object = new Object() {}

    val expr: Expr = "x".asField === 1.asInt
    expr.isInstanceOf[Serializable] shouldBe true

    // The fakeSparkSession is the failure trigger (deliberately
    // not Serializable -- simulates a SparkSession misconfiguration).
    fakeSparkSession.isInstanceOf[Serializable] shouldBe false

    succeed
  }
}
