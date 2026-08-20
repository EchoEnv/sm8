/*
 * SM8 Core — AggregateCallClosureSafetySpec (PR-131, ADR-008-T).
 *
 * The untyped `AggregateCall` is the MODEL-layer wire DTO returned
 * by the new `ExprSugar` extensions (`Expr.sum / .avg / .min / .max /
 * .countDistinct` and `String.countStar`). Per ADR-008-T v1.1
 * (post-review T3/R2 fix): the untyped case class is trivially
 * Serializable (no captured refs, no closure dependencies), so the
 * 3-test discipline from `TypedAggregateCallClosureSafetySpec` is
 * overkill. A single positive round-trip test proves the sugar-built
 * case class survives `ObjectOutputStream` + `ObjectInputStream`
 * round-trip byte-identically.
 *
 * Per `scala-spark-batch-bugs-mindset` §1 (closure-safety): the
 * case-class-derived `writeObject`/`readObject` MUST preserve
 * every field.
 */
package io.sm8.core.rel

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.sm8.core.expr.{Expr, ExprSugar}
import io.sm8.core.expr.ExprSugar._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AggregateCallClosureSafetySpec extends AnyFlatSpec with Matchers {

  "AggregateCall built via ExprSugar" should
      "survive ObjectOutputStream round-trip with every field preserved" in {
    val call: AggregateCall = Expr.FieldRef("los_days").sum

    val baos = new ByteArrayOutputStream(256)
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(call)
    oos.close()
    val ois     = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val recovered = ois.readObject().asInstanceOf[AggregateCall]

    recovered.fn       shouldBe AggregateFn.Sum
    recovered.input    shouldBe Some(Expr.FieldRef("los_days"))
    recovered.alias    shouldBe ""
    recovered.distinct shouldBe false
    recovered.arguments shouldBe Nil
  }
}
