/*
 * SM8 Core -- TypedPredicateSpec (PR-20, ADR-008-R §PR-20).
 *
 * Test categories per ADR-008-R §"Decision":
 *   1. Typed predicate factory shape (8 tests)
 *   2. AND/OR combinators (2 tests)
 *   3. Phantom variance (2 tests)
 *   4. Closure-safety round-trip (1 test)
 *
 * Per [[debug-mantra-mindset]] SS1: every test asserts the
 * EVALUATED shape (the underlying Predicate AST + the typed
 * wrapper name).
 */
package io.sm8.core.rel

import io.sm8.core.predicate.{CompareOp, Predicate}

import java.io._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TypedPredicateSpec extends AnyFunSuite with Matchers {

  // === Phantom-typed witnesses (object level, per PR-16 closure-safety) ===

  sealed trait Region
  sealed trait Amount
  sealed trait Id

  // === Category 1: Typed predicate factory shape (8 tests) ===

  test("TypedPredicate.eq: builds Compare(field, =, value)") {
    val p = TypedPredicate.eq[Region]("region", "east")
    p.name shouldBe "region=east"
    p.predicate shouldBe Predicate.Compare("region", CompareOp.Eq, "east")
  }

  test("TypedPredicate.ne: builds Compare(field, !=, value)") {
    val p = TypedPredicate.ne[Region]("region", "west")
    p.predicate shouldBe Predicate.Compare("region", CompareOp.Ne, "west")
  }

  test("TypedPredicate.lt: builds Compare(field, <, value)") {
    val p = TypedPredicate.lt[Amount]("amount", 100.0)
    p.predicate shouldBe Predicate.Compare("amount", CompareOp.Lt, 100.0)
  }

  test("TypedPredicate.le: builds Compare(field, <=, value)") {
    val p = TypedPredicate.le[Amount]("amount", 100.0)
    p.predicate shouldBe Predicate.Compare("amount", CompareOp.Le, 100.0)
  }

  test("TypedPredicate.gt: builds Compare(field, >, value)") {
    val p = TypedPredicate.gt[Amount]("amount", 0.0)
    p.predicate shouldBe Predicate.Compare("amount", CompareOp.Gt, 0.0)
  }

  test("TypedPredicate.ge: builds Compare(field, >=, value)") {
    val p = TypedPredicate.ge[Amount]("amount", 0.0)
    p.predicate shouldBe Predicate.Compare("amount", CompareOp.Ge, 0.0)
  }

  test("TypedPredicate.in: builds In(field, values, negate=false)") {
    val p = TypedPredicate.in[Region]("region", List("east", "west"))
    p.predicate shouldBe Predicate.In("region", List("east", "west"), negate = false)
  }

  test("TypedPredicate.isNull + isNotNull: builds IsNull(field, negate)") {
    val n  = TypedPredicate.isNull[Id]("id")
    val nn = TypedPredicate.isNotNull[Id]("id")
    n.predicate  shouldBe Predicate.IsNull("id", negate = false)
    nn.predicate shouldBe Predicate.IsNull("id", negate = true)
  }

  // === Category 2: AND/OR combinators (2 tests) ===

  test("TypedPredicate.and: combines 2 predicates of the same phantom via AND") {
    // Per ADR-008-R + [[karpathy-app-designmindset]] SS3.1: the AND
    // combinator's phantom is inferred from the FIRST child; both
    // children must share the phantom `[D]`.
    val p1 = TypedPredicate.eq[Region]("region", "east")
    val p2 = TypedPredicate.eq[Region]("region", "west")
    val combined = TypedPredicate.and[Region](p1, p2)
    combined.predicate shouldBe a [Predicate.And]
  }

  test("TypedPredicate.or: combines 2 predicates via OR") {
    val p1 = TypedPredicate.eq[Region]("region", "east")
    val p2 = TypedPredicate.eq[Region]("region", "west")
    val combined = TypedPredicate.or[Region](p1, p2)
    combined.predicate shouldBe a [Predicate.Or]
  }

  // === Category 3: Phantom variance (2 tests) ===

  test("phantom variance: TypedPredicate[Region] coerces to TypedPredicate[Nothing]") {
    val typed: TypedPredicate[Region] = TypedPredicate.eq[Region]("region", "east")
    // Per PR-18 documented pattern: asInstanceOf at the variance boundary.
    val coerced: TypedPredicate[Nothing] = typed.asInstanceOf[TypedPredicate[Nothing]]
    coerced.name shouldBe "region=east"
    coerced.predicate shouldBe Predicate.Compare("region", CompareOp.Eq, "east")
  }

  test("phantom variance: TypedPredicate.of[D] preserves phantom at construction site") {
    // The phantom [D] is purely type-level (zero runtime cost).
    // The witness INSTANCE carries the predicate payload --
    // different phantoms carry DIFFERENT predicates (the witness
    // is data, not a phantom-only marker).
    val p1: TypedPredicate[Region] = TypedPredicate.of[Region]("custom",
      Predicate.Compare("region", CompareOp.Eq, "east"))
    val p2: TypedPredicate[Amount] = TypedPredicate.of[Amount]("custom",
      Predicate.Compare("amount", CompareOp.Gt, 0.0))
    p1.name shouldBe "custom"
    p2.name shouldBe "custom"
    p1.predicate shouldBe Predicate.Compare("region", CompareOp.Eq, "east")
    p2.predicate shouldBe Predicate.Compare("amount", CompareOp.Gt, 0.0)
  }

  // === Category 4: Closure-safety round-trip (1 test) ===

  test("closure-safety: TypedPredicate survives ObjectOutputStream round-trip") {
    // Per PR-16 lesson: case class Impl (NOT anonymous-class) preserves
    // all fields through ObjectOutputStream round-trip.
    val p = TypedPredicate.eq[Region]("region", "east")
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(p)
    oos.close()
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val deserialized = ois.readObject().asInstanceOf[TypedPredicate[Nothing]]
    deserialized.name shouldBe "region=east"
    deserialized.predicate shouldBe Predicate.Compare("region", CompareOp.Eq, "east")
  }
}
