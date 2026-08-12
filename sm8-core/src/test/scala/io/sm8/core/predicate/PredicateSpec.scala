/*
 * SM8 Core — Predicate test.
 *
 * Per [[debug-mantra-mindset]] (verify the smart constructor's
 * shape normalization): And/Or collapse singleton lists.
 */
package io.sm8.core.predicate

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PredicateSpec extends AnyFlatSpec with Matchers {

  val leaf = Predicate.Compare("city", CompareOp.Eq, "NYC")

  "Predicate.And smart constructor" should "collapse singleton list to the child" in {
    val result = Predicate.and(List(leaf))
    result shouldBe leaf
  }

  it should "preserve multi-child as And" in {
    val other = Predicate.Compare("state", CompareOp.Eq, "NY")
    val result = Predicate.and(List(leaf, other))
    result shouldBe a [Predicate.And]
  }

  it should "handle empty list" in {
    val result = Predicate.and(List.empty)
    result shouldBe a [Predicate.And]
  }

  "Predicate.Or smart constructor" should "collapse singleton list to the child" in {
    val result = Predicate.or(List(leaf))
    result shouldBe leaf
  }

  it should "preserve multi-child as Or" in {
    val other = Predicate.Compare("state", CompareOp.Eq, "CA")
    val result = Predicate.or(List(leaf, other))
    result shouldBe a [Predicate.Or]
  }

  "Predicate.Compare" should "describe itself in human-readable form" in {
    leaf.describe shouldBe "city = NYC"
    leaf.fields shouldBe Set("city")
  }

  "Predicate.combine" should "AND via instance method" in {
    val other = Predicate.Compare("state", CompareOp.Eq, "NY")
    val combined = leaf.and(other)
    combined shouldBe a [Predicate.And]
  }

  it should "OR via instance method" in {
    val other = Predicate.Compare("state", CompareOp.Eq, "NY")
    val combined = leaf.or(other)
    combined shouldBe a [Predicate.Or]
  }

  it should "NOT via instance method" in {
    val negated = leaf.negatePredicate
    negated shouldBe a [Predicate.Not]
  }
}

class CompareOpSpec extends AnyFlatSpec with Matchers {
  "CompareOp" should "have 6 sealed cases with correct toString" in {
    CompareOp.Eq.toString shouldBe "="
    CompareOp.Ne.toString shouldBe "!="
    CompareOp.Lt.toString shouldBe "<"
    CompareOp.Le.toString shouldBe "<="
    CompareOp.Gt.toString shouldBe ">"
    CompareOp.Ge.toString shouldBe ">="
  }
}