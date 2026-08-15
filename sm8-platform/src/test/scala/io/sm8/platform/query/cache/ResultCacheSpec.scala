package io.sm8.platform.query.cache

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for `ResultCache` (PR-C5b-ext-α).
 *
 * Per [[scala-data-driven-refactor-mindset]]: pure type-level tests.
 * The trait's default methods are `NoOp` semantics; the `NoOp` instance
 * is the canonical no-op cache. PR-C5b-ext-β will add `InMemoryResultCache`
 * with real LRU + single-flight semantics.
 *
 * Per [[scala-jvm-safety-mindset]] `Serializable` is checked via
 * the `NoOp` instance cast — if the trait loses `extends Serializable`,
 * this test fails to compile.
 */
class ResultCacheSpec extends AnyFunSuite with Matchers {

  test("ResultCache.NoOp.getJournaled returns None for any key") {
    val cache = ResultCache.NoOp
    cache.getJournaled("any-key") shouldBe None
    cache.getJournaled("") shouldBe None
    cache.getJournaled("a" * 1000) shouldBe None
  }

  test("ResultCache.NoOp.putJournaledWithModelAndVersion is a no-op (does not throw)") {
    val cache = ResultCache.NoOp
    val row = RestateCachedRow(
      fieldNames = List("a"),
      fieldTypes = List(RestateCachedRow.T_STRING),
      rows = List(Array("x"))
    )
    noException should be thrownBy {
      cache.putJournaledWithModelAndVersion("k", row, "m", 1)
    }
    // Side-effect-free: subsequent get returns None.
    cache.getJournaled("k") shouldBe None
  }

  test("ResultCache: NoOp is Serializable (required for Restate.run closure capture)") {
    // Compile-time + runtime check: the NoOp instance must be
    // Serializable. If the trait loses `extends Serializable`, this
    // fails to compile (or fails at runtime when Restate.run tries
    // to journal the cache).
    val cache: java.io.Serializable = ResultCache.NoOp
    cache shouldBe ResultCache.NoOp
  }

  test("ResultCache: default methods are no-ops (subclass can override selectively)") {
    // A subclass that overrides only `getJournaled` inherits the
    // default `putJournaledWithModelAndVersion` (no-op) and vice-versa.
    val onlyGet = new ResultCache {
      override def getJournaled(key: String): Option[RestateCachedRow] =
        if (key == "hit") Some(RestateCachedRow(Nil, Nil, Nil)) else None
    }
    onlyGet.getJournaled("hit") shouldBe defined
    onlyGet.getJournaled("miss") shouldBe None
    // null-validation is the concrete impl's responsibility (not the
    // trait's); the default `putJournaledWithModelAndVersion` silently
    // no-ops on null. Real impls must validate.
    noException should be thrownBy {
      onlyGet.putJournaledWithModelAndVersion("k", null, "m", 1)
    }
  }

  test("ResultCache.getOrComputeJournaled default throws (contract guard)") {
    // Per the legacy convention: the default getOrComputeJournaled
    // throws UnsupportedOperationException. This forces caches that
    // override getJournaled + putJournaledWithModelAndVersion to
    // ALSO override getOrComputeJournaled (single-flight semantics).
    // Prevents the cache-stampede bug from entering silently.
    val cache = ResultCache.NoOp
    val compute: java.util.function.Supplier[RestateCachedRow] =
      new java.util.function.Supplier[RestateCachedRow] {
        override def get(): RestateCachedRow = RestateCachedRow(Nil, Nil, Nil)
      }
    an [UnsupportedOperationException] should be thrownBy {
      cache.getOrComputeJournaled("k", compute)
    }
  }
}