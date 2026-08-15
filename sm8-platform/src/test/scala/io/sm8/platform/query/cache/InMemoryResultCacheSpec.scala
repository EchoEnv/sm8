package io.sm8.platform.query.cache

import java.util.function.Supplier

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for `InMemoryResultCache` (PR-C5b-ext-β).
 *
 * Per [[scala-jvm-safety-mindset]] "null is a liar": all internal
 * maps are `ConcurrentHashMap`; reads + writes see a consistent
 * snapshot. Single-flight uses `computeIfAbsent` for atomic
 * per-key coalescing.
 *
 * Per [[scala-jar-packaging-mindset]] "no new Maven deps": all
 * types are JDK or Scala stdlib.
 */
class InMemoryResultCacheSpec extends AnyFunSuite with Matchers {

  private def row(name: String): RestateCachedRow =
    RestateCachedRow(
      fieldNames = List(name),
      fieldTypes = List(RestateCachedRow.T_STRING),
      rows       = List(Array("v"))
    )

  // -- getJournaled / putJournaledWithModelAndVersion --

  test("getJournaled: returns None on empty cache") {
    val cache = InMemoryResultCache()
    cache.getJournaled("k") shouldBe None
  }

  test("put + get round-trip") {
    val cache = InMemoryResultCache()
    val r = row("a")
    cache.putJournaledWithModelAndVersion("k", r, "m", 1)
    cache.getJournaled("k") shouldBe Some(r)
  }

  test("getJournaled: bumps LRU access order") {
    val cache = InMemoryResultCache(maxEntries = 2)
    val r1 = row("a"); val r2 = row("b")
    cache.putJournaledWithModelAndVersion("k1", r1, "m", 1)
    cache.putJournaledWithModelAndVersion("k2", r2, "m", 1)
    // Touch k1 (LRU bump); insert k3 → evicts k2 (LRU oldest)
    cache.getJournaled("k1")
    cache.putJournaledWithModelAndVersion("k3", row("c"), "m", 1)
    cache.getJournaled("k1") shouldBe defined
    cache.getJournaled("k2") shouldBe None
    cache.getJournaled("k3") shouldBe defined
  }

  // -- LRU eviction --

  test("LRU eviction: evicts the oldest-accessed entry when over capacity") {
    val cache = InMemoryResultCache(maxEntries = 2)
    cache.putJournaledWithModelAndVersion("k1", row("a"), "m", 1)
    cache.putJournaledWithModelAndVersion("k2", row("b"), "m", 1)
    // k1 is oldest. Insert k3 → evicts k1.
    cache.putJournaledWithModelAndVersion("k3", row("c"), "m", 1)
    cache.getJournaled("k1") shouldBe None
    cache.getJournaled("k2") shouldBe defined
    cache.getJournaled("k3") shouldBe defined
  }

  test("LRU eviction: model-tagged entries are dropped from modelIndex on eviction") {
    val cache = InMemoryResultCache(maxEntries = 1)
    cache.putJournaledWithModelAndVersion("k1", row("a"), "m1", 1)
    cache.putJournaledWithModelAndVersion("k2", row("b"), "m2", 1)
    // k1 was evicted. invalidateModel("m1") should return 0
    // (the modelIndex entry for "m1" no longer has any keys).
    cache.invalidateModel("m1") shouldBe 0
    cache.invalidateModel("m2") shouldBe 1
  }

  // -- invalidateModel --

  test("invalidateModel: drops all entries for the given model") {
    val cache = InMemoryResultCache()
    cache.putJournaledWithModelAndVersion("k1", row("a"), "m1", 1)
    cache.putJournaledWithModelAndVersion("k2", row("b"), "m1", 1)
    cache.putJournaledWithModelAndVersion("k3", row("c"), "m2", 1)
    cache.invalidateModel("m1") shouldBe 2
    cache.getJournaled("k1") shouldBe None
    cache.getJournaled("k2") shouldBe None
    cache.getJournaled("k3") shouldBe defined  // m2 unaffected
  }

  test("invalidateModel: returns 0 for unknown model") {
    val cache = InMemoryResultCache()
    cache.invalidateModel("nope") shouldBe 0
  }

  // -- getOrComputeJournaled single-flight --

  test("getOrComputeJournaled: cache hit does not invoke compute") {
    val cache = InMemoryResultCache()
    val r = row("hit")
    cache.putJournaledWithModelAndVersion("k", r, "m", 1)
    val compute = new Supplier[RestateCachedRow] {
      override def get(): RestateCachedRow = fail("compute must not run on cache hit")
    }
    cache.getOrComputeJournaled("k", compute) shouldBe r
  }

  test("getOrComputeJournaled: cache miss invokes compute and stores result") {
    val cache = InMemoryResultCache()
    val r = row("miss")
    val compute = new Supplier[RestateCachedRow] {
      override def get(): RestateCachedRow = r
    }
    cache.getOrComputeJournaled("k", compute) shouldBe r
    cache.getJournaled("k") shouldBe Some(r)
  }

  test("getOrComputeJournaled: single-flight coalesces N concurrent calls") {
    val cache = InMemoryResultCache()
    val r = row("coalesced")
    // Use a counter to verify compute is invoked exactly once
    // under concurrent calls.
    val counter = new java.util.concurrent.atomic.AtomicInteger(0)
    val compute = new Supplier[RestateCachedRow] {
      override def get(): RestateCachedRow = {
        counter.incrementAndGet()
        // Small sleep to widen the race window
        Thread.sleep(50)
        r
      }
    }
    val N = 8
    val futures: Seq[java.util.concurrent.Future[RestateCachedRow]] = (1 to N).map { _ =>
      java.util.concurrent.CompletableFuture.supplyAsync(() =>
        cache.getOrComputeJournaled("k", compute)
      )
    }
    val results: Seq[RestateCachedRow] = futures.map(_.get())
    // All N calls return the same value
    results.foreach(_ shouldBe r)
    // But compute was invoked exactly once
    counter.get shouldBe 1
  }

  test("getOrComputeJournaled: compute failure propagates to all waiters") {
    val cache = InMemoryResultCache()
    val cause = new RuntimeException("compute failed")
    val compute = new Supplier[RestateCachedRow] {
      override def get(): RestateCachedRow = throw cause
    }
    val futures: Seq[java.util.concurrent.Future[Option[Throwable]]] = (1 to 4).map { _ =>
      java.util.concurrent.CompletableFuture.supplyAsync[Option[Throwable]](() =>
        try { cache.getOrComputeJournaled("k", compute); None }
        catch { case e: Throwable => Some(e) }
      )
    }
    val results: Seq[Throwable] = futures.flatMap(_.get())
    // Per review pass #2 (JVM-reviewer MINOR #7): strengthen
    // assertion — verify all waiters receive the SAME exception
    // message, not just `a [Throwable]` (which is tautological).
    results should have size 4.toLong
    results.foreach { e =>
      e.getMessage shouldBe "compute failed"
    }
    // Cache was NOT populated on failure (errors are data).
    cache.getJournaled("k") shouldBe None
  }

  // -- Review pass #2: caching-recovery after a failure --

  test("getOrComputeJournaled: cache is usable after a compute failure (no poisoning)") {
    // Per review pass #2 (JVM-reviewer MINOR #8): after a
    // `compute.get()` throws, a subsequent caller for the same
    // key must trigger a fresh compute and succeed.
    val cache = InMemoryResultCache()
    var failFirst = true
    val good = row("fresh")
    val compute = new Supplier[RestateCachedRow] {
      override def get(): RestateCachedRow = {
        if (failFirst) {
          failFirst = false
          throw new RuntimeException("first attempt failed")
        }
        good
      }
    }
    // First call: throws.
    intercept[RuntimeException] {
      cache.getOrComputeJournaled("k", compute)
    }
    // Cache is empty.
    cache.getJournaled("k") shouldBe None
    // Second call: succeeds.
    val second = cache.getOrComputeJournaled("k", compute)
    second shouldBe good
    cache.getJournaled("k") shouldBe Some(good)
  }

  // -- Review pass #2: retag clean-up under invalidateModel --

  test("invalidateModel: retag under different model drops key from old model index (DE MINOR #11)") {
    // Per review pass #2 (DE-reviewer MINOR #11 + JVM-reviewer
    // MAJOR #4): put a key under model "A", retag under model "B"
    // (same key, different model), then `invalidateModel("A")`
    // must NOT find the key (it was removed from "A"'s index
    // during the retag). `invalidateModel("B")` must find it.
    val cache = InMemoryResultCache()
    cache.putJournaledWithModelAndVersion("k", row("a"), "A", 1)
    cache.putJournaledWithModelAndVersion("k", row("b"), "B", 1)
    cache.invalidateModel("A") shouldBe 0  // key was retagged away
    cache.invalidateModel("B") shouldBe 1
    cache.getJournaled("k") shouldBe None
  }

  // -- Review pass #2: real ObjectOutputStream round-trip --

  test("InMemoryResultCache: survives ObjectOutputStream round-trip (review pass #2 JVM CRITICAL #1)") {
    // Per review pass #2 (JVM-reviewer CRITICAL #1): the prior
    // test only verified a static-type ascription to
    // `java.io.Serializable`. The actual `Entry` class was not
    // Serializable — `ObjectOutputStream.writeObject` threw
    // `NotSerializableException`. This test does a real round-trip
    // and verifies the cache still serves `getJournaled`.
    //
    // Note: we can't use case-class `equals` on the round-tripped
    // row because `RestateCachedRow.rows` is `List[Array[String]]`
    // and Scala `Array.equals` is reference-equal (arrays don't
    // have structural `equals`). Compare field-by-field instead.
    val cache = InMemoryResultCache()
    val original = row("payload")
    cache.putJournaledWithModelAndVersion("k", original, "m", 1)
    val bout = new java.io.ByteArrayOutputStream()
    val out = new java.io.ObjectOutputStream(bout)
    out.writeObject(cache)
    out.close()
    val bytes = bout.toByteArray
    bytes.length should be > 0
    val in = new java.io.ObjectInputStream(
      new java.io.ByteArrayInputStream(bytes))
    val recovered = in.readObject().asInstanceOf[InMemoryResultCache]
    in.close()
    // Survived round-trip; entries still served.
    val recoveredRow = recovered.getJournaled("k")
    recoveredRow shouldBe defined
    val rr = recoveredRow.get
    rr.fieldNames shouldBe original.fieldNames
    rr.fieldTypes shouldBe original.fieldTypes
    rr.rows.size shouldBe original.rows.size
    rr.rows.zip(original.rows).foreach { case (a, b) =>
      a.toList shouldBe b.toList
    }
  }

  // -- Existing ResultCache contract --

  test("InMemoryResultCache: extends Serializable (Spark closure hygiene)") {
    val cache: java.io.Serializable = InMemoryResultCache()
    cache shouldBe a [ResultCache]
  }
}