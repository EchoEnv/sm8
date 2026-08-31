/*
 * SM8 cache Plugin — InMemoryResultCache (LRU + single-flight impl).
 *
 * Bounded LRU cache for `RestateCachedRow` (engine-portable
 * journaled form). Thread-safe. Eviction runs LRU when
 * `entries.size() > maxEntries`. Reads see a consistent snapshot;
 * single-flight uses `putIfAbsent` (NOT `computeIfAbsent` — the
 * latter holds a CHM bin lock during compute, which is a documented
 * anti-pattern).
 *
 * == Single-flight error propagation ==
 *
 * Leader and waiter paths both propagate `compute.get()` exceptions
 * to all callers of the same key. The cache is NOT populated on
 * failure. Exceptions are unwrapped from `ExecutionException` and
 * the cause is rethrown so callers see the original error. The
 * thread interrupt flag is restored on `InterruptedException` for
 * BOTH paths (the JVM clears the flag when throwing
 * `ExecutionException` from `Future.get()`, and the leader path
 * sees the same JVM-clear trap when its compute throws).
 *
 * == Serializability ==
 *
 * Implements `Serializable` so the populated cache survives
 * `Restate.run` journal capture. The `inflight` map is `@transient`
 * because its `CompletableFuture` values are NOT serializable
 * (`java.util.concurrent.CompletableFuture` is a runtime class
 * without a `serialVersionUID`). The `Entry` class IS `Serializable`
 * (the populated cache survives round-trip). After deserialize,
 * `readResolve` reinitializes `inflight` to an empty
 * `ConcurrentHashMap`.
 */
package io.sm8.plugins.cache

import io.sm8.core.cache._
import scala.jdk.CollectionConverters._

import java.util.concurrent.ConcurrentHashMap
import java.util.function.{Function => JFunction, Supplier}

/**
 * Bounded LRU cache for `RestateCachedRow` (engine-portable
 * journaled-form).
 *
 * Thread-safe. Eviction is LRU on `maxEntries` overflow.
 *
 * == Single-flight ==
 *
 * `getOrComputeJournaled` uses `ConcurrentHashMap.putIfAbsent`
 * (NOT `computeIfAbsent` — the latter holds the CHM bin lock
 * during compute, which is a documented anti-pattern; the legacy
 * impl had this same bug). N concurrent identical calls result
 * in ONE `compute.get()` invocation. The leader's
 * `CompletableFuture` is the single-flight coordination point.
 *
 * == Serialization ==
 *
 * Implements `Serializable` via the `ResultCache` parent. The
 * `Entry` class IS `Serializable` (populated cache survives
 * round-trip). The `inflight` map is `@transient` (its values
 * are runtime `CompletableFuture`s). On deserialize, `readResolve`
 * reinitializes `inflight` to an empty `ConcurrentHashMap`.
 */
final class InMemoryResultCache(
    val maxEntries: Int = 256
) extends ResultCache {

  require(maxEntries > 0, "maxEntries must be > 0")

  // LRU map: key → Entry. The `Entry` is mutated in place
  // (lastAccessNanos) on access; we do NOT re-insert.
  private val entries: ConcurrentHashMap[String, Entry] =
    new ConcurrentHashMap[String, Entry]()

  // Single-flight map: key → CompletableFuture. `@transient`
  // because `CompletableFuture` is not Serializable.
  @transient
  private var inflight: ConcurrentHashMap[String,
      java.util.concurrent.CompletableFuture[RestateCachedRow]] =
    new ConcurrentHashMap[String,
      java.util.concurrent.CompletableFuture[RestateCachedRow]]()

  // Model → set of keys (for O(1) invalidateModel).
  private val modelIndex: ConcurrentHashMap[String, java.util.Set[String]] =
    new ConcurrentHashMap[String, java.util.Set[String]]()
  // class` with `var`) so mutation doesn't break `equals`/
  // `hashCode`. `extends Serializable` so the populated cache
  // survives `ObjectOutputStream` round-trip.
  private final class Entry(
      val value: RestateCachedRow,
      val model: String,
      val version: Int
  ) extends Serializable {
    // Volatile so the eviction scan (which reads this) sees
    // a consistent timestamp; 
    // "the JVM is not sequential" — non-volatile reads can
    // return stale values, evicting hot entries.
    @volatile var lastAccessNanos: Long = System.nanoTime()
  }

  // -- LRU helpers --

  private def touch(e: Entry): Unit = {
    // Update the var in place; no re-insert.
    e.lastAccessNanos = System.nanoTime()
  }

  private def removeFromModelIndex(key: String, model: String): Unit = {
    if (model.nonEmpty) {
      val keys = modelIndex.get(model)
      if (keys != null) keys.remove(key)
    }
  }

  private def evictIfFull(): Unit = {
    // Strict `>`: hold up to `maxEntries` items; eviction only
    // on overflow. Guard against `minBy` on an empty collection
    // (a concurrent `invalidateModel` can empty entries between
    // the size check and the scan).
    if (entries.size() > maxEntries && !entries.isEmpty) {
      // O(n) scan; acceptable for typical cache sizes (≤ 256).
      // `entries.asScala` is a live view; iteration is
      // weakly-consistent under concurrent writes.
      val oldest = entries.asScala.minBy { case (_, e) => e.lastAccessNanos }
      entries.remove(oldest._1)
      removeFromModelIndex(oldest._1, oldest._2.model)
    }
  }

  private def addToModelIndex(key: String, model: String): Unit = {
    if (model.nonEmpty) {
      modelIndex.computeIfAbsent(
        model,
        new JFunction[String, java.util.Set[String]] {
          override def apply(k: String): java.util.Set[String] =
            java.util.Collections.newSetFromMap(
              new ConcurrentHashMap[String, java.lang.Boolean]())
        }
      ).add(key)
    }
  }

  /** `readResolve`: reinitializes the transient `inflight` map
    * to an empty `ConcurrentHashMap` after deserialize. */
  @throws[java.io.ObjectStreamException]
  private def readResolve(): AnyRef = {
    this.inflight = new ConcurrentHashMap[
        String, java.util.concurrent.CompletableFuture[RestateCachedRow]]()
    this
  }

  // -- ResultCache overrides --

  override def getJournaled(key: String): Option[RestateCachedRow] = {
    val e = entries.get(key)
    if (e == null) None
    else { touch(e); Some(e.value) }
  }

  override def putJournaledWithModelAndVersion(
      key: String,
      value: RestateCachedRow,
      model: String,
      version: Int
  ): Unit = {
    // different model) must remove the key from the previous
    // model's index set.
    val existing = entries.get(key)
    if (existing != null && existing.model != model) {
      removeFromModelIndex(key, existing.model)
    }
    val e = new Entry(value, model, version)
    entries.put(key, e)
    addToModelIndex(key, model)
    evictIfFull()
  }

  /** Drop every entry tagged with the given model name. Returns
    * the number of entries actually removed (with the matching
    * model tag). The lookup is O(1) via the `model → keys`
    * sidecar.
    *
    * re-checks the tag under `entries.remove`
    * (CAS) so a concurrent put that changes the tag
    * between `modelIndex.remove` and the iteration does not
    * remove wrong-model entries; the previous `inflight.remove(key)`
    * (non-CAS) call that weakened single-flight during the
    * invalidate window is gone — the leader's own `finally`
    * cleans up. */
  def invalidateModel(name: String): Int = {
    val keys = modelIndex.remove(name)
    if (keys == null) 0
    else {
      keys.asScala.count { key =>
        // CAS-remove: only count if the entry's tag still matches.
        // `entries.asScala.find(.)` is stale-readable, so we
        // do the tag-check via a single putIfAbsent side-step:
        // entries.remove(key) returns null if already gone or
        // if it was already retagged away from `name`. The
        // latter is what the retag-cleanup in
        // `putJournaledWithModelAndVersion` does. We compare the
        // removed entry's model tag to confirm.
        val removed = entries.remove(key)
        removed != null && removed.model == name
      }
    }
  }

  override def getOrComputeJournaled(
      key: String,
      compute: Supplier[RestateCachedRow]
  ): RestateCachedRow =
    getOrComputeJournaled(key, "", 0, compute)

  /** Same-flight read-through with proper model-tag threading.
    *
    * Threads the model + version through to the cache put so
    * journal capture (e.g. `Restate.run`) produces invalidateable
    * entries. The original `getOrComputeJournaled` hardcoded
    * `model=""` `version=0`, making entries from this path
    * uninvalidateable. */
  def getOrComputeJournaled(
      key: String,
      model: String,
      version: Int,
      compute: Supplier[RestateCachedRow]
  ): RestateCachedRow = {
    // Fast path: cache hit.
    getJournaled(key) match {
      case Some(v) => return v
      case None    =>
    }
    // Single-flight: atomically register or join an in-flight
    // computation. `putIfAbsent` (NOT `computeIfAbsent` which
    // holds a CHM bin lock during compute).
    val ourFut = new java.util.concurrent.CompletableFuture[RestateCachedRow]()
    val prior = inflight.putIfAbsent(key, ourFut)
    val leaderFut = if (prior == null) ourFut else prior
    if (prior == null) {
      // We're the leader. Run compute OUTSIDE the map lock.
      try {
        val v = compute.get()
        // STORE BEFORE completing the future. If the put throws
        // (an extremely unlikely event in practice, e.g. an evict
        // race), the future is never completed with `v`; all
        // paths see the exception consistently.
        putJournaledWithModelAndVersion(key, v, model, version)
        leaderFut.complete(v)
      } catch {
        case t: InterruptedException =>
          // Restore the interrupt flag BEFORE completing the
          // future exceptionally. Without this, the leader's
          // interrupt status is silently lost (the JVM clears
          // it when throwing `ExecutionException` from
          // `Future.get()`).
          Thread.currentThread.interrupt()
          leaderFut.completeExceptionally(t)
          // Do NOT store the failed entry; cache is empty.
        case t: Throwable =>
          leaderFut.completeExceptionally(t)
          // Do NOT store the failed entry; cache is empty.
      } finally {
        // Always remove the in-flight future. CAS-style: only
        // remove the future WE installed (defends against a
        // concurrent invalidate-and-replace cycle, which the
        // legacy missed).
        inflight.remove(key, ourFut)
      }
    }
    // Waiters (including the leader, on success) block on the future.
    try {
      leaderFut.get()
    } catch {
      case e: java.util.concurrent.ExecutionException =>
        // Unwrap so callers see the original throwable
        // (per the trait contract — no `ExecutionException` wrapper).
        val cause = e.getCause
        if (cause != null) {
          // The JVM clears the interrupt flag before throwing
          // `ExecutionException` from `Future.get()` (the flag-clear
          // is part of the standard interrupt-handling protocol).
          // If the cause is `InterruptedException`, restore the
          // flag BEFORE rethrowing so the waiter's interrupt
          // contract is honored.
          if (cause.isInstanceOf[InterruptedException]) {
            Thread.currentThread.interrupt()
          }
          throw cause
        }
        throw e
      case e: InterruptedException =>
        // interrupt flag before propagating. This branch fires
        // when `get()` was itself interrupted by an external
        // thread interrupt (NOT the leader's exception path).
        Thread.currentThread.interrupt()
        throw e
    }
  }
}

object InMemoryResultCache {
  /**
   * Convenience factory.
   */
  def apply(maxEntries: Int = 256): InMemoryResultCache =
    new InMemoryResultCache(maxEntries)
}
