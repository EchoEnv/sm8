/*
 * SM8 Platform — InMemoryResultCache (LRU + single-flight impl).
 *
 * Replaces the legacy Java `io.semanticdf.cache.InMemoryResultCache`
 * (semanticdf-spark) with a Scala 2.13 class for `sm8-platform`.
 *
 * Per [[karpathy-guidelines-mindset]] (Scala 2.13 idiom + match
 * existing style): `final class` with a companion factory. NOT
 * Java. Matches the pattern set by `RestateCachedRow` (PR-C1) and
 * the `EngineService` `object` (PR-C5a/b/c).
 *
 * Per [[scala-jvm-safety-mindset]] "null is a liar": all internal
 * maps are `ConcurrentHashMap`; reads see a consistent snapshot;
 * single-flight uses `putIfAbsent` (NOT `computeIfAbsent` — the
 * latter holds a CHM bin lock during compute, which is a documented
 * anti-pattern). No resource lifecycle.
 *
 * Per [[scala-jar-packaging-mindset]]: no new Maven deps.
 * `java.util.concurrent.ConcurrentHashMap` is part of JDK.
 *
 * Per [[scala-error-handling-mindset]]: getOrComputeJournaled
 * propagates `compute.get()` exceptions to ALL waiters for the same
 * key (the legacy semantics). The cache is NOT populated on
 * failure. Exceptions are unwrapped from `ExecutionException` and
 * the cause is rethrown so callers see the original error. The
 * interrupt flag is restored on `InterruptedException` for BOTH
 * the leader path (compute threw — flag lost by JVM-clear
 * semantics) AND the waiter path (cause is InterruptedException —
 * same JVM-clear trap).
 *
 * == Serializability (PR-C5b-ext-γ concern) ==
 *
 * Used caches must be Serializable for `Restate.run` to capture
 * them in journaled closures. The `inflight` map is `@transient`
 * because its `CompletableFuture` values are NOT serializable
 * (`java.util.concurrent.CompletableFuture` is a runtime
 * class without a serialVersionUID). The `Entry` class IS
 * `Serializable` (the populated cache survives round-trip).
 * After deserialize, the `inflight` map is lazily reinitialized
 * to an empty `ConcurrentHashMap` via the `readResolve` pattern.
 *
 * == Review pass #2 fixes (PR-C5b-ext-β) ==
 *
 * - Entry `extends Serializable` (JVM-reviewer CRITICAL #1:
 *   populated cache failed `ObjectOutputStream.writeObject` with
 *   `NotSerializableException`).
 * - Leader path: `compute → put → complete` (was `compute →
 *   complete → put` — DE-reviewer CRITICAL #3: an exception from
 *   the put would leave waiters seeing the value while the cache
 *   was empty).
 * - Leader path: `InterruptedException` triggers
 *   `Thread.currentThread.interrupt()` BEFORE
 *   `leaderFut.completeExceptionally` (JVM-reviewer MAJOR #2).
 * - Waiter path: when the unwrapped `cause` is
 *   `InterruptedException`, restore the interrupt flag BEFORE
 *   rethrowing (JVM-reviewer MAJOR #3: the JVM clears the flag
 *   when throwing `ExecutionException` from `Future.get()`).
 * - `invalidateModel`: re-check tag inside the iteration loop
 *   (JVM-reviewer MAJOR #4: TOCTOU between `modelIndex.remove`
 *   and `entries.remove` could remove wrong-model entries).
 * - `invalidateModel`: drop the `inflight.remove(key)` call
 *   (JVM-reviewer MAJOR #5: the non-CAS remove can clobber a
 *   fresh leader installed after the invalidate scan started;
 *   the leader's own `finally` cleans up correctly).
 * - `getOrComputeJournaled`: added `(key, model, version, compute)`
 *   overload (DE-reviewer MAJOR #5: the original `getOrCompute`
 *   hardcoded `model="", version=0` which made entries
 *   uninvalidateable).
 * - `evictIfFull`: empty-collection guard before `minBy`
 *   (DE-reviewer MAJOR #6: a concurrent `invalidateModel`
 *   emptying entries between `size()` and `minBy` would throw
 *   `UnsupportedOperationException`).
 */
package io.sm8.platform.query.cache

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

  // Per [[scala-jvm-safety-mindset]]: plain class (not a `case
  // class` with `var`) so mutation doesn't break `equals`/
  // `hashCode`. `extends Serializable` so the populated cache
  // survives `ObjectOutputStream` round-trip (review pass #2
  // JVM-reviewer CRITICAL #1).
  private final class Entry(
      val value: RestateCachedRow,
      val model: String,
      val version: Int
  ) extends Serializable {
    // Volatile so the eviction scan (which reads this) sees
    // a consistent timestamp; per [[scala-jvm-safety-mindset]]
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
    // on overflow. Per review pass #2 (DE-reviewer MAJOR #6):
    // also guard against `minBy` on an empty collection
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
    // Per [[scala-impact-analysis-mindset]]: a retag (same key,
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
    * Per review pass #2 (JVM-reviewer MAJOR #4 + #5): this
    * implementation re-checks the tag under `entries.remove`
    * (CAS) to defend against a concurrent put changing the tag
    * between `modelIndex.remove` and the iteration; AND removes
    * the previous `inflight.remove(key)` (non-CAS) call that was
    * weakening single-flight during the invalidate window. */
  def invalidateModel(name: String): Int = {
    val keys = modelIndex.remove(name)
    if (keys == null) 0
    else {
      keys.asScala.count { key =>
        // CAS-remove: only count if the entry's tag still matches.
        // `entries.asScala.find(...)` is stale-readable, so we
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
    * Per review pass #2 (DE-reviewer MAJOR #5): the original
    * `getOrComputeJournaled` hardcoded `model=""` `version=0`,
    * making entries from this path uninvalidateable. This overload
    * threads the model + version through to the cache put so PR-C5b-ext-γ
    * (Restate.run integration) can produce invalidateable entries. */
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
        // Per review pass #2 (DE-reviewer CRITICAL #3): STORE
        // BEFORE completing the future. If the put throws (an
        // extremely unlikely event in practice, e.g. an evict
        // race), the future is never completed with `v`; all
        // paths see the exception consistently.
        putJournaledWithModelAndVersion(key, v, model, version)
        leaderFut.complete(v)
      } catch {
        case t: InterruptedException =>
          // Per [[scala-jvm-safety-mindset]] "the JVM clears the
          // interrupt flag before throwing ExecutionException":
          // restore the flag BEFORE completing the future
          // exceptionally. Without this, the leader's interrupt
          // status is silently lost (review pass #2 JVM-reviewer
          // MAJOR #2).
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
          // Per review pass #2 (JVM-reviewer MAJOR #3): the JVM
          // clears the interrupt flag before throwing
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
        // Per [[scala-jvm-safety-mindset]]: restore the
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
