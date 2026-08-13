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
 * interrupt flag is restored on `InterruptedException`.
 *
 * == Serializability (PR-C5b-ext-γ concern) ==
 *
 * Used caches must be Serializable for `Restate.run` to capture
 * them in journaled closures. The `inflight` map is `@transient`
 * because its `CompletableFuture` values are NOT serializable
 * (`java.util.concurrent.CompletableFuture` is a runtime
 * class without a serialVersionUID). After deserialize, the
 * `inflight` map is lazily reinitialized to an empty
 * `ConcurrentHashMap` via the `readResolve` pattern.
 */
package io.sm8.platform.query

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
 * `inflight` map is `@transient` (its values are runtime
 * `CompletableFuture`s). On deserialize, `readResolve` reinitializes
 * it to an empty `ConcurrentHashMap`.
 */
final class InMemoryResultCache(
    val maxEntries: Int = 256
) extends ResultCache {

  require(maxEntries > 0, "maxEntries must be > 0")

  // LRU map: key → Entry. The `Entry` is mutated in place
  // (lastAccessNanos) on access; we do NOT re-insert (the standard
  // ConcurrentHashMap idiom is to update the value in place via
  // `replace` or `compute`, not `put`).
  private val entries: ConcurrentHashMap[String, Entry] =
    new ConcurrentHashMap[String, Entry]()

  // Single-flight map: key → CompletableFuture. Transients:
  // these are runtime-only state (not meaningful to journal).
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
  // `hashCode`. The `Entry` is internal — keyed by String in the
  // CHM, never compared by value.
  private final class Entry(
      val value: RestateCachedRow,
      val model: String,
      val version: Int
  ) {
    // Volatile so the eviction scan (which reads this) sees
    // a consistent timestamp; per [[scala-jvm-safety-mindset]]
    // "the JVM is not sequential" — non-volatile reads can
    // return stale values, evicting hot entries.
    @volatile var lastAccessNanos: Long = System.nanoTime()
  }

  // -- LRU helpers --

  private def touch(e: Entry): Unit = {
    // Update the var in place; no re-insert. `Entry` is keyed
    // by String in the CHM, never compared by value.
    e.lastAccessNanos = System.nanoTime()
  }

  private def removeFromModelIndex(key: String, model: String): Unit = {
    if (model.nonEmpty) {
      val keys = modelIndex.get(model)
      if (keys != null) keys.remove(key)
    }
  }

  private def evictIfFull(): Unit = {
    // Strict `>`: we hold up to `maxEntries` items; eviction
    // kicks in only on overflow (size > maxEntries). The test
    // `getJournaled: bumps LRU access order` relies on this — without
    // `>`, the second put would evict immediately.
    if (entries.size() > maxEntries) {
      // Find the entry with the oldest lastAccessNanos. O(n) scan;
      // acceptable for typical cache sizes (≤ 256 entries per the
      // docstring). `entries.asScala` is a live view; iteration is
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

  /** `readResolve` (per [[scala-jvm-safety-mindset]] "serializable
    * closure capture"): reinitializes the transient `inflight` map
    * to an empty `ConcurrentHashMap` after deserialize. This is the
    * standard Scala pattern for restoring transient state during
    * deserialization. */
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
    // model's index set. Otherwise `invalidateModel(oldModel)`
    // would either miss the entry (if added under new model)
    // or over-evict (if the previous-model index is stale).
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
    * the number of entries actually removed. The lookup is O(1)
    * via the `model → keys` sidecar. */
  def invalidateModel(name: String): Int = {
    val keys = modelIndex.remove(name)
    if (keys == null) 0
    else {
      val n = keys.asScala.count(entries.remove(_: String) != null)
      // Also clear any in-flight computation for these keys
      // (the cached result is no longer valid).
      keys.asScala.foreach(inflight.remove(_: String))
      n
    }
  }

  override def getOrComputeJournaled(
      key: String,
      compute: Supplier[RestateCachedRow]
  ): RestateCachedRow = {
    // Fast path: cache hit.
    getJournaled(key) match {
      case Some(v) => return v
      case None    =>
    }
    // Single-flight: atomically register or join an in-flight
    // computation. We use `putIfAbsent` + a follow-up
    // `compareAndSet`-style pattern (NOT `computeIfAbsent` which
    // holds a CHM bin lock during compute — a documented
    // anti-pattern that the legacy impl reproduced).
    val ourFut = new java.util.concurrent.CompletableFuture[RestateCachedRow]()
    val prior = inflight.putIfAbsent(key, ourFut)
    val leaderFut = if (prior == null) ourFut else prior
    if (prior == null) {
      // We're the leader. Run compute OUTSIDE the map lock.
      try {
        val v = compute.get()
        leaderFut.complete(v)
        // Store in the cache. Use the public path so eviction
        // and model-indexing apply.
        putJournaledWithModelAndVersion(
          key, v,
          // Leader's compute didn't take a model/version arg
          // (PR-C5b-ext-γ wraps the whole miss path in Restate.run
          // with the model/version context). For now, no model
          // tag. This means entries from this path are not
          // invalidateModel-able. The legacy had the same
          // limitation (see `getOrCompute` comment line 222).
          "", 0
        )
      } catch {
        case t: Throwable =>
          leaderFut.completeExceptionally(t)
          // Do NOT store the failed entry; cache is empty.
      } finally {
        // Always remove the in-flight future. Subsequent callers
        // will get a fresh leader. (Per [[scala-error-handling
        // -mindset]] "errors are data": the in-flight future
        // was the only place the failure was recorded; the cache
        // itself stays clean.)
        inflight.remove(key, ourFut)
      }
    }
    // Waiters (including the leader) block on the future.
    try {
      leaderFut.get()
    } catch {
      case e: java.util.concurrent.ExecutionException =>
        // Unwrap so callers see the original throwable
        // (per the trait contract — no `ExecutionException` wrapper).
        val cause = e.getCause
        if (cause != null) throw cause
        throw e
      case e: InterruptedException =>
        // Per [[scala-jvm-safety-mindset]]: restore the interrupt
        // flag before propagating.
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