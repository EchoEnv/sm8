/*
 * SM8 SDK — ScalaRestate (Scala 2.13 conversion helpers for the Restate Java SDK).
 *
 * Per [[karpathy-guidelinesmindset]] "smallest correct core": only the
 * patterns that actually repeat in the platform code, identified by
 * the audit of /tmp/semanticdf (e.g. /tmp/semanticdf/.../QueryService.java
 * lines 303, 309, 316, 446-454, 717, 726, 731). No framework, no DSL,
 * no runtime — just extension methods via `implicit class` (Scala 2.13
 * pattern; NOT Scala 3 `extension`).
 *
 * Per [[scala-jvm-safety-mindset]]: `Optional.isPresent` is null-safe;
 * `o.get` after `isPresent` is NPE-safe. No resource lifecycle.
 *
 * Per [[scala-error-handling-mindset]]: Optional → Option is the safe
 * path. The reverse uses `Option.map(Optional.of)` — null-clean
 * (Optional.of(null) would throw NPE; that's a caller bug, not ours).
 *
 * Per [[scala-impact-analysis-mindset]]: additive change. New file in
 * sm8-core. The 3 platform modules (PR-A/PR-B/PR-C in the Steps 10-11
 * reframe) will depend on this. No SDK trait changes; no source break.
 */
package io.sm8.sdk.restate

import java.util.{Optional, List, Map}

import scala.jdk.CollectionConverters._
import scala.collection.immutable

/**
 * Thin conversion helpers — no state, no runtime, just extension
 * methods on common Java types that PR-B (sm8-mcp) and PR-C
 * (sm8-platform) will use heavily.
 *
 * **Scala 2.13 syntax** — `implicit class X[T](val t: T) { def foo: R = ... }`.
 * Do NOT use Scala 3 `extension [T](t: T) def foo: R = ...` — that's a
 * different language.
 */
object ScalaRestate {

  // ---- Optional ↔ Option ----

  /**
   * `Optional[T] → Option[T]`. Null-safe via `isPresent` check.
   * @example `Optional.of("x").toScala` → `Some("x")`
   * @example `Optional.empty[String]().toScala` → `None`
   */
  implicit class OptionalOps[T](o: Optional[T]) {
    def toScala: Option[T] =
      if (o.isPresent) Some(o.get) else None
  }

  /**
   * `Option[T] → Optional[T]`. Uses `Option.map` which is null-clean.
   * @example `Some("x").toJava` → `Optional.of("x")`
   * @example `None.toJava` → `Optional.empty()`
   */
  implicit class OptionOps[T](o: Option[T]) {
    def toJava: Optional[T] =
      o.map((t: T) => java.util.Optional.of(t)).getOrElse(java.util.Optional.empty[T]())
  }

  // ---- Java collections → Scala ----

  /**
   * `java.util.List[T] → immutable.Seq[T]`. Wraps `JavaConverters.asScala.toSeq`.
   */
  implicit class JavaListOps[T](xs: java.util.List[T]) {
    def asScalaSeq: immutable.Seq[T] = xs.asScala.toSeq
  }

  /**
   * `java.util.Map[K, V] → immutable.Map[K, V]`. Wraps `JavaConverters.asScala.toMap`.
   */
  implicit class JavaMapOps[K, V](m: java.util.Map[K, V]) {
    def asScalaMap: immutable.Map[K, V] = m.asScala.toMap
  }

  // ---- Scala collections → Java ----

  /**
   * `immutable.Seq[T] → java.util.List[T]`. Wraps `JavaConverters.asJava`.
   */
  implicit class ScalaSeqOps[T](xs: immutable.Seq[T]) {
    def asJavaList: java.util.List[T] = xs.asJava
  }

  /**
   * `Iterator[T] → java.util.Iterator[T]`. Wraps `JavaConverters.asJava`.
   */
  implicit class ScalaIterOps[T](xs: Iterator[T]) {
    def asJavaIter: java.util.Iterator[T] = xs.asJava
  }
}