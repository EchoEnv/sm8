/*
 * SM8 Core — internal TransformerRegistry implementation.
 *
 * Thread-safe 
 *.
 */
package io.sm8.core

import java.util.concurrent.atomic.AtomicReference

import io.sm8.sdk.{Transformer, TransformerRegistry}

final class TransformerRegistryImpl extends TransformerRegistry {

 private val byName: scala.collection.mutable.LinkedHashMap[String, Transformer] =
 scala.collection.mutable.LinkedHashMap.empty

 // AtomicReference so concurrent setActive + register don't lose writes.
 private val activeRef: AtomicReference[Option[Transformer]] =
 new AtomicReference(Option.empty)

 override def register(transformer: Transformer): TransformerRegistry = {
 if (byName.contains(transformer.name)) {
  throw new IllegalArgumentException(
  s"sm8: Transformer '${transformer.name}' is already registered")
 }
 byName += (transformer.name -> transformer)
 // First Transformer becomes active automatically (per Q3 = swap).
 // compareAndSet so a concurrent register wins predictably.
 activeRef.compareAndSet(Option.empty, Some(transformer))
 this
 }

 override def setActive(name: String): Option[Transformer] =
 byName.get(name).map { t =>
  activeRef.set(Some(t))
  t
 }

 override def active: Option[Transformer] = activeRef.get()
}