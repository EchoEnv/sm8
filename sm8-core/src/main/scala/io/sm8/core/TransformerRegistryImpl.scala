/*
 * SM8 Core — internal TransformerRegistry implementation.
 */
package io.sm8.core

import io.sm8.sdk.{Transformer, TransformerRegistry}

final class TransformerRegistryImpl extends TransformerRegistry {

  private val byName: scala.collection.mutable.LinkedHashMap[String, Transformer] =
    scala.collection.mutable.LinkedHashMap.empty

  private var activeRef: Option[Transformer] = None

  override def register(transformer: Transformer): TransformerRegistry = {
    if (byName.contains(transformer.name)) {
      throw new IllegalArgumentException(
        s"sm8: Transformer '${transformer.name}' is already registered")
    }
    byName += (transformer.name -> transformer)
    // First Transformer becomes active automatically (per Q3 = swap).
    if (activeRef.isEmpty) activeRef = Some(transformer)
    this
  }

  override def setActive(name: String): Option[Transformer] = {
    byName.get(name).map { t =>
      activeRef = Some(t)
      t
    }
  }

  override def active: Option[Transformer] = activeRef
}