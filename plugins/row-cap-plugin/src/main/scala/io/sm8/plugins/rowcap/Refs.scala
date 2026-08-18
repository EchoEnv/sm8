/*
 * SM8 Row-Cap Plugin — phantom-typed witnesses (PR-16, ADR-008-Q §PR-16).
 *
 * Per the ADR §"Plugin Refs + example": the row-cap plugin's typed
 * dimension witnesses for the row-limit + offset fields.
 */
package io.sm8.plugins.rowcap

import io.sm8.core.model.TypedDimension

/**
 * Phantom-typed dimension witnesses for the row-cap plugin.
 *
 * Per ADR-008-Q §C9 + scala-jvm-safety-mindset §2: defined at `object`
 * level for Spark closure-safety (singleton, Serializable round-trip
 * via the trait's `extends Serializable`).
 */
object Refs {

  sealed trait RowIndex
  sealed trait GroupKey

  val rowIndex: TypedDimension[RowIndex] =
    TypedDimension.of[RowIndex]("row_index")

  val groupKey: TypedDimension[GroupKey] =
    TypedDimension.of[GroupKey]("group_key")
}
