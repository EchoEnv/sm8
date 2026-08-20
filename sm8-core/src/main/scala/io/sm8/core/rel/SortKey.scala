/*
 * SM8 Core — SortKey (engine-portable sort-key ADT).
 *
 * Per the v0.1.0 IR extension plan (ADR-007 + RFC §3): sort keys
 * are part of the relational plan tree (`RelOp.Sort`). Each
 * SortKey carries an `expression: Expr` + direction + null ordering.
 *
 * `/tmp/semanticdf/semanticdf-core/src/main/scala/io/semanticdf/core/rel/SortKey.scala`
 * with the same shape. The plan IR uses `Expr` because plans
 * carry expressions, not column-name strings.
 *
 * Per RFC §3: engine-portable; engine-specific compile lives in
 * the adapter (Spark's `Column.asc()` / `Column.desc_nulls_last()`,
 * Trino's `ORDER BY. ASC NULLS LAST`).
 *
 * (closed ADT), so the model validator can check that the
 * expression is well-formed (field names exist, operators are
 * valid, etc.) — no silent failures at engine-compile time.
 *
 * static / ThreadLocal state. Boundary contract:
 * `grep -r 'org.apache.spark' sm8-core/src/main/scala/io/sm8/core/rel/SortKey.scala`
 *
 * the simpler `core.field.SortKey` (Phase 1 mirror of the
 * spark-adapter's ordering API). Both survive per karpathy §3
 * (surgical, no opportunistic refactors).
 */
package io.sm8.core.rel

import io.sm8.core.expr.Expr

final case class SortKey(
 expression: Expr,
 direction: SortDirection,
 nullOrdering: NullOrdering) extends Product with Serializable
