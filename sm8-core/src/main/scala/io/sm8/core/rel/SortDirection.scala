/*
 * SM8 Core — SortDirection (engine-portable sort-direction ADT).
 *
 * Per the v0.1.0 IR extension plan (ADR-007 + RFC §3): sort direction
 * is part of the relational plan tree (`RelOp.Sort` keys).
 *
 * is fixed at compile time (Ascending / Descending) → sealed ADT
 * (NOT a Boolean — "Ascending" and "Descending" are closed enum
 * values, not just true/false).
 *
 * Per RFC §3: engine-portable; engine-specific compile lives in
 * the adapter (Spark's `.asc()` / `.desc()`, Trino's `ASC` / `DESC`).
 *
 * `/tmp/semanticdf/semanticdf-core/src/main/scala/io/semanticdf/core/rel/SortDirection.scala`
 * with the same 2 cases.
 *
 * contract: `grep -r 'org.apache.spark' sm8-core/src/main/scala/io/sm8/core/rel/SortDirection.scala`.
 */
package io.sm8.core.rel

sealed trait SortDirection extends Product with Serializable

object SortDirection {

 /** Ascending order (smallest to largest; A before Z; earliest
 * to latest). Maps to Spark `.asc()`, Trino `ASC`. */
 case object Ascending extends SortDirection

 /** Descending order (largest to smallest; Z before A; latest
 * to earliest). Maps to Spark `.desc()`, Trino `DESC`. */
 case object Descending extends SortDirection
}
