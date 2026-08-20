/*
 * SM8 Core — AggregateCall (engine-portable aggregate-call ADT).
 *
 * Per the v0.1.0 IR extension plan (ADR-007 + RFC §3): wraps an
 * `AggregateFn` with its input expression, alias, distinct flag,
 * and literal arguments.
 *
 * `/tmp/semanticdf/semanticdf-core/src/main/scala/io/semanticdf/core/rel/AggregateCall.scala`
 * with the same 5-field shape.
 *
 * Per RFC §3: engine-portable; engine-specific compile lives in
 * the adapter (Spark's `Column = functions.agg(...)`, Trino's
 * `SUM(x) AS total`, DuckDB's `SUM("x")`).
 *
 * makes `Count(*)` (no input) explicit at the ADT level — no
 * silent defaulting. `distinct: Boolean` is a closed enum-like
 * field, not a free-form string.
 *
 * List[LiteralValue]` (not `Map[String, LiteralValue]`) — the
 * argument shape is FIXED at compile time. A `Map` would let
 * callers pass `percentile = 0.95` or `p = 0.95` (typo) with
 * silent defaulting. The engine adapter pattern-matches on `fn`
 * to determine which arguments it expects.
 *
 * contract:
 * `grep -r 'org.apache.spark' sm8-core/src/main/scala/io/sm8/core/rel/AggregateCall.scala`
 */
package io.sm8.core.rel

import io.sm8.core.expr.{Expr, LiteralValue}

final case class AggregateCall(
 fn:  AggregateFn,
 input:  Option[Expr]  = None,
 alias:  String    = "",
 distinct: Boolean    = false,
 arguments: List[LiteralValue] = Nil,
) extends Product with Serializable
