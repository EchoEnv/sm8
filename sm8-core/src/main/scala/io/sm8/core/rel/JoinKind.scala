/*
 * SM8 Core — JoinKind (engine-portable join-kind ADT).
 *
 * Per the v0.1.0 IR extension plan (ADR-007 + RFC §3): joins are
 * the 4th deferred concern after Model / Filter / Aggregate /
 * DerivedMetric / Transform. This ADT is the foundation.
 *
 * Per `docs/rfcs/2026-08-12_v1_architecture-spec/semantic-layer-engine-architecture.md`
 * §3 "Core Boundary": joins are universal across query engines
 * (every SQL engine supports these 5 kinds). The engine-specific
 * compile lives in the adapter (Spark's `joinType`, Trino's
 * `INNER/LEFT/RIGHT/FULL/CROSS JOIN`).
 *
 * Per [[scala-data-driven-refactor-mindset]]: the join-kind set
 * is fixed at compile time → sealed ADT (NOT a Map or String).
 *
 * Per [[karpathy-guidelines-mindset]]: ported from the legacy
 * `/tmp/semanticdf/semanticdf-core/src/main/scala/io/semanticdf/core/rel/JoinKind.scala`
 * with the same 5 cases (Inner / Left / Right / Full / Cross).
 * The 5-case set matches ANSI SQL + Spark + Trino + DuckDB.
 *
 * Per [[scala-jvm-safety-mindset]]: zero spark imports, zero
 * static / ThreadLocal state. The boundary contract is verifiable
 * by:
 *   `grep -r 'org.apache.spark' sm8-core/src/main/scala/io/sm8/core/rel/JoinKind.scala`
 *
 * Per [[scala-error-handling-mindset]]: not-yet-supported join
 * kinds (e.g. Semi, Anti) are NOT silently mapped — they're absent
 * from the ADT. A future contributor adding them must extend the
 * ADT (which forces the engine adapter to handle them explicitly).
 */
package io.sm8.core.rel

sealed trait JoinKind extends Product with Serializable

object JoinKind {

  /** Inner join: rows where both sides match.
    * Maps to Spark `"inner"`, Trino `INNER JOIN`, DuckDB `INNER JOIN`. */
  case object Inner extends JoinKind

  /** Left outer join: all rows from left + matching rows from right.
    * Maps to Spark `"left"`, Trino `LEFT JOIN`, DuckDB `LEFT JOIN`. */
  case object Left extends JoinKind

  /** Right outer join: all rows from right + matching rows from left.
    * Maps to Spark `"right"`, Trino `RIGHT JOIN`, DuckDB `RIGHT JOIN`. */
  case object Right extends JoinKind

  /** Full outer join: all rows from both sides.
    * Maps to Spark `"outer"`/`"full"`, Trino `FULL JOIN`, DuckDB `FULL JOIN`. */
  case object Full extends JoinKind

  /** Cross join (Cartesian product, no condition).
    * Maps to Spark `"cross"`, Trino `CROSS JOIN`, DuckDB `CROSS JOIN`. */
  case object Cross extends JoinKind
}
