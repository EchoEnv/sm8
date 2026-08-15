/*
 * SM8 Core — CalculatedMeasure (engine-portable calculated-measure ADT).
 *
 * Per the v0.1.0 IR extension plan (ADR-007 + PR-J): a
 * [[CalculatedMeasure]] is a measure whose value is COMPUTED from
 * other measures (or fields). It carries the name + the
 * engine-portable expression that produces the value.
 *
 * ==Why a separate type from `Measure`==
 *
 * `Measure` (PR-J change) has an `expr: AggregateCall` — the
 * expression is a single aggregate call (`SUM(amount)`,
 * `COUNT(*)`, etc.). Calculated measures have an `expr: Expr` —
 * the expression is ANY engine-portable expression (`a + b`,
 * `field_a / field_b`, `CASE WHEN ... END`, etc.). The two are
 * semantically different shapes.
 *
 * ==Why `expr: Expr` (not `String`)==
 *
 * A `String` would let callers pass `"a + b"` / `"a/b"` / typos —
 * silent failures at engine-compile time. The `Expr` ADT (24
 * cases after PR-I) forces the model validator to check that
 * the expression is well-formed (the field names exist, the
 * operators are valid, etc.).
 *
 * Per [[karpathy-guidelines-mindset]]: ported from the legacy
 * `/tmp/semanticdf/semanticdf-core/src/main/scala/io/semanticdf/core/model/CalculatedMeasure.scala`
 * with the same 2-field shape.
 *
 * Per RFC §3: engine-portable; the engine-specific compile
 * (Spark's `Column = expr.fold(...)`, Trino's SQL
 * `SELECT (a + b) AS calc`, etc.) lives in the engine adapter.
 *
 * Per [[scala-error-handling-mindset]]: cycles in the
 * calculated-measure dependency DAG surface as
 * `ModelValidationError.CalculatedMeasureCycle` at model-load
 * time (fail loud, never silent).
 *
 * Per [[scala-jvm-safety-mindset]]: zero spark imports.
 * Boundary contract:
 *   `grep -r 'org.apache.spark' sm8-core/src/main/scala/io/sm8/core/model/CalculatedMeasure.scala`
 */
package io.sm8.core.model

import io.sm8.core.expr.Expr

final case class CalculatedMeasure(
    name: String,
    expr: Expr,
) extends Product with Serializable
