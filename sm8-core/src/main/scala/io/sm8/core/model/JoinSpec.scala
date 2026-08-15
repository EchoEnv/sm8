/*
 * SM8 Core — JoinSpec (engine-portable join-spec ADT).
 *
 * Per the v0.1.0 IR extension plan (ADR-007 + PR-J): the
 * model-level declaration of how this model joins to another
 * model. The user writes it when constructing a model; the
 * validator checks it.
 *
 * ==Why `keys: List[(String, String)]`==
 *
 * The join keys are pairs of (leftKey, rightKey) — the left key
 * is a column name on this model, the right key is a column name
 * on the joined-to model. The model validator checks that both
 * keys exist in their respective schemas.
 *
 * A `List[(String, String)]` is portable (engine-portable SQL
 * engines all support equi-joins on column names). For non-equi
 * joins, the user uses a join condition expression instead (the
 * design defers non-equi joins to v0.2.0+ per the deferred
 * features list).
 *
 * ==Why `rightModel: String` (not `rightModel: SourceRef`)==
 *
 * The right side of a join is another MODEL in the catalog, not a
 * raw source. The model loader resolves the model name to its
 * portable model definition, which in turn has its own source.
 *
 * Per [[karpathy-guidelines-mindset]]: ported from the legacy
 * `/tmp/semanticdf/semanticdf-core/src/main/scala/io/semanticdf/core/model/JoinSpec.scala`
 * with the same 4-field shape.
 *
 * Per RFC §3: engine-portable; the engine-specific compile
 * (Spark's `df.join(other, cond, joinType)`, Trino's `JOIN`)
 * lives in the engine adapter.
 *
 * Per [[scala-error-handling-mindset]]: multi-key joins
 * (`keys.size > 1`) surface as `EngineError.UnsupportedCapability`
 * in the engine adapter (v0.1.0 scope is single-key; multi-key
 * is v0.2.0+).
 *
 * Per [[scala-jvm-safety-mindset]]: zero spark imports.
 * Boundary contract:
 *   `grep -r 'org.apache.spark' sm8-core/src/main/scala/io/sm8/core/model/JoinSpec.scala`
 */
package io.sm8.core.model

import io.sm8.core.rel.JoinKind

final case class JoinSpec(
    name:       String,
    rightModel: String,
    kind:       JoinKind,
    keys:       List[(String, String)],
) extends Product with Serializable
