package io.sm8.core.model

import io.sm8.core.expr.Expr

/** Engine-portable filter-spec ADT — Phase 2 contract. Mirrors the
 * design doc §4.4.1 "FilterSpec" (the model-level declaration of a
 * row-level filter that applies to the model's source).
 *
 * ==Why a separate type from the existing `io.semanticdf.SemanticFilter`==
 *
 * The spark-coupled `SemanticFilter` carries a
 * `SemanticScope => Column` closure (Spark `Column` is engine-
 * specific). The portable `FilterSpec` carries an `Expr: Expr`
 * (engine-portable, from PR #359). The two coexist intentionally:
 * the spark-coupled version is used by the existing `SemanticTable`'s
 * row-filter API; the portable version is used by the future
 * `Model.of` API and the v2 manifest.
 *
 * Per karpathy §3 (surgical, no opportunistic refactors): the
 * existing `io.semanticdf.SemanticFilter` is untouched.
 *
 * ==Why a separate type from `core.predicate.Predicate`==
 *
 * The `Predicate` ADT (in `core.predicate.Predicate`) is the
 * filter language for the audit/cache chain (per the design's
 * `filters:` block in YAML — row-level hygiene filters). The
 * `FilterSpec.predicate: Expr` is the SAME shape but with `Expr`
 * instead of `Predicate` — the model-level filter spec uses `Expr`
 * because it's the runtime expression that the engine compiles.
 *
 * The design's "predicates are typed" rule (risk #4) applies: at
 * the MODEL level (declarative), filters are `Expr` (typed); at the
 * AUDIT/CACHE level (runtime), filters are `Predicate` (the
 * filter-language AST). The validator ensures they're consistent.
 *
 * ==Why `name: String`==
 *
 * Filters are NAMED at the model level (the user references them
 * by name in `explain` / `describe_model` output). The name is
 * unique within the model's filter list.
 *
 * ==Why core (engine-portable)==
 *
 * Filter specs are universal across query engines. The engine-
 * specific compile (Spark's `df.filter(...)`, Trino's `WHERE`,
 * etc.) lives in the engine adapter.
 *
 * ==Data-driven mantra compliance==
 *
 * - Pure data: case class (no behavior)
 * - Equality auto-derived
 * - `Product with Serializable`
 *
 * ==Boundary contract==
 *
 * Zero Spark imports. Verifiable by:
 * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/FilterSpec.scala`
 */
final case class FilterSpec(
 name:  String,
 predicate: Expr,
) extends Product with Serializable