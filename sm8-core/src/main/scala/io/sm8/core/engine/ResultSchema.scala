/*
 * SM8 Core — ResultSchema ADT.
 *
 * Engine-portable schema for query results. Per the legacy
 * semanticdf-core: `ResultSchema` was originally nested inside
 * `ExecutionPlan.scala` (lines 220-234). It's extracted into its
 * own file here so that `ResultRow` + `PortableQueryResult` can
 * reference it without pulling in the entire `ExecutionPlan` tree
 * (which depends on EngineIdentity, Capability, EngineWarning, etc.).
 *
 * Per [[scala-data-driven-refactor-mindset]] (pure data): pure case
 * class, no behavior except the 3 derived accessors (size, field,
 * isEmpty) — all cheap + total + pure, function of `fields` only.
 *
 * The legacy `ResultSchema` in `io.semanticdf.core.engine` (inside
 * `ExecutionPlan.scala`) is structurally identical. Per
 * [[karpathy-guidelines-mindset]] "touch only what you must": we
 * leave the legacy one alone. The two types are independent from
 * the Scala compiler's perspective (different package paths).
 */
package io.sm8.core.engine

import io.sm8.core.schema.Field

/**
 * Engine-portable schema for query results.
 *
 * @param fields the ordered list of result fields (per the design
 *               doc §4.5.4). Field names are wire-stable.
 */
final case class ResultSchema(
    fields: List[Field] = Nil
) extends Product with Serializable {

  /** Number of fields in the schema. */
  def size: Int = fields.size

  /** Look up a field by name. Returns `None` if not found. */
  def field(name: String): Option[Field] = fields.find(_.name == name)

  /** True iff the schema has no fields. */
  def isEmpty: Boolean = fields.isEmpty
}