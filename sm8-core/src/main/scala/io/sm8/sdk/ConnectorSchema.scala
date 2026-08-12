/*
 * SM8 SDK — ConnectorSchema.
 *
 * Concrete shape for what `Connector.schema()` returns. Promoted
 * from marker trait (Step 1) to a concrete case class (Step 2).
 *
 * The shape is intentionally minimal for Step 2: a list of column
 * names. The full portable schema (typed columns from the IR) lands
 * in Step 0 when `core.ResultSchema` moves from `semanticdf-core`.
 */
package io.sm8.sdk

/**
 * Minimal portable schema. Step 2's placeholder: list of column names.
 * Full shape (with typed columns from the IR) lands in Step 0.
 */
final case class ConnectorSchema(columns: List[String])