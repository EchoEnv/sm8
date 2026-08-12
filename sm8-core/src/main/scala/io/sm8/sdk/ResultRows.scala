/*
 * SM8 SDK — ResultRows.
 *
 * Concrete shape for the rows returned by `Connector.query()`. Promoted
 * from marker trait (Step 1) to a concrete case class (Step 2).
 *
 * The shape is intentionally minimal for Step 2: a Vector of
 * `Map<String, Any>` rows. The full portable row shape (typed columns
 * via the IR) lands in Step 0 when `core.ResultRows` moves from
 * `semanticdf-core`.
 *
 * Per RFC adapters.md Rule 3 + the Connector conformance contract:
 * `query()` returns rows that match `schema()`. Connector authors
 * enforce this by either:
 *   - extending `ConnectorContractSpec` (which asserts the match); or
 *   - overriding `rowsMatchSchema` for a richer notion of "match".
 */
package io.sm8.sdk

/**
 * Minimal portable row shape. Step 2's placeholder.
 * Full shape (with typed columns from the IR) lands in Step 0.
 */
final case class ResultRows(rows: Vector[Map[String, Any]])