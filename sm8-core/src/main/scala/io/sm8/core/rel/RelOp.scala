/*
 * SM8 Core — RelOp (engine-portable relational-plan IR).
 *
 * Per the v0.1.0 IR extension plan (ADR-007 + RFC §3): the
 * relational-plan IR is the runtime execution shape. It flows
 * through `Engine.compile(model: RelOp, ...)`, the engine adapter's
 * expression-compile step (each case becomes a native operation —
 * Spark `Dataset` ops, Trino SQL clauses, etc.), and the MCP wire
 * format (for `explain` tool output).
 *
 * Per [[karpathy-guidelines-mindset]]: ported from the legacy
 * `/tmp/semanticdf/semanticdf-core/src/main/scala/io/semanticdf/core/rel/RelOp.scala`
 * with the same 7 nodes (Scan, Filter, Project, Aggregate, Join,
 * Sort, Limit).
 *
 * Per [[scala-data-driven-refactor-mindset]]: sealed trait + 7
 * case classes. A free-form `plan: String` would let engines
 * invent new plan shapes that the validator and compiler couldn't
 * classify. The closed ADT forces every component to handle the
 * closed set of plan nodes.
 *
 * Per RFC §3: engine-portable; engine-specific compile lives in
 * the adapter (Spark's `LogicalPlan`, Trino's `LogicalPlanner`,
 * DuckDB's `QueryPlan`).
 *
 * Per [[scala-error-handling-mindset]]: set operations (Union,
 * Intersect, Except), window functions, and streaming sinks are
 * DEFERRED to v0.2.0+ per the legacy design. They can be expressed
 * via combination of the existing nodes if needed. Adding them
 * is a contract change (per `adapters.md` "If a new capability
 * type is needed... that's a contract change").
 *
 * Per [[scala-jvm-safety-mindset]]: zero spark imports. Boundary
 * contract:
 *   `grep -r 'org.apache.spark' sm8-core/src/main/scala/io/sm8/core/rel/RelOp.scala`
 *
 * Per [[scala-impact-analysis-mindset]]: `Filter.predicate: Expr`
 * (not `Predicate`) — at runtime, filters are expressions (e.g.
 * `price > 100`). The higher-level `core.predicate.Predicate`
 * filter language gets compiled into `Expr` for the IR. The IR
 * carries `Expr` because that's what engines actually execute.
 */
package io.sm8.core.rel

import io.sm8.core.expr.Expr
import io.sm8.core.schema.Field

sealed trait RelOp extends Product with Serializable

object RelOp {

  /** Read from a resolved source. The terminal node of any plan.
    *
    * @param sourceRef  the original `SourceRef` (engine-agnostic reference;
    *                   the engine adapter resolves it to a native table/view)
    * @param schema     the expected schema (after source resolution —
    *                   the engine adapter validates the actual source's
    *                   schema matches this; a mismatch yields
    *                   `EngineError.SourceSchemaChanged`)
    * @param projection the columns to read (column pruning — the engine
    *                   adapter reads only these from the source, not the
    *                   full set)
    */
  final case class Scan(
      sourceRef:  io.sm8.core.model.SourceRef,
      schema:     List[Field],
      projection: List[Expr],
  ) extends RelOp

  /** Apply a predicate to a child. Maps to Spark's `Filter`,
    * Trino's `WHERE` clause.
    *
    * @param input     the child node
    * @param predicate the predicate expression (returns a boolean)
    */
  final case class Filter(
      input:     RelOp,
      predicate: Expr,
  ) extends RelOp

  /** Compute expressions into named columns. Maps to Spark's
    * `Project`, Trino's `SELECT` clause.
    *
    * @param input       the child node
    * @param expressions the projected expressions and their
    *                    aliases (the `String` is the alias)
    */
  final case class Project(
      input:       RelOp,
      expressions: List[(Expr, String)],
  ) extends RelOp

  /** Group by expressions and apply aggregate calls. Maps to
    * Spark's `Aggregate`, Trino's `GROUP BY` clause.
    *
    * @param input      the child node
    * @param groupBy    the group-by expressions (the columns to
    *                   partition by)
    * @param aggregates the aggregate calls (Sum / Count / Avg /
    *                   etc.)
    */
  final case class Aggregate(
      input:      RelOp,
      groupBy:    List[Expr],
      aggregates: List[AggregateCall],
  ) extends RelOp

  /** Combine two children with a join kind and an optional
    * condition. Maps to Spark's `Join`, Trino's `JOIN` clause.
    *
    * @param left      the left child
    * @param right     the right child
    * @param kind      the join kind (Inner / Left / Right / Full
    *                  / Cross)
    * @param condition the join condition (for `Cross`, this is
    *                  unused — the join is unconditional)
    */
  final case class Join(
      left:      RelOp,
      right:     RelOp,
      kind:      JoinKind,
      condition: Expr,
  ) extends RelOp

  /** Order a child by sort keys. Maps to Spark's `Sort`, Trino's
    * `ORDER BY` clause.
    *
    * @param input the child node
    * @param keys  the sort keys (each is an `Expr` + direction +
    *              null ordering)
    */
  final case class Sort(
      input: RelOp,
      keys:  List[SortKey],
  ) extends RelOp

  /** Take a slice of a child. Maps to Spark's `Limit`, Trino's
    * `LIMIT ... OFFSET ...` clause.
    *
    * @param input  the child node
    * @param count  the maximum number of rows to return
    * @param offset the number of rows to skip before returning
    */
  final case class Limit(
      input:  RelOp,
      count:  Long,
      offset: Long = 0L,
  ) extends RelOp
}
