/*
 * SM8 Core — QueryParts: typed aggregate predicates + partition hints + window specs (PR-17, ADR-008-R).
 *
 * Per ADR-008-R §"PR-17 Core types": this file consolidates the 4
 * small typed builders (`Having[D]`, `PartitionBy[D]`, `ComparisonOp`,
 * `WindowFunction`, `TypedWindow[D, M]`) into one module because
 * they are tightly coupled (a `Having` carries a `ComparisonOp`; a
 * `TypedWindow` carries a `WindowFunction` + a partition + an order).
 *
 * Per 
 * concern per file is the rule, but these 4 types are NOT separate
 * concerns — they form a single typed-DSL primitive group.
 *
 * Per 
 * implementations): these are the Protocols in core. Witness
 * INSTANCES live in the consumer's code (`object Refs {. }`).
 *
 * Per 
 * explicit concern): all 5 types `extends Serializable`. Per PR-16
 * lesson: case-class `Impl` form (not anonymous-class) for the typed
 * builders that have phantom type params (`Having[D]`, `PartitionBy[D]`,
 * `TypedWindow[D, M]`). Per 
 * resource capture.
 *
 * Per 
 * `ComparisonOp` + `WindowFunction` are sealed ADTs (6 + 3 cases).
 * Compiler-enforced exhaustiveness prevents silent typos at the
 * consumer side (e.g. `ComparisonOp.EQ` vs `ComparisonOp.EQUEAL`).
 *
 * Per 
 * types are case-class instances that allocate once at query-build
 * time (driver-side). Zero per-row allocation.
 */
package io.sm8.core.rel

import io.sm8.core.expr.Expr

/**
 * Comparison operator for having predicates. Sealed ADT with 6 cases.
 * Per 
 * ComparisonOp]` would let callers pass `"eq"` / `"EQ"` / `"=="` with
 * silent defaulting — the sealed ADT prevents that.
 */
sealed trait ComparisonOp extends Serializable

object ComparisonOp {
 case object EQ extends ComparisonOp
 case object NE extends ComparisonOp
 case object LT extends ComparisonOp
 case object LE extends ComparisonOp
 case object GT extends ComparisonOp
 case object GE extends ComparisonOp
}

/**
 * Typed having predicate. The phantom `[D]` matches the column
 * identity — `Refs.totalAmount` (a `TypedMeasure[TotalAmount]`) can be
 * used as the dimension in a having clause; the compiler verifies
 * the type identity.
 *
 * Per 
 * is a typed `Expr` (NOT `Option[Expr]` or `String`) — no silent
 * defaulting.
 */
final case class Having[D](
 dimension: io.sm8.core.model.TypedDimension[D],
 op:  ComparisonOp,
 value:  Expr
) extends Serializable

object Having {
 /** Convenience factory. */
 def apply[D](
  dim: io.sm8.core.model.TypedDimension[D],
  op: ComparisonOp,
  value: Expr
 ): Having[D] = new Having[D](dim, op, value)
}

/**
 * Typed partition hint. Spark connector MAY honor it via
 * `df.partitionBy(col)` (best-effort + log per `scala-spark-batch-bugs-
 * mindset` §2: AQE may override). The phantom `[D]` matches the
 * dimension identity.
 *
 * Per 
 * the connector decides whether to honor it.
 */
final case class PartitionBy[D](
 dim: io.sm8.core.model.TypedDimension[D]
) extends Serializable

object PartitionBy {
 def apply[D](dim: io.sm8.core.model.TypedDimension[D]): PartitionBy[D] =
 new PartitionBy[D](dim)
}

/**
 * Window function family. Sealed ADT with 3 cases (rank-only minimal
 * per user choice + ADR-008-R §"Window function scope").
 *
 * Per ADR-008-R: future PRs may add Lag / Lead / PercentRank /
 * CumeDist / Ntile / FirstValue / LastValue. For v0.1.0, the rank-only
 * minimal set is sufficient (per `karpathy-` §2).
 */
sealed trait WindowFunction extends Serializable

object WindowFunction {
 /** `ROW_NUMBER() OVER (PARTITION BY. ORDER BY.)` — assigns a
 * unique sequential integer to each row in its partition. */
 case object RowNumber extends WindowFunction

 /** `RANK() OVER (PARTITION BY. ORDER BY.)` — assigns a rank with
 * gaps (ties get the same rank; next rank skipped). */
 case object Rank extends WindowFunction

 /** `DENSE_RANK() OVER (PARTITION BY. ORDER BY.)` — assigns a rank
 * without gaps (ties get the same rank; next rank NOT skipped). */
 case object DenseRank extends WindowFunction
}

/**
 * Typed window spec. Single combined shape per user choice (per
 * ADR-008-R §"Window shape"). The phantom `[D]` matches the
 * partition-by column identity; `[M]` matches the result column
 * identity (the rank column).
 *
 * Per  §3.1 (Protocols before
 * implementations): this is the Protocol. The witness INSTANCE
 * lives in `object Refs {. }`.
 */
final case class TypedWindow[D, M](
 partitionBy: io.sm8.core.model.TypedDimension[D],
 orderBy:  io.sm8.core.model.TypedDimension[D],
 windowFn: WindowFunction
) extends Serializable {
 def name: String = s"${windowFn}_${orderBy.name}"
}

object TypedWindow {
 def apply[D, M](
  partitionBy: io.sm8.core.model.TypedDimension[D],
  orderBy:  io.sm8.core.model.TypedDimension[D],
  windowFn: WindowFunction
 ): TypedWindow[D, M] = new TypedWindow[D, M](partitionBy, orderBy, windowFn)
}
