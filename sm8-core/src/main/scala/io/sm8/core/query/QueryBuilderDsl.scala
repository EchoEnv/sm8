/*
 * SM8 Core — QueryBuilderDsl: typed fluent builder (PR-18, ADR-008-R §PR-18).
 *
 * Per `debug-mantra` + `scala-bug-huntingmindset` §1: the phantom
 * `[D]` flows through fluent method wildcards `[_]`; the accumulator
 * stores `Nothing`-typed projections. At the typed→untyped boundary
 * (the accumulator field), we use explicit `.asInstanceOf[Seq[Foo[Nothing]]]`
 * casts with explanatory comments — this is the PR-16 documented
 * pattern for the variance-coercion boundary.
 */
package io.sm8.core.query

import io.sm8.core.engine.QueryRequest
import io.sm8.core.model.TypedDimension
import io.sm8.core.rel.{AggregateFn, ComparisonOp, Having, PartitionBy, TypedAggregateCall, TypedWindow, WindowFunction}

/** Typed fluent builder for `QueryRequest`.
 *
 * Per the "Both via overloads" shape decision: each fluent method has
 * TWO overloads — one with `[_]` wildcards (typed, any phantom) and
 * one with `String` (quick path for legacy/audit use cases).
 */
object QueryBuilderDsl {

  /** Internal accumulator (Nothing-typed fields for variance-safety
    * in the wire DTO; the typed witnesses are coerced to Nothing
    * at the call-site boundary via explicit `asInstanceOf`). */
  final case class BuiltQuery(
      aggregateMeasures: Seq[TypedAggregateCall[Nothing]]  = Nil,
      having:             Seq[Having[Nothing]]               = Nil,
      partitionBy:        Seq[PartitionBy[Nothing]]         = Nil,
      orderBy:            Seq[TypedDimension[Nothing]]       = Nil,
      window:             Seq[TypedWindow[Nothing, Nothing]] = Nil,
      limit:              Option[Long]                        = None,
  ) {

    /** Add typed aggregate measures (typed overload, any phantom).
      * Coerce `Seq[TypedAggregateCall[_]]` (varargs from typed input)
      * to `Seq[TypedAggregateCall[Nothing]]` (the accumulator field
      * type) via `asInstanceOf` at the variance boundary. */
    def aggregate(measures: TypedAggregateCall[_]*): BuiltQuery =
      copy(aggregateMeasures =
        (aggregateMeasures ++ measures.toSeq).asInstanceOf[Seq[TypedAggregateCall[Nothing]]]
      )

    /** Add typed aggregate measure NAMES (string overload). */
    def aggregateNames(names: String*): BuiltQuery =
      copy(aggregateMeasures =
        aggregateMeasures ++ names.map(n => TypedAggregateCall.of(n, AggregateFn.Count)).toSeq
      )

    /** Add typed group-by dimensions (typed overload, any phantom).
      * Per ADR-008-R: this also populates the default `orderBy`. */
    def groupBy(dims: TypedDimension[_]*): BuiltQuery =
      copy(orderBy =
        if (orderBy.isEmpty)
          (dims.toIndexedSeq.toSeq).asInstanceOf[Seq[TypedDimension[Nothing]]]
        else orderBy
      )

    /** Add typed group-by NAMES (string overload). */
    def groupByNames(names: String*): BuiltQuery =
      copy(orderBy =
        if (orderBy.isEmpty)
          (names.toIndexedSeq.map(n => TypedDimension.of(n)).toSeq).asInstanceOf[Seq[TypedDimension[Nothing]]]
        else orderBy
      )

    /** Add typed having predicates (typed overload, any phantom). */
    def having(predicates: Having[_]*): BuiltQuery =
      copy(having =
        (having ++ predicates.toSeq).asInstanceOf[Seq[Having[Nothing]]]
      )

    /** Add typed having predicate NAMES (string overload). */
    def havingNames(predicates: (String, ComparisonOp, io.sm8.core.expr.Expr)*): BuiltQuery =
      copy(having =
        (having ++ predicates.map { case (name, op, expr) =>
          Having(TypedDimension.of(name), op, expr)
        }.toSeq).asInstanceOf[Seq[Having[Nothing]]]
      )

    /** Add typed partition hints (typed overload, any phantom). */
    def partitionBy(dims: TypedDimension[_]*): BuiltQuery =
      copy(partitionBy =
        (partitionBy ++ dims.map(d => PartitionBy(d)).toSeq).asInstanceOf[Seq[PartitionBy[Nothing]]]
      )

    /** Add typed partition hint NAMES (string overload). */
    def partitionByNames(names: String*): BuiltQuery =
      copy(partitionBy =
        (partitionBy ++ names.map(n => PartitionBy(TypedDimension.of(n))).toSeq).asInstanceOf[Seq[PartitionBy[Nothing]]]
      )

    /** Add typed order-by columns (typed overload, any phantom). */
    def orderBy(dims: TypedDimension[_]*): BuiltQuery =
      copy(orderBy = (dims.toIndexedSeq.toSeq).asInstanceOf[Seq[TypedDimension[Nothing]]])

    /** Add typed order-by column NAMES (string overload). */
    def orderByNames(names: String*): BuiltQuery =
      copy(orderBy =
        (names.toIndexedSeq.map(n => TypedDimension.of(n)).toSeq).asInstanceOf[Seq[TypedDimension[Nothing]]]
      )

    /** Add typed window specs (typed overload, any phantom). */
    def window(windows: TypedWindow[_, _]*): BuiltQuery =
      copy(window =
        (window ++ windows.toSeq).asInstanceOf[Seq[TypedWindow[Nothing, Nothing]]]
      )

    /** Add typed window spec NAMES (string overload). */
    def windowNames(specs: (String, String, WindowFunction)*): BuiltQuery =
      copy(window =
        (window ++ specs.map { case (partition, order, fn) =>
          TypedWindow(TypedDimension.of(partition), TypedDimension.of(order), fn)
        }.toSeq).asInstanceOf[Seq[TypedWindow[Nothing, Nothing]]]
      )

    /** Add the typed limit. */
    def limit(n: Option[Long]): BuiltQuery =
      copy(limit = n)

    /** Build the typed `QueryRequest` wire DTO. */
    def build(model: String, dimensions: Seq[String]): QueryRequest =
      QueryRequest(
        model             = model,
        dimensions        = dimensions,
        aggregateMeasures = aggregateMeasures,
        having            = having,
        partitionBy       = partitionBy,
        orderBy           = orderBy,
        window            = window,
        limit             = limit
      )
  }

  /** Start the fluent builder (per `karpathy-guidelinesmindset` §2:
   * zero-arg factory for the empty accumulator). */
  def start(): BuiltQuery = BuiltQuery()
}
