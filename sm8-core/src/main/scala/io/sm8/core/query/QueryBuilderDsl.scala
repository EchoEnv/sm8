/*
 * SM8 Core -- QueryBuilderDsl: typed fluent builder (the current implementation, the design contract current implementation).
 * The phantom
 * `[D]` flows through fluent method wildcards `[_]`; the accumulator
 * stores `Nothing`-typed projections. At the typed-to-untyped
 * boundary (the accumulator field), we use explicit
 * `.asInstanceOf[Seq[Foo[Nothing]]]` casts with explanatory comments
 * -- this is the the current implementation documented pattern for the variance-coercion
 * boundary.
 * the current implementation (the design contract SSfilter/where): added `filter()` / `where()`
 * overloads + the `filters: Seq[TypedPredicate[Nothing]]` accumulator
 * field.
 */
package io.sm8.core.query

import io.sm8.core.engine.QueryRequest
import io.sm8.core.model.TypedDimension
import io.sm8.core.rel.{AggregateFn, ComparisonOp, Having, PartitionBy, SortDirection, TypedAggregateCall, TypedPredicate, TypedSortKey, TypedSortKeyOps, TypedWindow, WindowFunction}

/** Typed fluent builder for `QueryRequest`.
 * Per the "Both via overloads" shape decision: each fluent method has
 * TWO overloads -- one with `[_]` wildcards (typed, any phantom) and
 * one with `String` (quick path for legacy/audit use cases).
 */
object QueryBuilderDsl {

 /** Internal accumulator (Nothing-typed fields for variance-safety
 * in the wire DTO; the typed witnesses are coerced to Nothing
 * at the call-site boundary via explicit `asInstanceOf`). */
 final case class BuiltQuery(
  aggregateMeasures: Seq[TypedAggregateCall[Nothing]] = Nil,
  having:    Seq[Having[Nothing]]    = Nil,
  partitionBy:  Seq[PartitionBy[Nothing]]   = Nil,
  orderBy:   Seq[TypedDimension[Nothing]]  = Nil,
  window:    Seq[TypedWindow[Nothing, Nothing]] = Nil,
  limit:    Option[Long]      = None,
  whereFilters:  Seq[TypedPredicate[Nothing]]  = Nil,
  sortDirections:  Seq[SortDirection]     = Nil) {

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
  * Per the design contract: this also populates the default `orderBy`. */
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

 /** Per the current implementation (the design contract SSExtOrderBy) + senior reviews 2026-08-19:
  * typed order-by via the TypedSortKey extension (.asc /.desc).
  * Zips dim + direction into the parallel accumulator fields
  * `orderBy` + `sortKeys` (in lockstep). Per scala-bug-hunting-
  * mindset SS1 (erasure collision prevention): this is renamed
  * `orderByKeys` (NOT `orderBy`) -- the existing
  * `orderBy(dims: TypedDimension[_]*)` overload has the same
  * erasure signature (Seq[Any]) -- renaming to `orderByKeys`
  * preserves both APIs at the call site without Scala 2.13
  * erasure ambiguity.
  * Preserves backward compat: TypedDimension-only orderBy(.)
  * still produces only dim entries (no direction refinement).
  * Per  SS2 (Serializable preserved):
  * TypedSortKey extends Serializable (the current implementation closure-safety spec). */
 def orderByKeys(keys: TypedSortKey[_, _]*): BuiltQuery = {
  val dims: Seq[TypedDimension[Nothing]] =
  keys.toIndexedSeq.map(_.dimension).toSeq.asInstanceOf[Seq[TypedDimension[Nothing]]]
  val directs: Seq[SortDirection] =
  keys.toIndexedSeq.map(_.direction).toSeq.asInstanceOf[Seq[SortDirection]]
  copy(
  orderBy  = orderBy ++ dims,
  sortDirections = sortDirections ++ directs
  )
 }

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

 /** Add typed filter predicates (typed overload, any phantom).
  * Per the current implementation (the design contract SSfilter/where): the typed predicate is
  * applied via `df.filter(predicate)` BEFORE the aggregate path
  * (per the design contract current implementation spark connector end-to-end). The phantom
  * `[D]` is captured at construction (object-level Refs); the
  * accumulator coerces it to `Nothing` via `asInstanceOf` at the
  * variance boundary. */
 def filter(predicates: TypedPredicate[_]*): BuiltQuery =
  copy(whereFilters =
  (whereFilters ++ predicates.toSeq).asInstanceOf[Seq[TypedPredicate[Nothing]]]
  )

 /** Add typed filter predicate NAMES (string overload).
  * Per karpathy- §2 (simplicity): the string
  * overload is a convenience for quick YAML-style filtering;
  * it builds `Predicate.Compare(field, =, value)` AST nodes
  * and wraps them as `TypedPredicate[Nothing]`. */
 def filterNames(predicates: (String, io.sm8.core.predicate.CompareOp, Any)*): BuiltQuery =
  copy(whereFilters =
  (whereFilters ++ predicates.map { case (field, op, value) =>
   TypedPredicate.of(name = s"$field $op $value",
   predicate = io.sm8.core.predicate.Predicate.Compare(field, op, value))
  }.toSeq).asInstanceOf[Seq[TypedPredicate[Nothing]]]
  )

 /** Alias for `filter(.)` (the `where:` keyword in YAML
  * convention; per  SS1.3 -- mirror
  * the existing QueryRequest shape). */
 def where(predicates: TypedPredicate[_]*): BuiltQuery = filter(predicates: _*)

 /** Add the typed limit. */
 def limit(n: Option[Long]): BuiltQuery =
  copy(limit = n)
 /** Build the typed `QueryRequest` wire DTO. */
 def build(model: String, dimensions: Seq[String]): QueryRequest =
  QueryRequest(
  model    = model,
  dimensions  = dimensions,
  aggregateMeasures = aggregateMeasures,
  having   = having,
  partitionBy  = partitionBy,
  orderBy   = orderBy,
  window   = window,
  limit    = limit,
  whereFilters  = whereFilters,
  // Per the current implementation: forward sortDirections directly (set by
  // orderByKeys overload). Default Ascending for legacy 19
  // callers (sortDirections defaults to Nil).
  sortDirections = sortDirections
  )
 }

 /** Start the fluent builder (per karpathy- §2:
 * zero-arg factory for the empty accumulator). */
 def start(): BuiltQuery = BuiltQuery()
}
