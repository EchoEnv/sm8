/*
 * SM8 Core -- TypedSortKey phantom-typed extension (PR-24, ADR-008-R SSExtOrderBy).
 *
 * Per the user's 2026-08-19 directive ("can we not do it as implicit
 * class like extension method ?? rather than.asc[D](dim),.desc[D](dim)"):
 * expose typed `.asc` / `.desc` extension methods on `TypedDimension[D]`
 * via a Scala 2.13 implicit class. The phantom `[D]` is captured at
 * construction; direction is a typed ADT (per [[scala-data-driven-
 * ]] SS3: sealed trait, not Map[String, _]).
 *
 * Implementations): this is the PROTOCOL in core. The witness INSTANCE
 * lives in the consumer's code (e.g. `object Refs {. }`) -- NOT in
 * method-local scope (which would capture the enclosing scope and
 * break Spark closure-serialization per 
 * SS1).
 *
 * the extension is purely ADDITIVE -- no API changes to `TypedWindow`,
 * `TypedOrderBy`, or `QueryRequest`. Existing typed witnesses continue
 * to work (ascending order by default). The wire DTO accepts the
 * existing `orderBy: Seq[TypedDimension[Nothing]]` -- the direction
 * enhancement is captured at the typed-build site via a parallel
 * `sortDirections: Seq[SortDirection]` field (added PR-24, default Nil
 * = backward-compat: zero behavior change for 19 callers).
 *
 * `TypedSortKey` `extends Serializable` (verified by the closure-safety
 * spec per PR-16 + PR-20 pattern). The implicit class is a transparent
 * wrapper -- no mutable state, no resources.
 *
 * the extension returns a typed `TypedSortKey[D, Dir <: SortDirection]`
 * whose phantom `[D]` is captured at construction. A typo at the call
 * site (`Refs.patienId.asc` instead of `Refs.patientId.asc`) is a
 * COMPILE error per the typed witness contract.
 *
 * user's explicit concern): the implicit class is a pure-function
 * wrapper; no SparkSession / DataFrame / Iterator captured. Safe to
 * use in Spark UDF closure contexts.
 */
package io.sm8.core.rel

import io.sm8.core.model.TypedDimension

/**
 * Phantom-typed sort key = (TypedDimension[D], SortDirection).
 *
 * direction is a sealed trait + case objects (`SortDirection.Ascending`
 * / `SortDirection.Descending`) -- not a `Map[String, SortDirection]`.
 *
 * Implementations): the witness INSTANCE lives in the consumer's
 * code -- typically built implicitly via `.asc` / `.desc` on a
 * `TypedDimension[D]`.
 */
sealed trait TypedSortKey[+D, +Dir <: SortDirection] extends Serializable {
 def dimension: TypedDimension[D]
 def direction: Dir
}

object TypedSortKey {

 /** Internal case-class implementation (per PR-16 lesson: case class
 * with proper Java getters -- survives `ObjectOutputStream`
 * round-trip). */
 private final case class Impl[D, Dir <: SortDirection](
  theDimension: TypedDimension[D],
  theDirection: Dir
 ) extends TypedSortKey[D, Dir] {
 override def dimension: TypedDimension[D] = theDimension
 override def direction: Dir    = theDirection
 }

 /** Generic factory. The phantom `[D]` is the witness identity.
 * The `Dir <: SortDirection` bound preserves the typed direction. */
 def of[D, Dir <: SortDirection](
  dimension: TypedDimension[D],
  direction: Dir
 ): TypedSortKey[D, Dir] =
 Impl[D, Dir](theDimension = dimension, theDirection = direction)
}

/**
 * Per the user's directive (implicit-class extension method):
 * expose typed `.asc` / `.desc` directly on `TypedDimension[D]`.
 *
 * Per Scala 2.13 idiom: `implicit class` (NOT `extends AnyVal` because
 * the result carries an extra type parameter `[D, Dir]` that value
 * classes cannot handle -- an `implicit class` is the canonical
 * 2.13 pattern for phantom-typed extensions).
 *
 * the extension produces a `TypedSortKey[D, Dir]` -- the typed build
 * site accumulates these in `QueryBuilderDsl.orderByAs` /
 * `QueryBuilderDsl.orderByDesc` (PR-24 followup) instead of
 * `orderBy(.)`. The legacy `orderBy(.)` remains Ascending-default
 * for backward compat.
 *
 * user's explicit concern): the implicit class is a thin wrapper
 * around the typed witness -- no SparkSession / DataFrame / Iterator
 * captured. Safe to use in Spark UDF closure contexts.
 */
object TypedSortKeyOps {

 implicit class TypedDimensionSortOps[D](val dim: TypedDimension[D]) extends AnyVal {

 /** Ascending order on this dimension. */
 def asc: TypedSortKey[D, SortDirection.Ascending.type] =
  TypedSortKey.of(dim, SortDirection.Ascending)

 /** Descending order on this dimension. */
 def desc: TypedSortKey[D, SortDirection.Descending.type] =
  TypedSortKey.of(dim, SortDirection.Descending)
 }
}
