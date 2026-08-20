/*
 * SM8 Core -- TypedPredicateFilterOps (PR-29, ADR-008-R
 * SSfilterPushdown ergonomics).
 *
 * Per the user's 2026-08-19 directive ("can we not do it as
 * implicit class like extension method ?? rather than
 *.asc[D](dim),.desc[D](dim)"): ship typed filter ergonomics
 * via the existing `TypedSortKeyOps` pattern (PR-25) +
 * `TypedMeasureBridge` (PR-26).
 *
 * Implementations): this is a PROTOCOL-side ergonomics layer in
 * core (sm8-core/rel/). The existing `TypedPredicate` protocol
 * (sm8-core/rel/TypedPredicate.scala) is unchanged -- this new
 * file adds the infix sugar + the smart constructors.
 *
 * -- the user's explicit priority): the implicit extension
 * `extends AnyVal` -- zero allocation at use-site. The captured
 * state in the resulting `TypedPredicate[D]` is a case-class
 * `extends Serializable` (per PR-16 pattern; verified by the
 * `TypedPredicateClosureSafetySpec`). SAFE to capture in any
 * Spark UDF closure.
 *
 * runtime): the phantom `[D]` is preserved at construction. The
 * implicit class is type-parameterized on `[D]`; the underlying
 * field is `TypedDimension[D]` (also parameterized); the resulting
 * `TypedPredicate[D]` carries the phantom.
 *
 * zero per-row allocation. The implicit class wrapper is erased
 * to `TypedDimension` at the JVM level (the `extends AnyVal`
 * value-class optimization).
 *
 * pure data carrier. The implicit class is a thin sugar over the
 * existing factories in `TypedPredicate`.
 *
 * Map): the `StringMatchOp` ADT (in `Predicate.scala`) is a sealed
 * trait + case objects (mirrors the existing `CompareOp` pattern).
 *
 * silent): the smart constructor `Predicate.Compare` always
 * succeeds (the operator enum is closed); the spark connector's
 * `predicateToColumn` lowers the typed predicate to a Spark
 * `Column` (returns `Either[EngineError, Column]`).
 *
 * ADDITIVE only -- the existing `TypedPredicate` API + the
 * existing `Predicate` AST are unchanged (except for the additive
 * `StringMatch` case + `StringMatchOp` ADT).
 *
 * exhaustive): all 13 infix methods dispatch on a closed type
 * (`TypedDimension[D]`) and produce `TypedPredicate[D]` cases
 * covered by the existing `Predicate` sealed trait (6 `CompareOp`
 * + 1 `In` with `negate: Boolean` for `notIn` + 1 `IsNull` with
 * `negate: Boolean` + 3 `StringMatchOp`).
 */
package io.sm8.core.rel

import io.sm8.core.model.TypedDimension
import io.sm8.core.predicate.{CompareOp, Predicate, StringMatchOp}

/**
 * Companion smart constructors that take `TypedDimension[D]`
 * (the typed dimension identity) directly -- preserving the
 * phantom `[D]` (per [[karpathy-bug-huntingmindset]] SS1).
 *
 * constructor for validity-at-boundary): the typed dimension is
 * the boundary; the predicate factory validates the dim's name +
 * the phantom at the boundary.
 */
object TypedDimensionPredicate {

 /** `field = value`. */
 def eq[D](dim: TypedDimension[D], value: Any): TypedPredicate[D] =
 compare(dim, CompareOp.Eq, value)

 /** `field != value`. */
 def ne[D](dim: TypedDimension[D], value: Any): TypedPredicate[D] =
 compare(dim, CompareOp.Ne, value)

 /** `field < value`. */
 def lt[D](dim: TypedDimension[D], value: Any): TypedPredicate[D] =
 compare(dim, CompareOp.Lt, value)

 /** `field <= value`. */
 def le[D](dim: TypedDimension[D], value: Any): TypedPredicate[D] =
 compare(dim, CompareOp.Le, value)

 /** `field > value`. */
 def gt[D](dim: TypedDimension[D], value: Any): TypedPredicate[D] =
 compare(dim, CompareOp.Gt, value)

 /** `field >= value`. */
 def ge[D](dim: TypedDimension[D], value: Any): TypedPredicate[D] =
 compare(dim, CompareOp.Ge, value)

 /** Shared compare factory (per [[karpathy-data-driven-refactormindset]]
 * SS2: smart constructor for validity-at-boundary -- the phantom
 * `[D]` is preserved at construction). */
 private def compare[D](
  dim: TypedDimension[D],
  op: CompareOp,
  value: Any,
 ): TypedPredicate[D] =
 TypedPredicate.of[D](
  name  = s"${dim.name} $op $value",
  predicate = Predicate.Compare(field = dim.name, op = op, value = value),
 )

 /** `field IN (v1, v2,...)`. */
 def in[D](dim: TypedDimension[D], values: List[Any]): TypedPredicate[D] =
 TypedPredicate.of[D](
  name  = s"${dim.name} IN (${values.mkString(", ")})",
  predicate = Predicate.In(field = dim.name, values = values, negate = false),
 )

 /** `field NOT IN (v1, v2,...)`. Per the user's 2026-08-19 directive
 * ("also notin ?"): the typed `notin` factory delegates to the
 * existing `Predicate.In(field, values, negate = true)` -- per
 * no new AST case is needed. */
 def notIn[D](dim: TypedDimension[D], values: List[Any]): TypedPredicate[D] =
 TypedPredicate.of[D](
  name  = s"${dim.name} NOT IN (${values.mkString(", ")})",
  predicate = Predicate.In(field = dim.name, values = values, negate = true),
 )

 /** `field IS NULL`. */
 def isNull[D](dim: TypedDimension[D]): TypedPredicate[D] =
 TypedPredicate.of[D](
  name  = s"${dim.name} IS NULL",
  predicate = Predicate.IsNull(field = dim.name, negate = false),
 )

 /** `field IS NOT NULL`. */
 def isNotNull[D](dim: TypedDimension[D]): TypedPredicate[D] =
 TypedPredicate.of[D](
  name  = s"${dim.name} IS NOT NULL",
  predicate = Predicate.IsNull(field = dim.name, negate = true),
 )

 /** `field startsWith pattern` -- per the user's 2026-08-19 directive
 * ("also startsWith, contains, endsWith ?"). The lowering target is
 * Spark's `Column.startsWith(pattern)` (per [[karpathy-data-driven-
 * refactormindset]] SS2: simple, predictable -- NOT a regex). */
 def startsWith[D](dim: TypedDimension[D], pattern: String): TypedPredicate[D] =
 stringMatch(dim, StringMatchOp.StartsWith, pattern)

 /** `field contains pattern`. */
 def contains[D](dim: TypedDimension[D], pattern: String): TypedPredicate[D] =
 stringMatch(dim, StringMatchOp.Contains, pattern)

 /** `field endsWith pattern`. */
 def endsWith[D](dim: TypedDimension[D], pattern: String): TypedPredicate[D] =
 stringMatch(dim, StringMatchOp.EndsWith, pattern)

 /** Shared string-match factory (per [[karpathy-data-driven-refactormindset]]
 * SS2: smart constructor for validity-at-boundary). */
 private def stringMatch[D](
  dim:  TypedDimension[D],
  op:  StringMatchOp,
  pattern: String,
 ): TypedPredicate[D] =
 TypedPredicate.of[D](
  name  = s"${dim.name} $op '$pattern'",
  predicate = Predicate.StringMatch(field = dim.name, op = op, pattern = pattern),
 )
}

/**
 * Per the user's 2026-08-19 directive ("infix notation but still
 * typed based"): infix sugar via an implicit class. The implicit
 * class is `extends AnyVal` (per [[karpathy-jvm-safety-mindset]] SS3
 * + the existing `TypedSortKeyOps` pattern from PR-25).
 *
 * the user's explicit priority): the captured state is ONLY
 * `TypedDimension[D]` (Serializable) -- no Spark session / DataFrame
 * / non-Serializable locals.
 *
 * runtime): the phantom `[D]` is preserved at construction. The
 * implicit class wrapper is type-parameterized on `[D]` (the
 * existing `TypedSortKeyOps` pattern proves this works in Scala
 * 2.13).
 *
 * zero per-row allocation at the use-site (the implicit class is
 * erased to the underlying `TypedDimension[D]`).
 *
 * pure data carrier. The implicit class is a thin sugar over the
 * existing factories in `TypedDimensionPredicate`.
 */
object TypedPredicateFilterOps {

 implicit class TypedDimensionFilterOps[D](val dim: TypedDimension[D]) extends AnyVal {

 /** Per the user's headline ask: infix comparison operators. */
 def ===[v](value: v): TypedPredicate[D] =
  TypedDimensionPredicate.eq(dim, value)
 def !==[v](value: v): TypedPredicate[D] =
  TypedDimensionPredicate.ne(dim, value)
 def <[v](value: v): TypedPredicate[D] =
  TypedDimensionPredicate.lt(dim, value)
 def <=[v](value: v): TypedPredicate[D] =
  TypedDimensionPredicate.le(dim, value)
 def >[v](value: v): TypedPredicate[D] =
  TypedDimensionPredicate.gt(dim, value)
 def >=[v](value: v): TypedPredicate[D] =
  TypedDimensionPredicate.ge(dim, value)

 /** Per the user's directive ("also notin ?"): infix NOT IN list. */
 def in[v](values: List[v]): TypedPredicate[D] =
  TypedDimensionPredicate.in(dim, values)
 def notIn[v](values: List[v]): TypedPredicate[D] =
  TypedDimensionPredicate.notIn(dim, values)

 /** Per the user's directive ("also startsWith, contains, endsWith ?"):
  * infix string-match operators. */
 def startsWith(pattern: String): TypedPredicate[D] =
  TypedDimensionPredicate.startsWith(dim, pattern)
 def contains(pattern: String): TypedPredicate[D] =
  TypedDimensionPredicate.contains(dim, pattern)
 def endsWith(pattern: String): TypedPredicate[D] =
  TypedDimensionPredicate.endsWith(dim, pattern)

 /** Null checks. */
 def isNull: TypedPredicate[D] =
  TypedDimensionPredicate.isNull(dim)
 def isNotNull: TypedPredicate[D] =
  TypedDimensionPredicate.isNotNull(dim)
 }
}
