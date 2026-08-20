/*
 * SM8 Core -- Predicate AST.
 * Engine-portable filter AST. Sealed trait + case classes per
 * smart And/Or builders (per ).
 * * changes. the adapter consumes this.
 * * Implementations): this is the filter protocol in core (sm8-core).
 * The connector (sm8-connector) consumes it. The the current implementation ergonomic
 * sugar (typed factories + implicit-class extension) lives in
 * `sm8-core/rel/TypedPredicate.scala` -- the AST stays pure.
 * * the user's explicit priority): the AST is pure data (case classes
 * + sealed traits) -- no captured non-Serializable state. Safe to
 * capture in any Spark UDF closure.
 */
package io.sm8.core.predicate

/**
 * Filter AST for SM8 queries. Sealed trait + case classes. Each
 * leaf carries exactly the data needed for its semantics; no
 * engine-specific types in scope.
 */
sealed trait Predicate extends Product with Serializable {

 /** Field names referenced by this predicate. */
 def fields: Set[String]

 /** Human-readable description. */
 def describe: String

 /** Combine with another predicate via AND. */
 def and(other: Predicate): Predicate = Predicate.And(List(this, other))

 /** Combine via OR. */
 def or(other: Predicate): Predicate = Predicate.Or(List(this, other))

 /** Negate this predicate. */
 def negatePredicate: Predicate = Predicate.Not(this)
}

object Predicate {

 /** Field comparison: `field <op> value` where op is one of =, !=, <, <=, >, >=. */
 final case class Compare(
  field: String,
  op: CompareOp,
  value: Any
 ) extends Predicate {
 override def fields: Set[String] = Set(field)
 override def describe: String = s"$field $op $value"
 }

 /** `field IN (v1, v2,.) [NOT IN]`. */
 final case class In(
  field: String,
  values: List[Any],
  negate: Boolean = false
 ) extends Predicate {
 override def fields: Set[String] = Set(field)
 override def describe: String = if (negate) s"$field NOT IN (${values.mkString(", ")})"
        else s"$field IN (${values.mkString(", ")})"
 }

 /** `field IS [NOT] NULL`. */
 final case class IsNull(field: String, negate: Boolean = false) extends Predicate {
 override def fields: Set[String] = Set(field)
 override def describe: String = if (negate) s"$field IS NOT NULL" else s"$field IS NULL"
 }

 /** Logical AND of children. Smart constructor per --
 * folds a single child to avoid redundant wrapping. */
 final case class And(children: List[Predicate]) extends Predicate {
 override def fields: Set[String] = children.flatMap(_.fields).toSet
 override def describe: String = children.map(_.describe).mkString(" AND ")
 }

 /** Logical OR. Smart constructor folds single child. */
 final case class Or(children: List[Predicate]) extends Predicate {
 override def fields: Set[String] = children.flatMap(_.fields).toSet
 override def describe: String = children.map(_.describe).mkString(" OR ")
 }

 /** Logical NOT. */
 final case class Not(pred: Predicate) extends Predicate {
 override def fields: Set[String] = pred.fields
 override def describe: String = s"NOT (${pred.describe})"
 }

 /**
 * Smart constructor: collapses a singleton AND/OR into its only
 * child (per "smart constructor for
 * validity-at-boundary" applied to shape normalization).
 */
 def and(children: List[Predicate]): Predicate = children match {
 case Nil  => And(Nil) // empty AND is true
 case List(p) => p
 case many  => Predicate.And(many)
 }

 /** Smart constructor for OR (single-child collapses). */
 def or(children: List[Predicate]): Predicate = children match {
 case List(p) => p
 case many => Predicate.Or(many)
 }

 /**
 * the current implementation (the design contract SSfilterPushdown ergonomics): string-match
 * predicate. * (sealed over Map): a sealed trait + case objects (mirrors the
 * existing `CompareOp` ADT pattern). The pattern is NOT a regex
 * (
 * predictable). Spark's `Column.startsWith/contains/endsWith` is
 * the lowering target.
 * * ADDITIVE only -- existing match sites don't need to handle
 * the new case (Scala 2.13 sealed-trait matches warn but don't
 * break).
 */
 final case class StringMatch(
  field: String,
  op:  StringMatchOp,
  pattern: String
 ) extends Predicate {
 override def fields: Set[String] = Set(field)
 override def describe: String = s"$field ${op.toString.toUpperCase} '$pattern'"
 }
}

/** Comparison operator enum (sealed trait + case objects). */
sealed trait CompareOp
object CompareOp {
 case object Eq extends CompareOp { override def toString = "=" }
 case object Ne extends CompareOp { override def toString = "!=" }
 case object Lt extends CompareOp { override def toString = "<" }
 case object Le extends CompareOp { override def toString = "<=" }
 case object Gt extends CompareOp { override def toString = ">" }
 case object Ge extends CompareOp { override def toString = ">=" }
}

/** the current implementation: string-match operator enum (sealed trait + case objects per
 * cases mirror Spark's `Column.startsWith/contains/endsWith` API
 * (the lowering target). */
sealed trait StringMatchOp
object StringMatchOp {
 case object StartsWith extends StringMatchOp { override def toString = "startsWith" }
 case object Contains extends StringMatchOp { override def toString = "contains" }
 case object EndsWith extends StringMatchOp { override def toString = "endsWith" }
}
