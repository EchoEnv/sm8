/*
 * SM8 Core — ExprParseError.
 *
 * Typed parse error for the `Expr` parser. Per
 * returns `Either[ExprParseError, Expr]`, never throws.
 *
 * are separate"): ExprParseError covers PARSE failures (shape-level
 * — unclosed paren, unexpected token, invalid literal). The
 * downstream `Model.of(...)` / `ModelBuilder.build(...)` smart
 * constructor covers VALIDITY failures (domain-level — name not
 * blank). ExprParseError stays distinct from ManifestError so
 * callers can tell "the SQL expression is malformed" from "the
 * YAML parsed but the domain rules reject it".
 *
 * ==Spark concerns (per user directive)==
 *
 * sm8-core is Spark-free per the plan's inverted enforcer
 * pattern. The ExprParseError type has zero Spark references.
 *
 *
 */
package io.sm8.core.expr

/** Typed error from `Expr` parsing. */
sealed trait ExprParseError extends Product with Serializable {
 def message: String
}

object ExprParseError {

 /** The input string is empty. */
 final case object EmptyInput extends ExprParseError {
 val message: String = "Expression is empty"
 }

 /** An unexpected character was encountered. */
 final case class UnexpectedToken(
  token: String,
  position: Int,
  reason: String
 ) extends ExprParseError {
 val message: String = s"Unexpected token '$token' at position $position: $reason"
 }

 /** A literal value could not be parsed. */
 final case class InvalidLiteral(
  raw: String,
  reason: String
 ) extends ExprParseError {
 val message: String = s"Invalid literal '$raw': $reason"
 }

 /** An opening paren / bracket / block was not closed. */
 final case class UnclosedDelimiter(
  opening: Char,
  position: Int
 ) extends ExprParseError {
 val message: String =
  s"Unclosed delimiter '$opening' starting at position $position"
 }

 /** The parser ran out of tokens while expecting more. */
 final case object UnexpectedEnd extends ExprParseError {
 val message: String = "Unexpected end of expression"
 }

 /** Catch-all for other parse failures. */
 final case class Other(reason: String) extends ExprParseError {
 val message: String = s"Parse failure: $reason"
 }
}
