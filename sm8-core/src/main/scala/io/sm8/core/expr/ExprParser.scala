/*
 * SM8 Core — ExprParser.
 *
 * A small recursive-descent parser that converts a raw SQL-like
 * expression string into the engine-portable `io.sm8.core.expr.Expr`
 * AST. No Spark, no data-source knowledge, no plugin/hook — pure
 * core IR factory per RFC §3 Core Boundary.
 *
 * ==Scope (per "smallest correct change")==
 *
 * Supports the common subset:
 * - Literals: integers (42, -3), floats (3.14), strings ("hello"),
 *  booleans (true, false).
 * - Field references: bare identifiers ("age").
 * - Arithmetic: + - * / %, with left-associative precedence.
 * - Comparison: = != < <= > >=, with non-associative (binary only).
 *  Per SQL convention, single `=` is equality.
 * - Boolean: and, or, not (unary prefix).
 * - Parens for grouping.
 *
 * - `CAST`-style suffix (`x AS INT`) and `IS [NOT] NULL` postfix.
 * - `CASE WHEN c THEN v [WHEN c THEN v]* [ELSE e] END` (PR-M1,
 *  per ADR-008-L Appendix GAP 1). Missing ELSE lowers to a
 *  `Literal(NullValue)` per SQL semantics.
 * - `expr AS aliasName` when the token after AS is NOT a known
 *  type name (PR-M1): lowers to `Expr.Alias`. Known type names
 *  keep the `Cast` reading -- types win over column aliases
 *  (documented disambiguation rule).
 * - `all(name)` / `measure(name)` rewrite (PR-M1, mirrors the
 *  legacy CalcExpr DSL per DESIGN.md SS6.2): lower to
 *  `Expr.All(name)` / `Expr.MeasureRef(name)` instead of a
 *  generic FunctionCall.
 *
 * NOT in v1 (deferred):
 * - UDF registry resolution for arbitrary `FunctionCall` names.
 *
 * ==Recursive descent==
 *
 * + integer index. No `String.substring` (O(n) on a JDK `String`
 * slice — O(n²) over the parse).
 *
 * ==RFC + plan alignment==
 *
 * Per agile-kindling-beacon plan line 211: SM8 uses
 * `core.expr.Expr` directly (PR #45 design choice in
 * FilterSpec.scala). The ExprParser produces `Expr`, not a separate
 * `Predicate` type.
 *
 * ==Spark concerns (per user directive)==
 *
 * - mantras #1, #5: no Spark types, no executor-side closures.
 * - mantra #3 (schema-drift verify at boundary): typed ExprParseError.
 * - mantras #2, #4: N/A (no shuffle, no writes).
 *
 * Serialize: every `Expr` case class extends `Expr extends Product
 * with Serializable`. Parser output is auto-Serializable.
 */
package io.sm8.core.expr

import io.sm8.core.schema.SealedDataType

import scala.annotation.tailrec

/**
 * Recursive-descent parser for `Expr`.
 */
object ExprParser {

 private val ZeroInt: Expr =
 Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int)

 private val negate: Expr => Expr = e => Expr.Subtract(ZeroInt, e)

 private val addFn: (Expr, Expr) => Expr = Expr.Add
 private val subFn: (Expr, Expr) => Expr = Expr.Subtract
 private val mulFn: (Expr, Expr) => Expr = Expr.Multiply
 private val divFn: (Expr, Expr) => Expr = Expr.Divide
 private val modFn: (Expr, Expr) => Expr = Expr.Modulo
 private val eqFn: (Expr, Expr) => Expr = Expr.Equal
 private val neqFn: (Expr, Expr) => Expr = Expr.NotEqual
 private val ltFn: (Expr, Expr) => Expr = Expr.LessThan
 private val leFn: (Expr, Expr) => Expr = Expr.LessOrEqual
 private val gtFn: (Expr, Expr) => Expr = Expr.GreaterThan
 private val geFn: (Expr, Expr) => Expr = Expr.GreaterOrEqual

 def parseExpr(input: String): Either[ExprParseError, Expr] = {
 val src = input.trim
 if (src.isEmpty) Left(ExprParseError.EmptyInput)
 else parse(src)
 }

 private def parse(input: String): Either[ExprParseError, Expr] = {
 val p = new Cursor(input.toVector)
 p.skipWhitespace()
 p.parseOrExpr().flatMap { e =>
  p.skipWhitespace()
  if (!p.atEnd)
  Left(ExprParseError.UnexpectedToken(
   token = p.peekText(10),
   position = p.position,
   reason = "expected end of expression"))
  else
  Right(e)
 }
 }

 private final class Cursor(val chars: Vector[Char]) {
 var position: Int = 0

 def atEnd: Boolean = position >= chars.length

 def peekText(n: Int): String =
  chars.slice(position, math.min(position + n, chars.length)).mkString

 def peekChar(): Char =
  if (atEnd) '\u0000' else chars(position)

 def advance(): Char = {
  val c = chars(position)
  position += 1
  c
 }

 def skipWhitespace(): Unit =
  while (position < chars.length && chars(position).isWhitespace)
  position += 1

 def peekNonWs(): Char = {
  var p = position
  while (p < chars.length && chars(p).isWhitespace) p += 1
  if (p >= chars.length) '\u0000' else chars(p)
 }

 def startsWithWord(word: String): Boolean = {
  var p = position
  var i = 0
  while (p < chars.length && chars(p).isWhitespace) p += 1
  while (i < word.length && (p + i) < chars.length && chars(p + i) == word.charAt(i)) i += 1
  i == word.length && {
  val next = p + i
  next >= chars.length || !chars(next).isLetterOrDigit
  }
 }

 def consumeWord(word: String): Boolean = {
  if (startsWithWord(word)) {
  var q = position + word.length
  while (q < chars.length && chars(q).isWhitespace) q += 1
  position = q
  true
  } else false
 }

 /** PR #51 (smoke-test fix): case-insensitive variant.
  * boolean operators + AS keyword are conventionally
  * case-insensitive. The end-to-end smoke test encodes this
  * user contract; the parser must accept `AND`/`OR`/`NOT`/`AS`
  * in any case. Uses `startsWithWordCaseInsensitive` (PR #50
  * helper) for detection, then advances `word.length` chars
  * (case-preserved). */
 def consumeWordCaseInsensitive(word: String): Boolean = {
  if (startsWithWordCaseInsensitive(word)) {
  var q = position + word.length
  while (q < chars.length && chars(q).isWhitespace) q += 1
  position = q
  true
  } else false
 }

 def parseOrExpr(): Either[ExprParseError, Expr] =
  parseAndExpr().flatMap(loopOrExpr)
 
 /**
  * Iterative loop (while + Either short-circuit) for OR-chained operands.
  * Lifted from a nested `def` inside `parseOrExpr`'s `flatMap` per
  * ADR-008-AB (the nested closure-captured method cannot be
  * `@tailrec`-annotated in Scala 2.13). The `@tailrec` annotation
  * keeps the JVM stack at 1 frame per call, regardless of how many
 */
private def loopOrExpr(acc: Expr): Either[ExprParseError, Expr] = {
  var current: Expr = acc
  while (consumeWordCaseInsensitive("or")) {
   parseAndExpr() match {
    case Right(right) => current = Expr.Or(current, right)
    case left @ Left(_) => return left
   }
  }
  Right(current)
}
 
 def parseAndExpr(): Either[ExprParseError, Expr] =
  parseNotExpr().flatMap(loopAndExpr)
 
 /** Iterative loop (while + Either short-circuit) for AND-chained operands. See `loopOrExpr`. */
 private def loopAndExpr(acc: Expr): Either[ExprParseError, Expr] = {
  var current: Expr = acc
  while (consumeWordCaseInsensitive("and")) {
   parseNotExpr() match {
    case Right(right) => current = Expr.And(current, right)
    case left @ Left(_) => return left
   }
  }
  Right(current)
}
 
  def parseNotExpr(): Either[ExprParseError, Expr] =
   if (consumeWordCaseInsensitive("not")) parseNotExpr().map(Expr.Not)
   else parseCmpExpr()
 
  /** Comparison: `addExpr ((= | != | < | <= | > | >=) addExpr)?`
   * Per SQL convention, single `=` is equality. */
  def parseCmpExpr(): Either[ExprParseError, Expr] =
   parseAddExpr().flatMap { left =>
   skipWhitespace()
   val op: Option[(Expr, Expr) => Expr] = peekChar() match {
    case '=' if !chars.lift(position + 1).contains('=') =>
    advance(); Some(eqFn)
    case '!' if chars.lift(position + 1).contains('=') =>
    advance(); advance(); Some(neqFn)
    case '<' if chars.lift(position + 1).contains('=') =>
    advance(); advance(); Some(leFn)
    case '>' if chars.lift(position + 1).contains('=') =>
    advance(); advance(); Some(geFn)
    case '<' =>
    advance(); Some(ltFn)
    case '>' =>
    advance(); Some(gtFn)
    case _ => None
   }
   op match {
    case None => Right(left)
    case Some(b) =>
    skipWhitespace()
    parseAddExpr().map(right => b(left, right))
   }
   }
 
 def parseAddExpr(): Either[ExprParseError, Expr] =
  parseMulExpr().flatMap(loopAddExpr)
 
 /** Iterative loop (`@tailrec`) for +/- chained operands. See `loopOrExpr`. */
 private def loopAddExpr(acc: Expr): Either[ExprParseError, Expr] = {
  var current: Expr = acc
  var continue: Boolean = true
  while (continue) {
   skipWhitespace()
   val opFn: Option[(Expr, Expr) => Expr] = peekChar() match {
    case '+' => advance(); Some(addFn)
    case '-' => advance(); Some(subFn)
    case _ => None
   }
   opFn match {
    case None => continue = false
    case Some(b) =>
     parseMulExpr() match {
      case Right(right) => current = b(current, right)
      case left @ Left(_) => return left
     }
   }
  }
  Right(current)
}
 
 def parseMulExpr(): Either[ExprParseError, Expr] =
  parseUnary().flatMap(loopMulExpr)
 
 /** Iterative loop (`@tailrec`) for * / % chained operands. See `loopOrExpr`. */
 private def loopMulExpr(acc: Expr): Either[ExprParseError, Expr] = {
  var current: Expr = acc
  var continue: Boolean = true
  while (continue) {
   skipWhitespace()
   val opFn: Option[(Expr, Expr) => Expr] = peekChar() match {
    case '*' => advance(); Some(mulFn)
    case '/' => advance(); Some(divFn)
    case '%' => advance(); Some(modFn)
    case _ => None
   }
   opFn match {
    case None => continue = false
    case Some(b) =>
     parseUnary() match {
      case Right(right) => current = b(current, right)
      case left @ Left(_) => return left
     }
   }
  }
  Right(current)
}

 def parseUnary(): Either[ExprParseError, Expr] = {
  skipWhitespace()
  peekChar() match {
  case '-' => advance(); parseUnary().map(negate)
  case '+' => advance(); parseUnary()
  case _ => parsePrimary()
  }
 }

 def parsePrimary(): Either[ExprParseError, Expr] = {
  skipWhitespace()
  // PR-M1 (ADR-008-L Appendix GAP 1): CASE WHEN. THEN.
  // [WHEN. THEN.]* [ELSE.] END. Case-insensitive
  // keywords, consistent with AND/OR/NOT/AS/IS. Missing ELSE
  // lowers to Literal(NullValue) per SQL semantics.
  if (startsWithWordCaseInsensitive("case")) {
  parseCaseWhen().flatMap(parseAsSuffix)
  } else {
  val head: Either[ExprParseError, Expr] = peekChar() match {
  case '(' =>
   advance()
   skipWhitespace()
   val result = parseOrExpr()
   skipWhitespace()
   if (peekChar() != ')')
   Left(ExprParseError.UnclosedDelimiter('(', position))
   else {
   advance()
   result
   }
  case '"' | '\'' => parseStringLiteral()
  case c if c.isDigit || c == '-' => parseNumberLiteral()
  case c if c.isLetter => parseIdentifierOrBoolean()
  case _ =>
   Left(ExprParseError.UnexpectedToken(
   token = peekChar().toString,
   position = position,
   reason = "expected literal, identifier, or '('"))
  }
  head.flatMap(parseAsSuffix)
  }
 }

 /** PR-M1 (ADR-008-L Appendix GAP 1): parse a full CASE WHEN
  * expression. Grammar:
  * CASE WHEN cond THEN val [WHEN cond THEN val]* [ELSE val] END
  * Keywords are case-insensitive. Missing ELSE lowers to
  * `Literal(NullValue, Varchar)` (SQL: no ELSE yields NULL).
  * Cursor is pre-"CASE" on entry; consumes through "END". */
private def parseCaseWhen(): Either[ExprParseError, Expr] = {
  // consume "CASE" (4 chars) + ws
  position += 4
  skipWhitespace()
  val branches = scala.collection.mutable.ArrayBuffer.empty[(Expr, Expr)]
  def branch(): Either[ExprParseError, (Expr, Expr)] =
  for {
   cond <- parseOrExpr()
   _ = skipWhitespace()
   ok = consumeWordCaseInsensitive("then")
   _  = if (!ok) return Left(ExprParseError.UnexpectedToken(
      token = peekText(8),
      position = position,
      reason = "expected 'THEN' in CASE WHEN"))
   _ = skipWhitespace()
   value <- parseOrExpr()
  } yield (cond, value)
  // Iterative loop: collect WHEN-branches into the accumulator.
  // Stays at 1 JVM stack frame regardless of branch count.
  var continuing: Boolean = true
 while (continuing) {
  skipWhitespace()
  if (consumeWordCaseInsensitive("when")) {
   branch() match {
    case Right(b) => branches += b
    case Left(err) => return Left(err)
   }
  } else {
   continuing = false
  }
 }
  skipWhitespace()
  val otherwise: Either[ExprParseError, Expr] =
  if (consumeWordCaseInsensitive("else")) {
  skipWhitespace(); parseOrExpr()
  } else {
  // SQL: missing ELSE yields NULL.
  Right(Expr.Literal(LiteralValue.NullValue, SealedDataType.Varchar))
  }
  otherwise.flatMap { els =>
  skipWhitespace()
  if (!consumeWordCaseInsensitive("end"))
  Left(ExprParseError.UnexpectedToken(
   token = peekText(8),
   position = position,
   reason = "expected 'END' to close CASE WHEN"))
  else if (branches.isEmpty)
  Left(ExprParseError.UnexpectedToken(
   token = "CASE",
   position = position,
   reason = "CASE requires at least one WHEN. THEN branch"))
  else Right(Expr.CaseWhen(branches.toList, els))
  }
}

 /** PR #50 + #53: handle optional postfix clauses after a primary.
  * Returns `Right(e)` unchanged if no postfix follows.
  *
  * the postfix is matched at the primary level only. Real-
  * world usages: `column AS T`, `column IS NULL`,
  * `column IS NOT NULL`. For infix expressions, use
  * `(expr) IS NULL`.
  *
  * is a typed AST (`Expr.Cast` / `Expr.IsNull` / `Expr.IsNotNull`),
  * not string substitution. */
 def parseAsSuffix(e: Expr): Either[ExprParseError, Expr] =
  if (atEnd) Right(e)
  else {
  skipWhitespace()
  if (startsWithWordCaseInsensitive("as")) {
   // AS TYPE: consume "AS" (2 chars via 2 advance()), then the
   // SealedDataType literal. PR-M1: if the token after AS is
   // NOT a known type name, fall back to reading it as a
   // column ALIAS (Expr.Alias) -- types win over aliases
   // (documented disambiguation per ADR-008-L Appendix GAP 1).
   advance()
   advance()
   skipWhitespace()
   val saved = position
   parseTypeName() match {
   case Right(targetType) =>
    Right(Expr.Cast(expr = e, targetType = targetType))
   case Left(typeErr) =>
    // Backtrack: read the token. If it is NOT a known type
    // KEYWORD, it is a column alias -> Expr.Alias. If it IS
    // a type keyword (malformed DECIMAL without (p,s), etc.),
    // keep the original fail-loud cast error -- a malformed
    // cast must NOT silently degrade into a rename.
    position = saved
    val name = readIdentifier()
    val isTypeKeyword = name.toUpperCase match {
    case "INT" | "BIGINT" | "DOUBLE" | "VARCHAR" | "BOOLEAN"
     | "DATE" | "TIMESTAMP" | "DECIMAL" => true
    case _        => false
    }
    if (name.isEmpty)
    Left(ExprParseError.UnexpectedToken(
     token = peekText(8),
     position = position,
     reason = "expected type name or alias after 'AS'"))
    else if (isTypeKeyword) Left(typeErr)
    else Right(Expr.Alias(name = name, expr = e))
   }
  } else if (startsWithWordCaseInsensitive("is")) {
   // IS [NOT] NULL: consume "IS" (2 chars), optional "NOT",
   // then "NULL" (4 chars). The pattern is a single postfix
   // operator that wraps `e`.
   advance(); advance() // consume "IS"
   skipWhitespace()
   val isNot = if (startsWithWordCaseInsensitive("not")) {
   advance(); advance(); advance() // consume "NOT" (3 chars)
   skipWhitespace()
   true
   } else false
   if (startsWithWordCaseInsensitive("null")) {
   advance(); advance(); advance(); advance() // consume "NULL" (4)
   skipWhitespace()
   Right(if (isNot) Expr.IsNotNull(e) else Expr.IsNull(e))
   } else {
   // "IS" without "NULL" or "NOT NULL" is invalid SQL.
   // error, not silent.
   Left(ExprParseError.UnexpectedToken(
    token = peekChar().toString,
    position = position,
    reason = "expected 'NULL' or 'NOT NULL' after 'IS'"))
   }
  } else Right(e)
  }

 /** Case-insensitive variant of `startsWithWord`. */
 private def startsWithWordCaseInsensitive(word: String): Boolean = {
  var p = position
  while (p < chars.length && chars(p).isWhitespace) p += 1
  var i = 0
  while (i < word.length && (p + i) < chars.length &&
    Character.toLowerCase(chars(p + i)) == Character.toLowerCase(word.charAt(i))) i += 1
  i == word.length && {
  val next = p + i
  next >= chars.length || !chars(next).isLetterOrDigit
  }
 }

 /** PR #50: parse a `SealedDataType` literal.
  * Supports the common scalar types: INT, BIGINT, DOUBLE,
  * VARCHAR, BOOLEAN, DATE, TIMESTAMP, and DECIMAL(p, s). */
 def parseTypeName(): Either[ExprParseError, SealedDataType] = {
  val name = readIdentifier()
  val tpe: Option[SealedDataType] = name.toUpperCase match {
  case "INT"  => Some(SealedDataType.Int)
  case "BIGINT" => Some(SealedDataType.BigInt)
  case "DOUBLE" => Some(SealedDataType.Double)
  case "VARCHAR" => Some(SealedDataType.Varchar)
  case "BOOLEAN" => Some(SealedDataType.Boolean)
  case "DATE"  => Some(SealedDataType.Date)
  case "TIMESTAMP" => Some(SealedDataType.Timestamp)
  case "DECIMAL" =>
   if (!consumeChar('(')) None
   else {
   val p = readIntOrMinusOne()
   if (!consumeChar(',')) None
   else {
    val s = readIntOrMinusOne()
    if (!consumeChar(')')) None
    else if (p < 0 || s < 0) None
    else Some(SealedDataType.Decimal(p, s))
   }
   }
  case _ => None
  }
  tpe match {
  case Some(t) => Right(t)
  case None =>
   Left(ExprParseError.InvalidLiteral(
   raw = name,
   reason = "unknown or malformed cast target type; supported: INT, BIGINT, DOUBLE, VARCHAR, BOOLEAN, DATE, TIMESTAMP, DECIMAL(p, s)"))
  }
 }

 /** Read an identifier (sequence of letters/digits/underscore). */
 private def readIdentifier(): String = {
  val start = position
  while (!atEnd && (peekChar().isLetterOrDigit || peekChar() == '_'))
  advance()
  chars.slice(start, position).mkString
 }

 /** Consume one specific char if present (with optional whitespace). */
 private def consumeChar(c: Char): Boolean = {
  skipWhitespace()
  if (!atEnd && peekChar() == c) {
  advance(); true
  } else false
 }

 /** Read a (possibly signed) integer at the current cursor. */
 private def readIntOrMinusOne(): Int = {
  skipWhitespace()
  val start = position
  if (!atEnd && (peekChar().isDigit || peekChar() == '-')) advance()
  while (!atEnd && peekChar().isDigit) advance()
  val raw = chars.slice(start, position).mkString
  scala.util.Try(raw.toInt).getOrElse(-1)
 }

 private def parseStringLiteral(): Either[ExprParseError, Expr] = {
  val quote = advance()
  val start = position
  while (!atEnd && peekChar() != quote) advance()
  if (atEnd)
  Left(ExprParseError.UnclosedDelimiter(quote, start - 1))
  else {
  val raw = chars.slice(start, position).mkString
  advance()
  Right(Expr.Literal(
   value = LiteralValue.StringValue(raw),
   dataType = SealedDataType.Varchar))
  }
 }

 private def parseNumberLiteral(): Either[ExprParseError, Expr] = {
  val start = position
  if (peekChar() == '-') advance()
  while (!atEnd && peekChar().isDigit) advance()
  val isFloat =
  !atEnd && peekChar() == '.' &&
  chars.lift(position + 1).exists(_.isDigit)
  if (isFloat) {
  advance()
  while (!atEnd && peekChar().isDigit) advance()
  val raw = chars.slice(start, position).mkString
  scala.util.Try(raw.toDouble).toEither.left.map(err =>
   ExprParseError.InvalidLiteral(raw, err.getMessage)
  ).map { d =>
   Expr.Literal(
   value = LiteralValue.DoubleValue(d),
   dataType = SealedDataType.Double)
  }
  } else {
  val raw = chars.slice(start, position).mkString
  scala.util.Try(raw.toInt).toEither.left.map(err =>
   ExprParseError.InvalidLiteral(raw, err.getMessage)
  ).map { i =>
   Expr.Literal(
   value = LiteralValue.IntValue(i),
   dataType = SealedDataType.Int)
  }
  }
 }
 private def parseIdentifierOrBoolean(): Either[ExprParseError, Expr] = {
  val start = position
  while (!atEnd && (peekChar().isLetterOrDigit || peekChar() == '_'))
  advance()
  val word = chars.slice(start, position).mkString
  // Function-call detection: `name(` implies FunctionCall(name, args).
  // We peek past whitespace before checking — SQL allows `f (.)`.
  // identifier-then-paren pattern triggers function-call parsing.
  // Bare identifiers (no paren) remain FieldRef.
  skipWhitespace()
  if (!atEnd && peekChar() == '(' && !looksLikeBoolean(word)) {
  // Detected function call: parse arg list.
  advance() // opening paren
  skipWhitespace()
  // Empty arg list: `f()` is valid (returns the constant result).
  if (!atEnd && peekChar() == ')') {
   advance()
   return Right(Expr.FunctionCall(name = word, args = Seq.empty))
  }
  parseFunctionCallArgs(word).flatMap { args =>
   skipWhitespace()
   if (peekChar() != ')')
   Left(ExprParseError.UnclosedDelimiter('(', position))
   else {
   advance()
   // PR-M1 (ADR-008-L Appendix GAP 1): rewrite the legacy
   // CalcExpr DSL forms (DESIGN.md SS6.2) -- all(name) and
   // measure(name) -- into their typed Expr cases instead
   // of generic FunctionCalls.
   val lowered: Expr = (word.toLowerCase, args) match {
    case ("all", Seq(Expr.FieldRef(n)))  => Expr.All(n)
    case ("measure", Seq(Expr.FieldRef(n))) => Expr.MeasureRef(n)
    case _         => Expr.FunctionCall(name = word, args = args)
   }
   Right(lowered)
   }
  }
  } else {
  // Bare identifier or boolean literal — no function call.
  word.toLowerCase match {
   case "true" => Right(Expr.Literal(
   value = LiteralValue.BoolValue(true),
   dataType = SealedDataType.Boolean))
   case "false" => Right(Expr.Literal(
   value = LiteralValue.BoolValue(false),
   dataType = SealedDataType.Boolean))
   case _  => Right(Expr.FieldRef(word))
  }
  }
 }

 /** Distinguish `true`/`false` from a function name.
  * Per the AST contract: `true(1)` would be a function call on the
  * boolean literal — but semantically nonsense. Easier to reject
  * function-call parsing for `true`/`false` explicitly.
  * The cursor IS post-identifier when this is called. */
 private def looksLikeBoolean(word: String): Boolean =
  word.toLowerCase == "true" || word.toLowerCase == "false"

 /** Parse a comma-separated argument list. Per SQL convention,
  * each arg is a full expression (recursive `parseOrExpr`). Empty
  * args (trailing comma) is rejected with UnexpectedToken.
  *
  * Seq[Expr] (sealed-trait family at the boundary), not a raw
  * String list. Each arg parses to its own typed AST. */
 private def parseFunctionCallArgs(
  name: String
 ): Either[ExprParseError, Seq[Expr]] = {
  val buf = scala.collection.mutable.ArrayBuffer.empty[Expr]
 // Iterative loop: collect function-call arguments into the accumulator.
 // Stays at 1 JVM stack frame regardless of arg count (the 6th loop
 // lifted per ADR-008-AB v1.3; the 5 prior loops were lifted in PR-137).
 var continuing: Boolean = true
 while (continuing) {
  skipWhitespace()
  parseOrExpr() match {
   case Right(arg) =>
    buf += arg
    skipWhitespace()
    if (!atEnd && peekChar() == ',') {
     advance()
    } else {
     continuing = false
    }
    case Left(err) => return Left(err)
  }
 }
 Right(buf.toSeq)
 }
 }
}
