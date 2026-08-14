/*
 * SM8 Core — ExprParser.
 *
 * A small recursive-descent parser that converts a raw SQL-like
 * expression string into the engine-portable `io.sm8.core.expr.Expr`
 * AST. No Spark, no data-source knowledge, no plugin/hook — pure
 * core IR factory per RFC §3 Core Boundary.
 *
 * ==Scope (per karphy-guidags-mindset "smallest correct change")==
 *
 * Supports the common subset:
 *   - Literals: integers (42, -3), floats (3.14), strings ("hello"),
 *     booleans (true, false).
 *   - Field references: bare identifiers ("age").
 *   - Arithmetic: + - * / %, with left-associative precedence.
 *   - Comparison: = != < <= > >=, with non-associative (binary only).
 *     Per SQL convention, single `=` is equality.
 *   - Boolean: and, or, not (unary prefix).
 *   - Parens for grouping.
 *
 * NOT in v1 (deferred):
 *   - `Cast(...)`, `FunctionCall`, `MeasureRef`, `All`,
 *     `IsNull` / `IsNotNull` — future PRs.
 *
 * ==Recursive descent==
 *
 * Per [[scala-jvm-safety-mindset]]: the cursor is a `Vector[Char]`
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
 * Per [[scala-spark-batch-bugs-mindset]] mantras:
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
  private val eqFn:  (Expr, Expr) => Expr = Expr.Equal
  private val neqFn: (Expr, Expr) => Expr = Expr.NotEqual
  private val ltFn:  (Expr, Expr) => Expr = Expr.LessThan
  private val leFn:  (Expr, Expr) => Expr = Expr.LessOrEqual
  private val gtFn:  (Expr, Expr) => Expr = Expr.GreaterThan
  private val geFn:  (Expr, Expr) => Expr = Expr.GreaterOrEqual

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
          token    = p.peekText(10),
          position = p.position,
          reason   = "expected end of expression",
        ))
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
      * Per [[karphyaguids-mindset]] "smallest correct change": SQL
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
      parseAndExpr().flatMap { left =>
        def loop(acc: Expr): Either[ExprParseError, Expr] =
          if (consumeWordCaseInsensitive("or")) {
            parseAndExpr().flatMap(right => loop(Expr.Or(acc, right)))
          } else Right(acc)
        loop(left)
      }

    def parseAndExpr(): Either[ExprParseError, Expr] =
      parseNotExpr().flatMap { left =>
        def loop(acc: Expr): Either[ExprParseError, Expr] =
          if (consumeWordCaseInsensitive("and")) {
            parseNotExpr().flatMap(right => loop(Expr.And(acc, right)))
          } else Right(acc)
        loop(left)
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
          case None    => Right(left)
          case Some(b) =>
            skipWhitespace()
            parseAddExpr().map(right => b(left, right))
        }
      }

    def parseAddExpr(): Either[ExprParseError, Expr] =
      parseMulExpr().flatMap { left =>
        def loop(acc: Expr): Either[ExprParseError, Expr] = {
          skipWhitespace()
          val opFn: Option[(Expr, Expr) => Expr] = peekChar() match {
            case '+' => advance(); Some(addFn)
            case '-' => advance(); Some(subFn)
            case _   => None
          }
          opFn match {
            case None => Right(acc)
            case Some(b) =>
              parseMulExpr().flatMap(right => loop(b(acc, right)))
          }
        }
        loop(left)
      }

    def parseMulExpr(): Either[ExprParseError, Expr] =
      parseUnary().flatMap { left =>
        def loop(acc: Expr): Either[ExprParseError, Expr] = {
          skipWhitespace()
          val opFn: Option[(Expr, Expr) => Expr] = peekChar() match {
            case '*' => advance(); Some(mulFn)
            case '/' => advance(); Some(divFn)
            case '%' => advance(); Some(modFn)
            case _   => None
          }
          opFn match {
            case None => Right(acc)
            case Some(b) =>
              parseUnary().flatMap(right => loop(b(acc, right)))
          }
        }
        loop(left)
      }

    def parseUnary(): Either[ExprParseError, Expr] = {
      skipWhitespace()
      peekChar() match {
        case '-' => advance(); parseUnary().map(negate)
        case '+' => advance(); parseUnary()
        case _   => parsePrimary()
      }
    }

    def parsePrimary(): Either[ExprParseError, Expr] = {
      skipWhitespace()
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
            token    = peekChar().toString,
            position = position,
            reason   = "expected literal, identifier, or '('",
          ))
      }
      head.flatMap(parseAsSuffix)
    }

    /** PR #50 + #53: handle optional postfix clauses after a primary.
      * Returns `Right(e)` unchanged if no postfix follows.
      *
      * Per [[karphyaguids-mindset]] "smallest correct change":
      * the postfix is matched at the primary level only. Real-
      * world usages: `column AS T`, `column IS NULL`,
      * `column IS NOT NULL`. For infix expressions, use
      * `(expr) IS NULL`.
      *
      * Per [[scala-data-driven-refactor-mindset]] §1: the result
      * is a typed AST (`Expr.Cast` / `Expr.IsNull` / `Expr.IsNotNull`),
      * not string substitution. */
    def parseAsSuffix(e: Expr): Either[ExprParseError, Expr] =
      if (atEnd) Right(e)
      else {
        skipWhitespace()
        if (startsWithWordCaseInsensitive("as")) {
          // AS TYPE: consume "AS" (2 chars via 2 advance()), then the
          // SealedDataType literal.
          advance()
          advance()
          skipWhitespace()
          parseTypeName().map(targetType => Expr.Cast(expr = e, targetType = targetType))
        } else if (startsWithWordCaseInsensitive("is")) {
          // IS [NOT] NULL: consume "IS" (2 chars), optional "NOT",
          // then "NULL" (4 chars). The pattern is a single postfix
          // operator that wraps `e`.
          advance(); advance()  // consume "IS"
          skipWhitespace()
          val isNot = if (startsWithWordCaseInsensitive("not")) {
            advance(); advance(); advance()  // consume "NOT" (3 chars)
            skipWhitespace()
            true
          } else false
          if (startsWithWordCaseInsensitive("null")) {
            advance(); advance(); advance(); advance()  // consume "NULL" (4)
            skipWhitespace()
            Right(if (isNot) Expr.IsNotNull(e) else Expr.IsNull(e))
          } else {
            // "IS" without "NULL" or "NOT NULL" is invalid SQL.
            // Per [[scala-error-handling-mindset]]: surface as typed
            // error, not silent.
            Left(ExprParseError.UnexpectedToken(
              token    = peekChar().toString,
              position = position,
              reason   = "expected 'NULL' or 'NOT NULL' after 'IS'",
            ))
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
      val name   = readIdentifier()
      val tpe: Option[SealedDataType] = name.toUpperCase match {
        case "INT"       => Some(SealedDataType.Int)
        case "BIGINT"    => Some(SealedDataType.BigInt)
        case "DOUBLE"    => Some(SealedDataType.Double)
        case "VARCHAR"   => Some(SealedDataType.Varchar)
        case "BOOLEAN"   => Some(SealedDataType.Boolean)
        case "DATE"      => Some(SealedDataType.Date)
        case "TIMESTAMP" => Some(SealedDataType.Timestamp)
        case "DECIMAL"   =>
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
        case None    =>
          Left(ExprParseError.InvalidLiteral(
            raw    = name,
            reason = "unknown or malformed cast target type; supported: INT, BIGINT, DOUBLE, VARCHAR, BOOLEAN, DATE, TIMESTAMP, DECIMAL(p, s)",
          ))
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
          value    = LiteralValue.StringValue(raw),
          dataType = SealedDataType.Varchar,
        ))
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
            value    = LiteralValue.DoubleValue(d),
            dataType = SealedDataType.Double,
          )
        }
      } else {
        val raw = chars.slice(start, position).mkString
        scala.util.Try(raw.toInt).toEither.left.map(err =>
          ExprParseError.InvalidLiteral(raw, err.getMessage)
        ).map { i =>
          Expr.Literal(
            value    = LiteralValue.IntValue(i),
            dataType = SealedDataType.Int,
          )
        }
      }
    }
    private def parseIdentifierOrBoolean(): Either[ExprParseError, Expr] = {
      val start = position
      while (!atEnd && (peekChar().isLetterOrDigit || peekChar() == '_'))
        advance()
      val word = chars.slice(start, position).mkString
      // Function-call detection: `name(` implies FunctionCall(name, args).
      // We peek past whitespace before checking — SQL allows `f  (...)`.
      // Per [[karphyaguids-mindset]] "smallest correct change": only the
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
            Right(Expr.FunctionCall(name = word, args = args))
          }
        }
      } else {
        // Bare identifier or boolean literal — no function call.
        word.toLowerCase match {
          case "true"  => Right(Expr.Literal(
            value    = LiteralValue.BoolValue(true),
            dataType = SealedDataType.Boolean,
          ))
          case "false" => Right(Expr.Literal(
            value    = LiteralValue.BoolValue(false),
            dataType = SealedDataType.Boolean,
          ))
          case _       => Right(Expr.FieldRef(word))
        }
      }
    }

    /** Distinguish `true`/`false` from a function name.
      * Per the AST contract: `true(1)` would be a function call on the
      * boolean literal — but semantically nonsense.  Easier to reject
      * function-call parsing for `true`/`false` explicitly.
      * The cursor IS post-identifier when this is called. */
    private def looksLikeBoolean(word: String): Boolean =
      word.toLowerCase == "true" || word.toLowerCase == "false"

    /** Parse a comma-separated argument list.  Per SQL convention,
      * each arg is a full expression (recursive `parseOrExpr`).  Empty
      * args (trailing comma) is rejected with UnexpectedToken.
      *
      * Per [[scala-data-driven-refactor-mindset]]: args are a
      * Seq[Expr] (sealed-trait family at the boundary), not a raw
      * String list.  Each arg parses to its own typed AST. */
    private def parseFunctionCallArgs(
        name: String
    ): Either[ExprParseError, Seq[Expr]] = {
      val buf = scala.collection.mutable.ArrayBuffer.empty[Expr]
      def loop(): Either[ExprParseError, Seq[Expr]] = {
        skipWhitespace()
        parseOrExpr().flatMap { arg =>
          buf += arg
          skipWhitespace()
          if (!atEnd && peekChar() == ',') {
            advance()
            loop()
          } else {
            Right(buf.toSeq)
          }
        }
      }
      loop()
    }
  }
}
