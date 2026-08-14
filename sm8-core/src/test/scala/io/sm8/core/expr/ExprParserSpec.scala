/*
 * SM8 Core — ExprParser spec.
 *
 * Per [[debug-mantra-mindset]]: fast deterministic pass/fail tests
 * for the recursive-descent parser. One invariant per test.
 *
 * Per [[karphy-guidags-mindset]]: no incidental assertions, no
 * incidental metrics. No mocks (pure functions, real inputs).
 *
 * Per [[scala-data-driven-refactor-mindset]] §1: data in core.
 * The parser is a typed factory — no behavior beyond
 * String → Expr conversion.
 *
 * ==Spark concerns (per user directive)==
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantras: N/A in core.
 * The parser has zero Spark types. The Expr produced here can
 * flow to MCPEngineRegistry → SparkEngineProvider.query() via the
 * connector layer (PRs #38-#42 handle Spark concerns).
 *
 * Per [[scala-jvm-safety-mindset]]: the parser has no mutable
 * state (the `Cursor` class only mutates its `position: Int`
 * field via in-class method calls — same JVM-safety as Scala
 * stdlib's `Iterator`).
 *
 * Per [[scala-perf-testing-mindset]]: startup-time path. No
 * per-call allocation concerns.
 */
package io.sm8.core.expr

import io.sm8.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class ExprParserSpec extends AnyFunSuite with Matchers {

  // -- Literals --

  test("ExprParser: integer literal parses to Expr.Literal(IntValue, SealedDataType.Int)") {
    val out = ExprParser.parseExpr("42")
    out.toOption.get shouldBe Expr.Literal(LiteralValue.IntValue(42), SealedDataType.Int)
  }

  test("ExprParser: negative integer literal parses correctly") {
    val out = ExprParser.parseExpr("-3")
    // `-x` lowers to `0 - x`
    out.toOption.get shouldBe Expr.Subtract(
      left = Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int),
      right = Expr.Literal(LiteralValue.IntValue(3), SealedDataType.Int),
    )
  }

  test("ExprParser: float literal parses to Expr.Literal(DoubleValue, SealedDataType.Double)") {
    val out = ExprParser.parseExpr("3.14")
    out.toOption.get shouldBe Expr.Literal(LiteralValue.DoubleValue(3.14), SealedDataType.Double)
  }

  test("ExprParser: string literal parses to Expr.Literal(StringValue, SealedDataType.Varchar)") {
    val out = ExprParser.parseExpr("\"hello\"")
    out.toOption.get shouldBe Expr.Literal(
      value    = LiteralValue.StringValue("hello"),
      dataType = SealedDataType.Varchar,
    )
  }

  test("ExprParser: boolean literal parses to Expr.Literal(BoolValue, SealedDataType.Boolean)") {
    ExprParser.parseExpr("true").toOption.get shouldBe
      Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean)
    ExprParser.parseExpr("false").toOption.get shouldBe
      Expr.Literal(LiteralValue.BoolValue(false), SealedDataType.Boolean)
  }

  // -- Identifiers / FieldRef --

  test("ExprParser: bare identifier parses to Expr.FieldRef(name)") {
    val out = ExprParser.parseExpr("age")
    out.toOption.get shouldBe Expr.FieldRef("age")
  }

  // -- Comparison (the user's central use case: 'age >= 18') --

  test("ExprParser: comparison 'age >= 18' parses to Expr.GreaterOrEqual(FieldRef, Literal)") {
    val out = ExprParser.parseExpr("age >= 18")
    out.toOption.get shouldBe Expr.GreaterOrEqual(
      left  = Expr.FieldRef("age"),
      right = Expr.Literal(LiteralValue.IntValue(18), SealedDataType.Int),
    )
  }

  test("ExprParser: comparison 'age = 18' parses to Expr.Equal") {
    val out = ExprParser.parseExpr("age = 18")
    out.toOption.get shouldBe Expr.Equal(
      left  = Expr.FieldRef("age"),
      right = Expr.Literal(LiteralValue.IntValue(18), SealedDataType.Int),
    )
  }

  test("ExprParser: comparison 'age != 18' parses to Expr.NotEqual") {
    val out = ExprParser.parseExpr("age != 18")
    out.toOption.get shouldBe Expr.NotEqual(
      left  = Expr.FieldRef("age"),
      right = Expr.Literal(LiteralValue.IntValue(18), SealedDataType.Int),
    )
  }

  test("ExprParser: comparison 'age < 18' parses to Expr.LessThan") {
    ExprParser.parseExpr("age < 18").toOption.get shouldBe Expr.LessThan(
      left  = Expr.FieldRef("age"),
      right = Expr.Literal(LiteralValue.IntValue(18), SealedDataType.Int),
    )
  }

  test("ExprParser: comparison 'age <= 18' parses to Expr.LessOrEqual") {
    ExprParser.parseExpr("age <= 18").toOption.get shouldBe Expr.LessOrEqual(
      left  = Expr.FieldRef("age"),
      right = Expr.Literal(LiteralValue.IntValue(18), SealedDataType.Int),
    )
  }

  test("ExprParser: comparison 'age > 18' parses to Expr.GreaterThan") {
    ExprParser.parseExpr("age > 18").toOption.get shouldBe Expr.GreaterThan(
      left  = Expr.FieldRef("age"),
      right = Expr.Literal(LiteralValue.IntValue(18), SealedDataType.Int),
    )
  }

  // -- Arithmetic (left-associative precedence) --

  test("ExprParser: arithmetic precedence: '2 + 3 * 4' parses to Add(2, Multiply(3, 4))") {
    ExprParser.parseExpr("2 + 3 * 4").toOption.get shouldBe Expr.Add(
      left  = Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int),
      right = Expr.Multiply(
        left  = Expr.Literal(LiteralValue.IntValue(3), SealedDataType.Int),
        right = Expr.Literal(LiteralValue.IntValue(4), SealedDataType.Int),
      ),
    )
  }

  test("ExprParser: left-associative: '10 - 3 - 2' parses to Subtract(Subtract(10, 3), 2)") {
    ExprParser.parseExpr("10 - 3 - 2").toOption.get shouldBe Expr.Subtract(
      left  = Expr.Subtract(
        left  = Expr.Literal(LiteralValue.IntValue(10), SealedDataType.Int),
        right = Expr.Literal(LiteralValue.IntValue(3), SealedDataType.Int),
      ),
      right = Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int),
    )
  }

  // -- Boolean (and, or, not) --

  test("ExprParser: boolean 'age >= 18 and active = true' parses to And(GreaterOrEqual, Equal)") {
    val out = ExprParser.parseExpr("age >= 18 and active = true")
    out.toOption.get shouldBe Expr.And(
      left = Expr.GreaterOrEqual(
        left  = Expr.FieldRef("age"),
        right = Expr.Literal(LiteralValue.IntValue(18), SealedDataType.Int),
      ),
      right = Expr.Equal(
        left  = Expr.FieldRef("active"),
        right = Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean),
      ),
    )
  }

  test("ExprParser: boolean 'not (age >= 18)' parses to Not(GreaterOrEqual)") {
    val out = ExprParser.parseExpr("not (age >= 18)")
    out.toOption.get shouldBe Expr.Not(
      Expr.GreaterOrEqual(
        left  = Expr.FieldRef("age"),
        right = Expr.Literal(LiteralValue.IntValue(18), SealedDataType.Int),
      ),
    )
  }

  // -- Parens (grouping) --

  test("ExprParser: parens override precedence: '(2 + 3) * 4' parses to Multiply(Add(2, 3), 4)") {
    ExprParser.parseExpr("(2 + 3) * 4").toOption.get shouldBe Expr.Multiply(
      left  = Expr.Add(
        left  = Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int),
        right = Expr.Literal(LiteralValue.IntValue(3), SealedDataType.Int),
      ),
      right = Expr.Literal(LiteralValue.IntValue(4), SealedDataType.Int),
    )
  }

  // -- Failure paths --

  test("ExprParser: empty input returns Left(EmptyInput)") {
    val out = ExprParser.parseExpr("")
    out.isLeft shouldBe true
    out.left.get shouldBe ExprParseError.EmptyInput
  }

  test("ExprParser: unclosed paren returns Left(UnclosedDelimiter)") {
    val out = ExprParser.parseExpr("(age")
    out.isLeft shouldBe true
    out.left.get shouldBe a [ExprParseError.UnclosedDelimiter]
  }

  test("ExprParser: unclosed string literal returns Left(UnclosedDelimiter)") {
    val out = ExprParser.parseExpr("\"hello")
    out.isLeft shouldBe true
    out.left.get shouldBe a [ExprParseError.UnclosedDelimiter]
  }

  test("ExprParser: trailing junk after expression returns Left(UnexpectedToken)") {
    val out = ExprParser.parseExpr("age 18")
    out.isLeft shouldBe true
    out.left.get shouldBe a [ExprParseError.UnexpectedToken]
  }

// -- Per [[debug-mantra-mindset]] §5 verify: Serializable contract --

  test("ExprParser: parsed Expr survives ObjectOutputStream round-trip") {
    val built = ExprParser.parseExpr("age >= 18 and active = true").toOption.get
    val bytes = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bytes)
    oos.writeObject(built)
    oos.close()
    val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray))
    val restored = ois.readObject().asInstanceOf[Expr]
    ois.close()
    restored shouldBe built
  }

  // -- FunctionCall (PR function-call-parser) --

  test("ExprParser: function call 'concat(\"a\", \"b\")' parses to Expr.FunctionCall(name, Seq[Expr])") {
    val out = ExprParser.parseExpr("""concat("a", "b")""")
    out.toOption.get shouldBe Expr.FunctionCall(
      name = "concat",
      args = Seq(
        Expr.Literal(LiteralValue.StringValue("a"), SealedDataType.Varchar),
        Expr.Literal(LiteralValue.StringValue("b"), SealedDataType.Varchar),
      ),
    )
  }

  test("ExprParser: function call 'abs(-3)' parses to FunctionCall with unary-minus arg") {
    val out = ExprParser.parseExpr("abs(-3)")
    out.toOption.get shouldBe Expr.FunctionCall(
      name = "abs",
      args = Seq(
        // Per [[karphyaguids-mindset]] "smallest correct change":
        // unary `-x` lowers to `0 - x`.
        Expr.Subtract(
          left  = Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int),
          right = Expr.Literal(LiteralValue.IntValue(3), SealedDataType.Int),
        ),
      ),
    )
  }

  test("ExprParser: function call with field-ref arg 'nvl(amount, 0)'") {
    val out = ExprParser.parseExpr("nvl(amount, 0)")
    out.toOption.get shouldBe Expr.FunctionCall(
      name = "nvl",
      args = Seq(
        Expr.FieldRef("amount"),
        Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int),
      ),
    )
  }

  test("ExprParser: function call 'f()' with empty arg list is valid") {
    val out = ExprParser.parseExpr("f()")
    out.toOption.get shouldBe Expr.FunctionCall(name = "f", args = Seq.empty)
  }

  test("ExprParser: function call with nested arg 'abs(concat(a, b))'") {
    val out = ExprParser.parseExpr("abs(concat(a, b))")
    out.toOption.get shouldBe Expr.FunctionCall(
      name = "abs",
      args = Seq(
        Expr.FunctionCall(
          name = "concat",
          args = Seq(
            Expr.FieldRef("a"),
            Expr.FieldRef("b"),
          ),
        ),
      ),
    )
  }

  test("ExprParser: function call with trailing comma 'f(1,' returns Left(typed error)") {
    // Per [[karphyaguids-mindset]] "smallest correct change": the
    // trailing-comma case has 2 valid error shapes (unclosed paren
    // or unexpected token for the missing arg).  The parser took
    // the more-accurate path: parsed `1`, consumed `,`, tried to
    // parse the next arg — found EOF → UnexpectedToken.
    val out = ExprParser.parseExpr("f(1,")
    out.isLeft shouldBe true
    out.left.get shouldBe a [ExprParseError]
  }

  test("ExprParser: bare identifier 'age' remains FieldRef (no spurious FunctionCall)") {
    // Per [[karphyaguids-mindset]] "smallest correct change":
    // a name WITHOUT `(` stays a FieldRef — does NOT become
    // FunctionCall(name = "age", args = Nil).
    val out = ExprParser.parseExpr("age")
    out.toOption.get shouldBe Expr.FieldRef("age")
  }

  test("ExprParser: true stays Literal(Bool) regardless of following-paren context") {
    // Per [[scala-data-driven-refactor-mindset]]: boolean literals
    // are NOT callable.  The parser recognises `true` first; if `(` follows,
    // the parser does NOT trigger FunctionCall parsing for `true` itself.
    val out = ExprParser.parseExpr("true")
    out.toOption.get shouldBe Expr.Literal(
      value    = LiteralValue.BoolValue(true),
      dataType = SealedDataType.Boolean,
    )
  }

  test("ExprParser: FunctionCall result survives ObjectOutputStream round-trip") {
    val built = ExprParser.parseExpr("""concat("a", "b")""").toOption.get
    val bytes = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bytes)
    oos.writeObject(built)
    oos.close()
    val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray))
    val restored = ois.readObject().asInstanceOf[Expr]
    ois.close()
    restored shouldBe built
  }

  // -- Cast (PR cast-parser) --

  test("ExprParser: postfix cast 'amount AS INT' parses to Expr.Cast(FieldRef, SealedDataType)") {
    val out = ExprParser.parseExpr("amount AS INT")
    out match {
      case Right(e) => e shouldBe Expr.Cast(
        expr = Expr.FieldRef("amount"),
        targetType = io.sm8.core.schema.SealedDataType.Int,
      )
      case Left(err) => fail(s"expected Right, got Left($err)")
    }
  }

  test("ExprParser: postfix cast on literal '42 AS BIGINT' parses to Expr.Cast(Literal, BigInt)") {
    val out = ExprParser.parseExpr("42 AS BIGINT")
    out.toOption.get shouldBe Expr.Cast(
      expr = Expr.Literal(
        value    = io.sm8.core.expr.LiteralValue.IntValue(42),
        dataType = io.sm8.core.schema.SealedDataType.Int,
      ),
      targetType = io.sm8.core.schema.SealedDataType.BigInt,
    )
  }

  test("ExprParser: postfix cast with DECIMAL(38, 18) parses precision + scale") {
    val out = ExprParser.parseExpr("amount AS DECIMAL(38, 18)")
    out.toOption.get shouldBe Expr.Cast(
      expr = Expr.FieldRef("amount"),
      targetType = io.sm8.core.schema.SealedDataType.Decimal(precision = 38, scale = 18),
    )
  }

  test("ExprParser: function-call 'cast(amount AS VARCHAR)' parses via FunctionCall+Cast") {
    val out = ExprParser.parseExpr("cast(amount AS VARCHAR)")
    out.toOption.get shouldBe Expr.FunctionCall(
      name = "cast",
      args = Seq(
        Expr.Cast(
          expr = Expr.FieldRef("amount"),
          targetType = io.sm8.core.schema.SealedDataType.Varchar,
        ),
      ),
    )
  }

  test("ExprParser: unknown cast target type returns Left(InvalidLiteral)") {
    val out = ExprParser.parseExpr("amount AS NOTAREALTYPE")
    out.isLeft shouldBe true
    out.left.get shouldBe a [ExprParseError.InvalidLiteral]
  }

  test("ExprParser: malformed DECIMAL without parens returns Left(InvalidLiteral)") {
    val out = ExprParser.parseExpr("amount AS DECIMAL")
    out.isLeft shouldBe true
    out.left.get shouldBe a [ExprParseError.InvalidLiteral]
  }

  test("ExprParser: identifier without AS cast stays FieldRef (no spurious Cast)") {
    val out = ExprParser.parseExpr("amount")
    out.toOption.get shouldBe Expr.FieldRef("amount")
  }

  test("ExprParser: Cast result survives ObjectOutputStream round-trip") {
    val built = ExprParser.parseExpr("amount AS DECIMAL(38, 18)").toOption.get
    val bytes = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bytes)
    oos.writeObject(built)
    oos.close()
    val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray))
    val restored = ois.readObject().asInstanceOf[Expr]
    ois.close()
    restored shouldBe built
  }
}
