package io.sm8.core.expr

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.sm8.core.schema.{Field, SealedDataType}

/** Phase 2 contract: prove `Expr` is a usable, Spark-free data
  * record + the closed 21-variant enumeration (added `Expr.All`
  * in v0.3.1 per the Gap 2 feature-parity backlog). Per scala-data-
  * driven-refactor, this is pure data: the EXPRESSION SHAPE is
  * engine-portable; the engine-specific compile (Spark's
  * `Expr.eval`, Trino's compile, Databricks' value) is behavior
  * in the engine adapter.
  */
class ExprSpec extends AnyFunSuite with Matchers {

  // -- Literals --

  test("Literal carries value and dataType") {
    val e = Expr.Literal(LiteralValue.IntValue(42), SealedDataType.Int)
    e.value shouldBe LiteralValue.IntValue(42)
    e.dataType shouldBe SealedDataType.Int
  }

  // -- References --

  test("FieldRef carries the field name") {
    Expr.FieldRef("amount").name shouldBe "amount"
  }

  test("MeasureRef carries the measure name") {
    Expr.MeasureRef("total_passengers").name shouldBe "total_passengers"
  }

  // -- Arithmetic (5) --

  test("Add, Subtract, Multiply, Divide, Modulo carry two operands") {
    val a = Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int)
    val b = Expr.Literal(LiteralValue.IntValue(3), SealedDataType.Int)
    Expr.Add(a, b) shouldBe Expr.Add(a, b)
    Expr.Subtract(a, b) shouldBe Expr.Subtract(a, b)
    Expr.Multiply(a, b) shouldBe Expr.Multiply(a, b)
    Expr.Divide(a, b) shouldBe Expr.Divide(a, b)
    Expr.Modulo(a, b) shouldBe Expr.Modulo(a, b)
  }

  // -- Comparison (6) --

  test("Equal, NotEqual, LessThan, LessOrEqual, GreaterThan, GreaterOrEqual") {
    val a = Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)
    val b = Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int)
    Expr.Equal(a, b) shouldBe Expr.Equal(a, b)
    Expr.NotEqual(a, b) shouldBe Expr.NotEqual(a, b)
    Expr.LessThan(a, b) shouldBe Expr.LessThan(a, b)
    Expr.LessOrEqual(a, b) shouldBe Expr.LessOrEqual(a, b)
    Expr.GreaterThan(a, b) shouldBe Expr.GreaterThan(a, b)
    Expr.GreaterOrEqual(a, b) shouldBe Expr.GreaterOrEqual(a, b)
  }

  // -- Boolean (3) --

  test("And, Or, Not carry one or two operands") {
    val a = Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean)
    val b = Expr.Literal(LiteralValue.BoolValue(false), SealedDataType.Boolean)
    Expr.And(a, b) shouldBe Expr.And(a, b)
    Expr.Or(a, b) shouldBe Expr.Or(a, b)
    Expr.Not(a) shouldBe Expr.Not(a)
  }

  // -- Null checks (2) --

  test("IsNull, IsNotNull") {
    val a = Expr.FieldRef("amount")
    Expr.IsNull(a) shouldBe Expr.IsNull(a)
    Expr.IsNotNull(a) shouldBe Expr.IsNotNull(a)
  }

  // -- Cast (1) --

  test("Cast carries expr and targetType") {
    val a = Expr.Literal(LiteralValue.IntValue(42), SealedDataType.Int)
    val c = Expr.Cast(a, SealedDataType.BigInt)
    c.expr shouldBe a
    c.targetType shouldBe SealedDataType.BigInt
  }

  // -- Function call (1) --

  test("FunctionCall carries name and args") {
    val a = Expr.Literal(LiteralValue.StringValue("hello"), SealedDataType.Varchar)
    val c = Expr.FunctionCall("LOWER", Seq(a))
    c.name shouldBe "LOWER"
    c.args.size shouldBe 1
    c.args(0) shouldBe a
  }

  // -- closed enumeration + sealed exhaustiveness --

  test("Expr has exactly 21 cases") {
    val lit = Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int)
    val all: Set[Expr] = Set(
      // 1 literal
      lit,
      // 2 references
      Expr.FieldRef("a"),
      Expr.MeasureRef("a"),
      // 5 arithmetic
      Expr.Add(lit, lit),
      Expr.Subtract(lit, lit),
      Expr.Multiply(lit, lit),
      Expr.Divide(lit, lit),
      Expr.Modulo(lit, lit),
      // 6 comparison
      Expr.Equal(lit, lit),
      Expr.NotEqual(lit, lit),
      Expr.LessThan(lit, lit),
      Expr.LessOrEqual(lit, lit),
      Expr.GreaterThan(lit, lit),
      Expr.GreaterOrEqual(lit, lit),
      // 3 boolean
      Expr.And(lit, lit),
      Expr.Or(lit, lit),
      Expr.Not(lit),
      // 2 null checks
      Expr.IsNull(lit),
      Expr.IsNotNull(lit),
      // 1 cast
      Expr.Cast(lit, SealedDataType.BigInt),
      // 1 function call
      Expr.FunctionCall("f", Seq.empty),
    )
    all.size shouldBe 21
  }

  // -- nested + equality invariants --

  test("nested expressions carry the inner expressions") {
    val a = Expr.FieldRef("a")
    val b = Expr.FieldRef("b")
    val c = Expr.FieldRef("c")
    val e = Expr.Add(Expr.Multiply(a, b), c)
    e shouldBe Expr.Add(Expr.Multiply(a, b), c)
  }

  test("Deeply nested expression: Add(Add(a, b), Subtract(c, Multiply(d, e)))") {
    val a = Expr.FieldRef("a")
    val b = Expr.FieldRef("b")
    val c = Expr.FieldRef("c")
    val d = Expr.FieldRef("d")
    val e = Expr.FieldRef("e")
    val nested = Expr.Add(
      Expr.Add(a, b),
      Expr.Subtract(c, Expr.Multiply(d, e)),
    )
    nested shouldBe Expr.Add(
      Expr.Add(a, b),
      Expr.Subtract(c, Expr.Multiply(d, e)),
    )
  }

  // -- Serializable round-trip --

  test("Expr tree round-trips through Java serialization") {
    val lit = Expr.Literal(LiteralValue.IntValue(42), SealedDataType.Int)
    val e = Expr.Add(
      Expr.Multiply(Expr.FieldRef("a"), lit),
      Expr.FunctionCall("ABS", Seq(Expr.FieldRef("b"))),
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(e)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[Expr]
    restored shouldBe e
  }

  // -- Percent-of-total (Gap 2 closure) --

  test("All carries the measure name (percent-of-total / t.all form)") {
    Expr.All("total_amount").measureName shouldBe "total_amount"
  }

  test("Expr.All within a Divide tree round-trips through Java serialization") {
    val pct = Expr.Divide(
      Expr.FieldRef("amount"),
      Expr.All("total_amount"),
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(pct)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    ois.readObject().asInstanceOf[Expr] shouldBe pct
  }
}