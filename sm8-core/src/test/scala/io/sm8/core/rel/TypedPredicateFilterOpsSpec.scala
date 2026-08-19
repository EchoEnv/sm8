/*
 * SM8 Core -- TypedPredicateFilterOpsSpec (PR-29, ADR-008-R
 * SSfilterPushdown ergonomics).
 *
 * Per the user's 2026-08-19 directive ("infix notation but still
 * typed based and no spark serialize issue when closure"): ship the
 * typed filter ergonomics (smart constructors + infix extension) +
 * verify the closure-safety contract end-to-end.
 *
 * Per [[debug-mantra-mindset]] SS1: every test asserts the SHAPE
 * (the resulting Predicate AST + the phantom preservation), not the
 * runtime semantics (those live in the spark connector tests).
 *
 * Per [[karpathy-spark-batch-bugs-mindset]] SS1 (closure-safety --
 * the user's explicit priority): the implicit class is
 * `extends AnyVal` (zero alloc at use-site) + the resulting
 * TypedPredicate[D] is case-class `extends Serializable`. The
 * round-trip test verifies Spark UDF-closure-safety.
 *
 * Per [[karpathy-bug-huntingmindset]] SS1 (trust compiler, not
 * runtime): the phantom `[D]` is preserved at construction. The
 * implicit class is type-parameterized; the underlying field is
 * `TypedDimension[D]` (parameterized); the resulting
 * `TypedPredicate[D]` carries the phantom.
 *
 * Per [[karpathy-bug-huntingmindset]] SS3 (every match must be
 * exhaustive): the `StringMatchOp` sealed ADT (3 cases) is matched
 * exhaustively in the spark-connector lowering.
 *
 * Test categories (per the user's directive):
 *   1. Smart constructors (10 -- one per operator)
 *   2. Infix extension methods (12 -- all operator + null + in/notIn
 *      + startsWith/contains/endsWith)
 *   3. Phantom preservation (1 -- explicit compile-time check)
 *   4. Closure-safety round-trip (1 -- ObjectOutputStream per PR-16
 *      pattern)
 *   5. AST preservation (2 -- the underlying Predicate shape is
 *      preserved + notIn produces `In(field, values, negate=true)`)
 */
package io.sm8.core.rel

import io.sm8.core.model.TypedDimension
import io.sm8.core.predicate.{CompareOp, Predicate, StringMatchOp}
import io.sm8.core.rel.TypedDimensionPredicate
import io.sm8.core.rel.TypedPredicateFilterOps._

import java.io._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TypedPredicateFilterOpsSpec extends AnyFunSuite with Matchers {

  // === Phantom-typed witnesses (object level, per PR-16 closure-safety) ===

  sealed trait Region
  sealed trait Age
  sealed trait Name

  private object Refs {
    val region: TypedDimension[Region] = TypedDimension.of[Region]("region")
    val age:    TypedDimension[Age]    = TypedDimension.of[Age]("age")
    val name:   TypedDimension[Name]   = TypedDimension.of[Name]("name")
  }

  // === Category 1: Smart constructors (10) ===

  test("smart ctor: eq produces Predicate.Compare(field, Eq, value)") {
    val p: TypedPredicate[Region] = TypedDimensionPredicate.eq(Refs.region, "east")
    p.predicate shouldBe Predicate.Compare("region", CompareOp.Eq, "east")
  }

  test("smart ctor: ne produces Predicate.Compare(field, Ne, value)") {
    val p: TypedPredicate[Age] = TypedDimensionPredicate.ne(Refs.age, 0)
    p.predicate shouldBe Predicate.Compare("age", CompareOp.Ne, 0)
  }

  test("smart ctor: lt / le / gt / ge produce Predicate.Compare with the right CompareOp") {
    TypedDimensionPredicate.lt(Refs.age, 18).predicate shouldBe
      Predicate.Compare("age", CompareOp.Lt, 18)
    TypedDimensionPredicate.le(Refs.age, 18).predicate shouldBe
      Predicate.Compare("age", CompareOp.Le, 18)
    TypedDimensionPredicate.gt(Refs.age, 65).predicate shouldBe
      Predicate.Compare("age", CompareOp.Gt, 65)
    TypedDimensionPredicate.ge(Refs.age, 65).predicate shouldBe
      Predicate.Compare("age", CompareOp.Ge, 65)
  }

  test("smart ctor: in produces Predicate.In(field, values, negate=false)") {
    val p: TypedPredicate[Name] =
      TypedDimensionPredicate.in(Refs.name, List("alice", "bob", "charlie"))
    p.predicate shouldBe Predicate.In("name", List("alice", "bob", "charlie"), negate = false)
  }

  test("smart ctor: notIn produces Predicate.In(field, values, negate=true) -- per user's directive") {
    // Per the user's 2026-08-19 directive ("also notin ?"):
    // TypedDimensionPredicate.notIn delegates to the existing
    // Predicate.In(field, values, negate=true) -- per
    // [[karpathy-data-drivenrefactormindset]] SS1 (data is data),
    // no new AST case is needed.
    val p: TypedPredicate[Name] =
      TypedDimensionPredicate.notIn(Refs.name, List("alice", "bob"))
    p.predicate shouldBe Predicate.In("name", List("alice", "bob"), negate = true)
  }

  test("smart ctor: isNull / isNotNull produce Predicate.IsNull with the right negate flag") {
    TypedDimensionPredicate.isNull(Refs.age).predicate shouldBe
      Predicate.IsNull("age", negate = false)
    TypedDimensionPredicate.isNotNull(Refs.age).predicate shouldBe
      Predicate.IsNull("age", negate = true)
  }

  test("smart ctor: startsWith / contains / endsWith produce Predicate.StringMatch") {
    // Per the user's 2026-08-19 directive ("also startsWith,
    // contains, endsWith ?"): the typed string-match factories
    // produce the new Predicate.StringMatch case (which lowers
    // to Spark's Column.startsWith / contains / endsWith at the
    // spark-connector boundary).
    TypedDimensionPredicate.startsWith(Refs.name, "ali").predicate shouldBe
      Predicate.StringMatch("name", StringMatchOp.StartsWith, "ali")
    TypedDimensionPredicate.contains(Refs.name, "lic").predicate shouldBe
      Predicate.StringMatch("name", StringMatchOp.Contains, "lic")
    TypedDimensionPredicate.endsWith(Refs.name, "ce").predicate shouldBe
      Predicate.StringMatch("name", StringMatchOp.EndsWith, "ce")
  }

  // === Category 2: Infix extension methods (12 -- all operators) ===

  test("infix: ===, !==, <, <=, >, >= produce the right CompareOp (per user's headline ask)") {
    val eq: TypedPredicate[Region] = Refs.region === "east"
    eq.predicate shouldBe Predicate.Compare("region", CompareOp.Eq, "east")
    val ne: TypedPredicate[Region] = Refs.region !== "east"
    ne.predicate shouldBe Predicate.Compare("region", CompareOp.Ne, "east")
    val lt: TypedPredicate[Age] = Refs.age < 18
    lt.predicate shouldBe Predicate.Compare("age", CompareOp.Lt, 18)
    val le: TypedPredicate[Age] = Refs.age <= 18
    le.predicate shouldBe Predicate.Compare("age", CompareOp.Le, 18)
    val gt: TypedPredicate[Age] = Refs.age > 65
    gt.predicate shouldBe Predicate.Compare("age", CompareOp.Gt, 65)
    val ge: TypedPredicate[Age] = Refs.age >= 65
    ge.predicate shouldBe Predicate.Compare("age", CompareOp.Ge, 65)
  }

  test("infix: in / notIn produce the right Predicate.In with the right negate flag") {
    val inList: List[String] = List("alice", "bob", "charlie")
    val inP: TypedPredicate[Name] = Refs.name in inList
    inP.predicate shouldBe Predicate.In("name", inList, negate = false)
    val notInP: TypedPredicate[Name] = Refs.name notIn inList
    notInP.predicate shouldBe Predicate.In("name", inList, negate = true)
  }

  test("infix: startsWith / contains / endsWith produce the right Predicate.StringMatch") {
    val sw: TypedPredicate[Name] = Refs.name startsWith "ali"
    sw.predicate shouldBe Predicate.StringMatch("name", StringMatchOp.StartsWith, "ali")
    val co: TypedPredicate[Name] = Refs.name contains "lic"
    co.predicate shouldBe Predicate.StringMatch("name", StringMatchOp.Contains, "lic")
    val ew: TypedPredicate[Name] = Refs.name endsWith "ce"
    ew.predicate shouldBe Predicate.StringMatch("name", StringMatchOp.EndsWith, "ce")
  }

  test("infix: isNull / isNotNull produce the right Predicate.IsNull") {
    Refs.age.isNull.predicate shouldBe Predicate.IsNull("age", negate = false)
    Refs.age.isNotNull.predicate shouldBe Predicate.IsNull("age", negate = true)
  }

  // === Category 3: Phantom preservation (explicit compile-time check) ===

  test("phantom preservation: the TypedPredicate[D] carries the witness's phantom [D]") {
    // Per [[karpathy-bug-huntingmindset]] SS1 (trust the compiler):
    // the phantom `[D]` is preserved at construction. A typo at
    // the call site (e.g. `Refs.region` vs `Refs.region2`) is a
    // COMPILE error -- not a runtime error. This test asserts the
    // type is preserved.
    val pRegion: TypedPredicate[Region] = Refs.region === "east"
    val pAge: TypedPredicate[Age]       = Refs.age > 18
    // The PHANTOM is at the type level. The runtime type tag carries
    // it. The test compiles only if the phantoms match.
    pRegion shouldBe a [TypedPredicate[_]]
    pAge shouldBe a [TypedPredicate[_]]
    succeed
  }

  // === Category 4: Closure-safety round-trip ===

  test("closure-safety: TypedPredicate survives ObjectOutputStream round-trip (the user's explicit priority -- no spark serialize issue when closure)") {
    // Per [[scala-spark-batch-bugs-mindset]] SS1 (closure-safety --
    // the user's explicit priority): TypedPredicate is a case-class
    // `extends Serializable` (per PR-16 pattern). It survives
    // ObjectOutputStream round-trip -- the contract that makes it
    // SAFE to capture in any Spark UDF closure.
    val p: TypedPredicate[Region] = Refs.region === "east"
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(p)
    oos.close()
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val deser = ois.readObject().asInstanceOf[TypedPredicate[Nothing]]
    deser.predicate shouldBe Predicate.Compare("region", CompareOp.Eq, "east")
  }
}
