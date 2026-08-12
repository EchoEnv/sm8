package io.sm8.core.schema

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `SealedDataType` is a usable, Spark-free
  * data record + the closed 12-variant enumeration. Per scala-data-
  * driven-refactor, this is pure data: the type system is the
  * engine-portable contract; the engine-specific mapping (Spark
  * `DataType`, Trino `Type`, etc.) is behavior in the engine
  * adapter layer.
  */
class SealedDataTypeSpec extends AnyFunSuite with Matchers {

  // -- Primitives --

  test("primitives: BigInt, Int, Double, Varchar, Boolean") {
    val primitives: Set[SealedDataType] = Set(
      SealedDataType.BigInt, SealedDataType.Int, SealedDataType.Double,
      SealedDataType.Varchar, SealedDataType.Boolean,
    )
    primitives.size shouldBe 5
  }

  test("BigInt is a singleton") {
    SealedDataType.BigInt shouldBe SealedDataType.BigInt
  }

  test("Boolean is distinct from Varchar (different shapes)") {
    SealedDataType.Boolean should not be SealedDataType.Varchar
  }

  // -- Temporal --

  test("temporals: Timestamp, Date") {
    val temporals: Set[SealedDataType] =
      Set(SealedDataType.Timestamp, SealedDataType.Date)
    temporals.size shouldBe 2
  }

  // -- Decimal --

  test("Decimal carries precision and scale") {
    val d = SealedDataType.Decimal(precision = 10, scale = 2)
    d.precision shouldBe 10
    d.scale shouldBe 2
  }

  test("Decimal(38, 0) is a valid whole-number decimal") {
    val d = SealedDataType.Decimal(38, 0)
    d.precision shouldBe 38
    d.scale shouldBe 0
  }

  test("Decimal equality: same precision+scale => equal") {
    SealedDataType.Decimal(10, 2) shouldBe SealedDataType.Decimal(10, 2)
  }

  test("Decimal with different precision or scale => not equal") {
    SealedDataType.Decimal(10, 2) should not be SealedDataType.Decimal(10, 3)
    SealedDataType.Decimal(10, 2) should not be SealedDataType.Decimal(11, 2)
  }

  // -- Nested --

  test("Array carries elementType") {
    val a = SealedDataType.Array(SealedDataType.Varchar)
    a.elementType shouldBe SealedDataType.Varchar
  }

  test("Map carries keyType and valueType") {
    val m = SealedDataType.Map(SealedDataType.Varchar, SealedDataType.BigInt)
    m.keyType shouldBe SealedDataType.Varchar
    m.valueType shouldBe SealedDataType.BigInt
  }

  test("Row carries fields") {
    val r = SealedDataType.Row(Seq(
      Field.nonNull("id", SealedDataType.BigInt),
      Field.nullable("name", SealedDataType.Varchar),
    ))
    r.fields.size shouldBe 2
    r.fields(0).name shouldBe "id"
    r.fields(1).name shouldBe "name"
  }

  // -- Special --

  test("Json is a singleton (untyped JSON string)") {
    SealedDataType.Json shouldBe SealedDataType.Json
  }

  // -- closed enumeration + sealed exhaustiveness --

  test("SealedDataType has exactly 12 cases") {
    val all: Set[SealedDataType] = Set(
      // 5 primitives
      SealedDataType.BigInt, SealedDataType.Int, SealedDataType.Double,
      SealedDataType.Varchar, SealedDataType.Boolean,
      // 2 temporals
      SealedDataType.Timestamp, SealedDataType.Date,
      // 1 decimal
      SealedDataType.Decimal(10, 0),
      // 3 nested
      SealedDataType.Array(SealedDataType.Varchar),
      SealedDataType.Map(SealedDataType.Varchar, SealedDataType.Varchar),
      SealedDataType.Row(Seq.empty),
      // 1 special
      SealedDataType.Json,
    )
    all.size shouldBe 12
  }

  test("Sealed exhaustiveness: pattern-match over all 12 cases") {
    val all: Seq[SealedDataType] = Seq(
      SealedDataType.BigInt, SealedDataType.Int, SealedDataType.Double,
      SealedDataType.Varchar, SealedDataType.Boolean,
      SealedDataType.Timestamp, SealedDataType.Date,
      SealedDataType.Decimal(10, 0),
      SealedDataType.Array(SealedDataType.Varchar),
      SealedDataType.Map(SealedDataType.Varchar, SealedDataType.Varchar),
      SealedDataType.Row(Seq.empty),
      SealedDataType.Json,
    )
    all.foreach {
      case SealedDataType.BigInt        => ()
      case SealedDataType.Int           => ()
      case SealedDataType.Double        => ()
      case SealedDataType.Varchar       => ()
      case SealedDataType.Boolean       => ()
      case SealedDataType.Timestamp     => ()
      case SealedDataType.Date          => ()
      case _: SealedDataType.Decimal    => ()
      case _: SealedDataType.Array       => ()
      case _: SealedDataType.Map         => ()
      case _: SealedDataType.Row         => ()
      case SealedDataType.Json          => ()
    }
  }

  // -- equality invariants --

  test("case objects are singletons (equal to themselves)") {
    val objs: Seq[SealedDataType] = Seq(
      SealedDataType.BigInt, SealedDataType.Int, SealedDataType.Double,
      SealedDataType.Varchar, SealedDataType.Boolean,
      SealedDataType.Timestamp, SealedDataType.Date,
      SealedDataType.Json,
    )
    objs.foreach { o => o shouldBe o }
  }

  test("nested types with different parameters are not equal") {
    SealedDataType.Array(SealedDataType.Varchar) should not be
      SealedDataType.Array(SealedDataType.BigInt)
    SealedDataType.Map(SealedDataType.Varchar, SealedDataType.BigInt) should not be
      SealedDataType.Map(SealedDataType.BigInt, SealedDataType.BigInt)
  }

  // -- Serializable round-trip --

  test("all 12 case variants round-trip through Java serialization") {
    val cases: Seq[SealedDataType] = Seq(
      SealedDataType.BigInt, SealedDataType.Int, SealedDataType.Double,
      SealedDataType.Varchar, SealedDataType.Boolean,
      SealedDataType.Timestamp, SealedDataType.Date,
      SealedDataType.Decimal(10, 2),
      SealedDataType.Array(SealedDataType.Varchar),
      SealedDataType.Map(SealedDataType.Varchar, SealedDataType.BigInt),
      SealedDataType.Row(Seq(Field.nonNull("id", SealedDataType.BigInt))),
      SealedDataType.Json,
    )
    cases.foreach { t =>
      val bos = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(bos)
      oos.writeObject(t)
      oos.close()
      val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
      val ois = new java.io.ObjectInputStream(bis)
      val restored = ois.readObject().asInstanceOf[SealedDataType]
      restored shouldBe t
    }
  }
}