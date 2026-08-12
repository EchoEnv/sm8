package io.sm8.core.schema

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `Field` is a usable, Spark-free
  * data record (name + dataType + nullable). Per scala-data-driven-
  * refactor, this is pure data: the field SHAPE is engine-portable;
  * the engine-specific implementation is data the resolver USES to
  * produce this shape.
  */
class FieldSpec extends AnyFunSuite with Matchers {

  // -- constructor --

  test("Field carries name, dataType, nullable") {
    val f = Field("amount", SealedDataType.Double, nullable = true)
    f.name shouldBe "amount"
    f.dataType shouldBe SealedDataType.Double
    f.nullable shouldBe true
  }

  // -- sugar factories --

  test("Field.nonNull factory creates a non-nullable field") {
    val f = Field.nonNull("id", SealedDataType.BigInt)
    f.name shouldBe "id"
    f.dataType shouldBe SealedDataType.BigInt
    f.nullable shouldBe false
  }

  test("Field.nullable factory creates a nullable field") {
    val f = Field.nullable("email", SealedDataType.Varchar)
    f.name shouldBe "email"
    f.dataType shouldBe SealedDataType.Varchar
    f.nullable shouldBe true
  }

  // -- equality --

  test("Field equality: same name+dataType+nullable => equal") {
    Field("x", SealedDataType.BigInt, nullable = true) shouldBe
      Field("x", SealedDataType.BigInt, nullable = true)
    Field.nonNull("y", SealedDataType.Varchar) shouldBe
      Field.nonNull("y", SealedDataType.Varchar)
  }

  test("Field with different name => not equal") {
    Field("x", SealedDataType.BigInt, nullable = true) should not be
      Field("y", SealedDataType.BigInt, nullable = true)
  }

  test("Field with different dataType => not equal") {
    Field("x", SealedDataType.BigInt, nullable = true) should not be
      Field("x", SealedDataType.Varchar, nullable = true)
  }

  test("Field with different nullability => not equal") {
    Field("x", SealedDataType.BigInt, nullable = true) should not be
      Field("x", SealedDataType.BigInt, nullable = false)
  }

  // -- nested SealedDataType --

  test("Field with Array element type carries the element type") {
    val f = Field("tags", SealedDataType.Array(SealedDataType.Varchar), nullable = true)
    f.dataType shouldBe SealedDataType.Array(SealedDataType.Varchar)
  }

  test("Field with Map type carries both key and value types") {
    val f = Field("counts", SealedDataType.Map(SealedDataType.Varchar, SealedDataType.BigInt), nullable = false)
    f.dataType shouldBe SealedDataType.Map(SealedDataType.Varchar, SealedDataType.BigInt)
  }

  test("Field with Decimal type carries precision and scale") {
    val f = Field("price", SealedDataType.Decimal(10, 2), nullable = true)
    f.dataType shouldBe SealedDataType.Decimal(10, 2)
  }

  test("Field with Row type carries nested fields") {
    val f = Field(
      "address",
      SealedDataType.Row(Seq(
        Field.nonNull("street", SealedDataType.Varchar),
        Field.nullable("zip", SealedDataType.Varchar),
      )),
      nullable = true,
    )
    f.dataType shouldBe SealedDataType.Row(Seq(
      Field.nonNull("street", SealedDataType.Varchar),
      Field.nullable("zip", SealedDataType.Varchar),
    ))
  }

  // -- Serializable round-trip --

  test("Field round-trips through Java serialization") {
    val f = Field("amount", SealedDataType.Decimal(10, 2), nullable = true)
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(f)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[Field]
    restored shouldBe f
  }

  test("Field with nested SealedDataType (Row) round-trips") {
    val f = Field(
      "nested",
      SealedDataType.Row(Seq(
        Field.nonNull("a", SealedDataType.BigInt),
        Field.nullable("b", SealedDataType.Varchar),
      )),
      nullable = false,
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(f)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[Field]
    restored shouldBe f
  }
}