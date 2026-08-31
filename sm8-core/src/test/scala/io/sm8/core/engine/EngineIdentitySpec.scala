package io.sm8.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `EngineIdentity` is a usable, Spark-free
  * data record. Per [[scala-data-driven-refactor-mindset]], this is pure data:
  * the identity SHAPE is engine-portable; the VALUES are engine-
  * specific.
  */
class EngineIdentitySpec extends AnyFunSuite with Matchers {

  test("EngineIdentity carries name + nativeVersion + engineAdapterVersion") {
    val id = EngineIdentity(
      name                 = "trino",
      nativeVersion        = "0.286",
      engineAdapterVersion = "0.3.0",
    )
    id.name shouldBe "trino"
    id.nativeVersion shouldBe "0.286"
    id.engineAdapterVersion shouldBe "0.3.0"
  }

  test("realistic: spark adapter identity") {
    val id = EngineIdentity(
      name                 = "spark",
      nativeVersion        = "3.5.8",
      engineAdapterVersion = "0.3.0",
    )
    id.name shouldBe "spark"
  }

  test("realistic: databricks adapter identity") {
    val id = EngineIdentity(
      name                 = "databricks",
      nativeVersion        = "13.3",
      engineAdapterVersion = "0.3.0",
    )
    id.name shouldBe "databricks"
  }

  test("EngineIdentity is a value, not a singleton — two with same fields are equal") {
    val a = EngineIdentity("trino", "0.286", "0.2.4")
    val b = EngineIdentity("trino", "0.286", "0.2.4")
    a shouldBe b
  }

  test("EngineIdentity with different names are not equal") {
    val a = EngineIdentity("trino", "0.286", "0.2.4")
    val b = EngineIdentity("spark", "3.5.8", "0.2.4")
    a should not be b
  }

  test("EngineIdentity with different versions are not equal") {
    val a = EngineIdentity("trino", "0.286", "0.2.4")
    val b = EngineIdentity("trino", "0.287", "0.2.4")
    a should not be b
  }

  test("EngineIdentity round-trips through Java serialization") {
    val id = EngineIdentity(
      name                 = "trino",
      nativeVersion        = "0.286",
      engineAdapterVersion = "0.3.0",
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(id)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[EngineIdentity]
    restored shouldBe id
  }
}