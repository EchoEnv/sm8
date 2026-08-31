/*
 * SM8 Trino Connector — typed realize() spec.
 *
 * Verifies the typed realization contract for the `jdbc:trino://`
 * grammar, mirroring `SparkEngineProviderRealizeSpec`:
 *   - `realize` returns `None` for blank / non-`jdbc:trino://` URLs
 *     and `Some(provider)` for a grammar-valid URL.
 *   - The realized provider's `identity.name` is the wire-stable
 *     engine name (`"trino"`).
 *   - The realized provider is a fresh instance (no self-mutation
 *     of the descriptor).
 *   - The realized provider survives Java-serialization round-trip
 *     (closure-safety: `EngineProvider extends Serializable`, so
 *     the `EngineRegistry` can journal it).
 */
package io.sm8.connectors.trino

import io.sm8.core.engine.EngineProvider

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TrinoEngineProviderRealizeSpec extends AnyFunSuite with Matchers {

  test("realize(blank) → None (per-connector grammar: non-blank required)") {
    val stub = new TrinoEngineProviderDescriptor()
    stub.realize("") shouldBe None
    stub.realize("   ") shouldBe None
    stub.realize(null) shouldBe None
  }

  test("realize(non-jdbc:trino URL) → None (grammar mismatch)") {
    val stub = new TrinoEngineProviderDescriptor()
    stub.realize("http://not-a-jdbc-url") shouldBe None
    stub.realize("jdbc:mysql://wrong-engine") shouldBe None
  }

  test("realize(valid jdbc:trino URL) → Some(provider)") {
    val realized =
      new TrinoEngineProviderDescriptor().realize("jdbc:trino://localhost:8080")
    realized shouldBe defined
  }

  test("realize returns a provider with the wire-stable identity name") {
    val realized =
      new TrinoEngineProviderDescriptor().realize("jdbc:trino://localhost:8080")
    realized.get.identity.name shouldBe "trino"
  }

  test("realize: typed result is a TrinoEngineProvider (not a generic cast)") {
    val realized: Option[EngineProvider] =
      new TrinoEngineProviderDescriptor().realize("jdbc:trino://localhost:8080")
    realized.get shouldBe a[TrinoEngineProvider]
  }

  test("realize returns a DIFFERENT instance than the descriptor (no self-mutation)") {
    val stub = new TrinoEngineProviderDescriptor()
    val realized = stub.realize("jdbc:trino://localhost:8080")
    realized.get should not be theSameInstanceAs(stub)
  }

  test("realized provider survives Java-serialization round-trip (closure-safety)") {
    val p =
      new TrinoEngineProviderDescriptor().realize("jdbc:trino://localhost:8080").get
    val bytes = {
      val bos = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(bos)
      oos.writeObject(p)
      oos.close()
      bos.toByteArray
    }
    val back = {
      val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes))
      ois.readObject().asInstanceOf[TrinoEngineProvider]
    }
    back.identity.name shouldBe "trino"
  }
}
