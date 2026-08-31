/*
 * SM8 in-memory Connector — typed realize() spec.
 *
 * Verifies the typed realization contract for the always-realized
 * in-memory engine, mirroring `SparkEngineProviderRealizeSpec`:
 *   - `realize` returns `Some(provider)` (in-memory has no URL
 *     grammar — any string realizes the provider).
 *   - The realized provider's `identity.name` is the wire-stable
 *     engine name (`"in-memory"`), distinct from any adapter-version
 *     literal.
 *   - The realized provider is a fresh instance (no self-mutation
 *     of the descriptor).
 *   - The realized provider survives Java-serialization round-trip
 *     (closure-safety: `EngineProvider extends Serializable`, so
 *     the `EngineRegistry` can journal it).
 */
package io.sm8.connectors.inmemory

import io.sm8.core.engine.EngineProvider

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class InMemoryEngineProviderRealizeSpec extends AnyFunSuite with Matchers {

  test("realize(any url) → Some(provider) (in-memory has no URL grammar)") {
    val stub = new InMemoryEngineProviderDescriptor()
    stub.realize("") shouldBe defined
    stub.realize("whatever") shouldBe defined
    stub.realize(null) shouldBe defined
  }

  test("realize returns a provider with the wire-stable identity name") {
    val realized = new InMemoryEngineProviderDescriptor().realize("in-memory")
    realized shouldBe defined
    realized.get.identity.name shouldBe "in-memory"
  }

  test("realize: typed result is an InMemoryEngineProvider (not a generic cast)") {
    val realized: Option[EngineProvider] =
      new InMemoryEngineProviderDescriptor().realize("in-memory")
    realized.get shouldBe an[InMemoryEngineProvider]
  }

  test("realize returns a DIFFERENT instance than the descriptor (no self-mutation)") {
    val stub = new InMemoryEngineProviderDescriptor()
    val realized = stub.realize("in-memory")
    realized.get should not be theSameInstanceAs(stub)
  }

  test("realized provider survives Java-serialization round-trip (closure-safety)") {
    val p = new InMemoryEngineProviderDescriptor().realize("in-memory").get
    val bytes = {
      val bos = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(bos)
      oos.writeObject(p)
      oos.close()
      bos.toByteArray
    }
    val back = {
      val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes))
      ois.readObject().asInstanceOf[InMemoryEngineProvider]
    }
    back.available shouldBe true
    back.identity.name shouldBe "in-memory"
  }
}
