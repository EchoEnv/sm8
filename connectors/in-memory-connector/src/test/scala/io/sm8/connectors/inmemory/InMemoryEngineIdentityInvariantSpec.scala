/*
 * SM8 In-Memory Connector — engine-identity invariant spec.
 *
 * Pins the routing invariants that keep engine dispatch correct
 * (mirrors `SparkEngineIdentityInvariantSpec`):
 *
 * 1. The provider's `identity.name` equals the URL parser's
 *    wire-stable engine name.
 * 2. The descriptor's instance identity and object-companion
 *    identity agree.
 */
package io.sm8.connectors.inmemory

import io.sm8.core.engine.EngineUrl

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class InMemoryEngineIdentityInvariantSpec extends AnyFunSuite with Matchers {

  test("provider name equals URL-parser wire name (routing invariant)") {
    val provider = new InMemoryEngineProvider()
    provider.identity.name shouldBe InMemoryEngineConstants.WireName
    provider.identity.name shouldBe new InMemoryEngineUrlParser().engineName
    provider.identity.name shouldBe EngineUrl.InMemory().engineName
  }

  test("descriptor instance identity equals object-companion identity") {
    val instance = new InMemoryEngineProviderDescriptor()
    instance.identity shouldBe InMemoryEngineProviderDescriptor.identity
  }

  test("embedded engine carries a single native version across all sites") {
    val provider = new InMemoryEngineProvider()
    val descriptor = new InMemoryEngineProviderDescriptor()
    provider.identity.nativeVersion shouldBe InMemoryEngineConstants.NativeVersion
    descriptor.identity.nativeVersion shouldBe InMemoryEngineConstants.NativeVersion
    InMemoryEngineProviderDescriptor.identity.nativeVersion shouldBe
      InMemoryEngineConstants.NativeVersion
  }

  test("identity adapter version is uniform across all construction sites") {
    val provider = new InMemoryEngineProvider()
    val descriptor = new InMemoryEngineProviderDescriptor()
    provider.identity.engineAdapterVersion shouldBe InMemoryEngineConstants.AdapterVersion
    descriptor.identity.engineAdapterVersion shouldBe InMemoryEngineConstants.AdapterVersion
    InMemoryEngineProviderDescriptor.identity.engineAdapterVersion shouldBe
      InMemoryEngineConstants.AdapterVersion
  }
}
