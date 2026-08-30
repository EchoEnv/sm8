/*
 * SM8 Trino Connector — engine-identity invariant spec.
 *
 * Pins the routing invariants that keep engine dispatch correct
 * (mirrors `SparkEngineIdentityInvariantSpec`):
 *
 * 1. The provider's `identity.name` equals the URL parser's
 *    wire-stable engine name.
 * 2. The descriptor's instance identity and object-companion
 *    identity agree.
 * 3. The unrealized native-version sentinel is uniform across
 *    the descriptor's identity surfaces (the historic drift: the
 *    object companion said "unknown" where the instance said
 *    "<uninitialized>").
 */
package io.sm8.connectors.trino

import io.sm8.core.engine.EngineUrl

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TrinoEngineIdentityInvariantSpec extends AnyFunSuite with Matchers {

  test("provider name equals URL-parser wire name (routing invariant)") {
    val provider = new TrinoEngineProvider()
    provider.identity.name shouldBe TrinoEngineConstants.WireName
    provider.identity.name shouldBe new TrinoEngineUrlParser().engineName
    provider.identity.name shouldBe EngineUrl.Trino(jdbcUrl = "").engineName
  }

  test("descriptor instance identity equals object-companion identity") {
    val instance = new TrinoEngineProviderDescriptor()
    instance.identity shouldBe TrinoEngineProviderDescriptor.identity
  }

  test("descriptor identity carries uniform unrealized sentinel") {
    val instance = new TrinoEngineProviderDescriptor()
    instance.identity.nativeVersion shouldBe TrinoEngineConstants.UnrealizedNativeVersion
    TrinoEngineProviderDescriptor.identity.nativeVersion shouldBe
      TrinoEngineConstants.UnrealizedNativeVersion
  }

  test("realized provider (URL given) carries the realized-stub sentinel") {
    val provider = new TrinoEngineProvider("jdbc:trino://host:8080")
    provider.identity.nativeVersion shouldBe TrinoEngineConstants.RealizedStubNativeVersion
  }

  test("identity adapter version is uniform across all construction sites") {
    val provider = new TrinoEngineProvider()
    provider.identity.engineAdapterVersion shouldBe TrinoEngineConstants.AdapterVersion
    new TrinoEngineProviderDescriptor().identity.engineAdapterVersion shouldBe
      TrinoEngineConstants.AdapterVersion
    TrinoEngineProviderDescriptor.identity.engineAdapterVersion shouldBe
      TrinoEngineConstants.AdapterVersion
  }
}
