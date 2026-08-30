/*
 * SM8 Spark Connector — engine-identity invariant spec.
 *
 * Pins the routing invariants that keep engine dispatch correct:
 *
 * 1. The realized provider's `identity.name` equals the URL
 *    parser's wire-stable engine name. The registry keys engines
 *    by `identity.name`; if the provider's name drifted from the
 *    parser's name, a `--engine spark` request would resolve a
 *    URL but never find its provider.
 *
 * 2. The descriptor's instance identity and object-companion
 *    identity agree (they are two views of the same SPI entry
 *    point).
 *
 * 3. The unrealized native-version sentinel is uniform across
 *    the descriptor's identity surfaces (the historic drift: the
 *    object companion said "unknown" where the instance said
 *    "<uninitialized>").
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.EngineUrl

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkEngineIdentityInvariantSpec extends AnyFunSuite with Matchers {

  test("provider default ctor name equals URL-parser wire name (routing invariant)") {
    val provider = new SparkEngineProvider(spark = null, bridge = SparkTypeBridge)
    provider.identity.name shouldBe SparkEngineConstants.WireName
    provider.identity.name shouldBe new SparkEngineUrlParser().engineName
    provider.identity.name shouldBe EngineUrl.Spark(master = "").engineName
  }

  test("descriptor instance identity equals object-companion identity") {
    val instance = new SparkEngineProviderDescriptor()
    instance.identity shouldBe SparkEngineProviderDescriptor.identity
  }

  test("descriptor identity carries uniform unrealized sentinel") {
    val instance = new SparkEngineProviderDescriptor()
    instance.identity.nativeVersion shouldBe SparkEngineConstants.UnrealizedNativeVersion
    SparkEngineProviderDescriptor.identity.nativeVersion shouldBe
      SparkEngineConstants.UnrealizedNativeVersion
  }

  test("realize() produces a provider whose name matches the wire name") {
    val realized = new SparkEngineProviderDescriptor().realize("local[1]")
    realized shouldBe defined
    realized.get.identity.name shouldBe SparkEngineConstants.WireName
  }

  test("identity adapter version is uniform across all construction sites") {
    val provider = new SparkEngineProvider(spark = null, bridge = SparkTypeBridge)
    provider.identity.engineAdapterVersion shouldBe SparkEngineConstants.AdapterVersion
    new SparkEngineProviderDescriptor().identity.engineAdapterVersion shouldBe
      SparkEngineConstants.AdapterVersion
    SparkEngineProviderDescriptor.identity.engineAdapterVersion shouldBe
      SparkEngineConstants.AdapterVersion
  }
}
