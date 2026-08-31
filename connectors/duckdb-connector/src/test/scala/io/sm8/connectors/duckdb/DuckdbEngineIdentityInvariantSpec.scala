/*
 * SM8 DuckDB connector — engine-identity invariant spec.
 *
 * Pins the routing invariant: the realized provider's
 * `identity.name` equals the wire-stable engine name
 * (`DuckdbEngineConstants.WireName = "duckdb"`), and the realized
 * URL carrier's `engineName` matches too. Mirrors
 * `SparkEngineIdentityInvariantSpec`,
 * `TrinoEngineIdentityInvariantSpec`, and
 * `InMemoryEngineIdentityInvariantSpec` for pattern parity.
 */
package io.sm8.connectors.duckdb

import io.sm8.core.engine.{EngineUrl, TypedRealizationProvider}
import io.sm8.core.model.{Model, SourceRef}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DuckdbEngineIdentityInvariantSpec extends AnyFunSuite with Matchers {

  test("realized provider identity.name == DuckdbEngineConstants.WireName") {
    val provider = new DuckdbEngineProviderDescriptor().realize("jdbc:duckdb:")
    provider shouldBe defined
    provider.get.identity.name shouldBe DuckdbEngineConstants.WireName
  }

  test("realized provider identity.engineAdapterVersion matches DuckdbEngineConstants.AdapterVersion") {
    val provider = new DuckdbEngineProviderDescriptor().realize("jdbc:duckdb:")
    provider.get.identity.engineAdapterVersion shouldBe DuckdbEngineConstants.AdapterVersion
  }

  test("realized descriptor's identity (not the realized provider's) carries the UnrealizedNativeVersion sentinel") {
    val descriptor = new DuckdbEngineProviderDescriptor()
    descriptor.identity.nativeVersion shouldBe DuckdbEngineConstants.UnrealizedNativeVersion
  }

  test("URL parser's engineName matches the descriptor's identity.name (wire-stable invariant)") {
    val parser = new DuckdbEngineUrlParser()
    val parsed: EngineUrl = parser.parse("jdbc:duckdb:").toOption.get
    parsed.engineName shouldBe "duckdb"
    new DuckdbEngineProviderDescriptor().identity.name shouldBe "duckdb"
  }

  test("realized EngineUrl.DuckDb case carries the same engine name as the descriptor + provider identity") {
    val parsed: EngineUrl = new DuckdbEngineUrlParser().parse("jdbc:duckdb:/tmp/x.duckdb").toOption.get
    parsed shouldBe an[EngineUrl.DuckDb]
    parsed.engineName shouldBe DuckdbEngineConstants.WireName
    // The wire-stable invariant: parser, descriptor, realized
    // provider, and the URL carrier all carry the same name.
    val realized = new DuckdbEngineProviderDescriptor().realize("jdbc:duckdb:/tmp/x.duckdb")
    realized.get.identity.name shouldBe parsed.engineName
  }

  test("realized provider is a real DuckdbEngineProvider (typed, not a generic EngineProvider)") {
    val provider = new DuckdbEngineProviderDescriptor().realize("jdbc:duckdb:")
    provider.get shouldBe a[DuckdbEngineProvider]
  }
}
