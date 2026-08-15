/*
 * SM8 Trino Connector — typed realize() spec (PR-B parity).
 */
package io.sm8.connectors.trino

import io.sm8.core.engine.MCPEngineProvider

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TrinoEngineProviderSpec extends AnyFunSuite with Matchers {

  test("no-arg ctor: contract-gap stub (available = false)") {
    val p = new TrinoEngineProvider()
    p.available shouldBe false
    p.identity.name shouldBe "trino"
  }

  test("realize(jdbc:trino://...) → Some with available = true") {
    val realized = new TrinoEngineProvider().realize("jdbc:trino://host:8080/catalog")
    realized shouldBe defined
    realized.get.available shouldBe true
    realized.get.identity.name shouldBe "trino"
  }

  test("realize(non-trino URL) → None (per-connector grammar)") {
    new TrinoEngineProvider().realize("spark://host:7077") shouldBe None
    new TrinoEngineProvider().realize("local[*]") shouldBe None
    new TrinoEngineProvider().realize("") shouldBe None
    new TrinoEngineProvider().realize(null) shouldBe None
  }

  test("query on stub: typed FeatureDeferred error (never a silent no-op)") {
    val stub = new TrinoEngineProvider()
    stub.available shouldBe false
    // The stub itself never queries; the realized stub defers loudly.
    val realized = stub.realize("jdbc:trino://h:8080").get
    realized.query(null, null, null).isLeft shouldBe true
  }
}
