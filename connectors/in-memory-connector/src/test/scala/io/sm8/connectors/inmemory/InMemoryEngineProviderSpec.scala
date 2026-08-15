/*
 * SM8 in-memory Connector — engine-provider parity spec (PR-B).
 */
package io.sm8.connectors.inmemory

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class InMemoryEngineProviderSpec extends AnyFunSuite with Matchers {

  test("always available (reference engine on a bare classpath)") {
    val p = new InMemoryEngineProvider()
    p.available shouldBe true
    p.identity.name shouldBe "in-memory"
  }

  test("realize(any url) → None (in-memory has no URL grammar; already realized)") {
    val p = new InMemoryEngineProvider()
    p.realize("anything") shouldBe None
    p.realize("local[*]") shouldBe None
  }

  test("query returns an empty PortableQueryResult with engine marker") {
    val p = new InMemoryEngineProvider()
    val out = p.query(null, null, null)
    out.isRight shouldBe true
    out.toOption.get.metadata("engine") shouldBe "in-memory"
  }
}
