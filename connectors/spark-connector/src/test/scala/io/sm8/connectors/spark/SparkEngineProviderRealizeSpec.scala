/*
 * SM8 Spark Connector — typed realize() spec (PR-B per RFC
 * `adapters.md` Rule 4 + ADR-006 Post-#65 Refinement).
 *
 * Verifies the TYPED realization contract that replaces the
 * (String)-ctor reflection: `realize(url)` returns
 * `Some(realizedProvider)` for valid URLs, `None` for blank.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.MCPEngineProvider

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkEngineProviderRealizeSpec extends AnyFunSuite with Matchers {

  test("realize(blank) → None (per-connector grammar: non-blank required)") {
    val stub = new SparkEngineProviderDescriptor()
    stub.realize("") shouldBe None
    stub.realize("   ") shouldBe None
    stub.realize(null) shouldBe None
  }

  test("realize(valid master url) → Some with available=true + real session") {
    val p = new SparkEngineProviderDescriptor()
    val realized = p.realize("local[1]")
    realized shouldBe defined
    val r = realized.get.asInstanceOf[SparkEngineProvider]
    r.available shouldBe true
    r.spark should not be null
    r.spark.version should not be null
  }

  test("realize: typed result is a SparkEngineProvider (not a generic cast)") {
    val realized: Option[MCPEngineProvider] =
      new SparkEngineProviderDescriptor().realize("local[1]")
    realized.get shouldBe a [SparkEngineProvider]
  }

  test("realize returns a DIFFERENT instance than the stub (no self-mutation)") {
    val stub = new SparkEngineProviderDescriptor()
    val realized = stub.realize("local[1]")
    realized.get should not be theSameInstanceAs (stub)
  }

  test("realize(local[1]) survives Java-serialization round-trip (closure-safety)") {
    val p = new SparkEngineProviderDescriptor().realize("local[1]").get
    val bytes = {
      val bos = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(bos)
      oos.writeObject(p); oos.close(); bos.toByteArray
    }
    val back = {
      val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes))
      ois.readObject().asInstanceOf[SparkEngineProvider]
    }
    back.available shouldBe true
    back.identity.name shouldBe "spark-3.5"
  }
}
