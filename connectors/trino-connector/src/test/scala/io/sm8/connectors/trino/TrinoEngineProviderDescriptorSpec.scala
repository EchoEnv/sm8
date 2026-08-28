/*
 * SM8 Trino Connector — descriptor discovery + realize spec (PR-195).
 *
 * Mirrors `SparkEngineProviderDiscoverySpec` + `SparkEngineProviderDescriptorRealizeSpec`.
 * Verifies:
 *   1. ServiceLoader discovers the descriptor (not the heavy provider)
 *      through the standard `META-INF/services/io.sm8.core.engine.EngineProvider`.
 *   2. The no-arg ctor yields a descriptor with the documented wire
 *      identity (`"trino"`, `available = false`).
 *   3. `realize(jdbc:trino://...)` returns the realized provider.
 *   4. `realize(non-trino URL)` returns `None` (silent legacy contract).
 *   5. `realizeTyped(EngineUrl.Trino)` returns `Right(provider)`.
 *   6. `realizeTyped(non-Trino)` returns typed `Left(ConnectionFailed)`.
 *
 * PR-195 closes the descriptor-pattern gap: trino's META-INF used to
 * register the heavy `TrinoEngineProvider` directly; now it registers
 * the descriptor (PR-O4g parity with spark).
 */
package io.sm8.connectors.trino

import io.sm8.core.engine.{EngineProvider, EngineUrl}

import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TrinoEngineProviderDescriptorSpec extends AnyFunSuite with Matchers {

  // -- Portal: ServiceLoader discovery --

  test("Portal: META-INF/services declares TrinoEngineProviderDescriptor (not the heavy provider)") {
    val lines = scala.collection.mutable.Set[String]()
    val classLoader = classOf[TrinoEngineProviderDescriptor].getClassLoader
    val urls = classLoader.getResources("META-INF/services/io.sm8.core.engine.EngineProvider")
    urls.asScala.foreach { u =>
      scala.io.Source.fromURL(u).getLines().foreach(lines += _)
    }
    val set = lines.toSet
    set should contain (classOf[TrinoEngineProviderDescriptor].getName)
    set should not contain classOf[TrinoEngineProvider].getName
  }

  test("Portal: ServiceLoader discovers TrinoEngineProviderDescriptor from the classpath") {
    val providers = ServiceLoader
      .load(classOf[EngineProvider], classOf[TrinoEngineProviderDescriptor].getClassLoader)
      .iterator()
      .asScala
      .toList
    val classes = providers.map(_.getClass.getName).toSet
    classes should contain (classOf[TrinoEngineProviderDescriptor].getName)
  }

  // -- No-arg ctor: contract-gap stub shape --

  test("no-arg ctor: produces the contract-gap stub (available = false, name = 'trino')") {
    val d = new TrinoEngineProviderDescriptor()
    d.available shouldBe false
    d.identity.name shouldBe "trino"
    d.identity.engineAdapterVersion shouldBe "0.1.0"
  }

  // -- realize(url) legacy contract --

  test("realize(jdbc:trino://...) returns the heavy provider (available = true)") {
    val realized = new TrinoEngineProviderDescriptor().realize("jdbc:trino://host:8080/catalog")
    realized shouldBe defined
    realized.get.available shouldBe true
    realized.get.identity.name shouldBe "trino"
  }

  test("realize(non-trino URL) returns None (silent legacy contract)") {
    new TrinoEngineProviderDescriptor().realize("spark://host:7077") shouldBe None
    new TrinoEngineProviderDescriptor().realize("local[*]") shouldBe None
    new TrinoEngineProviderDescriptor().realize("") shouldBe None
    new TrinoEngineProviderDescriptor().realize(null) shouldBe None
  }

  // -- realizeTyped(parsedUrl) typed contract (PR-15 / ADR-008-Q §C2) --

  test("realizeTyped(EngineUrl.Trino) returns Right(heavy provider)") {
    val parsed = EngineUrl.Trino(jdbcUrl = "jdbc:trino://h:8080/c")
    val out = new TrinoEngineProviderDescriptor().realizeTyped(parsed)
    out.isRight shouldBe true
    out.toOption.get.available shouldBe true
    out.toOption.get.identity.name shouldBe "trino"
  }

  test("realizeTyped(non-Trino EngineUrl) returns typed Left(ConnectionFailed)") {
    val out = new TrinoEngineProviderDescriptor().realizeTyped(
      EngineUrl.InMemory(seed = None)
    )
    out.isLeft shouldBe true
    out.swap.toOption.get.engine shouldBe "trino"
  }

  // -- Descriptor carries no TrinoClient: query() returns typed UnsupportedCapability --

  test("query on descriptor returns typed UnsupportedCapability (no TrinoClient to drive)") {
    val d = new TrinoEngineProviderDescriptor()
    val out = d.query(null, null, null)
    out.isLeft shouldBe true
    out.swap.toOption.get.engine shouldBe "trino"
  }
}