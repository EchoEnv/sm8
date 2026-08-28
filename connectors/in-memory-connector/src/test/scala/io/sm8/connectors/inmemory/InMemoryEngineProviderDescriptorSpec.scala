/*
 * SM8 in-memory Connector — descriptor discovery + realize spec (PR-195).
 *
 * Mirrors `SparkEngineProviderDiscoverySpec`. Verifies:
 *   1. ServiceLoader discovers the descriptor (not the heavy provider)
 *      through the standard `META-INF/services/io.sm8.core.engine.EngineProvider`.
 *   2. The no-arg ctor yields a descriptor with the documented wire
 *      identity (`"in-memory"`, `available = true` — in-memory has no
 *      remote connection to set up, so it IS available immediately,
 *      unlike spark where `available = false` until `realize()`).
 *   3. `realize(any url)` returns the always-realized provider.
 *   4. `realizeTyped(EngineUrl.InMemory)` returns `Right(provider)`.
 *   5. `realizeTyped(non-InMemory)` returns typed `Left(ConnectionFailed)`.
 *
 * PR-195 closes the descriptor-pattern gap: in-memory's META-INF used
 * to register the heavy `InMemoryEngineProvider` directly; now it
 * registers the descriptor (PR-O4g parity with spark + trino).
 */
package io.sm8.connectors.inmemory

import io.sm8.core.engine.{EngineProvider, EngineUrl}

import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class InMemoryEngineProviderDescriptorSpec extends AnyFunSuite with Matchers {

  // -- Portal: ServiceLoader discovery --

  test("Portal: META-INF/services declares InMemoryEngineProviderDescriptor (not the heavy provider)") {
    val lines = scala.collection.mutable.Set[String]()
    val classLoader = classOf[InMemoryEngineProviderDescriptor].getClassLoader
    val urls = classLoader.getResources("META-INF/services/io.sm8.core.engine.EngineProvider")
    urls.asScala.foreach { u =>
      scala.io.Source.fromURL(u).getLines().foreach(lines += _)
    }
    val set = lines.toSet
    set should contain (classOf[InMemoryEngineProviderDescriptor].getName)
    set should not contain classOf[InMemoryEngineProvider].getName
  }

  test("Portal: ServiceLoader discovers InMemoryEngineProviderDescriptor from the classpath") {
    val providers = ServiceLoader
      .load(classOf[EngineProvider], classOf[InMemoryEngineProviderDescriptor].getClassLoader)
      .iterator()
      .asScala
      .toList
    val classes = providers.map(_.getClass.getName).toSet
    classes should contain (classOf[InMemoryEngineProviderDescriptor].getName)
  }

  // -- No-arg ctor: in-memory is always available (no remote to set up) --

  test("no-arg ctor: always available (in-memory has no remote to connect to)") {
    val d = new InMemoryEngineProviderDescriptor()
    d.available shouldBe true
    d.identity.name shouldBe "in-memory"
    d.identity.engineAdapterVersion shouldBe "0.1.0"
  }

  // -- realize(url) always returns the realized provider --

  test("realize(any url) returns the heavy provider (already realized)") {
    val realized = new InMemoryEngineProviderDescriptor().realize("anything")
    realized shouldBe defined
    realized.get.available shouldBe true
    realized.get.identity.name shouldBe "in-memory"
  }

  test("realize(null) returns the heavy provider (in-memory ignores the URL)") {
    new InMemoryEngineProviderDescriptor().realize(null) shouldBe defined
  }

  // -- realizeTyped(parsedUrl) typed contract (PR-15 / ADR-008-Q §C2) --

  test("realizeTyped(EngineUrl.InMemory) returns Right(heavy provider)") {
    val parsed = EngineUrl.InMemory(seed = None)
    val out = new InMemoryEngineProviderDescriptor().realizeTyped(parsed)
    out.isRight shouldBe true
    out.toOption.get.available shouldBe true
    out.toOption.get.identity.name shouldBe "in-memory"
  }

  test("realizeTyped(EngineUrl.InMemory with seed) returns Right(heavy provider)") {
    val parsed = EngineUrl.InMemory(seed = Some("test-seed"))
    val out = new InMemoryEngineProviderDescriptor().realizeTyped(parsed)
    out.isRight shouldBe true
  }

  test("realizeTyped(non-InMemory EngineUrl) returns typed Left(ConnectionFailed)") {
    val out = new InMemoryEngineProviderDescriptor().realizeTyped(
      EngineUrl.Spark(master = "local[*]")
    )
    out.isLeft shouldBe true
    out.swap.toOption.get.engine shouldBe "in-memory"
  }

  // -- query() forwards to the heavy provider (which is always realized) --

  test("query on descriptor forwards to the heavy provider (empty PortableQueryResult)") {
    val d = new InMemoryEngineProviderDescriptor()
    val out = d.query(null, null, null)
    out.isRight shouldBe true
    out.toOption.get.metadata("engine") shouldBe "in-memory"
  }
}