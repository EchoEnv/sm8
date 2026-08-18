/*
 * SM8 Server — EngineLoader test.
 *
 * Per [[scala-error-handlingmindset]] §1 + ADR-008-Q §C1: tests
 * verify the typed-error path (per-provider typed errors).
 *
 * Per [[scala-bug-huntingmindset]] §3 (every match must be
 * exhaustive): tests cover the typed-error paths — UnknownEngineName
 * (no parser found) and ValidUrlPath (parser succeeds, default
 * TypedRealizationProvider.delegate maps realize(url)=None → Left).
 */
package io.sm8.server

import io.sm8.core.engine.{EngineError, EngineProvider}

import scala.jdk.CollectionConverters._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EngineLoaderSpec extends AnyFlatSpec with Matchers {

  /** Discover via classpath SPI. The test SPI registers
    * `TestEngineProvider` (EngineProvider) + `StubEngineUrlParser`
    * (EngineUrlParser) in `src/test/resources/META-INF/services/`. */
  private def discover(): List[EngineProvider] = {
    java.util.ServiceLoader
      .load(classOf[EngineProvider], getClass.getClassLoader)
      .iterator().asScala.toList
  }

  "EngineLoader.discoverAndRealize" should "return providers unchanged when rawUrl is None" in {
    val providers = discover()
    val result = EngineLoader.discoverAndRealize(
      classLoader = getClass.getClassLoader,
      engineName  = "stub-spark",
      rawUrl      = None
    )
    result.size shouldBe providers.size
    result.foreach { r => r.isRight shouldBe true }
  }

  it should "return typed Left(ConnectionFailed) on valid URL (default-delegate path)" in {
    // TestEngineProvider extends TypedRealizationProvider but doesn't
    // override realizeTyped, so the default delegate (via
    // realize(url:String)=None) returns Left(ConnectionFailed). This
    // verifies the typed-error default-path is wired correctly.
    val providers = discover()
    val result = EngineLoader.discoverAndRealize(
      classLoader = getClass.getClassLoader,
      engineName  = "stub-spark",
      rawUrl      = Some("local[1]")
    )
    result.size shouldBe providers.size
    result.foreach {
      case Left(EngineError.ConnectionFailed(engine, _, _)) =>
        // engineName comes from the parsed EngineUrl (not the parser name)
        engine shouldBe "spark"
      case other =>
        fail(s"expected typed Left(ConnectionFailed), got $other")
    }
  }

  it should "return typed Left(EngineUnavailable) on unknown engineName" in {
    val providers = discover()
    val result = EngineLoader.discoverAndRealize(
      classLoader = getClass.getClassLoader,
      engineName  = "no-such-engine-name",
      rawUrl      = Some("any-url")
    )
    result.size shouldBe providers.size
    result.foreach {
      case Left(EngineError.EngineUnavailable(engine, _, _, _)) =>
        engine shouldBe "no-such-engine-name"
      case Right(_) =>
        fail("expected typed Left(EngineUnavailable) for unknown engine")
    }
  }
}
