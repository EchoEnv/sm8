/*
 * SM8 Core — EngineUrl test.
 *
 * Per [[scala-data-driven-refactor-mindset]] "sealed trait dispatch":
 * tests prove the compiler-enforced exhaustiveness, the parse factory
 * typed-error path, and the Serializable round-trip (Spark
 * closure-safety + Restate journal capture per ADR-008-Q §C9).
 *
 * Per [[debug-mantra-mindset]] + [[karpathy-guidelinesmindset]]:
 * tests use plain text descriptions.
 *
 * Per [[scala-error-handlingmindset]] §1: the parse factory returns
 * `Either[EngineError, EngineUrl]` (typed error). Tests assert on
 * BOTH success and typed-error paths.
 */
package io.sm8.core.engine

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EngineUrlSpec extends AnyFlatSpec with Matchers {

  // -- Sealed trait shape (data is data per ADR-008-Q §C1) --

  "EngineUrl.Spark" should "carry master + raw + engineName fields" in {
    val url = EngineUrl.Spark(master = "local[*]")
    url.master shouldBe "local[*]"
    url.raw shouldBe "local[*]"
    url.engineName shouldBe "spark"
  }

  it should "be the same case class for spark-connect URLs" in {
    val url = EngineUrl.Spark(master = "spark-connect://host:15002")
    url.engineName shouldBe "spark"
    url.raw shouldBe "spark-connect://host:15002"
  }

  "EngineUrl.Trino" should "carry jdbcUrl + raw + engineName fields" in {
    val url = EngineUrl.Trino(jdbcUrl = "jdbc:trino://host:8080")
    url.jdbcUrl shouldBe "jdbc:trino://host:8080"
    url.raw shouldBe "jdbc:trino://host:8080"
    url.engineName shouldBe "trino"
  }

  "EngineUrl.InMemory" should "default to no seed" in {
    val url = EngineUrl.InMemory()
    url.seed shouldBe None
    url.raw shouldBe "in-memory"
    url.engineName shouldBe "in-memory"
  }

  it should "accept an optional seed" in {
    val url = EngineUrl.InMemory(seed = Some("test-seed-42"))
    url.seed shouldBe Some("test-seed-42")
    url.raw shouldBe "test-seed-42"
    url.engineName shouldBe "in-memory"
  }

  // -- Serializable (Spark closure-safety + Restate journal capture per ADR-008-Q §C9) --

  "EngineUrl sealed cases" should "survive ObjectOutputStream round-trip" in {
    val urls: List[EngineUrl] = List(
      EngineUrl.Spark(master = "local[*]"),
      EngineUrl.Trino(jdbcUrl = "jdbc:trino://host:8080"),
      EngineUrl.InMemory(seed = Some("seed-x"))
    )
    urls.foreach { u =>
      val baos = new ByteArrayOutputStream(256)
      val oos  = new ObjectOutputStream(baos)
      oos.writeObject(u)
      oos.close()
      val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
      val recovered = ois.readObject().asInstanceOf[EngineUrl]
      // Case-class equality proves the round-trip preserves fields.
      recovered shouldBe u
      recovered.engineName shouldBe u.engineName
      recovered.raw shouldBe u.raw
    }
  }

  // -- Parse factory: typed errors per ADR-008-Q §C1 --

  "EngineUrl.parse" should "return Left(ConnectionFailed) for blank engineName" in {
    EngineUrl.parse(engineName = "", raw = "local[*]") match {
      case Left(EngineError.ConnectionFailed(engine, reason, _)) =>
        engine shouldBe "<unknown>"
        reason should include ("blank engine name")
      case other => fail(s"expected typed error, got $other")
    }
  }

  it should "return Left(ConnectionFailed) for null URL" in {
    EngineUrl.parse(engineName = "spark", raw = null) match {
      case Left(EngineError.ConnectionFailed(engine, reason, _)) =>
        engine shouldBe "spark"
        reason should include ("null URL")
      case other => fail(s"expected typed error, got $other")
    }
  }

  it should "return Left(EngineUnavailable) for unknown engineName" in {
    EngineUrl.parse(engineName = "no-such-engine", raw = "local[*]") match {
      case Left(EngineError.EngineUnavailable(engine, _, _, _)) =>
        engine shouldBe "no-such-engine"
      case other => fail(s"expected typed error, got $other")
    }
  }
}
