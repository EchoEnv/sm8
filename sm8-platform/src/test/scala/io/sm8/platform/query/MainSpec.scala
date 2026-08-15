/*
 * SM8 Platform — Main spec (Step 11 production entry point).
 *
 * Per [[debug-mantra-mindset]]: each test exercises ONE observable
 * contract of the entry point — arg parsing, typed exit paths,
 * provider discovery, wiring determinism, and the Serializable
 * contract of everything Main captures.
 *
 * Per [[karphyaguidsmindset]] "name what done looks like":
 * `run(bad args)` returns 2; `run(missing model)` returns 1;
 * `wire(...)` produces a Serializable registry; the ServiceLoader
 * path finds the test provider.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1 (closure-safety,
 * per user directive): the wired registry — which in production may
 * hold SparkEngineProvider — must survive an ObjectOutputStream
 * round-trip. This is the driver-side proof of the standing
 * "must be serializable every part" rule.
 */
package io.sm8.platform.query

import io.sm8.core.model.{Model, SourceRef}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.Paths

class MainSpec extends AnyFunSuite with Matchers {

  private def sampleModel(name: String = "main-test"): Model = Model.of(
    name    = name,
    version = 1,
    source  = SourceRef.ByName("default", "stub_table"),
  ).toOption.get

  // ---- CLI parsing (pure) ----

  test("parseArgs: --model + --port + --engine all parse") {
    val args = List("--model", "/tmp/m.yaml", "--port", "9090", "--engine", "spark-3.5")
    Main.parseArgs(args) match {
      case Right(a) =>
        a.modelPath shouldBe Paths.get("/tmp/m.yaml")
        a.port shouldBe 9090
        a.engine shouldBe Some("spark-3.5")
      case Left(e) => fail(s"unexpected parse error: ${e.reason}")
    }
  }

  test("parseArgs: defaults are port 8080, engine None") {
    Main.parseArgs(List("--model", "m.yaml")) match {
      case Right(a) =>
        a.port shouldBe 8080
        a.engine shouldBe None
      case Left(e) => fail(s"unexpected: ${e.reason}")
    }
  }

  test("parseArgs: missing --model flag value is a typed error") {
    Main.parseArgs(List("--model")) shouldBe Left(Main.CliError.MissingValue("--model"))
  }

  test("parseArgs: non-integer --port is a typed error") {
    Main.parseArgs(List("--model", "m.yaml", "--port", "abc")) shouldBe
      Left(Main.CliError.BadInt("--port", "abc"))
  }

  test("parseArgs: unknown flag is a typed error") {
    Main.parseArgs(List("--model", "m.yaml", "--bogus")) shouldBe
      Left(Main.CliError.UnknownFlag("--bogus"))
  }

  // ---- run() exit codes (typed failure paths, no server started) ----

  test("run: no args prints usage and exits 2") {
    val exit = Main.run(Nil)
    exit shouldBe 2
  }

  test("run: --help exits 0") {
    Main.run(List("--help")) shouldBe 0
  }

  test("run: bad flag exits 2") {
    Main.run(List("--nope")) shouldBe 2
  }

  test("run: missing model file exits 1 (typed model-load failure)") {
    val exit = Main.run(List("--model", "/nonexistent/sm8-should-not-exist.yaml"))
    exit shouldBe 1
  }

  // ---- ServiceLoader discovery (real classpath mechanism) ----

  test("discoverProviders: finds the test provider via META-INF/services") {
    val found = Main.discoverProviders(getClass.getClassLoader)
    found.map(_.identity.name) should contain ("test-engine")
  }

  // ---- wiring (pure construction) ----

  test("wire: empty provider list fails loud with a typed message") {
    Main.wire(sampleModel(), providers = Nil, engineName = None) match {
      case Left(msg) => msg should include ("no MCPEngineProvider")
      case Right(_)  => fail("expected boot failure for empty providers")
    }
  }

  test("wire: named engine not discovered fails loud") {
    val providers = Main.discoverProviders(getClass.getClassLoader)
    Main.wire(sampleModel(), providers, engineName = Some("no-such-engine")) match {
      case Left(msg) => msg should include ("no-such-engine")
      case Right(_)  => fail("expected boot failure for unknown engine")
    }
  }

  test("wire: default engine = first available (sorted) when --engine absent") {
    val providers = Main.discoverProviders(getClass.getClassLoader)
    Main.wire(sampleModel(), providers, engineName = None) match {
      case Right((registry, transport)) =>
        registry.defaultEngine shouldBe "test-engine"
        // transport not started — safe to drop
      case Left(msg) => fail(s"unexpected: $msg")
    }
  }

  // ---- Serializable contract (per user's standing rule) ----

  test("wire: the wired registry survives ObjectOutputStream round-trip") {
    val providers = Main.discoverProviders(getClass.getClassLoader)
    Main.wire(sampleModel(), providers, engineName = None) match {
      case Right((registry, _)) =>
        val bytes = {
          val bos = new ByteArrayOutputStream()
          val oos = new ObjectOutputStream(bos)
          oos.writeObject(registry); oos.close(); bos.toByteArray
        }
        val back = {
          val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
          ois.readObject().asInstanceOf[io.sm8.core.engine.MCPEngineRegistry]
        }
        back.defaultEngine shouldBe registry.defaultEngine
        back.availableProviders shouldBe registry.availableProviders
      case Left(msg) => fail(s"unexpected: $msg")
    }
  }
}
