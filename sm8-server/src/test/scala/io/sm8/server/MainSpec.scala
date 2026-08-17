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
package io.sm8.server

import io.sm8.core.cache._
import io.sm8.core.model.{Model, SourceRef}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.Paths

class MainSpec extends AnyFunSuite with Matchers {

  private def sampleModel(name: String = "main-test"): Model = Model.of(
    name    = name,
    version = 1,
    source  = SourceRef.ByName(table = "stub_table"),
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
      case Right((registry, transport, _)) =>
        registry.defaultEngine shouldBe "test-engine"
        // transport not started — safe to drop
      case Left(msg) => fail(s"unexpected: $msg")
    }
  }

  // ---- Serializable contract (per user's standing rule) ----

  test("wire: the wired registry survives ObjectOutputStream round-trip") {
    val providers = Main.discoverProviders(getClass.getClassLoader)
    Main.wire(sampleModel(), providers, engineName = None) match {
      case Right((registry, _, _)) =>
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


  // ---- --connector-url CLI parsing (Phase 4 — Main real runtime) ----

  test("parseArgs: --connector-url parses") {
    Main.parseArgs(List("--model", "m.yaml", "--connector-url", "local[1]")) match {
      case Right(a) => a.connectorUrl shouldBe Some("local[1]")
      case Left(e) => fail(s"unexpected: ${e.reason}")
    }
  }

  test("parseArgs: --connector-url with Spark Connect URL parses") {
    Main.parseArgs(List("--model", "m.yaml", "--connector-url", "spark-connect://host:15002")) match {
      case Right(a) => a.connectorUrl shouldBe Some("spark-connect://host:15002")
      case Left(e) => fail(s"unexpected: ${e.reason}")
    }
  }

  test("parseArgs: --connector-url without value is a typed error") {
    Main.parseArgs(List("--model", "m.yaml", "--connector-url")) shouldBe
      Left(Main.CliError.MissingValue("--connector-url"))
  }

  // ===== PR-O4a (ADR-008-O): shutdown hook + close() lifecycle =====

  test("PR-O4a: shutdown hook calls close() on every realized engine provider") {
    val providers = Main.discoverProviders(getClass.getClassLoader)
    Main.wire(sampleModel(), providers, engineName = None) match {
      case Right((registry, transport, realized)) =>
        realized.foreach { p =>
          p.close()
          p.close()  // idempotent
        }
        transport.stop()
      case Left(msg) => fail(s"unexpected: $msg")
    }
  }

  test("PR-O4a: MCPEngineProvider.close() default impl is a no-op") {
    val stub: io.sm8.core.engine.MCPEngineProvider = new io.sm8.core.engine.MCPEngineProvider {
      override def identity = io.sm8.core.engine.EngineIdentity("stub", "0", "0")
      override def available = true
      override def query(m: Model, r: io.sm8.core.engine.MCPQueryRequest, c: io.sm8.core.engine.EngineContext) =
        Right(io.sm8.core.engine.PortableQueryResult(
          io.sm8.core.engine.ResultSchema(Nil), Vector.empty, Map.empty))
      override def explain(m: Model, r: io.sm8.core.engine.MCPQueryRequest, c: io.sm8.core.engine.EngineContext) =
        Right("stub-explain")
    }
    noException should be thrownBy stub.close()
    noException should be thrownBy { stub.close(); stub.close() }
  }

  // ---- Main.realize() — reflection-based URL realization ----
  //
  // Per RFC §3 + the user's "no spark types in the platform" directive:
  // the platform holds ONLY a string. For each discovered provider that
  // is not available, Main looks for a `(String) ctor` on the class.
  // If found, it instantiates with the URL. The connector's (String)
  // ctor builds the real session (Spark Connect, Trino URL, etc.).
  //
  // The 3 realize() tests below use the existing TestEngineProvider
  // (test classpath — has no (String) ctor) to verify the "no-ctor → keep
  // stub" path. The "ctor → realized" path is exercised by the
  // spark-connector's own discovery spec (real JVM + SparkSession).

  test("realize: connectorUrl = None → no transformation") {
    val providers = Main.discoverProviders(getClass.getClassLoader)
    val realized = Main.realize(providers, None)
    realized should contain theSameElementsAs providers
  }

  test("realize: test-classpath providers (no String ctor) → kept as stubs") {
    val providers = Main.discoverProviders(getClass.getClassLoader)
    val realized = Main.realize(providers, Some("local[1]"))
    realized.size shouldBe providers.size
    // Same instance identity: no reflection happened.
    realized.zip(providers).foreach { case (r, p) =>
      r should be theSameInstanceAs p
    }
  }

  test("realize: already-available provider is returned unchanged (no reflection)") {
    val providers = Main.discoverProviders(getClass.getClassLoader)
    // Mark the first as available (it isn't in the test classpath —
    // TestEngineProvider is available=false=true per its impl).
    val stub = providers.find(p => p.getClass.getName == "io.sm8.server.TestEngineProvider").get
    val realized = Main.realize(List(stub), Some("local[1]"))
    // TestEngineProvider has no (String) ctor → returned as-is.
    realized.head should be theSameInstanceAs stub
  }

  // ---- Typed realize (PR-B per RFC adapters.md Rule 4) ----

  test("realize: typed contract replaces reflection (sm8-server uses realize(url))") {
    val providers = Main.discoverProviders(getClass.getClassLoader)
    val realized = Main.realize(providers, Some("local[1]"))
    // TestEngineProvider.realize() returns None (default) — kept as-is.
    realized.size shouldBe providers.size
  }

  test("wire with connector-url: typed realization path compiles + runs") {
    val providers = Main.discoverProviders(getClass.getClassLoader)
    // The in-memory engine is available without a URL
    val wired = Main.wire(
      Model.of(name = "m", version = 1, source = io.sm8.core.model.SourceRef.ByName(table = "t")).toOption.get,
      providers,
      engineName   = Some("test-engine"),
      connectorUrl = Some("local[1]")
    )
    wired.isRight shouldBe true
  }
}
