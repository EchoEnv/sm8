/*
 * SM8 Platform — StdioEndToEndSpec.
 *
 * End-to-end test of the in-process stdio MCP transport. Spawns the
 * actual Java process with stdin connected to a piped output stream
 * and stdout/stderr captured, then sends the full MCP handshake
 * (initialize + notifications/initialized + tools/list), closes
 * stdin (EOF), and verifies the responses + clean process exit.
 *
 * 4 tests:
 * 1. End-to-end stdio handshake: initialize returns serverInfo.name=sm8
 * 2. tools/list returns a result with the tools array
 * 3. Closing stdin triggers EOF path + process exits within timeout
 * 4. tools/call delegates to the Restate ingress and returns a result
 *    (uses an in-process mock HTTP server as the ingress; closes the
 *    coverage gap from C5-de-M1 where tool execution was untested)
 */
package io.sm8.platform.mcp

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}
import scala.jdk.CollectionConverters._

class StdioEndToEndSpec extends AnyFunSuite with Matchers {

  // The subprocess needs the FULL runtime classpath (sm8-server jar +
  // connector jar + all transitive deps). The test resolves it from
  // the cached $JCODE_SCRATCH_DIR/sm8-smoke-cp.txt file (the same one
  // scripts/smoke-e2e.sh uses); when absent or stale, it runs
  // `mvn dependency:build-classpath` lazily to regenerate. When
  // neither works (no maven, no network) the test cancels cleanly.
  private def scratchDir: String =
    Option(System.getenv("JCODE_SCRATCH_DIR")).filter(_.nonEmpty).getOrElse("/tmp")

  private def cpFile: String = s"$scratchDir/sm8-smoke-cp.txt"

  // Per C5-arch-L1: previously hardcoded `/home/emilio/app/projects/sm8`.
  // Now resolves via SM8_REPO_ROOT env var > system property > walking
  // up from user.dir looking for a pom.xml with `<modules>` (the
  // sm8 root). Module-level pom.xml files (sm8-platform/pom.xml, etc.)
  // do NOT have a <modules> section; the root pom.xml does. We want
  // the root.
  private def repoRoot: String = {
    Option(System.getenv("SM8_REPO_ROOT")).filter(_.nonEmpty)
      .orElse(Option(System.getProperty("sm8.repoRoot")).filter(_.nonEmpty))
      .getOrElse {
        def isRepoRoot(f: java.io.File): Boolean = {
          if (!f.isDirectory || !new java.io.File(f, "pom.xml").exists()) return false
          // The root pom.xml has a `<modules>` section; module pom.xml
          // files do not. Detect by reading for `<module>` in pom.xml.
          val src = scala.io.Source.fromFile(new java.io.File(f, "pom.xml"))
          try src.mkString.contains("<modules>") finally src.close()
        }
        var dir = new java.io.File(System.getProperty("user.dir"))
        // Walk up at most 6 levels looking for the root pom.xml.
        var i = 0
        while (i < 6 && !isRepoRoot(dir)) {
          val parent = dir.getParentFile
          if (parent == null) sys.error(
            "sm8 repo root not found: walked up from " + System.getProperty("user.dir") +
            " without finding a root pom.xml (one with <modules>). Set SM8_REPO_ROOT or -Dsm8.repoRoot=<path>."
          )
          dir = parent
          i += 1
        }
        if (!isRepoRoot(dir)) sys.error(
          "sm8 repo root not found within 6 levels of " + System.getProperty("user.dir") +
          ". Set SM8_REPO_ROOT or -Dsm8.repoRoot=<path>."
        )
        dir.getAbsolutePath
      }
  }

  private def classpathOrSkip(): Option[String] = {
    def tryRegen(): Unit = {
      val p = new ProcessBuilder(
        "mvn", "-q", "-pl", "sm8-server", "-am", "dependency:build-classpath",
        s"-Dmdep.outputFile=$cpFile"
      )
      p.directory(new java.io.File(repoRoot))
      p.redirectErrorStream(true)
      p.redirectOutput(new java.io.File(s"$scratchDir/sm8-smoke-cp.log"))
      p.start().waitFor(120, TimeUnit.SECONDS)
      ()
    }
    val cache = new java.io.File(cpFile)
    if (!cache.exists() || !scala.io.Source.fromFile(cache).mkString.contains("modelcontextprotocol")) {
      try tryRegen() catch { case _: Throwable => () }
    }
    if (!cache.exists()) cancel("sm8-smoke-cp.txt not buildable (CI-only test)")
    val deps = scala.io.Source.fromFile(cache).mkString.trim
    val full =
      s"$repoRoot/sm8-server/target/sm8-server_2.13-0.1.0-SNAPSHOT.jar:" +
      s"$repoRoot/connectors/in-memory-connector/target/in-memory-connector_2.13-0.1.0-SNAPSHOT.jar:" +
      deps
    Some(full)
  }

  test("End-to-end stdio handshake: initialize returns serverInfo.name=sm8") {
    classpathOrSkip().foreach { c =>
      runSmoke(c, assertInitialize = true, assertToolsList = false, assertProcessExit = false)
    }
  }

  test("tools/list returns a result with the tools array") {
    classpathOrSkip().foreach { c =>
      runSmoke(c, assertInitialize = false, assertToolsList = true, assertProcessExit = false)
    }
  }

  test("Closing stdin triggers EOF path + process exits within timeout") {
    classpathOrSkip().foreach { c =>
      runSmoke(c, assertInitialize = false, assertToolsList = false, assertProcessExit = true)
    }
  }

  // Edge-case coverage: the MCP protocol requires the server to
  // tolerate garbage input without crashing the JVM. The SDK's
  // StdioServerTransportProvider catch on deserializeJsonRpcMessage
  // throws -> break (per mcp-core 2.0.1 source line ~258); this is
  // SDK policy: a single unparseable message closes the session. The
  // test asserts that:
  // (a) the JVM stays alive (no SIGException)
  // (b) the initialize response is returned (the message arrived
  //     before the garbage)
  // (c) the process exits cleanly on EOF
  // We do NOT assert that the tools/list message is processed after
  // the garbage — that's documented SDK behavior to close on bad
  // JSON, and fighting it would mean either wrapping every SDK read
  // with our own recover-or-die (out of scope) or pushing an upstream
  // PR. The "no crash" assertion is the load-bearing contract.
  test("garbage JSON input does not crash the server (the SDK closes the session, which is the documented policy)") {
    val cp = classpathOrSkip()
    cp.foreach { c =>
      val modelFile = java.nio.file.Files.createTempFile("sm8-e2e-garbage-", ".yaml")
      java.nio.file.Files.writeString(
        modelFile,
        "name: e2e-spec-model\nversion: 1\nsource:\n  byName:\n    table: e2e_spec_table\n"
      )
      val pb = new ProcessBuilder(
        "java", "-cp", c, "io.sm8.server.Main",
        "--model", modelFile.toString,
        "--port", "0",
        "--metrics-port", "0",
        "--mcp-transport", "stdio",
        "--ingress-url", "http://127.0.0.1:8080"
      )
      pb.directory(new java.io.File(repoRoot))
      val proc = pb.start()
      val writer = new PrintWriter(proc.getOutputStream, true)
      val reader = new BufferedReader(new InputStreamReader(proc.getInputStream, StandardCharsets.UTF_8))
      try {
        // Send: initialize, valid ack, garbage. The SDK's catch
        // closes the session after the garbage. We expect the
        // initialize response (arrived before the garbage) and a
        // clean exit on EOF.
        writer.println("""{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}""")
        writer.println("""{"jsonrpc":"2.0","method":"notifications/initialized"}""")
        writer.println("this is not valid json {[(")
        writer.close()

        val messages = scala.collection.mutable.ListBuffer.empty[String]
        val deadline = System.currentTimeMillis() + 5000L
        while (messages.size < 1 && System.currentTimeMillis() < deadline) {
          if (reader.ready()) {
            val line = reader.readLine()
            if (line != null && line.nonEmpty) messages += line
          } else Thread.sleep(50)
        }
        withClue(s"server must return initialize response even after garbage (got ${messages.size}): ${messages.mkString("\n")}") {
          messages.size shouldBe 1
        }
        messages.head should include ("\"serverInfo\"")
        // JVM exits cleanly (exit code 0, no SIGException). Per
        // McpStdioRoute's CountDownLatch + System.exit(0).
        val exited = proc.waitFor(5, TimeUnit.SECONDS)
        exited shouldBe true
        proc.exitValue() shouldBe 0
      } finally {
        if (proc.isAlive) {
          proc.destroyForcibly()
          proc.waitFor(2, TimeUnit.SECONDS)
        }
        java.nio.file.Files.deleteIfExists(modelFile)
      }
    }
  }

  // Shared runner: spawn Java, send handshake, close stdin, wait, assert.
  private def runSmoke(
      cp: String,
      assertInitialize: Boolean,
      assertToolsList: Boolean,
      assertProcessExit: Boolean
  ): Unit = {
    val modelFile = java.nio.file.Files.createTempFile("sm8-e2e-spec-", ".yaml")
    java.nio.file.Files.writeString(
      modelFile,
      "name: e2e-spec-model\nversion: 1\nsource:\n  byName:\n    table: e2e_spec_table\n"
    )
    val pb = new ProcessBuilder(
      "java",
      "-cp", cp,
      "io.sm8.server.Main",
      "--model", modelFile.toString,
      "--port", "0",
      "--metrics-port", "0",
      "--mcp-transport", "stdio",
      "--ingress-url", "http://127.0.0.1:8080"
    )
    pb.directory(new java.io.File(repoRoot))
    val proc = pb.start()
    val writer = new PrintWriter(proc.getOutputStream, true)
    val reader = new BufferedReader(new InputStreamReader(proc.getInputStream, StandardCharsets.UTF_8))

    try {
      // Send the 3-message handshake. PR-264 writes synchronously in
      // LatchOnCloseTransport.sendMessage (see McpStdioRoute scaladoc),
      // so each response is on the wire before the next read.
      writer.println("""{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke-mcp-stdio","version":"0"}}}""")
      writer.println("""{"jsonrpc":"2.0","method":"notifications/initialized"}""")
      writer.println("""{"jsonrpc":"2.0","id":2,"method":"tools/list"}""")
      // Close stdin so the SDK gets EOF and the server exits — without
      // this, reader.ready() is false until we drain.
      writer.close()

      // Read both expected responses.
      val messages = scala.collection.mutable.ListBuffer.empty[String]
      val deadline = System.currentTimeMillis() + 5000L
      while (messages.size < 2 && System.currentTimeMillis() < deadline) {
        if (reader.ready()) {
          val line = reader.readLine()
          if (line != null && line.nonEmpty) messages += line
        } else {
          Thread.sleep(50)
        }
      }

      if (assertInitialize) {
        val init = messages.headOption.getOrElse("(no messages)")
        init should include ("\"serverInfo\"")
        init should include ("\"name\":\"sm8\"")
        init should include ("\"protocolVersion\":\"2024-11-05\"")
      }
      if (assertToolsList) {
        val toolsResp = messages.find(_.contains("\"id\":2")).orElse(messages.lastOption).getOrElse("(no messages)")
        // Per C5-arch-L2: the previous `should include ("\"tools\":")`
        // would pass even for an empty `"tools":[]` array. Parse the
        // JSON (via jackson, the same mapper the MCP wire uses) and
        // assert the actual count matches the smoke script's
        // expectation of 5 tools.
        val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
        val toolCount = try {
          val parsed = mapper.readTree(toolsResp)
          if (parsed.has("result") && parsed.get("result").has("tools")) {
            parsed.get("result").get("tools").size()
          } else 0
        } catch {
          case _: Throwable => 0
        }
        toolsResp should include ("\"result\"")
        toolsResp should include ("\"tools\":")
        withClue(s"tools/list response should have 5 tools, got $toolCount: $toolsResp") {
          toolCount shouldBe 5
        }
      }

      if (assertProcessExit) {
        // The SDK got EOF above; the server exited cleanly.
        // waitFor returns true + exitValue == 0 proves the new latch
        // wiring (per McpStdioRoute scaladoc).
        val exited = proc.waitFor(5, TimeUnit.SECONDS)
        exited shouldBe true
        proc.exitValue() shouldBe 0
      }
    } finally {
      if (proc.isAlive) {
        proc.destroyForcibly()
        proc.waitFor(2, TimeUnit.SECONDS)
      }
      java.nio.file.Files.deleteIfExists(modelFile)
    }
  }

  // The stdio MCP transport's load-bearing contract is that a
  // tools/call request reaches the Restate ingress and returns the
  // ingress's response. The sibling tests cover only the handshake +
  // tools/list metadata; tool execution was delegated to
  // scripts/smoke-e2e.sh (which bypasses the stdio MCP layer entirely).
  // This test exercises the full stdio → MCP → HttpIngressClient →
  // Restate ingress path by:
  // 1. starting an in-process HTTP server as the "ingress",
  // 2. pointing the spawned sm8-server at that local URL,
  // 3. sending a tools/call for `list_models` over stdio,
  // 4. asserting the ingress received the expected POST and the stdio
  //    response carries the ingress's body verbatim.
  test("End-to-end tools/call: list_models reaches the ingress and the response is relayed back over stdio") {
    classpathOrSkip().foreach { cp =>
      val capturedBodies = new ConcurrentLinkedQueue[String]()
      val capturedPaths  = new ConcurrentLinkedQueue[String]()

      // Mock Restate ingress: replies 200 with a fixed JSON body on any
      // POST. Records what it got so the test can assert on the path +
      // body the sm8-server forwarded. GET/HEAD are short-circuited with
      // a 204 so the probeIngressOrWarn HEAD at startup doesn't pollute
      // the capturedPaths assertion.
      val mockServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      mockServer.createContext("/", new HttpHandler {
        def handle(ex: HttpExchange): Unit = {
          val method = ex.getRequestMethod
          if (method == "HEAD" || method == "GET") {
            ex.sendResponseHeaders(204, -1)
            ex.getResponseBody.close()
            return
          }
          val body = new String(ex.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
          capturedBodies.add(body)
          capturedPaths.add(ex.getRequestURI.getPath)
          val responseBody =
            """{"models":[{"name":"m1"},{"name":"m2"}]}"""
          val bytes = responseBody.getBytes(StandardCharsets.UTF_8)
          ex.getResponseHeaders.add("Content-Type", "application/json")
          ex.sendResponseHeaders(200, bytes.length)
          ex.getResponseBody.write(bytes)
          ex.getResponseBody.close()
        }
      })
      val mockExecutor = Executors.newSingleThreadExecutor()
      mockServer.setExecutor(mockExecutor)
      mockServer.start()
      val mockPort = mockServer.getAddress.getPort
      val ingressUrl = s"http://127.0.0.1:$mockPort"

      try {
        val modelFile = java.nio.file.Files.createTempFile("sm8-e2e-tools-call-", ".yaml")
        java.nio.file.Files.writeString(
          modelFile,
          "name: e2e-tools-call-model\nversion: 1\nsource:\n  byName:\n    table: e2e_tools_call_table\n"
        )
        val pb = new ProcessBuilder(
          "java",
          "-cp", cp,
          "io.sm8.server.Main",
          "--model", modelFile.toString,
          "--port", "0",
          "--metrics-port", "0",
          "--mcp-transport", "stdio",
          "--ingress-url", ingressUrl
        )
        pb.directory(new java.io.File(repoRoot))
        val proc = pb.start()
        val writer = new PrintWriter(proc.getOutputStream, true)
        val reader = new BufferedReader(new InputStreamReader(proc.getInputStream, StandardCharsets.UTF_8))
        try {
          // Send 4 messages: initialize, notifications/initialized, tools/list (warmup), tools/call
          writer.println("""{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"sm8-e2e-tools-call","version":"0"}}}""")
          writer.println("""{"jsonrpc":"2.0","method":"notifications/initialized"}""")
          writer.println("""{"jsonrpc":"2.0","id":2,"method":"tools/list"}""")
          writer.println("""{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_models","arguments":{}}}""")
          writer.close()

          // Read responses until we have all 3 expected (initialize,
          // tools/list, tools/call) or the deadline expires.
          val messages = scala.collection.mutable.ListBuffer.empty[String]
          val deadline = System.currentTimeMillis + 8000L
          while (messages.size < 3 && System.currentTimeMillis < deadline) {
            if (reader.ready()) {
              val line = reader.readLine()
              if (line != null && line.nonEmpty) messages += line
            } else Thread.sleep(50)
          }

          val toolCallResp = messages.find(_.contains("\"id\":3")).getOrElse("(no tools/call response)")
          toolCallResp should include ("\"result\"")
          // The ingress body is wrapped twice: once by the Restate SDK
          // (JSON object) and once by the MCP SDK (text field with the
          // Restate body stringified + escaped). So the model names
          // appear with escaped quotes (\"m1\", \"m2\"), not raw.
          toolCallResp should include ("\\\"m1\\\"")
          toolCallResp should include ("\\\"m2\\\"")
          toolCallResp should include ("\"isError\":false")

          // The ingress should have received exactly one POST (from
          // the tools/call); tools/list does not POST. The path is
          // the Restate SDK convention `<Service>/<Method>`, here
          // `/ModelService/listModels` (singular "Model", per
          // Sm8ToolHandlers.scala).
          val paths = capturedPaths.asScala.toList
          paths should have size 1
          paths.head should (include ("ModelService").and(include ("listModels")))
        } finally {
          if (proc.isAlive) {
            proc.destroyForcibly()
            proc.waitFor(2, TimeUnit.SECONDS)
          }
          java.nio.file.Files.deleteIfExists(modelFile)
        }
      } finally {
        mockServer.stop(0)
        mockExecutor.shutdown()
        mockExecutor.awaitTermination(2, TimeUnit.SECONDS)
      }
    }
  }
}
