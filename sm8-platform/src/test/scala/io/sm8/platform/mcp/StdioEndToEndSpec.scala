/*
 * SM8 Platform — StdioEndToEndSpec.
 *
 * End-to-end test of the in-process stdio MCP transport. Spawns the
 * actual Java process with stdin connected to a piped output stream
 * and stdout/stderr captured, then sends the full MCP handshake
 * (initialize + notifications/initialized + tools/list), closes
 * stdin (EOF), and verifies the responses + clean process exit.
 *
 * 3 tests:
 * 1. End-to-end stdio handshake: initialize returns serverInfo.name=sm8
 * 2. tools/list returns a result with the tools array
 * 3. Closing stdin triggers EOF path + process exits within timeout
 */
package io.sm8.platform.mcp

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

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

  private def repoRoot: String = "/home/emilio/app/projects/sm8"

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
        toolsResp should include ("\"result\"")
        toolsResp should include ("\"tools\":")
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
}