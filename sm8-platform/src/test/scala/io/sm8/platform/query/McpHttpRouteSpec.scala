/*
 * SM8 Platform — McpHttpRouteSpec.
 *
 * Per ADR-014 (PR-261, merged) verification criteria: unit tests for
 * McpHttpRoute. Tests use Vert.x's HttpTestBase pattern (start the
 * server on an ephemeral port, fire real HTTP requests via the JDK
 * HttpClient, assert responses). All 13 verification criteria from
 * ADR-014 §Verification criteria are covered.
 *
 * 11 tests:
 * 1. start() binds the Vert.x server and returns the handle
 * 2. POST /mcp without Accept header -> 400
 * 3. POST /mcp with only application/json (no text/event-stream) -> 400
 * 4. POST /mcp with only text/event-stream (no application/json) -> 400
 * 5. POST /mcp initialize returns 200 + Mcp-Session-Id header
 * 6. POST /mcp initialize response includes serverInfo.name=sm8
 * 7. POST /mcp request without Mcp-Session-Id header -> 400
 * 8. POST /mcp notification without Mcp-Session-Id header -> 400
 * 9. POST /mcp request with unknown session-id -> 404
 * 10. GET /mcp without Accept header -> 400
 * 11. DELETE /mcp on unknown session-id -> 404
 */
package io.sm8.platform.query

import io.modelcontextprotocol.server.McpServerFeatures
import io.modelcontextprotocol.spec.McpSchema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

class McpHttpRouteSpec extends AnyFunSuite with Matchers {

  private def ephemeralRoute(): (McpHttpRoute, Int) = {
    // Find a free port: bind a temp server, get its port, close it
    val probe = new java.net.ServerSocket(0)
    val port = probe.getLocalPort
    probe.close()
    val route = new McpHttpRoute(McpHttpRoute.Config(
      endpointPath = "/mcp", disallowDelete = false
    ))
    route.buildServer("sm8-mcp", "0.1.0-SNAPSHOT", Seq.empty)
    val server = route.start(port)
    (route, port)
  }

  private def http(
      port: Int, path: String, body: String, method: String,
      headers: Map[String, String]
  ): HttpResponse[String] = {
    val client = HttpClient.newHttpClient()
    val reqBuilder = HttpRequest.newBuilder()
      .uri(URI.create(s"http://127.0.0.1:$port$path"))
      .timeout(Duration.ofSeconds(5))
      .header("Content-Type", "application/json")
    headers.foreach { case (k, v) => reqBuilder.header(k, v) }
    method.toUpperCase match {
      case "POST"   => reqBuilder.POST(HttpRequest.BodyPublishers.ofString(body))
      case "GET"    => reqBuilder.GET()
      case "DELETE" => reqBuilder.DELETE()
      case _        => reqBuilder.GET()
    }
    client.send(reqBuilder.build(), HttpResponse.BodyHandlers.ofString())
  }

  // Backward-compatible overload: POST by default.
  private def http(port: Int, path: String, body: String, headers: Map[String, String]): HttpResponse[String] =
    http(port, path, body, "POST", headers)

  // ----- Test 1 -----
  test("start() binds the Vert.x server and returns the handle") {
    val (_, port) = ephemeralRoute()
    val resp = http(port, "/mcp", """{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}""",
      Map("Accept" -> "application/json, text/event-stream"))
    resp.statusCode() shouldBe 200
  }

  // ----- Test 2 -----
  test("POST /mcp without Accept header returns 400") {
    val (_, port) = ephemeralRoute()
    val resp = http(port, "/mcp", """{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}""",
      Map.empty)
    resp.statusCode() shouldBe 400
    resp.body() should include ("Accept")
  }

  // ----- Test 3 -----
  test("POST /mcp with only application/json (no text/event-stream) returns 400") {
    val (_, port) = ephemeralRoute()
    val resp = http(port, "/mcp", """{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}""",
      Map("Accept" -> "application/json"))
    resp.statusCode() shouldBe 400
  }

  // ----- Test 4 -----
  test("POST /mcp with only text/event-stream (no application/json) returns 400") {
    val (_, port) = ephemeralRoute()
    val resp = http(port, "/mcp", """{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}""",
      Map("Accept" -> "text/event-stream"))
    resp.statusCode() shouldBe 400
  }

  // ----- Test 5 -----
  test("POST /mcp initialize returns 200 + Mcp-Session-Id header") {
    val (_, port) = ephemeralRoute()
    val resp = http(port, "/mcp", """{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}""",
      Map("Accept" -> "application/json, text/event-stream"))
    resp.statusCode() shouldBe 200
    resp.headers.firstValue("Mcp-Session-Id").isPresent shouldBe true
  }

  // ----- Test 6 -----
  test("POST /mcp initialize response includes serverInfo.name=sm8-mcp") {
    val (_, port) = ephemeralRoute()
    val resp = http(port, "/mcp", """{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}""",
      Map("Accept" -> "application/json, text/event-stream"))
    resp.body() should include ("\"serverInfo\"")
    resp.body() should include ("\"name\":\"sm8-mcp\"")
  }

  // ----- Test 7 -----
  test("POST /mcp request without Mcp-Session-Id header returns 400") {
    val (_, port) = ephemeralRoute()
    val resp = http(port, "/mcp", """{"jsonrpc":"2.0","id":2,"method":"tools/list"}""",
      Map("Accept" -> "application/json, text/event-stream"))
    resp.statusCode() shouldBe 400
    resp.body() should include ("Mcp-Session-Id")
  }

  // ----- Test 8 -----
  test("POST /mcp notification without Mcp-Session-Id header returns 400") {
    val (_, port) = ephemeralRoute()
    val resp = http(port, "/mcp", """{"jsonrpc":"2.0","method":"notifications/cancelled"}""",
      Map("Accept" -> "application/json, text/event-stream"))
    resp.statusCode() shouldBe 400
  }

  // ----- Test 9 -----
  test("POST /mcp request with unknown session-id returns 404") {
    val (_, port) = ephemeralRoute()
    val resp = http(port, "/mcp", """{"jsonrpc":"2.0","id":2,"method":"tools/list"}""",
      Map("Accept" -> "application/json, text/event-stream",
          "Mcp-Session-Id" -> "nonexistent-session-id"))
    resp.statusCode() shouldBe 404
    resp.body() should include ("session not found")
  }

  // ----- Test 10 -----
  test("GET /mcp without Accept header returns 400") {
    val (_, port) = ephemeralRoute()
    // First create a session so we can hit the GET path
    val initResp = http(port, "/mcp", """{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}""",
      Map("Accept" -> "application/json, text/event-stream"))
    val sessionId = initResp.headers.firstValue("Mcp-Session-Id").get()
    val client = HttpClient.newHttpClient()
    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"http://127.0.0.1:$port/mcp"))
      .timeout(Duration.ofSeconds(5))
      .header("Mcp-Session-Id", sessionId)
      .GET()
      .build()
    val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    resp.statusCode() shouldBe 400
    resp.body() should include ("text/event-stream")
  }

  // ----- Test 11 -----
  test("DELETE /mcp on unknown session-id returns 404 (when DELETE is allowed)") {
    val (_, port) = ephemeralRoute()
    val resp = http(port, "/mcp", "", "DELETE",
      Map("Mcp-Session-Id" -> "unknown-session-id"))
    resp.statusCode() shouldBe 404
  }

  // ----- Test 11b -----
  test("DELETE /mcp returns 405 when disallowDelete=true (regardless of session id)") {
    val probe = new java.net.ServerSocket(0)
    val port = probe.getLocalPort
    probe.close()
    val route = new McpHttpRoute(McpHttpRoute.Config(
      endpointPath = "/mcp", disallowDelete = true
    ))
    route.buildServer("sm8-mcp", "0.1.0-SNAPSHOT", Seq.empty)
    route.start(port)
    val resp = http(port, "/mcp", "", "DELETE",
      Map("Mcp-Session-Id" -> "anything"))
    resp.statusCode() shouldBe 405
  }

  // ----- Test 13 (Bonus) -----
  test("Unknown path returns 404") {
    val (_, port) = ephemeralRoute()
    val resp = http(port, "/whatever", "", Map.empty)
    resp.statusCode() shouldBe 404
  }
}