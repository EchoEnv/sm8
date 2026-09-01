/*
 * SM8 MCP — HttpIngressClient.
 *
 * Thin typed wrapper over java.net.http.HttpClient. Each method
 * POSTs to a Restate ingress endpoint and returns the JSON body
 * as a String (the SDK's McpJsonMapper parses it on its own).
 *
 * ==Why java.net.http.HttpClient (JDK built-in)==
 *
 * Per ADR-013 §"Server transport in the SDK": MCP clients use the
 * JDK HttpClient by default — no extra dependency. For our server,
 * we use the same built-in client to POST tool calls to the
 * Restate ingress. This avoids pulling in another HTTP library
 * (Apache, OkHttp, etc.).
 *
 * ==Error model==
 *
 * Per [[scala-error-handling-mindset]]: errors are data. We do NOT
 * throw on non-2xx; we return `IngressResult(body, statusCode)`.
 * The tool handler converts that to an MCP `CallToolResult` with
 * `isError = (statusCode >= 400)`. This way the LLM agent sees
 * the error body (a Restate error response, e.g. a not-found model)
 * as the tool's error output — not as a thrown exception that
 * would terminate the MCP session.
 *
 * ==Connection failures==
 *
 * Connection refused, read timeout, etc. throw — these are
 * "couldn't reach the backend" failures, not "the backend
 * rejected my request." The caller catches and wraps into a
 * CallToolResult with `isError=true` and the exception message
 * in the content text.
 */
package io.sm8.mcp

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpResponse.BodyHandlers
import java.time.Duration

object HttpIngressClient {

  /** Plain value class for the result of a POST. `statusCode` is
    * the HTTP status line; `body` is the response body verbatim
    * (Restate ingress returns JSON). */
  final case class IngressResult(statusCode: Int, body: String)

  /** Wraps java.net.http.HttpClient. One instance per sm8-mcp
    * process; the underlying HttpClient manages its own connection
    * pool. */
  class Impl(
      ingressUrl: String,
      requestTimeout: Duration
  ) {

    /** JDK HttpClient. JDK 11+ built-in; no extra dependency.
      * `connectTimeout` bounds the initial TCP/TLS handshake;
      * `requestTimeout` bounds the full response (matches the
      * `--request-timeout` CLI flag). */
    private val client: HttpClient = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build()

    /** Base URL of the Restate ingress. Includes the scheme +
      * host:port; per-tool methods append the path. */
    private val baseUrl: String =
      if (ingressUrl.endsWith("/")) ingressUrl.dropRight(1) else ingressUrl

    /** POST `jsonBody` to `path` (e.g. `/QueryService/runQuery`).
      * Returns the parsed JSON body and HTTP status. Throws on
      * connection failure (caller wraps). */
    def post(path: String, jsonBody: String): IngressResult = {
      val uri = URI.create(baseUrl + path)
      val req = HttpRequest.newBuilder(uri)
        .timeout(requestTimeout)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
        .build()
      val resp: HttpResponse[String] =
        client.send(req, BodyHandlers.ofString())
      IngressResult(resp.statusCode(), resp.body())
    }
  }
}