/*
 * SM8 MCP — HttpIngressClient.
 *
 * Thin typed wrapper over java.net.http.HttpClient. Each method
 * POSTs to a Restate ingress endpoint and returns the JSON body
 * as a String (the SDK's McpJsonMapper parses it on its own).
 *
 * ==Why java.net.http.HttpClient (JDK built-in)==
 *
 * Per the MCP design §"Server transport in the SDK": MCP clients use the
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
package io.sm8.platform.mcp

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
 * host:port; per-tool methods append the path.
 *
 * Per C5-r2-de-L3: the reconstruction DROPS any userinfo
 * component (http://user:pass@host -> http://host). The Restate
 * ingress does not use basic auth, so this is safe; passing
 * credentials in the URL is intentionally unsupported.
 *
 * Per C5-arch-L3: strip the path component as well as trailing slash
 * so `--ingress-url http://host:8080/api` doesn't produce requests
 * to `/api/QueryService/runQuery`. The CLI surface is documented
 * as 'Restate ingress base URL' but tools append their own paths
 * (`/QueryService/runQuery`, etc.); the operator must be able to
 * pass either a host root or a host:port without path-stripping
 * surprises.
 */
 // private[mcp] (not private): exposed to the package for the
 // C5-r2-arch-002 regression test (HttpIngressClientSpec).
 private[mcp] val baseUrl: String = {
 val parsed = new java.net.URI(ingressUrl)
 val hostAndPort =
   if (parsed.getPort > 0) s"${parsed.getScheme}://${parsed.getHost}:${parsed.getPort}"
   else s"${parsed.getScheme}://${parsed.getHost}"
 hostAndPort
 }

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