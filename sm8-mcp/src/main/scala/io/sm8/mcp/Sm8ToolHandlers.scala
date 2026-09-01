/*
 * SM8 MCP — ToolHandlers.
 *
 * Per ADR-013 (PR-259): 5 MCP tools that delegate 1:1 to the existing
 * Restate ingress endpoints. Each tool:
 * 1. Parses its tool arguments from the `CallToolRequest.arguments` map.
 * 2. POSTs the canonical Restate ingress request body to the
 *    corresponding endpoint via `HttpIngressClient`.
 * 3. Returns a `CallToolResult` with the ingress response body
 *    as the content text, plus `isError=true` if status >= 400.
 *
 * ==Why delegate (vs call sm8-platform handlers directly)==
 *
 * Per ADR-013 §"Why delegate to Restate ingress and not call
 * sm8-platform handlers directly": sm8-platform handlers depend on
 * `dev.restate.sdk.Context` (journal + awakeables + side effects).
 * Unwiring that seam is invasive. The Restate ingress endpoints are
 * already the public, journal-correct surface — routing MCP requests
 * through them gets journaling for free.
 *
 * ==Argument mapping==
 *
 * The MCP `CallToolRequest.arguments` is `Map<String, Object>`. We
 * serialize the map to JSON via Jackson 2 (already on classpath via
 * sm8-platform's transitive Restate Serde). The same JSON shape is
 * what the Restate ingress expects as request body.
 *
 * ==Error model==
 *
 * Per [[scala-error-handling-mindset]] "errors are data":
 * - HTTP 2xx: CallToolResult(isError=false, content=body)
 * - HTTP 4xx/5xx: CallToolResult(isError=true, content=body)
 *   — the Restate ingress body IS the error info (e.g. a not-found
 *   model returns a JSON `EngineError`). The LLM sees the actual
 *   backend error, not a generic "tool failed."
 * - Connection refused / read timeout: caught + wrapped into
 *   CallToolResult(isError=true, content="connection refused to
 *   <ingress-url>: ..."). Doesn't terminate the MCP session.
 */
package io.sm8.mcp

import com.fasterxml.jackson.databind.ObjectMapper
import io.modelcontextprotocol.server.McpServerFeatures
import io.modelcontextprotocol.server.McpSyncServerExchange
import io.modelcontextprotocol.spec.McpSchema

import scala.util.control.NonFatal

object Sm8ToolHandlers {

  /** Build all 5 SyncToolSpecification instances for the
    * `McpServer.sync(...).tools(...)` builder. */
  def build(client: HttpIngressClient.Impl)
      : Seq[McpServerFeatures.SyncToolSpecification] = {
    // Plain Jackson 2 ObjectMapper (no ScalaModule): the request
    // bodies we serialize are Map<String,Object> with String values,
    // which vanilla Jackson handles without the Scala case-class
    // extension. Avoids the jackson-module-scala vs jackson-databind
    // version-coupling trap (PR-260 r0 smoke caught this).
    val mapper = new ObjectMapper()
    Seq(
      buildQueryTool(client, mapper),
      buildListModelsTool(client, mapper),
      buildDescribeModelTool(client, mapper),
      buildListEnginesTool(client, mapper),
      buildGetMetricsTool(client, mapper)
    )
  }

  /** Wrap a POST call as a CallToolResult. HTTP status < 400 -> success;
    * otherwise isError=true with the body as the content text. */
  private def callAndWrap(
      client: HttpIngressClient.Impl,
      mapper: ObjectMapper,
      path: String,
      requestBody: AnyRef
  ): McpSchema.CallToolResult = {
    val jsonArgs = mapper.writeValueAsString(requestBody)
    val result =
      try client.post(path, jsonArgs)
      catch {
        case NonFatal(e) =>
          return McpSchema.CallToolResult.builder()
            .addTextContent(
              s"sm8-mcp: failed to POST $path: ${e.getClass.getSimpleName}: ${e.getMessage}"
            )
            .isError(java.lang.Boolean.TRUE)
            .build()
      }
    McpSchema.CallToolResult.builder()
      .addTextContent(result.body)
      .isError(java.lang.Boolean.valueOf(result.statusCode >= 400))
      .build()
  }

  // ----- query -----

  private def buildQueryTool(
      client: HttpIngressClient.Impl,
      mapper: ObjectMapper
  ): McpServerFeatures.SyncToolSpecification = {
    val tool = McpSchema.Tool.builder()
      .name("query")
      .title("Run an sm8 query")
      .description(
        "Run a query against an sm8 model. " +
        "Returns the query result as JSON in the MCP content text. " +
        "Forwards to POST /QueryService/runQuery on the Restate ingress."
      )
      .inputSchema(McpSchema.JsonSchema.builder()
        .`type`("object")
        .properties(java.util.Map.of(
          "modelName",  McpSchema.JsonSchema.builder().`type`("string").build(),
          "measures",   new java.util.LinkedHashMap[String, Object]() {{
  put("type", "array"); put("items", new java.util.LinkedHashMap[String, Object]() {{ put("type", "string"); }});
}},
          "dimensions", new java.util.LinkedHashMap[String, Object]() {{
  put("type", "array"); put("items", new java.util.LinkedHashMap[String, Object]() {{ put("type", "string"); }});
}},
          "where",      McpSchema.JsonSchema.builder().`type`("string").build(),
          "engine",     McpSchema.JsonSchema.builder().`type`("string").build()
        ))
        .required(java.util.List.of("modelName"))
        .build())
      .build()
    McpServerFeatures.SyncToolSpecification.builder()
      .tool(tool)
      .callHandler(new java.util.function.BiFunction[
          McpSyncServerExchange, McpSchema.CallToolRequest, McpSchema.CallToolResult] {
        def apply(
            exch: McpSyncServerExchange,
            req:  McpSchema.CallToolRequest
        ): McpSchema.CallToolResult = {
          // The Restate ingress expects a `QueryRequest` JSON body:
          // {"modelName": "...", "measures": [...], "dimensions": [...],
          //  "where": "...", "engine": "..."}.
          val args = req.arguments()
          val requestBody = new java.util.LinkedHashMap[String, Object]()
          copyString(args, "modelName",  requestBody)
          copyList(args, "measures",     requestBody)
          copyList(args, "dimensions",   requestBody)
          copyString(args, "where",      requestBody)
          copyString(args, "engine",     requestBody)
          callAndWrap(client, mapper, "/QueryService/runQuery", requestBody)
        }
      })
      .build()
  }

  // ----- list_models -----

  private def buildListModelsTool(
      client: HttpIngressClient.Impl,
      mapper: ObjectMapper
  ): McpServerFeatures.SyncToolSpecification = {
    val tool = McpSchema.Tool.builder()
      .name("list_models")
      .title("List loaded sm8 models")
      .description(
        "List the models loaded by the sm8 deployment. " +
        "Forwards to POST /ModelService/listModels on the Restate ingress. " +
        "No arguments required."
      )
      .inputSchema(McpSchema.JsonSchema.builder().`type`("object").build())
      .build()
    McpServerFeatures.SyncToolSpecification.builder()
      .tool(tool)
      .callHandler(new java.util.function.BiFunction[
          McpSyncServerExchange, McpSchema.CallToolRequest, McpSchema.CallToolResult] {
        def apply(
            exch: McpSyncServerExchange,
            req:  McpSchema.CallToolRequest
        ): McpSchema.CallToolResult =
          callAndWrap(client, mapper, "/ModelService/listModels", new java.util.LinkedHashMap[String, Object]())
      })
      .build()
  }

  // ----- describe_model -----

  private def buildDescribeModelTool(
      client: HttpIngressClient.Impl,
      mapper: ObjectMapper
  ): McpServerFeatures.SyncToolSpecification = {
    val tool = McpSchema.Tool.builder()
      .name("describe_model")
      .title("Describe an sm8 model")
      .description(
        "Get the full schema (dimensions, measures, joins) for a loaded model. " +
        "Forwards to POST /ModelService/describe on the Restate ingress. " +
        "Requires `modelName`."
      )
      .inputSchema(McpSchema.JsonSchema.builder()
        .`type`("object")
        .properties(java.util.Map.of(
          "modelName", McpSchema.JsonSchema.builder().`type`("string").build()
        ))
        .required(java.util.List.of("modelName"))
        .build())
      .build()
    McpServerFeatures.SyncToolSpecification.builder()
      .tool(tool)
      .callHandler(new java.util.function.BiFunction[
          McpSyncServerExchange, McpSchema.CallToolRequest, McpSchema.CallToolResult] {
        def apply(
            exch: McpSyncServerExchange,
            req:  McpSchema.CallToolRequest
        ): McpSchema.CallToolResult = {
          val args = req.arguments()
          val requestBody = new java.util.LinkedHashMap[String, Object]()
          copyString(args, "modelName", requestBody)
          callAndWrap(client, mapper, "/ModelService/describe", requestBody)
        }
      })
      .build()
  }

  // ----- list_engines -----

  private def buildListEnginesTool(
      client: HttpIngressClient.Impl,
      mapper: ObjectMapper
  ): McpServerFeatures.SyncToolSpecification = {
    val tool = McpSchema.Tool.builder()
      .name("list_engines")
      .title("List available sm8 engines")
      .description(
        "List the engines that successfully realized at sm8 boot. " +
        "Use this to discover valid values for the `engine` field of `query`. " +
        "Forwards to POST /EngineService/listEngines on the Restate ingress. " +
        "No arguments required."
      )
      .inputSchema(McpSchema.JsonSchema.builder().`type`("object").build())
      .build()
    McpServerFeatures.SyncToolSpecification.builder()
      .tool(tool)
      .callHandler(new java.util.function.BiFunction[
          McpSyncServerExchange, McpSchema.CallToolRequest, McpSchema.CallToolResult] {
        def apply(
            exch: McpSyncServerExchange,
            req:  McpSchema.CallToolRequest
        ): McpSchema.CallToolResult =
          callAndWrap(client, mapper, "/EngineService/listEngines", new java.util.LinkedHashMap[String, Object]())
      })
      .build()
  }

  // ----- get_metrics -----

  private def buildGetMetricsTool(
      client: HttpIngressClient.Impl,
      mapper: ObjectMapper
  ): McpServerFeatures.SyncToolSpecification = {
    val tool = McpSchema.Tool.builder()
      .name("get_metrics")
      .title("Get sm8 invocation metrics")
      .description(
        "Get the current invocation counters (total/succeeded/failed), " +
        "cache hits/misses, and audit-sink + timeout errors. " +
        "Forwards to POST /MetricsService/snapshot on the Restate ingress. " +
        "No arguments required."
      )
      .inputSchema(McpSchema.JsonSchema.builder().`type`("object").build())
      .build()
    McpServerFeatures.SyncToolSpecification.builder()
      .tool(tool)
      .callHandler(new java.util.function.BiFunction[
          McpSyncServerExchange, McpSchema.CallToolRequest, McpSchema.CallToolResult] {
        def apply(
            exch: McpSyncServerExchange,
            req:  McpSchema.CallToolRequest
        ): McpSchema.CallToolResult =
          callAndWrap(client, mapper, "/MetricsService/snapshot", new java.util.LinkedHashMap[String, Object]())
      })
      .build()
  }

  // ----- arg helpers -----

  /** Copy a string arg from `args` to `dst` under `key`. No-op if
    * the key is missing or the value isn't a String. */
  private def copyString(
      args: java.util.Map[String, Object],
      key:  String,
      dst:  java.util.HashMap[String, Object]
  ): Unit = {
    val v = args.get(key)
    if (v != null && v.isInstanceOf[String]) dst.put(key, v)
  }

  /** Copy a string-list arg from `args` to `dst` under `key`.
    * Accepts a Java `List<Object>` or a Scala `List[String]`.
    * No-op if missing. */
  private def copyList(
      args: java.util.Map[String, Object],
      key:  String,
      dst:  java.util.HashMap[String, Object]
  ): Unit = {
    val v = args.get(key)
    if (v == null) return
    v match {
      case list: java.util.List[_] =>
        // Trust the wire — MCP JSON deserialization yields List<String>.
        val out = new java.util.ArrayList[String]()
        val it  = list.iterator()
        while (it.hasNext) {
          val n = it.next()
          if (n != null) out.add(n.toString)
        }
        dst.put(key, out)
      case _ =>
        // Not a list; leave the key out of dst.
        ()
    }
  }
}