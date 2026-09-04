/*
 * SM8 MCP — ToolHandlers.
 *
 * Per the MCP design (a prior PR): 5 MCP tools that delegate 1:1 to the existing
 * Restate ingress endpoints. Each tool:
 * 1. Parses its tool arguments from the `CallToolRequest.arguments` map.
 * 2. POSTs the canonical Restate ingress request body to the
 * corresponding endpoint via `HttpIngressClient`.
 * 3. Returns a `CallToolResult` with the ingress response body
 * as the content text, plus `isError=true` if status >= 400.
 *
 * ==Why delegate (vs call sm8-platform handlers directly)==
 *
 * Per the MCP design §"Why delegate to Restate ingress and not call
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
 * — the Restate ingress body IS the error info (e.g. a not-found
 * model returns a JSON `EngineError`). The LLM sees the actual
 * backend error, not a generic "tool failed."
 * - Connection refused / read timeout: caught + wrapped into
 * CallToolResult(isError=true, content="connection refused to
 * <ingress-url>: ..."). Doesn't terminate the MCP session.
 */
package io.sm8.platform.mcp

import com.fasterxml.jackson.databind.ObjectMapper
import io.modelcontextprotocol.server.McpServerFeatures
import io.modelcontextprotocol.server.McpSyncServerExchange
import io.modelcontextprotocol.spec.McpSchema

import scala.util.control.NonFatal

object Sm8ToolHandlers {

 /** Build all 7 SyncToolSpecification instances for the
 * `McpServer.sync(...).tools(...)` builder. C10-PR-C adds
 * list_plugins + list_hooks (registry inspector handlers from
 * PR-B). */
 def build(client: HttpIngressClient.Impl)
 : Seq[McpServerFeatures.SyncToolSpecification] = {
 // Plain Jackson 2 ObjectMapper (no ScalaModule): the request
 // bodies we serialize are Map<String,Object> with String values,
 // which vanilla Jackson handles without the Scala case-class
 // extension. Avoids the jackson-module-scala vs jackson-databind
 // version-coupling trap (a prior PR r0 smoke caught this).
 val mapper = new ObjectMapper()
 Seq(
 buildQueryTool(client, mapper),
 buildListModelsTool(client, mapper),
 buildDescribeModelTool(client, mapper),
 buildListEnginesTool(client, mapper),
 buildGetMetricsTool(client, mapper),
 // C10-PR-C: registry inspector surfaces (PR-B handlers).
 buildListPluginsTool(client, mapper),
 buildListHooksTool(client, mapper)
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
 s"sm8: failed to POST $path: ${e.getClass.getSimpleName}: ${e.getMessage}"
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
   "modelName", McpSchema.JsonSchema.builder().`type`("string").build(),
   // Per C5-arch-M3 (REVISED after javap on mcp-core-2.0.1.jar):
   // `McpSchema.JsonSchema.Builder` does NOT expose a typed
   // `.items(...)` method. So nested schemas (e.g., for `array`
   // types) must be expressed as raw `LinkedHashMap<String, Object>`.
   // The MCP host's JSON-Schema parser sees the resulting map and
   // treats it as a regular schema. The "inconsistency" between
   // typed-builder properties and raw-map properties is forced by
   // the SDK API itself, not by us. Tightening to a typed
   // array/items builder requires an SDK upgrade (or contribution).
   "measures", new java.util.LinkedHashMap[String, Object]() {{
     put("type", "array");
     put("items", new java.util.LinkedHashMap[String, Object]() {{ put("type", "string"); }});
   }},
   "dimensions", new java.util.LinkedHashMap[String, Object]() {{
     put("type", "array");
     put("items", new java.util.LinkedHashMap[String, Object]() {{ put("type", "string"); }});
   }},
   "where", McpSchema.JsonSchema.builder().`type`("string").build(),
   "engine", McpSchema.JsonSchema.builder().`type`("string").build()
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
 req: McpSchema.CallToolRequest
): McpSchema.CallToolResult = {
 // The Restate ingress expects a `QueryRequest` JSON body:
 // {"modelName": "...", "measures": [...], "dimensions": [...],
 // "where": "...", "engine": "..."}.
 val args = req.arguments()
 val requestBody = new java.util.LinkedHashMap[String, Object]()
 copyString(args, "modelName", requestBody)
 copyList(args, "measures", requestBody)
 copyList(args, "dimensions", requestBody)
 copyString(args, "where", requestBody)
 copyString(args, "engine", requestBody)
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
 req: McpSchema.CallToolRequest
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
 req: McpSchema.CallToolRequest
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
 req: McpSchema.CallToolRequest
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
 req: McpSchema.CallToolRequest
): McpSchema.CallToolResult =
 callAndWrap(client, mapper, "/MetricsService/snapshot", new java.util.LinkedHashMap[String, Object]())
 })
 .build()
 }

 // ----- list_plugins (C10-PR-C) -----

 private def buildListPluginsTool(
 client: HttpIngressClient.Impl,
 mapper: ObjectMapper
): McpServerFeatures.SyncToolSpecification = {
 val tool = McpSchema.Tool.builder()
 .name("list_plugins")
 .title("List discovered sm8 plugins")
 .description(
 "List the plugins the sm8 deployment discovered at boot, " +
 "with a per-plugin `registered` flag (true = hooks are live " +
 "via Engine.use). Forwards to POST /RegistryInspectorService/listPlugins " +
 "on the Restate ingress. No arguments required."
)
 .inputSchema(McpSchema.JsonSchema.builder().`type`("object").build())
 .build()
 McpServerFeatures.SyncToolSpecification.builder()
 .tool(tool)
 .callHandler(new java.util.function.BiFunction[
 McpSyncServerExchange, McpSchema.CallToolRequest, McpSchema.CallToolResult] {
 def apply(
 exch: McpSyncServerExchange,
 req: McpSchema.CallToolRequest
): McpSchema.CallToolResult =
 callAndWrap(client, mapper, "/RegistryInspectorService/listPlugins", new java.util.LinkedHashMap[String, Object]())
 })
 .build()
 }

 // ----- list_hooks (C10-PR-C) -----

 private def buildListHooksTool(
 client: HttpIngressClient.Impl,
 mapper: ObjectMapper
): McpServerFeatures.SyncToolSpecification = {
 val tool = McpSchema.Tool.builder()
 .name("list_hooks")
 .title("List registered sm8 hooks")
 .description(
 "List every hook registered on the sm8 engine (name, pipeline " +
 "stage, priority, origin, registering plugin). Useful for " +
 "debugging hook ordering and plugin wiring. Forwards to POST " +
 "/RegistryInspectorService/listHooks on the Restate ingress. " +
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
 req: McpSchema.CallToolRequest
): McpSchema.CallToolResult =
 callAndWrap(client, mapper, "/RegistryInspectorService/listHooks", new java.util.LinkedHashMap[String, Object]())
 })
 .build()
 }

 // ----- arg helpers -----

 /** Copy a string arg from `args` to `dst` under `key`. No-op if
 * the key is missing or the value isn't a String. */
 private def copyString(
 args: java.util.Map[String, Object],
 key: String,
 dst: java.util.LinkedHashMap[String, Object]
): Unit = {
 val v = args.get(key)
 if (v != null && v.isInstanceOf[String]) dst.put(key, v)
 }

 /** Copy a string-list arg from `args` to `dst` under `key`.
   * Accepts a `java.util.List<Object>` (the type produced by Jackson 3
   * deserialization of a JSON array on the MCP wire). Per C5-arch-M2:
   * a Scala `scala.collection.immutable.List[String]` is NOT a
   * `java.util.List` (Scala `Seq` does not extend `java.util.List`),
   * so the previous docstring claim was misleading. The wire path is
   * always Jackson-deserialized into Java collections so this doesn't
   * manifest as a runtime bug today, but the docstring was wrong.
   * No-op if missing. */
 private def copyList(
 args: java.util.Map[String, Object],
 key: String,
 dst: java.util.LinkedHashMap[String, Object]
): Unit = {
 val v = args.get(key)
 if (v == null) return
 v match {
 case list: java.util.List[_] =>
 // Trust the wire — MCP JSON deserialization yields List<String>.
 val out = new java.util.ArrayList[String]()
 val it = list.iterator()
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