/*
 * SM8 MCP — ToolRegistry helper (ADR-0021).
 *
 * Plugin-side helper for `Sm8ToolHandlers`: the 7 MCP tools (query,
 * list_models, describe_model, list_engines, get_metrics,
 * list_plugins, list_hooks) all share the same construction pattern:
 *
 *   1. build an `McpSchema.Tool` (name + title + description + inputSchema + required)
 *   2. wrap a `(McpSyncServerExchange, CallToolRequest) => CallToolResult`
 *      in `BiFunction`, copy args from `req.arguments()`, call the
 *      Restate ingress via `HttpIngressClient.Impl`, return `CallToolResult`
 *
 * Per ADR-0021 §Sm8ToolHandlers → ToolRegistry helper: factor the
 * construction pattern so each tool becomes a 1-line
 * `ToolRegistry.register(ToolRegistry.Entry(name, build))` plus a
 * 20-30-LOC build body. Adding tool #8 is now 1 register call, not
 * 50 LOC of copy-paste.
 *
 * The registry is process-scoped (a Scala object with a
 * `ConcurrentHashMap` for the entries). The `Sm8ToolHandlers` object
 * calls `registerAll()` on load; `apply` (or `build`) collects
 * registered tools.
 *
 * No SDK change — the registry composes the existing
 * `McpServerFeatures.SyncToolSpecification` builder.
 */
package io.sm8.platform.mcp

import com.fasterxml.jackson.databind.ObjectMapper
import io.modelcontextprotocol.server.McpServerFeatures
import io.modelcontextprotocol.server.McpSyncServerExchange
import io.modelcontextprotocol.spec.McpSchema

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

/**
 * ADR-0021: a registered tool entry. `name` is the MCP tool name (must
 * be unique across the registry). `build` is the per-request factory
 * that takes the platform's `HttpIngressClient.Impl` (for the
 * Restate-ingress delegation) + the Jackson `ObjectMapper` and
 * produces the MCP `SyncToolSpecification`.
 *
 * The factory takes the client + mapper per-build because the
 * production wiring hands these at server-startup time (not at
 * registration time) — the per-build call lets the same registration
 * survive across multiple client/map configurations in tests.
 */
final case class ToolRegistryEntry(
    name: String,
    build: (HttpIngressClient.Impl, ObjectMapper) => McpServerFeatures.SyncToolSpecification
)

/**
 * Process-scoped tool registry. Plugin-style mutation via `register`;
 * query via `tools`.
 *
 * Concurrency: `entries` is a `ConcurrentHashMap` (thread-safe). The
 * `registerAll()` call from `Sm8ToolHandlers` happens at class-load
 * time (Scala object initialization), single-threaded by JVM semantics;
 * subsequent `tools` reads are thread-safe.
 */
object ToolRegistry {

  private val store: ConcurrentHashMap[String, ToolRegistryEntry] =
    new ConcurrentHashMap[String, ToolRegistryEntry]()

  /** Register a tool entry. Overwrites if `name` already exists
    * (callers are responsible for unique names). */
  def register(entry: ToolRegistryEntry): Unit = store.put(entry.name, entry)

  /** Unregister a tool by name. Returns true if removed, false if not
    * present (used by tests for cleanup).
    *
    * @param name the tool name to remove
    * @return     true if the entry was present and removed; false otherwise
    */
  def unregister(name: String): Boolean = Option(store.remove(name)).isDefined

  /** All currently registered tool entries, in registration order
    * (matches `ConcurrentHashMap.values` iteration semantics —
    * deterministic per JVM run, not lexicographic).
    *
    * @return the ordered list of currently-registered tool entries
    */
  def entries: Seq[ToolRegistryEntry] = store.values.asScala.toSeq

  /** Build all registered tools for the given client + mapper.
    *
    * @param client the platform's Restate-ingress client (one per
    *                `McpHttpRoute` / `McpStdioRoute` instance)
    * @param mapper the Jackson `ObjectMapper` for JSON encoding/decoding
    * @return       the ordered `SyncToolSpecification` sequence, ready
    *                to be passed to `buildServer(serverName, version, ...)`
    */
  def apply(client: HttpIngressClient.Impl, mapper: ObjectMapper)
    : Seq[McpServerFeatures.SyncToolSpecification] =
    entries.map(_.build(client, mapper))
}
