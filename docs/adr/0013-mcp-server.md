# ADR-013: Model Context Protocol (MCP) server

> **Status:** **Implemented** — was Proposed (2026-09-01), promoted in the 2026-09-06 stale-ADR batch. Implementation: PR-259 (docs commit `17e0f8a`) + PR-260 (server commit `3024dc8`, stdio transport, 5 tools, separate sm8-mcp binary). **Date:** 2026-09-01. **Author:** SM8 agent (per user directive 2026-09-01T19:54Z — "sm8 exposing its tools to an LLM via stdio/SSE so any agent can query, info, update via mcp instead of cli or manual").

## Context and Problem Statement

sm8 today exposes its capabilities through the **Restate ingress** on `--port` (default 8080): `/QueryService/runQuery`, `/ModelService/listModels`, `/ModelService/describe`, `/MetricsService/snapshot`, etc. A human operator uses `sm8-cli` (HTTP+JSON) or curl to drive these endpoints. An LLM-driven agent today has no first-class way to call sm8: the Restate bidi-stream protocol is not something Claude Desktop, LangChain, or the OpenAI function-calling layer knows how to speak.

The [Anthropic Model Context Protocol](https://modelcontextprotocol.io) (MCP) is the de facto standard for LLM-tool integration. Any MCP-aware agent can call `tools/list` to discover capabilities and `tools/call` to invoke them — over **stdio** (subprocess pipe, used by Claude Desktop) or **Streamable HTTP** (used by remote agents). Adding an MCP server to sm8 means:

- A user can point Claude Desktop at `sm8 mcp` and ask "which models are configured?" / "run a query against model X" without writing curl.
- An LLM agent in a container can connect to a remote sm8 MCP endpoint and drive queries programmatically.
- sm8 gains a **discovery surface** for agents: `tools/list` exposes exactly the same wire surface a human would see, with the same typed schemas.

This ADR does NOT replace the Restate ingress — it sits alongside it and delegates 1:1.

## Decision

Add a new MCP server module `sm8-mcp/` in the **adapter** layer (mirrors `sm8-cli`'s position — see `semantic-layer-engine-architecture.md` §3 Core Boundary: deployment-side modules that compose the transport library go in the adapter layer, alongside `sm8-cli` and `sm8-server`). The MCP server **delegates** to the existing Restate ingress endpoints over HTTP — no engine logic, no duplicate parsing.

> **Layer taxonomy caveat** (raised by dual-review r1): §3's strict
> "Adapter" definition is `EngineProvider` implementations that "know
> about a specific data source." sm8-mcp doesn't fit that — it doesn't
> touch any data source. The pragmatic placement is "adapter-like
> deployment module," alongside `sm8-cli` (which similarly delegates
> to the REST surface without being an EngineProvider). If a future
> RFC update formalizes the "transport adapter" tier, sm8-mcp belongs
> there.

**Process model (v1):**

`sm8-mcp` is a SEPARATE executable (`io.sm8.mcp.Main`), NOT a mode of
sm8-server. The two run as two processes:

- **sm8-server** (daemon, unchanged): binds the Restate ingress on
  `--port` (8080) and Prometheus metrics on `--metrics-port` (9090).
- **sm8-mcp** (stdio subprocess, launched by the MCP client): binds
  NO ports. Reads JSON-RPC on stdin, writes JSON-RPC on stdout.
  Tool calls become HTTP POSTs to the sm8-server ingress at
  `--ingress-url` (default `http://127.0.0.1:8080`).

Why a separate binary (not a `--mcp-transport stdio` flag on
sm8-server): stdio MCP requires exclusive ownership of process stdout
— every `println` in the same process corrupts the JSON-RPC stream.
sm8-server prints startup lines ("server listening on port ...",
"metrics endpoint listening ...") and any slf4j output defaults to
stdout. A separate process eliminates the entire stdout-pollution risk
class and keeps sm8-server's contract untouched. The MCP-aware agent
(Claude Desktop, etc.) is designed to spawn subprocesses — this is the
normal deployment shape.

> **Note on RFC §11a wording** (raised by dual-review r1): §11a says
> "the deployment module is the single binary that hosts them [HTTP
> server, MCP wire, REST]" and "Wire shape (MCP/REST) is decided by
> the transport handler chosen at bind time, not by separate
> deployment modules." Read literally, §11a forbids sm8-mcp as a
> separate binary. The **pragmatic reading**: §11a forbids parallel
> deployment modules that each host the SAME wire shape redundantly
> (e.g. one binary for Restate + another for REST doing the same
> thing). **This ADR does not duplicate wire shapes**: the Restate
> ingress (the only ingress) stays in sm8-server; sm8-mcp is a thin
> transport adapter that **delegates** to that ingress over HTTP. If
> a future maintainer reads §11a more strictly, the in-process
> alternative is achievable by removing the subprocess boundary and
> routing through the Restate `HandlerContext` (dev.restate.sdk package; the existing handlers depend on it per ADR-006) directly — left as
> a follow-up. The separate-binary path is the safe v1 default
> because it eliminates the stdout-pollution risk class entirely.

Precondition: sm8-server (or another Restate-ingress-compatible
backend) must be reachable at `--ingress-url`. If not, every tool call
fails with a typed MCP error (`isError=true`, connection-refused
detail in the content text) — the MCP handshake itself still succeeds
so `tools/list` works offline.

**CLI surface (`sm8-mcp` binary):**

```
java -cp <classpath> io.sm8.mcp.Main [--ingress-url <url>] [--request-timeout <secs>]
```

| Flag | Default | Notes |
|---|---|---|
| `--ingress-url <u>` | `http://127.0.0.1:8080` | Where to POST tool calls (the Restate ingress) |
| `--request-timeout <n>` | `30` | Per-tool-call HTTP timeout (seconds). `query` can legitimately take minutes — see Risks. |

**Transport scope (v1, this ADR):**

| Transport | Status | Rationale |
|---|---|---|
| **stdio** | v1 scope | Default per Anthropic's recommended MCP setup. Spawned as a subprocess; reads stdin / writes stdout. Works with Claude Desktop out of the box. |
| **Streamable HTTP** | DEFERRED to a follow-up (ADR-014) | Requires writing a custom Vert.x → MCP transport adapter (the SDK ships a Servlet one only; sm8-server is Vert.x, not Servlet). Out of scope for v1. |

**Tools exposed (5 total: 4 forwarding to existing Restate handlers + 1 new handler):**

| Tool name | Forwards to | Notes |
|---|---|---|
| `query` | `POST /QueryService/runQuery` | Argument shape mirrors `QueryRequest` (`modelName`, `measures`, `dimensions`, `where`, `engine`) |
| `list_models` | `POST /ModelService/listModels` | No args. Returns `ListModelsResponse`. |
| `describe_model` | `POST /ModelService/describe` | Args: `{modelName: String}`. Returns `DescribeResponse`. |
| `list_engines` | `POST /EngineService/listEngines` | NEW Restate handler. The MCP server benefits from this even if humans don't — agents need it to discover engines for the `query` tool's `engine` field. Adds 1 handler + 1 unit test in sm8-platform. |
| `get_metrics` | `POST /MetricsService/snapshot` | No args. Returns `MetricsSnapshot`. Already wired (PR-254/PR-256). |

**Why delegate to Restate ingress and not call sm8-platform handlers directly?**

sm8-platform's handlers are `HandlerRunner.of(...)` closures that depend on Restate's `dev.restate.sdk.Context` (journal, awakeables, side effects). Calling them outside a Restate session would require unwiring the journal seam — invasive and brittle. The Restate ingress endpoints are already the **public, journal-correct** surface; routing MCP requests through them gets journaling for free. Trade-off: one extra HTTP hop (~1ms LAN, ~20ms remote). Acceptable.

**Why stdio first?**

- Lowest blast radius: no new port, no auth, no concurrency concerns.
- Anthropic's recommended path: every MCP-aware desktop client supports it.
- The `StdioServerTransportProvider` lifecycle is well-defined: spawn → block on stdin read loop → exit on EOF. JVM shutdown hook just calls `closeGracefully()`.
- `sm8-mcp`'s own logging goes to stderr (via SLF4J binding or explicit `System.err.println`), never stdout — stdout is reserved for JSON-RPC frames. No slf4j config ships with `sm8-mcp`; the default JDK `SimpleLogger` writes to stderr. This is a documented implementation constraint, not a runtime check.

## Why not a new wire surface in sm8-platform

Per RFC §11a "transport library contains zero deployment concerns":

> Wire shape (MCP / REST) is decided by the transport handler chosen at bind time, not by separate deployment modules.

The MCP SDK is a transport. Adding it to sm8-platform would pull
**deployment concerns** (subprocess lifecycle, log routing, typed
CLI parsing, the entire `io.sm8.mcp.Main` entry point) into the
transport library — exactly what the RFC forbids. The MCP server
therefore lives in `sm8-mcp/` (a separate module), which is itself
a deployment-side module by the §11a definition. The §11a "single
binary" language is addressed in the **§11a note** above (Decision
section): sm8-mcp does not duplicate any wire shape; it delegates.

## Wiring

### Module layout (NEW)

```
sm8-mcp/                              # NEW adapter-layer module
├── pom.xml                           # depends on sm8-platform (only — no core/plugin reach-in)
└── src/main/scala/io/sm8/mcp/
    ├── Main.scala                    # io.sm8.mcp.Main entry point; --ingress-url + --request-timeout
    ├── McpServer.scala               # builds io.modelcontextprotocol.server.McpServer via the SDK
    ├── Sm8ToolHandlers.scala         # 5 BiFunction tool handlers; each delegates to Restate ingress via java.net.http.HttpClient
    ├── HttpIngressClient.scala       # thin typed wrapper over java.net.http.HttpClient (timeout, error mapping)
    └── package.scala
```

### `io.sm8.mcp.Main` lifecycle (sm8-mcp's Main.scala)

`sm8-mcp` runs as a single-process stdio server. Lifecycle:

1. Parse `--ingress-url` + `--request-timeout` (typed parse, same pattern as sm8-server's CLI).
2. Build a `StdioServerTransportProvider(new JacksonMcpJsonMapper(new tools.jackson.databind.json.JsonMapper()))` — the SDK ships Jackson 3 in `mcp-json-jackson3` (verified via `unzip -l mcp-json-jackson3-2.0.1.jar`); `JacksonMcpJsonMapper`'s only ctor takes a `tools.jackson.databind.json.JsonMapper` (the Jackson 3 core). The `StdioServerTransportProvider(jsonMapper)` ctor then defaults to `System.in` / `System.out` (the two-arg overload with explicit streams is also available).
3. Build the typed `McpServer.sync(stdioTransport)` with:
   - `.serverInfo("sm8-mcp", "0.1.0-SNAPSHOT")` (matches sm8-server's version)
   - `.capabilities(ServerCapabilities.builder().tools(true).build())` (no resources/prompts per scope)
   - `.tools(queryTool, listModelsTool, describeModelTool, listEnginesTool, getMetricsTool)` (each `SyncToolSpecification.builder().tool(...).callHandler(BiFunction<...>)`)
4. Call `.build()` — returns an `McpSyncServer`. The SDK's stdio provider begins the stdin read loop on `.build()` (verified via `javap -p` on `StdioServerTransportProvider`: the provider's only public session-related method is `setSessionFactory(McpServerSession.Factory)`; the McpServer builder calls this internally, which creates a `StdioMcpSessionTransport` private inner class and spawns two `Schedulers.fromExecutorService(...)` threads — one for the stdin read loop, one for the stdout write loop. The threads run until stdin EOF or `closeGracefully()` is invoked.)
5. Register a JVM shutdown hook: `mcpServer.closeGracefully().block(Duration.ofSeconds(5))` on SIGTERM. (No HTTP sockets to coordinate; no providers to close; the only resource is the SDK's internal schedulers which `closeGracefully` will dispose.)
6. `Thread.currentThread().join()` (the main thread is idle; the SDK's scheduler threads handle all I/O).

**No port binding.** No `Vertx.vertx()`. No `bind`/`listen` future. The process is single-purpose: read JSON-RPC, call HTTP, write JSON-RPC.

### New `EngineService.listEngines` Restate handler (sm8-platform)

Exposes the engine registry as a typed wire DTO. ~25 LOC: a `ListEnginesRequest` + `ListEnginesResponse` case class, a `ServiceDefinition` factory that calls `EngineRegistry.availableProviders` (returns the sorted names of providers that successfully realized — i.e. **available**, not every discovered; verification criterion: matches the sm8-cli `engines list` semantics already shipped), and the same `HandlerRunner.of(...)` pattern as the other services. Linter-clean, ~3 unit tests.

## Open thread: sm8-cli dep precedent (corrected from r1)

`sm8-cli/pom.xml` actually has **zero `sm8-*` dependencies** — only
jackson-databind, scala-library, scalatest. Its module description
string mentions "sm8-core for SDK types" but the pom does not declare
it (sm8-cli's wire-DTOs are accessed via its own copy of the
case-class shapes, or via an unrelated artifact). The correct precedent
for sm8-mcp's dependency direction is therefore **`sm8-server`,
which depends on `sm8-platform_2.13`** — that direction is established
(verified via `sm8-server/pom.xml:40-52`). sm8-mcp will follow it:
depends on `sm8-platform` only, no core or plugin reach-in.

## Verification criteria

| # | What | How |
|---|---|---|
| 1 | stdio transport exposes the 5 tools | `npx @modelcontextprotocol/inspector --stdio "java -cp ... io.sm8.mcp.Main --ingress-url http://127.0.0.1:8080"` shows 5 tools in the tool list |
| 2 | `query` tool round-trip returns the same wire shape as `POST /QueryService/runQuery` | inspector invokes `query` with a known model; result JSON is byte-equal (modulo whitespace) to a direct `curl /QueryService/runQuery` |
| 3 | `list_models` returns the same payload as `POST /ModelService/listModels` | unit test in sm8-mcp asserts JSON-shape parity |
| 4 | Lifecycle: SIGTERM during MCP session closes cleanly without hanging | shutdown hook integration test in `McpServerSpec` |
| 5 | Layer discipline: `sm8-mcp/pom.xml` depends on `sm8-platform` only — no core or plugin reach-in | linter/rule check |
| 6 | Smoke: existing scripts/smoke-e2e.sh still passes (no regression on Restate ingress or metrics endpoint) | unchanged smoke script |
| 7 | New smoke assertion: MCP handshake + `tools/list` returns all 5 tool names | `scripts/smoke-mcp.sh` (NEW) — starts a tiny sm8-server in the background, then `java -cp ... io.sm8.mcp.Main --ingress-url ...` as a subprocess. Pipes the full MCP handshake sequence to stdin (newline-delimited JSON): `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}` + `{"jsonrpc":"2.0","method":"notifications/initialized"}` + `{"jsonrpc":"2.0","id":2,"method":"tools/list"}` + EOF. Greps stdout for all 5 tool names. Without the `initialize` + `notifications/initialized` exchange the SDK rejects `tools/list` with a "not initialized" error (per MCP spec §lifecycle). |
| 8 | Streamable HTTP `--mcp-port` (deferred ADR-014) is NOT a flag in v1; the smoke CLI args must not reference it | grep verification — no `--mcp-port` in the v1 CLI |

## Out of scope for this PR (deferred)

- **Streamable HTTP MCP transport.** Needs a custom Vert.x → MCP transport adapter (~150 LOC). Reserved for ADR-014.
- **Auth.** Stdio subprocess has no network attack surface; the deferred Streamable HTTP version will need it.
- **Resources/prompts.** MCP supports `resources/list`, `resources/read`, `prompts/list`, `prompts/get`. The 5 tools above are sufficient for the user's stated use case ("any agent can query, info, update"); resources/prompts can be added in a follow-up if requested.

## Why NOT defer MCP entirely

The MCP server is a thin transport wrapper over already-built handlers. Net new code is ~200 LOC + 1 new `listEngines` Restate handler (~25 LOC). The risk profile is low: no new wire surface, no new engine logic, no plugin reach-in. The benefit (LLM agents can drive sm8) is concrete and requested. The blast radius of the change is bounded — the existing Restate ingress, MetricsService, and ModelService handlers are unchanged; the new module just wraps them.

## Sources

- Anthropic MCP specification: <https://modelcontextprotocol.io/specification/latest>
- Java MCP SDK: <https://github.com/modelcontextprotocol/java-sdk> (`io.modelcontextprotocol.sdk:mcp:2.0.1`, MIT, 2026-08-19)
- Java MCP SDK package `io.modelcontextprotocol.server.McpServer` (verified via `javap` on `~/.m2/repository/io/modelcontextprotocol/sdk/mcp-core/2.0.1/mcp-core-2.0.1.jar`):
  - `McpServer.sync(McpServerTransportProvider)` → `SingleSessionSyncSpecification`
  - `.serverInfo(name, version)`, `.instructions(String)`, `.capabilities(ServerCapabilities)`, `.tools(SyncToolSpecification...)`, `.build()` → `McpSyncServer`
- `StdioServerTransportProvider` source (verified): `McpServerTransportProvider`, ctor takes `McpJsonMapper`, lifecycle is `setSessionFactory` (called by SDK) → `closeGracefully()`. Reads stdin / writes stdout / logs to stderr.
- RFC: `docs/rfcs/2026-08-12_v1_architecture-spec/semantic-layer-engine-architecture.md` §11a (transport library must not contain deployment concerns; deployment module is the single binary that hosts wire shapes).
- RFC: `docs/rfcs/2026-08-12_v1_architecture-spec/adapters.md` §"Conformance" + §"Where Adapters Live" (sm8-cli adapter pattern applies to sm8-mcp).
