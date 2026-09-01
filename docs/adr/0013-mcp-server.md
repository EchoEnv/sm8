# ADR-013: Model Context Protocol (MCP) server

> **Status:** Proposed. **Date:** 2026-09-01. **Author:** SM8 agent (per user directive 2026-09-01T19:54Z — "sm8 exposing its tools to an LLM via stdio/SSE so any agent can query, info, update via mcp instead of cli or manual").

## Context and Problem Statement

sm8 today exposes its capabilities through the **Restate ingress** on `--port` (default 8080): `/QueryService/runQuery`, `/ModelService/listModels`, `/ModelService/describe`, `/MetricsService/snapshot`, etc. A human operator uses `sm8-cli` (HTTP+JSON) or curl to drive these endpoints. An LLM-driven agent today has no first-class way to call sm8: the Restate bidi-stream protocol is not something Claude Desktop, LangChain, or the OpenAI function-calling layer knows how to speak.

The [Anthropic Model Context Protocol](https://modelcontextprotocol.io) (MCP) is the de facto standard for LLM-tool integration. Any MCP-aware agent can call `tools/list` to discover capabilities and `tools/call` to invoke them — over **stdio** (subprocess pipe, used by Claude Desktop) or **Streamable HTTP** (used by remote agents). Adding an MCP server to sm8 means:

- A user can point Claude Desktop at `sm8 mcp` and ask "which models are configured?" / "run a query against model X" without writing curl.
- An LLM agent in a container can connect to a remote sm8 MCP endpoint and drive queries programmatically.
- sm8 gains a **discovery surface** for agents: `tools/list` exposes exactly the same wire surface a human would see, with the same typed schemas.

This ADR does NOT replace the Restate ingress — it sits alongside it and delegates 1:1.

## Decision

Add a new MCP server module `sm8-mcp/` in the **adapter** layer (mirrors `sm8-cli`'s adapter status — see `semantic-layer-engine-architecture.md` §3 Core Boundary: deployment-side modules that compose the transport library go in the adapter layer). The MCP server **delegates** to the existing Restate ingress endpoints over HTTP — no engine logic, no duplicate parsing.

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
| **stdio** | SHIPPED | Default per Anthropic's recommended MCP setup. Spawned as a subprocess; reads stdin / writes stdout. Works with Claude Desktop out of the box. |
| **Streamable HTTP** | DEFERRED to a follow-up (ADR-014) | Requires writing a custom Vert.x → MCP transport adapter (the SDK ships a Servlet one only; sm8-server is Vert.x, not Servlet). Out of scope for v1. |

**Tools exposed (5, 1:1 with existing Restate handlers):**

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
- Stdio's only gotcha: the MCP server reads stdin; sm8-server's `MetricsService/snapshot` debug logging MUST go to stderr only (PR-256 added System.err routing; need to verify the same convention holds for any future log added by MCP tool wrappers — explicit guard in the implementation).

## Why not a new wire surface in sm8-platform

Per RFC §11a "transport library contains zero deployment concerns":

> Wire shape (MCP / REST) is decided by the transport handler chosen at bind time, not by separate deployment modules.

The MCP SDK is a transport. sm8-server (deployment module) is the natural host for it, alongside the existing Restate + Prometheus transports. Adding it to sm8-platform would pull deployment concerns (subprocess lifecycle, log routing) into the transport library — exactly what the RFC forbids.

## Wiring

### Module layout (NEW)

```
sm8-mcp/                              # NEW adapter-layer module
├── pom.xml                           # depends on sm8-platform (only — no core/plugin reach-in)
└── src/main/scala/io/sm8/mcp/
    ├── McpServer.scala               # builds io.modelcontextprotocol.server.McpServer via the SDK
    ├── Sm8ToolHandlers.scala         # 5 BiFunction tool handlers; each delegates to Restate ingress via java.net.http.HttpClient
    ├── HttpIngressClient.scala       # thin typed wrapper over java.net.http.HttpClient (timeout, error mapping)
    └── package.scala
```

### `--mcp-transport stdio` lifecycle (sm8-server/Main.scala additions)

1. After existing `--port` + `--metrics-port` binding, check `cli.mcpTransport`.
2. If `stdio`: build `McpServer.sync(StdioServerTransportProvider(jsonMapper))`, register the 5 tools, call `.build()` — the SDK's stdio provider begins the stdin read loop immediately. Block the main thread on `Thread.currentThread().join()` (already there for the Restate + metrics case; stdio is process-bound so join is correct).
3. JVM shutdown hook: call `mcpServer.closeGracefully()` BEFORE the existing transport + metrics hooks (cleanest teardown order: stop accepting new MCP requests first, then close the HTTP sockets).

### New `EngineService.listEngines` Restate handler (sm8-platform)

Exposes the engine registry as a typed wire DTO. ~25 LOC: a `ListEnginesRequest` + `ListEnginesResponse` case class, a `ServiceDefinition` factory that calls `EngineRegistry.names` (returns the `EngineIdentity.name` of every discovered provider), and the same `HandlerRunner.of(...)` pattern as the other services. Linter-clean, ~3 unit tests.

## Open thread: does sm8 already have a separate module for deployment-side tooling?

`sm8-cli` is an **adapter** module (`semantic-layer-engine-architecture.md` §3) — it depends on `sm8-core` only. `sm8-mcp` will depend on `sm8-platform` (NOT core) because the MCP tool wrappers need the wire DTOs (`QueryRequest`, `ListModelsResponse`, etc.) which live in sm8-platform. This is the same dependency direction `sm8-server` already has.

## Verification criteria

| # | What | How |
|---|---|---|
| 1 | stdio transport exposes the 5 tools | `npx @modelcontextprotocol/inspector --stdio "java -cp ... io.sm8.mcp.Main --ingress-url http://127.0.0.1:8080"` shows 5 tools in the tool list |
| 2 | `query` tool round-trip returns the same wire shape as `POST /QueryService/runQuery` | inspector invokes `query` with a known model; result JSON is byte-equal (modulo whitespace) to a direct `curl /QueryService/runQuery` |
| 3 | `list_models` returns the same payload as `POST /ModelService/listModels` | unit test in sm8-mcp asserts JSON-shape parity |
| 4 | Lifecycle: SIGTERM during MCP session closes cleanly without hanging | shutdown hook integration test in `McpServerSpec` |
| 5 | Layer discipline: `sm8-mcp/pom.xml` depends on `sm8-platform` only — no core or plugin reach-in | linter/rule check |
| 6 | Smoke: existing scripts/smoke-e2e.sh still passes (no regression on Restate ingress or metrics endpoint) | unchanged smoke script |
| 7 | New smoke assertion: MCP `tools/list` returns all 5 tool names | `scripts/smoke-mcp.sh` (NEW) — starts a tiny sm8-server in the background, then `java -cp ... io.sm8.mcp.Main --ingress-url ...`, pipes `{"jsonrpc":"2.0","method":"tools/list","id":1}\n` to stdin + closes stdin (EOF), greps stdout for the 5 tool names |
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
