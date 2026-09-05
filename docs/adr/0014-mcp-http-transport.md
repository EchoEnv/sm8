# ADR-014: MCP server — Streamable HTTP transport + in-process stdio

> **Status:** **Implemented** — was Proposed (2026-09-01), promoted in the 2026-09-06 stale-ADR batch. Implementation: PR-261 (ADR + design, `3012e58`) + PR-263 (Streamable HTTP transport, `4121ec7`). **Date:** 2026-09-01. **Author:** SM8 agent (per user directive 2026-09-01T21:51Z — "ADR-014 candidates Streamable HTTP MCP transport + in-process --mcp-transport stdio (after redirecting sm8-server stdout to stderr)"). Succeeds ADR-013 (PR-259) which shipped the stdio MCP server as a separate binary; this ADR adds the two deferred paths.

## Context and Problem Statement

ADR-013 (PR-259, merged) shipped the Anthropic MCP server for sm8 over **stdio transport** only. That ADR's own dual-review r1 explicitly deferred two follow-ups:

1. **Streamable HTTP MCP transport** — required for remote agents (LangChain, Claude API, agents in containers) that connect over HTTP+SSE rather than spawning a subprocess. The SDK ships a Servlet implementation; sm8 uses Vert.x.
2. **In-process `--mcp-transport stdio`** — the original RFC §11a "single binary" preference; needs every sm8-server stdout write redirected to stderr so the in-process MCP server can own stdout exclusively.

This ADR proposes both. They are independent (no shared code; they ship as two PRs).

> **Context attribution note** (raised by r1 dual-review): the §11a contradiction was first caught by the PR-259 ADR-013 dual-review, not by PR-260. The "ADR-013 review pattern" mentioned throughout this ADR refers to PR-259's review chain (panda + peacock), not the implementation PR-260.

## Scope decision: two PRs

| PR | Path | LOC | Status |
|---|---|---|---|
| **PR-261** | Streamable HTTP MCP transport via custom Vert.x adapter | ~250 LOC + ~30 tests + smoke script | THIS ADR's primary deliverable |
| **PR-262** (deferred to ADR-015) | stdout-redirect + in-process `--mcp-transport stdio` flag | ~30 LOC mechanical + ~10 tests | Logged as ADR-015 candidate |

The split keeps PR-261 small (no coupling to the stdout-redirect refactor); PR-262 is a follow-up PR with its own ADR. This document covers BOTH paths but only PR-261 ships now.

## Decision (PR-261): Streamable HTTP MCP transport

Add a new module-internal Vert.x HTTP server in sm8-platform that implements the MCP `McpStreamableServerTransportProvider` interface (verified via javap on `mcp-core-2.0.1.jar`):

```
io.modelcontextprotocol.spec.McpStreamableServerTransportProvider extends
  io.modelcontextprotocol.spec.McpServerTransportProviderBase {
  // declared on the streamable interface:
  void setSessionFactory(McpStreamableServerSession.Factory)
  Mono<Void> notifyClients(String, Object)
  Mono<Void> closeGracefully()
  // declared on the parent base interface (NOT on the streamable
  // interface itself — javap-verified):
  Mono<Void> notifyClient(String, String, Object)   // from McpServerTransportProviderBase
  default void close()                               // from McpServerTransportProviderBase
}
```

Note (raised by r1): the interface declaration block above lists only the
methods declared on the streamable interface. `notifyClient` and the
default `close()` are inherited from `McpServerTransportProviderBase` —
the Vert.x adapter must implement the inherited methods too, but they
don't re-appear in the source-level interface declaration.

### What the transport needs to do (per the SDK's HttpServlet reference)

Verified by reading the SDK source at `HttpServletStreamableServerTransportProvider.java`:

| HTTP method | Path | Headers | Body | Response |
|---|---|---|---|---|
| POST | `/mcp` | `Accept: application/json, text/event-stream` + body is `initialize` (no `mcp-session-id`) | JSON-RPC `initialize` request | 200 OK + JSON-RPC `result` + `mcp-session-id` header (new session) |
| POST | `/mcp` | `Accept: application/json, text/event-stream` + `mcp-session-id` + JSON-RPC request | JSON-RPC request | 200 OK + SSE stream with the JSON-RPC response(s) |
| POST | `/mcp` | `Accept: application/json, text/event-stream` + `mcp-session-id` + JSON-RPC notification | JSON-RPC notification | 202 Accepted (no body) |
| POST | `/mcp` | `Accept: application/json, text/event-stream` + `mcp-session-id` + JSON-RPC response | JSON-RPC response (client→server flow) | 202 Accepted |
| GET | `/mcp` | `mcp-session-id` + `Accept: text/event-stream` | none | SSE listening stream (server→client notifications; supports `Last-Event-ID` resume) |
| DELETE | `/mcp` | `mcp-session-id` | none | 200 OK + session closed (or 405 if `--mcp-http-disallow-delete` is set) |

**Accept-header validation** (raised by r1, omitted in first draft): the SDK's `doPost` rejects requests whose `Accept` header is missing either `application/json` or `text/event-stream` with a 400 Bad Request. The Vert.x adapter must replicate this — both POST initialize and all subsequent POSTs require BOTH media types in `Accept`. The `GET` listening stream requires only `text/event-stream` (not `application/json`). Verification criterion #10 below covers this end-to-end via `curl -H "Accept: ..."` invocations.

**GET `Last-Event-ID` replay** (raised by r1): the SDK's `doGet` checks for the `Last-Event-ID` header and, if present, replays buffered messages from that event ID before switching to live listening mode. PR-261 ships with replay SUPPORTED (matching the SDK's `doGet` behavior) because the implementation is essentially free once the session's `replay()` is invoked — the underlying session already buffers messages for replay. Verification criterion #11 covers the replay path.

**`disallowDelete` config knob** (raised by r1 — HIGH): the SDK's `doDelete` short-circuits with 405 Method Not Allowed when `disallowDelete=true` is set. The smoke criterion #6 ("DELETE closes the session and returns 200 OK") applies when `--mcp-http-disallow-delete=false` (the default). Criterion #12 covers the 405 path. **Resolved the HIGH**: explicit default stated (`--mcp-http-disallow-delete=false`); both code paths now have criterion coverage.

SSE event format per response:
```
id: <event-id>
event: message
data: <jsonrpc-message-json>

```

### Why custom Vert.x instead of pulling in vertx-web

`sm8-platform` and `sm8-mcp` already use `vertx-core` 4.5.11 (via `RestateHttpServer.fromEndpoint` and `MetricsHttpRoute`). `vertx-web` is NOT on the classpath — adding it pulls in ~5MB and a Router API we don't otherwise need. Plain `vertx-core.HttpServer` with `requestHandler` + manual SSE chunked-write is ~200 LOC and matches the PR-258 `MetricsHttpRoute` style exactly.

### Module layout

**No new module.** The Streamable HTTP MCP server lives in `sm8-platform` (alongside `MetricsHttpRoute`) as a transport — not in `sm8-mcp` (which would put it back behind the Restate ingress delegation boundary). The architecture:

```
sm8-platform/.../query/
├── MetricsHttpRoute.scala          (existing, PR-258)
├── McpHttpRoute.scala              (NEW, PR-261)
├── ...
```

`sm8-server` wires `McpHttpRoute.start(cli.mcpHttpPort, ...)` in `Main.run` next to `MetricsHttpRoute.start` — same pattern, same AtomicReference slot, same shutdown-hook ordering.

### Why not `sm8-mcp` instead

`sm8-mcp` (PR-260) is the stdio subprocess server. It delegates to the Restate ingress via HTTP. Putting the HTTP MCP transport there would create a self-loop (the HTTP MCP server calling itself through the ingress). Worse, the HTTP MCP server is process-independent (it works fine when launched in the same JVM as sm8-server) — it belongs in sm8-platform alongside the existing transport-layer routes.

### CLI surface (added in `sm8-server/Main.scala`)

| Flag | Default | Notes |
|---|---|---|
| `--mcp-http-port <n>` | disabled | If set, bind the Streamable HTTP MCP server on `n` (Vert.x HttpServer on `/mcp`). |
| `--mcp-http-endpoint <path>` | `/mcp` | Per MCP spec — overridable for path-prefix deployments behind a reverse proxy. |
| `--mcp-http-disallow-delete` | `false` | If set, the SDK short-circuits DELETE with 405 Method Not Allowed. Off by default — MCP clients expect to close sessions via DELETE. See criterion #12. |

Mutually exclusive with the stdio MCP transport (PR-260): a single sm8-server can either run HTTP MCP (PR-261) OR launch the stdio subprocess (PR-260). The two transports have distinct lifecycle hooks; running both at once adds confusion without a use case.

### Why a separate Vert.x HttpServer (NOT a route on the Restate ingress)

Same reasoning as PR-258 `--metrics-port`: Restate's `HttpServer.requestHandler` is wired to `HttpEndpointRequestHandler` internally; calling `requestHandler` again would silently overwrite it. A separate `HttpServer` on its own port is the only safe binding. PR-258 established the sidecar-port pattern; PR-261 follows it.

### Lifecycle (mirror PR-258)

1. `Main.run` calls `McpHttpRoute.start(cli.mcpHttpPort, cli.mcpHttpEndpoint, ...)` after the Restate + Prometheus binds.
2. Bind awaits with 30s timeout — port-in-use fails LOUD.
3. JVM shutdown hook: one shared Runnable registered via `Runtime.getRuntime().addShutdownHook` runs the teardown in this exact order — (a) MCP HTTP server `closeGracefully()` (drains in-flight SSE responses, 5s timeout), (b) `MetricsHttpRoute.stop(server)` (drain Prometheus client connections), (c) `HttpTransport.stop()` (close Restate HTTP ingress). **The ordering is enforced inside ONE Runnable, not across hook-registration order** — JVM shutdown hooks run concurrently in unspecified order, so sequencing within multiple hooks is unenforceable (raised by r1; this corrects the original "BEFORE the existing transport + metrics hooks" wording). If MCP SSE streams must close before the Restate ingress teardown, sequence it inside one hook — that's what we do here.
4. Bind failure is fail-LOUD on stderr; doesn't abort the process (the Restate ingress stays useful).

### Skill: building-restate-services

Per the building-restate-services skill: the MCP HTTP server is OUTSIDE Restate's journal pipeline (it's a plain HTTP server, not a Restate handler). `Instant.now()` is correct here even though it's a no-no inside Restate handler closures.

## Decision (PR-262, deferred to ADR-015): In-process `--mcp-transport stdio`

To honor RFC §11a's "single binary" preference, ADR-013 acknowledged an alternative path: redirect every sm8-server stdout write to stderr, then merge `sm8-mcp`'s stdio transport into sm8-server as a `--mcp-transport stdio` flag.

### What the redirect requires

Verified via grep on `sm8-server/src/main/scala/io/sm8/server/Main.scala`:

- Line 318: `println(Usage); return if (args.isEmpty) 2 else 0` — `--help` path; redirect → stderr
- Line 388: `println(s"sm8: server listening on port $boundPort ...)` — startup banner; redirect → stderr
- Line 424: `println(s"sm8: metrics endpoint listening on port ${cli.metricsPort}")` — startup banner; redirect → stderr

3 mechanical changes. Plugins do NOT write to stdout (verified — no `println`/`System.out` in `plugins/*/src/main/`). The Restate SDK's slf4j default is stderr. The redirect is bounded to sm8-server.

### Why deferred

Per the locked "smallest correct change" principle, PR-261 ships first. PR-262 can be merged later without disturbing PR-261's contract (it's purely additive: a new flag on sm8-server + redirect of 3 lines). The deferred ADR (ADR-015) will own this scope and produce the implementation PR.

### Cancellation rule

If a future change adds stdout writes anywhere else (e.g. a new plugin or a future ADR), the in-process stdio MCP server breaks. The guard is documentation + a smoke assertion: `--mcp-transport stdio` smoke verifies stdout is JSON-RPC only (no other content). ADR-015 will own that smoke.

## Verification criteria (PR-261)

| # | What | How |
|---|---|---|
| 1 | Streamable HTTP MCP transport implements the full 6-method surface | javap + unit tests for each method |
| 2 | POST initialize returns JSON + sets `mcp-session-id` header | unit test + smoke `curl -i` |
| 3 | POST JSON-RPC request returns SSE stream with the response | unit test (uses a fake `ctx` for session.handle()) |
| 4 | POST JSON-RPC notification returns 202 Accepted | unit test |
| 5 | GET opens SSE listening stream; server→client notifications emit as SSE events | unit test + smoke `curl -N` |
| 6 | DELETE closes the session and returns 200 OK | unit test |
| 7 | Unknown path (`/whatever`) returns 404 | unit test (mirrors PR-258 criterion #4) |
| 8 | Bind failure (port in use) is fail-LOUD on stderr; process continues | unit test (smoke) |
| 9 | Existing `scripts/smoke-e2e.sh` still green (no Restate ingress / MetricsService / Prometheus regression) | unchanged smoke script |
| 10 | New `scripts/smoke-mcp-http.sh` exercises initialize + tools/call + DELETE — AND validates `Accept` header enforcement (POST without `application/json` returns 400; POST without `text/event-stream` returns 400) | new script |
| 11 | GET with `Last-Event-ID` header replays buffered messages from that ID before live listening | unit test (verify the SSE event stream contains the replayed event) |
| 12 | `--mcp-http-disallow-delete` short-circuits DELETE with 405 Method Not Allowed | unit test + smoke `curl -X DELETE` with the flag set |
| 13 | Layer discipline: `sm8-platform` adds no new dep; vertx-core already on classpath | pom diff check |
| 14 | Scaladoc linter clean (shape + noise) on changed files | linter run |

## Open questions / risks

- **Authentication.** Streamable HTTP over a public port needs auth. PR-261 ships a `ServerTransportSecurityValidator` using the SDK's `NOOP` validator (no real auth). Future ADR will add a real auth scheme; per ADR-013 §Out of scope, deferred. (The `disallowDelete=true` flag is a SEPARATE config knob — it short-circuits DELETE with 405 — and is OFF by default; see the CLI table above.)
- **Concurrent sessions.** The SDK's HttpServlet impl uses `java.util.concurrent.ConcurrentHashMap` keyed by `mcp-session-id`. PR-261 mirrors that with Vert.x's plain `java.util.concurrent.ConcurrentHashMap[String, McpStreamableServerSession]` (raised by r1; the original wording "immutable map updated via .put" was self-contradictory). The Vert.x route handlers do reads (`sessions.get(id)`) and writes (`sessions.put(id, session)`) inside the map; ConcurrentHashMap's lock-striping handles concurrent SSE-stream adds/removes safely. No cluster mode — sessions live in the single sm8-server JVM.
- **HTTP/2 + TLS.** Vert.x supports both; PR-261 is plain HTTP/1.1 (matches `MetricsHttpRoute`). TLS termination is the operator's responsibility (reverse proxy).

## Sources

- MCP SDK `mcp-core-2.0.1.jar` (`io.modelcontextprotocol.spec.McpStreamableServerTransportProvider` + `McpStreamableServerSession.Factory`) — verified via javap
- MCP SDK `HttpServletStreamableServerTransportProvider` source — read in full on `github.com/modelcontextprotocol/java-sdk/main/mcp-core/src/main/java/io/modelcontextprotocol/server/transport/HttpServletStreamableServerTransportProvider.java` (32KB, ~830 LOC, the reference implementation we mirror in Vert.x)
- Vert.x `vertx-core` 4.5.11: `HttpServer.requestHandler(Handler<HttpServerRequest>)` + `HttpServerResponse.write(Buffer)` for SSE chunked-write; `HttpServerOptions.setPort/setHost` (per PR-258 javap)
- ADR-013 (PR-259, merged) §"Out of scope" — the deferred-candidate list
- RFC §11a "single binary" — acknowledged contradiction; logged ADR-015 as the path to close it

## Why ship both options in one ADR but one PR

The user asked for both paths in one task. Writing a single ADR covers the design decisions for both (so a future maintainer sees the trade-off), but PR-261 ships only the Streamable HTTP piece. PR-262 follows with its own ADR-015 (stdout refactor) when the user wants it. This keeps the diff small + each PR independently revertable.