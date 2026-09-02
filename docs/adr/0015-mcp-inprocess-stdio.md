# ADR-015: MCP server — in-process `--mcp-transport stdio`

> **Status:** Proposed. **Date:** 2026-09-02. **Author:** SM8 agent (per user directive 2026-09-02T06:19Z — "PR-262 / ADR-015 in-process --mcp-transport stdio (stdout redirect refactor)"). Succeeds ADR-014 (PR-261 + PR-263) which shipped the Streamable HTTP MCP transport. This ADR closes the other path deferred from ADR-013 §11a note.

## Context and Problem Statement

ADR-013 (PR-259, merged) shipped the Anthropic MCP server for sm8 over **stdio transport** as a **separate executable** (`io.sm8.mcp.Main`). That ADR explicitly deferred one follow-up: an in-process `--mcp-transport stdio` flag on sm8-server, so a single binary could host both the Restate ingress + the stdio MCP server. The reason for the deferral was the **stdout-pollution risk**: sm8-server prints startup banners to stdout ("server listening on port …", "metrics endpoint listening on port …", "MCP Streamable HTTP endpoint listening on port …"). An in-process stdio MCP server would have its JSON-RPC stream on stdout corrupted by those lines.

ADR-014 (PR-263) closed the Streamable HTTP side; this ADR closes the in-process stdio side. With both paths shipped, sm8 can serve every MCP deployment shape:
- **stdio subprocess** (PR-260): launch sm8-mcp as a child of Claude Desktop / stdio-aware clients
- **Streamable HTTP** (PR-263): `--mcp-http-port` for remote agents
- **in-process stdio** (this ADR): `--mcp-transport stdio` for clients that want one binary

## Decision

Add a `--mcp-transport stdio` flag to sm8-server. When set, sm8-server hosts both the Restate ingress AND an in-process stdio MCP server. Two mechanical pieces:

1. **Redirect 4 stdout writes to stderr** in sm8-server. Bounded audit: 4 `println` calls in `sm8-server/src/main/scala/io/sm8/server/Main.scala` (lines 334, 404, 440, 465). Zero in plugins (verified via `grep -rn "println\|System\.out" plugins/*/src/main/scala/` — no hits). Zero in sm8-mcp. The Restate SDK's slf4j default writes to stderr.
2. **Wire sm8-mcp's stdio transport in-process.** Reuse the existing `Sm8ToolHandlers` + `HttpIngressClient` (the 5 PR-260 tools). The only new code is a Main flag + an in-process transport orchestrator (creates the SDK's `StdioServerTransportProvider` + a `McpServer.sync(transport).build()` and blocks the main thread on stdin).

### Why not a new module

PR-263 established the pattern: `McpHttpRoute` lives in `sm8-platform` (transport library). For symmetry, the in-process stdio transport would be `McpStdioRoute` in `sm8-platform`, mirroring `McpHttpRoute`'s shape. The 5 tools are already registered in `sm8-mcp/Sm8ToolHandlers.scala`; we'd move that object to `sm8-platform` so BOTH transports (HTTP via PR-263 + stdio via this ADR) can share the same tool set.

### Tool sharing: the design decision

`Sm8ToolHandlers.build(client)` currently takes an `HttpIngressClient` and produces 5 `SyncToolSpecification`s. We refactor it to:
- Take a `tools: Seq[ToolSpec]` (already 5 hardcoded `query`/`list_models`/etc.) directly + a `client: HttpIngressClient` for the wire calls
- Move the `HttpIngressClient` instantiation to a NEW `McpStdioRoute` that owns the stdio lifecycle
- `McpHttpRoute.buildServer(...)` and `McpStdioRoute.start(...)` both call `Sm8ToolHandlers.build(client)` to get the same 5 tool specs

This is a non-breaking refactor: the public surface (`McpServer.sync(...).tools(...).build()`) is unchanged.

### Layer discipline (RFC §3)

Per the RFC §3 layer table, deployment-side modules (sm8-server, sm8-mcp) are the outermost layer. The stdio MCP transport is a deployment concern (lifecycle: spawn → read stdin → respond → exit on EOF). It belongs in sm8-server (the existing deployment binary), not a new module.

sm8-platform already has the MCP SDK integration (via PR-263); adding a stdio transport there too mirrors the pattern.

## Wiring (per ADR-013 §Lifecycle + r1 fix pattern)

When `--mcp-transport stdio` is set:

1. Main's run() detects the flag (gated on `cli.mcpTransport == "stdio"`).
2. If stdio: build `McpStdioRoute` + the SDK `McpSyncServer`, register a JVM shutdown hook (closeGracefully, 5s drain), then `Thread.currentThread().join()`.
3. **stdout must be 100% JSON-RPC** — every other log path is already `System.err` or the 4 we redirect in this PR.
4. The Restate HTTP ingress binds BEFORE the stdio transport (per ADR-013 r1 fix: install-hook-before-bind + AtomicReference slot pattern; sequencing INSIDE one Runnable).

## Files changed

```
sm8-server/src/main/scala/io/sm8/server/Main.scala      (+15 LOC: 4 println→System.err, --mcp-transport flag + parse)
sm8-platform/src/main/scala/io/sm8/platform/query/
    McpStdioRoute.scala                                (NEW ~100 LOC, mirror of McpHttpRoute)
sm8-platform/src/test/scala/io/sm8/platform/query/
    McpStdioRouteSpec.scala                             (NEW ~6 tests: stdio round-trip with real pipes)
sm8-mcp/src/main/scala/io/sm8/mcp/
    Sm8ToolHandlers.scala                              (REFACTOR: extract tools to a public list;
                                                       McpStdioRoute builds the HttpIngressClient
                                                       and calls Sm8ToolHandlers.build(tools, client))
scripts/smoke-mcp-stdio.sh                             (NEW ~40 LOC: spawn sm8-server with --mcp-transport stdio,
                                                       pipe initialize + tools/list, assert 5 tools + JSON-RPC envelope
                                                       is the ONLY stdout content)
```

Total: ~150 LOC new + ~10 LOC refactor.

## Verification criteria

1. **stdout cleanliness**: with `--mcp-transport stdio`, the only stdout content is JSON-RPC lines from the SDK. Verifiable by `smoke-mcp-stdio.sh` parsing the entire stdout and asserting no `^sm8: ` prefix is present (the startup banner format).
2. **stdio round-trip**: `initialize` + `notifications/initialized` + `tools/list` works via subprocess pipe. Verifiable by smoke.
3. **No regression on PR-263**: `--mcp-http-port` mode (Streamable HTTP) still works. Verifiable by re-running `scripts/smoke-mcp-http.sh`.
4. **No regression on PR-260**: the standalone `sm8-mcp` binary still works. Verifiable by re-running `scripts/smoke-mcp.sh`.
5. **Mutual exclusion**: `--mcp-transport stdio` + `--mcp-http-port N` together → typed CLI error (one of the two — same pattern as PR-263's stdio-vs-http mutual exclusion note in ADR-013 §Decision).
6. **Layer discipline**: `McpStdioRoute` lives in sm8-platform, `Sm8ToolHandlers` is refactored to expose a public `tools: Seq[ToolSpec]` list. sm8-server has no new transport code beyond the flag + a 3-line startup of the route.

## Open questions / risks

- **slf4j startup noise**: the Restate SDK pulls slf4j 2.0.13; if any plugin or the Restate SDK itself logs at INFO level to STDOUT (not stderr) at startup, the JSON-RPC stream would be corrupted. We add a smoke assertion that `^sm8: ` is never seen on stdout (verifies the redirect works) but does NOT verify other libraries. Document as a v1 caveat.
- **stdio stdin EOF**: the SDK's `StdioServerTransportProvider` exits cleanly on stdin EOF. The JVM hook calls `closeGracefully()` to drain pending responses before exit. Tested by smoke (close stdin → assert process exits with code 0 within 2s).
- **In-process + slf4j binding**: when running as a Claude Desktop subprocess, the parent's stdout is fully owned by us. We DO NOT redirect slf4j globally (only the 4 startup banners) — that's a v2 concern if any library logs to stdout.

## Sources

- ADR-013 (PR-259) §Decision: explains why stdio MCP was a separate binary in v1
- ADR-014 (PR-261) §Decision: explains the Streamable HTTP side of the same trade-off
- PR-260 commit message: documents the 3 `println` line audit (now 4 after PR-263 added the MCP HTTP banner)
- MCP SDK `StdioServerTransportProvider` (mcp-core-2.0.1.jar, javap-verified): reads stdin via `BufferedReader.readLine` in `startInboundProcessing`; writes stdout via `PrintWriter` with newline delimiter; lifecycle is `setSessionFactory` (set by `McpServer.sync(transport).build()`), then `closeGracefully().block()` for clean shutdown
- Restate SDK 2.1.1: slf4j default is stderr (verified)
- PR-258 lifecycle pattern: install-hook-before-bind + AtomicReference slot (mirrored in PR-263 r1 fix)

## Why ship both options in one ADR but one PR (same pattern as ADR-014)

The user asked for "in-process --mcp-transport stdio (stdout redirect refactor)" as one task. One ADR covers the design. One implementation PR ships the code, smoke, and tests. The follow-up (if any) — e.g. bridging the 5 PR-260 tools into the HTTP transport so the tool list isn't empty — is a separate PR-265+ candidate (per the r1 de-review "happy-path session flow untested" LOW note).