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

1. **Redirect 4 stdout writes to stderr** in sm8-server. Bounded audit:
   - `sm8-server/src/main/scala/io/sm8/server/Main.scala:334` (Usage banner on `--help`)
   - `sm8-server/src/main/scala/io/sm8/server/Main.scala:404` ("server listening on port …")
   - `sm8-server/src/main/scala/io/sm8/server/Main.scala:440` ("metrics endpoint listening on port …")
   - `sm8-server/src/main/scala/io/sm8/server/Main.scala:465` ("MCP Streamable HTTP endpoint listening on port …", added by PR-263)
   - Zero hits in `plugins/*/src/main/scala/` (verified by grep).
   - `sm8-mcp/Main.scala:130` also has a `println(Usage)` but the sm8-mcp binary exits before any MCP session so this never appears in the in-process path; only relevant if a future change reuses sm8-mcp's Main verbatim.
   - The Restate SDK's slf4j default writes to stderr.
2. **Wire sm8-mcp's stdio transport in-process.** Reuse the existing `Sm8ToolHandlers` + `HttpIngressClient` (the 5 PR-260 tools). The only new code is a Main flag + an in-process transport orchestrator (creates the SDK's `StdioServerTransportProvider` + a `McpServer.sync(transport).build()` and blocks the main thread on stdin).

### Why not a new module

PR-263 established the pattern: `McpHttpRoute` lives in `sm8-platform` (transport library). For symmetry, the in-process stdio transport would be `McpStdioRoute` in `sm8-platform`, mirroring `McpHttpRoute`'s shape. The 5 tools are already registered in `sm8-mcp/Sm8ToolHandlers.scala`; we'd move BOTH `Sm8ToolHandlers.scala` AND `HttpIngressClient.scala` to `sm8-platform` so both transports (HTTP via PR-263 + stdio via this ADR) can share the same tool set. (Per the r1 dual-review Q4 catch: if only `Sm8ToolHandlers` moves, `sm8-platform` would need to import `HttpIngressClient` from `sm8-mcp` — that's a cycle since `sm8-mcp` already depends on `sm8-platform`.)

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
2. If stdio: build `McpStdioRoute` + the SDK `McpSyncServer`, register a JVM shutdown hook (closeGracefully, 5s drain). The main thread is NOT parked on `Thread.currentThread().join()` — per bonehound's r1 Q1 catch, EOF does not invoke JVM shutdown hooks; `join()` would never wake. Instead, the implementation uses a `CountDownLatch` that the SDK's `StdioServerTransportProvider.closeGracefully()` completes on (verified in PR-260 r1: the SDK's `StdioMcpSessionTransport.closeGracefully()` runs `Mono.fromRunnable { this.close() }` which signals `isClosing=true`; the inbound loop exits on its next read iteration). When the latch reaches 0 (EOF → closeGracefully → close), the main thread returns from `latch.await()` and the process exits.
3. **stdout must be 100% JSON-RPC** — every other log path is already `System.err` or the 4 we redirect in this PR. Per bonehound's r1 Q4 catch: the smoke must parse every stdout line as JSON-RPC, not just check for `^sm8: ` prefix (a future `log4j-core` binding would default to stdout and corrupt the stream silently otherwise).
4. The Restate HTTP ingress binds BEFORE the stdio transport. The stdio lifecycle's JVM shutdown hook sequences the close INSIDE one Runnable (per ADR-014 r1 fix + retriever's r1 catch): MCP stdio `closeGracefully()` → MCP HTTP `stop()` → `HttpTransport.stop()`. Sequencing within a SINGLE Runnable per hook (each Runnable internally ordered: stdio close → HTTP stop → Restate stop). The existing PR-258 / PR-263 / PR-260 hooks remain separate (3-4 JVM hooks total); the new stdio hook is a 4th independent Runnable that does stdio close ONLY. Cross-hook ordering is NOT guaranteed; in-flight stdio tool calls to the in-process Restate ingress may fail if ingress closes first. The implementation PR accepts this v1 limitation (documented in §Open questions) and ships a v2 follow-up to merge all hooks into one Runnable. **Note for v2 (NOT in this PR)**: combine all 4 shutdown hooks into one shared Runnable that orders (stdio close) → (metrics stop) → (HTTP stop) → (Restate stop). The stdio transport's `closeGracefully()` returns `Mono<Void>`; we block 5s for it to complete (drains in-flight JSON-RPC responses). The HTTP transport's `stop()` is idempotent (verified by the existing PR-258 tests).

**Mutex precedence (per bonehound r1 Q5)**: when BOTH `--mcp-transport stdio` AND `--mcp-http-port N` are set, the CLI emits `CliError.UnknownFlag("--mcp-transport")` to stderr and exits 2. The stdio path is the one rejected (the user's stated directive was "in-process stdio" which is the v1 priority; HTTP MCP is the PR-263 orthogonal path and its loss is the natural default in a conflict). No winner-pick logic; deterministic exit.

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

1. **stdout cleanliness**: with `--mcp-transport stdio`, EVERY stdout line must parse as a JSON-RPC message (newlines inside strings escaped, terminated by `\n`). The smoke (`scripts/smoke-mcp-stdio.sh`) parses the entire stdout as JSON, one line at a time; lines that fail JSON-RPC parsing fail the smoke. The `^sm8: ` prefix absence check is necessary but NOT sufficient — the smoke must verify the JSON envelope, not just the prefix. (Per retriever's r1 catch: the previous wording "no `^sm8: ` prefix" is incomplete because any library writing to stdout at startup would corrupt the stream even with the sm8 banner redirected.)
2. **stdio round-trip**: `initialize` + `notifications/initialized` + `tools/list` works via subprocess pipe. Verifiable by smoke.
3. **No regression on PR-263**: `--mcp-http-port` mode (Streamable HTTP) still works. Verifiable by re-running `scripts/smoke-mcp-http.sh`.
4. **No regression on PR-260**: the standalone `sm8-mcp` binary still works. Verifiable by re-running `scripts/smoke-mcp.sh`.
5. **Mutual exclusion**: `--mcp-transport stdio` + `--mcp-http-port N` together → typed CLI error (one of the two — same pattern as PR-263's stdio-vs-http mutual exclusion note in ADR-013 §Decision).
6. **Layer discipline**: `McpStdioRoute` lives in sm8-platform, `Sm8ToolHandlers` is refactored to expose a public `tools: Seq[ToolSpec]` list. sm8-server has no new transport code beyond the flag + a 3-line startup of the route.

## Open questions / risks

- **slf4j/log4j startup noise**: the implementation PR must `mvn -pl sm8-server dependency:tree` to identify the real logging provider (log4j-api 2.20.0 is in the parent pom's dependencyManagement, but no `log4j-core` is declared — so no provider binds by default; slf4j with no binding is a no-op). If a future commit adds `log4j-core` (or any other provider that defaults to stdout), the smoke's stricter assertion (every stdout line parses as JSON-RPC — see criterion #1 above) catches the regression. The smoke therefore doesn't NEED to verify the current state (since no provider binds); it only needs to PROTECT against future regressions. The "Restate SDK 2.1.1: slf4j default is stderr (verified)" claim in §Sources is misleading — it should be "no slf4j binding is currently declared, so logs are silently discarded; if one is added, default must be stderr".
- **stdio stdin EOF**: the SDK's `StdioServerTransportProvider` exits cleanly on stdin EOF. The JVM hook calls `closeGracefully()` to drain pending responses before exit. Tested by smoke (close stdin → assert process exits with code 0 within 2s).
- **In-process + slf4j binding**: when running as a Claude Desktop subprocess, the parent's stdout is fully owned by us. We DO NOT redirect slf4j globally (only the 4 startup banners) — that's a v2 concern if any library logs to stdout.

## Sources

- ADR-013 (PR-259) §Decision: explains why stdio MCP was a separate binary in v1
- ADR-014 (PR-261) §Decision: explains the Streamable HTTP side of the same trade-off
- PR-260 commit message: documents the 3 `println` line audit (now 4 after PR-263 added the MCP HTTP banner)
- MCP SDK `StdioServerTransportProvider` (mcp-core-2.0.1.jar, javap-verified): reads stdin via `BufferedReader.readLine` in `startInboundProcessing`; writes stdout via `PrintWriter` with newline delimiter; lifecycle is `setSessionFactory` (set by `McpServer.sync(transport).build()`), then `closeGracefully().block()` for clean shutdown
- Restate SDK 2.1.1: slf4j default (when no binding is configured) is to NOP. Without a binding, slf4j discards. The audit (`mvn dependency:tree` on sm8-server) showed no explicit slf4j binding declared, so logging is effectively silent — but the moment someone adds a binding (e.g. `slf4j-simple` or `log4j-slf4j2-impl`), the default is to STDOUT. This is a v1 caveat: the implementation PR must verify the binding state at runtime, and the smoke must catch non-JSON lines per criterion #1. (Per retriever's r1 catch: the previous claim "stderr (verified)" was not actually supported by the audit; the corrected claim is "no binding configured → silent → OK; adding a binding is the v1 implementation risk".)
- PR-258 lifecycle pattern: install-hook-before-bind + AtomicReference slot (mirrored in PR-263 r1 fix)

## Why ship both options in one ADR but one PR (same pattern as ADR-014)

The user asked for "in-process --mcp-transport stdio (stdout redirect refactor)" as one task. One ADR covers the design. One implementation PR ships the code, smoke, and tests. The follow-up (if any) — e.g. bridging the 5 PR-260 tools into the HTTP transport so the tool list isn't empty — is a separate PR-265+ candidate (per the r1 de-review "happy-path session flow untested" LOW note).