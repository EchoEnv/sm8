# Wayfinder map — Control-plane robustness & decoupling (2026-09-05)

**Status:** charted, awaiting Ticket #1.
**Surveyed by:** session-level architecture analysis + codegraph_explore on `sm8-platform.query`.
**Driver ADR:** none new (this is an internal cleanup; no architectural change). Each ticket produces its own ADR.

---

## Destination

Three tickets that, taken together, lift `sm8-platform/query/` from "works in the happy path, fragile at the seams" to "operable + testable + change-friendly":

1. **Ticket #1** — `executeEngine` non-fatal catch-ladder. Today the catch collapses 10+ typed `EngineError` variants into `ProviderInvocationFailed(reason = e.getClass.getSimpleName)`. Operators cannot programmatically distinguish a Spark `AnalysisException` from a `SQLException` from a network blip. Ticket: classify the catch at the typed ADT so the wire contract carries the subtype.
2. **Ticket #2** — generalize typed-error surfacing (ADR-0010-a §6 deferral). Today `EngineService.scala:530` pattern-matches `ctx.meta.get("semanticGraphError")` against ONE hard-coded string key. Any new plugin wanting to surface a typed error must edit the platform's `case Some(...) =>` ladder. Ticket: a `*.error` meta-key namespace convention the platform pattern-matches, plus a plugin-side helper that writes to the convention.
3. **Ticket #3** — split `McpHttpRoute` (and `McpStdioRoute`) into 3 collaborators + factor `Sm8ToolHandlers`. Today `McpHttpRoute.scala:671` owns Vert.x lifecycle + MCP session state + MCP protocol handlers + Sm8ToolHandlers `398` LOC copies the same tool-definition boilerplate. Ticket: separate concerns so testing + bugfix have a small cognitive surface.

## Notes

Skills every ticket must apply (per the standing 9-rule Execution Rules Checklist):

- `scala-error-handling-mindset` (typed `Either` + ADT fold; non-local return banned; PR-176 NonFatal discipline)
- `scala-jvm-safety-mindset` (InterruptException re-set; fatal `Error` must propagate; `NonFatal` only)
- `scala-impact-analysis-mindset` (blast-radius check via `mcp__codegraph__codegraph_explore`)
- `karpathy-guidelines-mindset` (smallest correct change; surface assumptions; verifiable; success criteria)
- `karpathy-app-design-mindset` (frozen core + plugin extension portal; protocols in core; impls in platform)
- `scala-data-driven-refactor-mindset` (sealed-trait dispatch; smart constructors; pure data)
- `scala2-scaladoc-mindset` (skill scripts MUST run on every `.scala` file changed: `check_scaladoc_noise.py` + `check_scaladoc_shape.py`)
- `debug-mantra-mindset` (reproduce → trace → falsify → cross-reference → verify; rule #7 says verify every flagged review item against primary source before fixing)
- `building-restate-services` (any change touching `sm8-platform/.../query/ServiceDefinition` / `HandlerRunner` / wire DTOs)

Standing preferences this workstream honors:
- Per AGENTS.md: never PR to `main` directly (rule #5); user is sole merger (rule #9 STOP discipline).
- Per memory: `sm8-platform.query` is the **control plane** (memory `sm8-architecture-control-plane`).
- Per memory: `main` has branch-protection; direct push is rejected (memory `sm8-main-branch-protection-rule`).
- Per memory: review-clone freshness (memory `jcode-review-clone-freshness`) — verify `/tmp/review-*` exists with the current feature-branch HEAD before spawning workers.
- Per memory: codegraph-first analysis (memory `sm8-architecture-analysis-method`) — used during chart-time investigation; will also be used by each ticket's dual reviewers.

Each ticket's "non-goal" / "explicit deferral" section names what it does NOT do, to keep the scope bounded. The full Execution Rules Checklist (9 rules) applies per ticket.

---

## Decisions so far

- (this map is the first decision: chart the 3 tickets)

---

## Not yet specified

- (none — all 3 tickets scoped at chart-time)

---

## Out of scope

- Anything not in the control plane (`sm8-core`, `sm8-cli`, `sm8-mcp`, plugins, connectors) is out of scope for this map.
- McpStdioRoute and McpHttpRoute share many patterns; Ticket #3 will split BOTH in the same PR to avoid drift between them. If they diverge in shape (one gets session state, the other doesn't), that is the ticket's job to discover, not pre-resolved here.
- Performance / OOM / Spark-closure concerns are not the driver of this map; if any ticket surfaces one, it gets filed as a follow-up.

---

## Tickets (decision tickets, not build slices)

Each ticket is bounded to ~1 session and produces its own ADR + branch + dual-review + PR. None of these tickets overlap in file scope with the others; they can be done in either order, but the recommended order is #1 → #2 → #3 because #2 builds on the wire contract fixed by #1, and #3 is orthogonal (MCP surface only).

### Ticket #1 — `executeEngine` non-fatal catch-ladder

**Question:** how do we classify the existing `NonFatal` catch-all in `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:284-290` against the typed `EngineError` ADT so that operators can programmatically distinguish Spark `AnalysisException`, `SQLException`, `IOException`, and `RuntimeException` instead of seeing `ProviderInvocationFailed(reason = "<class-name>")` for all of them?

**Scope:** `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala` only.

**Acceptance criteria:**
1. New catch arms classify these exception types into typed `EngineError` variants (preserving subtype info):
   - `org.apache.spark.sql.AnalysisException` → `EngineError.UnsupportedCapability`
   - `org.apache.spark.sql.catalyst.analysis.NoSuchTableException` / similar → `EngineError.EngineUnavailable` (with `available` list)
   - `org.apache.spark.sql.execution.QueryExecutionException` + `Decimal out of range` message → `EngineError.DecimalOverflow`
   - `java.io.IOException` → `EngineError.ConnectionFailed`
   - `java.lang.NullPointerException` from a buggy plugin → propagate as `EngineError.HookFailed` with the plugin name (or NPE → `EngineError.ProviderInvocationFailed` if not plugin-bound)
   - `java.lang.AssertionError` (an `Error`, not `NonFatal`) → propagate unchanged (do NOT catch)
2. Existing tests in `sm8-platform/src/test/scala/io/sm8/platform/query/EngineServiceSpec.scala` pass unchanged.
3. New tests:
   - One per exception class above, asserting the typed `Left(err)` value matches the expected `EngineError` subtype.
   - One test asserting a Spark `AnalysisException` round-trips with `capability` field populated.
   - One test asserting `AssertionError` (a fatal `Error`) is NOT swallowed (it propagates out of `executeEngine` to the caller).
4. ADR-0019 (Proposed): rationale for the catch-ladder structure + which subtypes we mapped + which we intentionally left under `ProviderInvocationFailed` and why.
5. Scaladoc noise + shape scripts both clean on every changed `.scala` file.
6. Layer discipline preserved: `sm8-core` unchanged; `sm8-platform` only adds new typed-error-construction sites inside `executeEngine`.
7. Dual review (architect + data-eng) via codegraph-enabled reviewers; same reviewers for final gate.
8. PR opened + STOP.

**Non-goals:** do NOT change the `Either[EngineError, _]` shape returned by `executeEngine`. Do NOT change the wire contract on `ErrorDetail`. Do NOT touch the `provider.query(...)` happy-path return channel (it already returns typed errors correctly).

**Effort:** ~1 session.

---

### Ticket #2 — generalize typed-error surfacing (ADR-0010-a §6 follow-up)

**Question:** how do we replace the hard-coded `ctx.meta.get("semanticGraphError")` pattern match at `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:530` with a typed convention that ANY plugin can write a typed error to without editing the platform?

**Scope:**
- `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala` (the consumer)
- `sm8-platform/src/main/scala/io/sm8/platform/query/HookFiringAuditPlugin`-side: needs a small helper if useful, but otherwise no change to PR-315 plugin
- `sm8-core/src/main/scala/io/sm8/sdk/Hooks.scala` if we need a new SDK convention (LAST resort — see non-goal)

**Acceptance criteria:**
1. New typed convention: any `EngineError` written to a meta key matching the pattern `<plugin-name>:error` is automatically surfaced by the platform (replacing today's hard-coded `"semanticGraphError"` lookup). A single `ctx.meta.collect { case (k, v: EngineError) if k.endsWith(":error") => v }.headOption` replaces the string-keyed match.
2. The semantic-graph plugin's existing behavior is unchanged (its key happens to fit the convention once renamed to `io.sm8.plugins.semanticgraph:error`).
3. Hook-firing-audit's `HookNotFired` anomaly type continues to surface.
4. New `EngineErrorContractHook`-style unit test: simulating a plugin that writes a typed `UnsupportedCapability` to `my-plugin:error` is surfaced correctly.
5. ADR-0020 (Proposed): the meta-key namespace convention + the platform's contract to scan for `<x>:error` keys + the rollback story (single hard-coded key as fallback).
6. Scaladoc scripts clean.
7. Layer discipline preserved: the SDK (`sm8-core`) does NOT change. The convention lives in `sm8-platform.query` (the control plane that consumes it).
8. Dual review + final gate.
9. PR opened + STOP.

**Non-goals:** do NOT add an SDK-level error-meta protocol (would touch frozen types). Do NOT rename the existing `"semanticGraphError"` key out from under the hook-firing-audit PR-315 unless the new convention subsumes it gracefully (verify with `HookFiringAuditOrchestrationSpec`).

**Effort:** ~1 session.

---

### Ticket #3 — split `McpHttpRoute` + `McpStdioRoute` + factor `Sm8ToolHandlers`

**Question:** how do we split `sm8-platform/src/main/scala/io/sm8/platform/query/McpHttpRoute.scala:671` (Vert.x lifecycle + MCP session state + MCP protocol handlers in one class) into collaborators with single responsibilities, and reduce `sm8-platform/src/main/scala/io/sm8/platform/mcp/Sm8ToolHandlers.scala:398` (10+ nearly-identical tool-definition methods) to a registry pattern?

**Scope:**
- `sm8-platform/src/main/scala/io/sm8/platform/query/McpHttpRoute.scala` (split into 3)
- `sm8-platform/src/main/scala/io/sm8/platform/mcp/McpStdioRoute.scala` (split into 3, mirror)
- `sm8-platform/src/main/scala/io/sm8/platform/mcp/Sm8ToolHandlers.scala` (factor registry)
- New files: `McpHttpServer.scala`, `McpSessionRegistry.scala`, `McpMessageRouter.scala` (and stdio mirrors)

**Acceptance criteria:**
1. After the split:
   - `McpHttpServer` owns ONLY Vert.x lifecycle (~150-200 LOC).
   - `McpSessionRegistry` owns ONLY the session map + transport factory (~100-150 LOC).
   - `McpMessageRouter` owns ONLY the MCP protocol handlers (initialize, request, notification, response, SSE) (~250-300 LOC).
   - `McpHttpRoute.scala` is now ≤ 50 LOC (just composition + the `start`/`stop` factory).
2. `McpStdioRoute` mirrors the split (3 collaborators + thin composition).
3. `Sm8ToolHandlers` switches to a `ToolRegistry` helper: registering a new tool becomes a one-line `registry.register(name, description, inputSchema, handler)` call. The 10 existing tool methods are preserved (the registry is the new implementation; the existing call sites continue to work).
4. Existing tests in `sm8-platform/src/test/scala/io/sm8/platform/query/McpHttpRouteSpec.scala` pass unchanged (or with minimal signature updates).
5. New contract specs:
   - `McpHttpServerContractSpec` — Vert.x lifecycle contract.
   - `McpSessionRegistryContractSpec` — session create/get/delete contract.
   - `McpMessageRouterContractSpec` — protocol handler contract.
6. Scaladoc scripts clean.
7. Layer discipline preserved: zero `dev.restate` imports added to the new collaborators (they're MCP-only; Restate lives in `sm8-platform.query.{QueryService, ModelService, ...}` which still uses the McpHttpRoute as an HTTP transport).
8. Dual review + final gate.
9. PR opened + STOP.

**Non-goals:** do NOT add new MCP features (Streamable HTTP / SSE / etc. is already in McpHttpRoute.scala:165 `setSessionFactory`). Do NOT change the wire shape of any MCP message. Do NOT touch the `HookFiringAuditPlugin` (no MCP interaction today).

**Effort:** ~1 session.

---

## Sequencing recommendation

`#1 → #2 → #3`. Ticket #1 closes a real bug surface (operators can't classify errors today). Ticket #2 builds on the wire contract #1 fixes (so the "typed error" promise is consistent across engine call + hook short-circuit). Ticket #3 is orthogonal and could be done at any time, but doing it last lets the reviewer focus on a structural refactor without noise from the typed-error changes.

Total: 3 sessions. All three land via PR + STOP per the standing rule.

---

## Validation

- Each ticket produces a dual-reviewer-verified PR with green reactor.
- Each ticket's PR body cites the wayfinder map (`docs/wayfinder/2026-09-05-control-plane-robustness.md`) so future readers can trace the design rationale.
- After all 3 PRs merge, this map gets a "**Status:** Closed" header and is moved to `docs/wayfinder/_closed/`.

Local-only `.omp/WATCHDOG.yml` (gitignored) tunes the advisor roster for this project.
