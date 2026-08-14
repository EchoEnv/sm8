# ADR-006: Step 11 — SM8 MCP server integration

**Status:** Accepted. **Date:** 2026-08-15. **Author:** SM8 agent (per user directive "Yes, proceed with Step 11 + ADR-006").

## Context and Problem Statement

Per `agile-kindling-beacon.md` line 290:

> *"Step 11: Update `semanticdf-mcp`'s `Query.handle` (line 130) and `Query.explain` (line 176) to call `engine.run(request). `Main.scala` line 85 swaps `MCPEngineRegistry` for the SM8 engine. MCP becomes engine-agnostic. Optional `engine` field on `query` request selects an adapter."*

The SM8 reactor already has the typed engine-portable contract (`MCPEngineProvider`, `MCPEngineRegistry`, `MCPQueryRequest`, `PortableQueryResult`, `EngineError`) per PR #32 and the subsequent engine-portable refactor (PRs #44-#54). Per ADR-001 ("compat facade REVERTED per user direction; SM8 and semanticdf NOT integrated"), the legacy `semanticdf-mcp` is **NOT** a consumer of SM8's typed contract.

**Problem statement**: Without an SM8-native MCP server, the typed `MCPEngineProvider` contract has **no consumer**. The engine infrastructure (PRs #32-#54) is unused without a transport-level caller.

## Decision

SM8 ships its **own MCP server** in `sm8-platform` that consumes the typed engine-portable contract. Per the plan line 290's intent ("MCP becomes engine-agnostic"), the server wraps the typed `EngineService.runQueryWithHooks` + `MCPEngineProvider` (the latter selected from `MCPEngineRegistry` based on the optional `engine` field on the wire request).

The server lives in `sm8-platform` (NOT `sm8-core`) because:
- Per RFC `semantic-layer-engine-architecture.md` §3 Core Boundary, the server is **NOT core** — it's a transport-level façade (HTTP / Restate / stdio), not an engine.
- The server captures the typed `MCPEngineRegistry` (typed, Serializable per ADR-004's IR contract) but does NOT contain Spark or any data-source knowledge — those live in the connector layer (`connectors/spark-connector`).
- The server may wrap a **Restate endpoint** (`RestateHttpServer.listen`) OR a **plain HTTP server** (e.g. `dev.restate.sdk.http.vertx`). The choice of transport is per-deployment.

Per the standing memory rule ("don't add features without consumer demand"): the **existing typed pipeline** (per `EngineService.runQueryWithHooks` + `MCPEngineProvider.query`) has **NO production caller**. Step 11 ships the FIRST caller.

## Consequences

**Positive:**
- SM8's engine-portable contract (PRs #32-#54) has a real consumer.
- The legacy `semanticdf-mcp` continues to work via `semanticdf-platform`'s `EngineService.runQueryWithHooks` (its own path, per plan line 250). SM8 does NOT replace it (per ADR-001).
- The new SM8 MCP server is a thin HTTP/Restate façade — no engine logic duplicated.
- The server surfaces the typed IR's consumer demand: when real users need typed aggregates / joins / sorts / calc measures, the server is the consumer that drives future IR additions (ADR-007 candidate).

**Negative:**
- The server must not contain Spark or any data-source-specific types (per §3 Core Boundary).
- The server captures the typed `MCPEngineRegistry` + `ResultCache` + `EngineHookDispatcher`. Per `scala-jvm-safetymindset` mantra #3 (long-lived state): these must be `Serializable` for Restate's journal rehydration. **The existing `EngineServiceSpec.scala:547` ("runQuery: serializable-safe") verifies this contract.** Per ADR-004 (typed-Expr family), all captured types are case-class-derived and Serializable.

**Reversibility:** N/A. The server is a thin façade; adding it does not modify any other artifact.

## RF References

- **`semantic-layer-engine-architecture.md` §3 Core Boundary** (line 25-34): the server is in `sm8-platform` (NOT core); it knows no data source.
- **`semantic-layer-engine-architecture.md` §5 Pipeline** (line 48-52): the server is a **transport-level wrapper** around the 4-stage pipeline (parse → resolve → execute → format). It invokes `engine.run(request)` which drives the pipeline.
- **`semantic-layer-engine-architecture.md` §7 Contracts** (line 235-260): the server consumes `MCPEngineProvider` (per `MCPEngineRegistry`), `MCPQueryRequest` (wire shape), and returns `PortableQueryResult` (typed result).
- **`plugins.md` Rule 4** (line 121-124): the server is **NOT a plugin** — it's a transport-level endpoint. It does NOT register via `Plugin.setup`. Per the same rule, the server does NOT swallow errors — the typed `EngineError` ADT propagates to the wire.

## Plan References

- **`agile-kindling-beacon.md` line 290** ("Step 11 — Update semanticdf-mcp's Query.handle"): the SM8 MCP server implements the same architectural intent (wire-level `Query.handle` / `Query.explain` calling `engine.run(request)`) but **inside SM8** (not by integrating with the legacy). Per ADR-001, SM8 does NOT touch the legacy repo.
- **`agile-kindling-beacon.md` line 247** ("Definition of Done"): the server lands + tests pass + ADRs documented.

## Spark Concerns (per user directive)

Per `scala-spark-batch-bugs-mindset` mantras:
- **Mantra #1 (closure-safety)**: the server captures the typed `MCPEngineRegistry` + `ResultCache` + `EngineHookDispatcher`. ALL are case-class-derived and `Serializable` (verified by `EngineServiceSpec.scala:547`). No transient state.
- **Mantra #2 (data skew)**: N/A — the server is a transport façade; it does NOT execute queries itself.
- **Mantra #3 (schema-drift)**: the server returns the typed `PortableQueryResult` (per §7 contract).
- **Mantra #4 (write correctness)**: N/A — the server does NOT write.
- **Mantra #5 (driver/executor)**: the server runs in the **driver process**. The captured `MCPEngineRegistry` selects an engine (e.g. `SparkEngineProvider`) which compiles + collects in the driver. **No executor-side resources leak through the server.**

Per `scala-perf-testingmindset` mantras:
- The server is **startup-time init** + **per-request dispatch**. The hot path is `engine.runQueryWithHooks` (already perf-optimized per PRs #32-#54). The server adds a thin HTTP/Restate layer; no extra allocation per request beyond the existing typed-IR machinery.

## Skills Applied (per user directive)

- **karphyaguidsmindset "smallest correct change"**: 1 new file in `sm8-platform` + 1 new ADR. The server is a thin façade — no engine logic duplicated.
- **karphyaguidsmindset "name what done looks like"**: `McpServer.start()` binds + listens; `McpServer.stop()` shuts down. `McpServer.handleQuery()` calls `EngineService.runQueryWithHooks(request, model, registry, cache, dispatcher)` and returns the typed `QueryResult`.
- **scala-data-drivenrefactor-mindset**: shape (typed wire shapes) and validity (the handler body) are separated.
- **scala-error-handlingmindset**: typed `EngineError` propagates; never swallowed.
- **scala-impact-analysismindset**: 1 new file in `sm8-platform`; 0 changes to other artifacts.
- **scala-jvm-safetymindset**: HTTP server lifecycle (`bindAndListen` returns port; shutdown via `close()`); captured state is `Serializable`.
- **scala-spark-batch-bugs-mindset**: see "Spark Concerns" above.
- **scala-perf-testingmindset**: startup-time init; per-request dispatch.
- **debug-mantramindset**: reproduce first (verify the existing typed pipeline works end-to-end via the new server).

## Hierarchy of Artifacts Used (per standing memory rule)

1. **RFC first** (§3, §5, §7, §9, §13 + plugins.md Rule 4).
2. **PLAN second** (line 290 — Step 11 is the explicit next major work).
3. **ADR third** (this is ADR-006; complements ADR-001-ADR-005).

## Pre-commit Gates (per user directive)

- **Pre-flight**: memory + disk + codegraph state checked BEFORE commit (will be applied at commit time).
- **LSP + codegraph pre-commit gate**: not applicable for ADR-only; will be applied for the code commit.
- **Post-PR-push monitor rule**: will be applied after each push (per standing rule 2026-08-14).
- **PR description rule**: comprehensive body (RFC matrix + plan alignment + Spark concerns + skills + pre-commit gates) — per standing rule 2026-08-15.

## What's Next

This ADR documents the decision. **The code implementation follows** (PR for the SM8 MCP server skeleton + tests + Spark concerns verification). The SM8 MCP server is **the consumer that surfaces the next IR gap** (typed aggregates / joins / sorts / calc measures) — which would be ADR-007 candidate.

## Provenance

This ADR was authored on 2026-08-15 as part of the agile-kindling-beacon plan execution, after PRs #32-#55 (the engine-portable refactor + reviews). The decision: ship SM8's own MCP server in `sm8-platform` per plan line 290, **NOT integrate with the legacy `semanticdf-mcp`** (per ADR-001).
