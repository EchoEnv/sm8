# ADR-012-c: ConfigService — Restate-handler surface for runtime config

> **Status:** Accepted (negative decision — hold; see Revisions). **Date:** 2026-09-01. **Author:** SM8 agent (per user directive "can we draft these as new ADR-012 series and pass to dual reviewers to approve first").

## Revision history

| Version | Date | Author | Change |
|---|---|---|
| 1 | 2026-09-01 | SM8 agent | Initial ADR. Proposed status, negative decision. |
| 2 | 2026-09-01 | SM8 agent (PR-253) | **Revisit-gate-1 verdict**: scanned the 5 triggers below against the period from initial ADR through PR-252. **None fired.** Status moved from Proposed → Accepted (hold). Next revisit-gate scheduled by the **first of**: (a) any new `docs/adr/*.md` file landing (the existing CI is just docs — no auto-gate, this is a manual review trigger when the reviewer adds a "ConfigService" mention); (b) the pre-release-tag-cut PR (likely the v0.1.0 cut per ADR-008-P §Cross-P0-B); (c) one calendar year from this ADR (2027-09-01) as a hard backstop. PR-number lotto is NOT a reliable cadence anchor; the previous draft's PR-272 reference was an arbitrary threshold and has been removed. |

## Context and Problem Statement

Today, **sm8 has no mutable runtime config surface**. Configuration lives in:

- CLI flags (`--model`, `--port`, `--engine`, `--connector-url`) — set at boot
- Environment variables — set at process start
- YAML manifests on the host filesystem — read at boot, immutable per process lifetime

If an operator needs to change `cache.maxEntries` from 10k to 50k, they restart the server. If they need to flip `audit.sinkEnabled = true` for a hot-fix, they can't.

The skill `karpathy-app-design` recommends "smart defaults + override at boot." The locked ADR conventions (ADR-008-P) recommend typed error surfacing (no silent retries). The question is **whether sm8 needs dynamic config at all**, or whether the existing CLI + env-var surface is sufficient.

## Decision

**HOLD.** Do not add a `ConfigService` in this ADR cycle. The current CLI + env-var surface covers the documented operator workflows, and dynamic config adds 4 questions that don't have engine-shaped answers:

| # | | Question | Why sm8 isn't shaped to answer it |
|---|---|---|---|
| 1 | | **Auth** — who can change `cache.maxEntries`? Every Restate caller? An admin role? | sm8 has no auth surface today; introducing one is a separate ADR (likely with a separate identity provider). |
| 2 | | **Persistence** — where do config changes live across process restarts? | sm8 is "pure engine"; adding persistent state (file, RocksDB, etcd) crosses the layer boundary into "infrastructure service." |
| 3 | | **Conflict resolution** — two operators, two conflicting writes | Requires optimistic concurrency / last-writer-wins / merge semantics. Engine-portable path doesn't own this problem. |
| 4 | | **Audit** — every change must be logged for compliance | Requires an audit subsystem (a sibling of the proposed AuditService, not its parent). |

Adding a ConfigService to sm8 would either:

- Be **toy config** (in-memory only, no persistence, no auth) — adds API surface without solving real problems
- Be a **full config service** — out of scope for sm8 (above 4 questions are "configuration-management service" shape, not "engine" shape)

### Alternatives Considered

| Alternative | Reason rejected |
|---|---|
| **Toy config** (in-memory map, `get(key)` / `set(key, value)`) | Adds wire surface but no value beyond the existing CLI; lulls operators into thinking it's persistent when it's not. |
| **File-backed config** (`config.yaml` on disk, watched by sm8) | Reads config from disk; changes via `kubectl edit configmap`. Solves persistence but ignores auth + audit. |
| **env-var reload via SIGHUP** | Same as CLI — already supported; no wire surface needed. |
| **REST API gateway in front of sm8** | That's the right shape for a config service, but it's a separate service, not sm8. |
| **Defer to the per-handler config in `QueryRequest`** (e.g. `cacheTtlMillis: Long` per call) | Pushes config to callers; scales horizontally per request rather than per deployment. ADR-008-j's model extensions are the precedent — fields-on-`QueryRequest` are already the pattern for per-call knobs. |

### Triggers for revisiting (future ADR-cycle)

This ADR-cycle has to be revisited if **any** of the following become real:

1. **A real operator asks for runtime-mutable config** in a support thread (not a theoretical "I might want it someday")
2. **A REST API is added** that needs per-tenant configuration (the multi-tenant problem implies an auth model — revisit ADR together with that auth model)
3. **A feature requires A/B-testing model versions in production** (e.g. canary deploy of new query plans)
4. **Schema evolution** — a config field needs to change type or semantics across versions (forward-compatibility requires versioning the config DTO, which is its own design)
5. **A real-time config push** is needed (operator changes a value, sm8 picks it up within seconds without restart) — that's the difference between "config service" and "config file"; revisit only if real-time matters

Until one of these fires, the CLI + env-var + YAML-on-disk surface is sufficient.

## Consequences

### Positive

- Avoids building a feature that solves no real problem
- Documented decision means a future ADR-cycle that wants dynamic config must answer the 4 questions above first
- Prevents scope creep: MetricsService (ADR-012-b) and ModelService (ADR-012-a) ship without a ConfigService baggage

### Negative

- If a real user later asks "I want to flip `cache.maxEntries` at runtime," this ADR cycle has to be revisited
- Some callers may find the CLI-restart workflow annoying
- No way to A/B-test model versions without a redeploy

## References

- `karpathy-app-design` skill: smart defaults + boot override convention
- ADR-008-P: post-review follow-up plan; precedent for typed error surfacing
- ADR-008-j: precedent for fields-on-`QueryRequest` as the per-call knob pattern
- ADR-009-f: paired persist lifecycle — precedent that sm8 handles lifecycle carefully; ConfigService would inherit the same discipline, but the question is persistence itself
- `sm8-platform/.../QueryService.scala:248`: TerminalException pattern (not directly applicable to ConfigService, but the auth-failure shape would mirror it)
- `docs/rfcs/2026-08-12_v1_architecture-spec/adapters.md`: layer discipline — ConfigService as proposed here would violate the engine-portable layer