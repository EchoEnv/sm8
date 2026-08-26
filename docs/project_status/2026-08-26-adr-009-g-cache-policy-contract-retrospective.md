# SM8 — ADR-009-g CachePolicy Contract Closure Retrospective

**Date:** 2026-08-26
**Branch:** `main` @ `8d5e4cb` (PR-182 merged)
**Scope:** the cache-feature contract closure — both `CacheReadPreHook` and `CacheWritePostHook` honor the folded `CachePolicy` from `model.defaultPolicies.cache`; dual `CachePolicy` ADT collapsed to one; `EngineContext.cachePolicy` repurposed from dead field to fold target.
**ADR:** [ADR-009-g](docs/adr/0009-g-cache-policy-contract.md) — promoted to **Implemented**
**PR:** [PR-182](https://github.com/EchoEnv/sm8/pull/182) (4 atomic implementation commits + 1 P3 prose fold, squash-merged)
**Skill alignment:** `karpathy-app-design`, `karpathy-guidelines`, `scala-bug-hunting`, `scala-impact-analysis`, `scala-error-handling`, `scala-jvm-safety`, `scala-data-driven-refactor`, `scala2-scaladoc`, `debug-mantra`, `karpathy-guidelines` (single-source-of-truth discipline)

---

## 1. TL;DR

The cache-feature contract is now **fully wired** end-to-end. `model.cache` is honored: a `NoCache` model pays zero cache cost (no lookup, no counter, no write-through); `ReadThrough(name)` reads only (counter on lookup; HIT short-circuits engine); `WriteThrough(name)` reads + writes (counter on both paths). The dual `CachePolicy` ADT (engine-side vs model-side) is collapsed to one source of truth at `io.sm8.core.model.CachePolicy`. `EngineContext.cachePolicy` is repurposed from a dead field to a fold target (a deliberate inversion of the ADR-009-f Fix 4 deletion — justified by the new consumer).

The ADR went through **4 numbered rounds** (v0.1 → v1.0 → v1.1 → v3.0) + a closeout v3.1 row marking Implemented. v0.1 was BLOCKED by architect review with 5 load-bearing findings; v1.0 folded all 5 (correct fold site; both-hook gate; ADT undercount correction; explicit per-case policy matrix; dispatcher API surface documented). The v3.0 final docs tightening + the closeout P3 folds (indentation drift, double-blank-line) are non-blocking.

**Final state:** full reactor green (15 modules, **634/634 core + 241/241 spark-connector + 103/103 platform + 6/6 cli + 33/33 server**). Dual senior review APPROVED (architect 0.94, data-eng 0.97). Pre-merge author-gated 3-reviewer code review (cache-plugin 0.95, core 0.95, platform 0.93) APPROVED with 2 P3 whitespace folds. No follow-ups from this ADR — `Multi-region cache routing` deferred per §Out-of-scope as a separate ADR candidate.

---

## 2. What landed

### 2.1 The paired-gate invariant in `applyPostCompilePipeline` (and its cousin `runQueryWithHooks`)

The runtime gate is a **fold** from `model.defaultPolicies.cache` into `initialCtx.meta("sm8.cache.policy")`, performed **once** at the start of `EngineService.runQueryWithHooks` *before* `dispatcher.run` fires. Both `CacheReadPreHook` (priority 50, PreExecute) and `CacheWritePostHook` (priority 60, PostExecute) consult the same `ctx.meta` value and gate per the per-case policy matrix:

| `model.cache` | PreExecute (`CacheReadPreHook`) | PostExecute (`CacheWritePostHook`) |
|---|---|---|
| `NoCache` | no-op (counter NOT incremented) | no-op (counter NOT incremented) |
| `ReadThrough(name)` | lookup; HIT short-circuits via `stop = true`; MISS continues to executor; counter incremented | no-op (counter NOT incremented) |
| `WriteThrough(name)` | lookup; HIT short-circuits via `stop = true`; MISS continues to executor; counter incremented | write-through on every successful executor result; counter incremented |

The fold follows the **ADR-009-d DecisionHints fold pattern** — engine-portable SDK channel, no model-attached type leakage into hooks. The fold lives in `initialCtx` construction (NOT in `engineExecutor`) because `EngineHookDispatcher.run` (lines 100-123) fires `firePre` BEFORE `execute`; a fold in `engineExecutor` would be unreachable from any PreExecute hook (this was the v0.1 BLOCK finding).

### 2.2 Single-source `CachePolicy` ADT

The engine-side `io.sm8.core.engine.CachePolicy` (with `ReadOnly` + case-object `ReadThrough`/`WriteThrough` lacking `name`) is **deleted entirely**. The model-side `io.sm8.core.model.CachePolicy` (3 cases: `NoCache`, `ReadThrough(name)`, `WriteThrough(name)`) is the single source. All 4 spec test sites in `EngineContextSpec.scala` (lines 14-24, 16-20, 100, 109-115) + the spark-connector rejection message at `PortableQueryCompiler.scala:534` are migrated. `EngineContext.cachePolicy` (the field) is retyped to `io.sm8.core.model.CachePolicy` and defaults to `NoCache`.

The `ReadOnly` case was the dead-legacy zero-reader; `ReadThrough` / `WriteThrough` on the engine-side were a strict subset that would silently lose the `name` field if any future code wired them. Both shapes removed; one shape remains.

### 2.3 `EngineContext.cachePolicy` repurposed

The field was dead (zero production readers; only 4 spec test sites referenced it before migration). After ADR-009-f Fix 4 deleted `EngineContext.materializePolicy` (no consumer), ADR-009-g **repurposes** this field as a fold target — a deliberate inversion justified by the new consumer (the cache hook, after Fix 1 + Fix 5). The field itself is still unused at runtime (the fold goes through `ctx.meta`, not the field), but it documents the folded value for the spec seam at `EngineContext.defaultContext has sensible defaults` (line 100). Future ADRs may use the field as a typed contract seam; for now it's the spec seam.

### 2.4 The non-empty `EngineServiceRunQueryWithHooksSpec` integration tests

4 new integration tests construct `ReadThrough("region-a")` + `WriteThrough("region-a")` models, run the full dispatcher, and assert the fold reaches the hook via counter increments per the per-case matrix:
- `ReadThrough("region-a")` MISS — `readFires++`, `misses++`, executor runs, `writeFires == 0`
- `WriteThrough("region-a")` MISS — `readFires++`, `misses++`, executor runs, `writeFires == 1`
- `WriteThrough("region-a")` HIT — pre-hook short-circuits, executor skipped, `writeFires` stays at the prime value (post-hook has `runsOnStop = false` per ADR-008-P T1-D2)
- `NoCache` — all counters at zero, the gate ate the cache lookup

These are the load-bearing acceptance tests for acceptance #4 (the fold reaching the hook); existing `NoCache`-only tests in the spec are backwards-compat assertions.

### 2.5 The per-case counter discipline (Fix 6)

`readFires` is incremented ONLY for `ReadThrough`/`WriteThrough` (not `NoCache`); `writeFires` is incremented ONLY for `WriteThrough` (not `NoCache`, not `ReadThrough`); `hits` / `misses` increment ONLY for the read-hook's read branch. The counter `incrementAndGet()` calls were MOVED inside the active branches (from the previous unconditional `run` top) so they cannot fire for `NoCache`. This is the user-visible "cache metrics for the model that declared caching" fix — pre-fix, every query incremented `misses` regardless of `cache` policy, which is exactly the silent-no-op class the ADR closes.

### 2.6 Local-only `.omp/WATCHDOG.yml` (post-merge, not in PR-182)

After the PR-182 merge, the user added a project-scoped `.omp/WATCHDOG.yml` (gitignored) tuning the omp advisor roster for this project: 2 disjoint watchers (`Code` for code-level defects with `best-reasoning:auto`; `Verify` for claim verification + agent behavior with `best-coding`). The roster explicitly avoids version-specific ADR IDs (cites by topic — "the persist-lifecycle ADR", "the hook-fold ADR") so future renumbering doesn't break the references. The `Verify` advisor's `model: minimax-code/MiniMax-M2.7:auto` may silently fall back to user-level `modelRoles.advisor` if M2.7 isn't in the local catalog; the instructions explicitly tell the advisor to report its resolved model so the fallback is loud, not silent.

---

## 3. The review chain — 4 numbered revisions + closeout

| Round | Reviewer | Verdict | Folded |
|---|---|---|---|
| v0.1 (Proposed) | — | Initial draft; 4 findings (unconditional-fire, dual `CachePolicy` ADT, dead `EngineContext.cachePolicy` field, missing fold); 4 options considered (Option A recommended; Options B/C/D rejected) | — |
| v0.1 → v1.0 | architect (best-reasoning) | **BLOCKED** 0.45 confidence, 5 load-bearing findings | All 5: (a) fold-site correction to `initialCtx.meta` BEFORE `dispatcher.run` (the v0.1 site in `engineExecutor` was unreachable from `CacheReadPreHook`); (b) post-hook gate extension (Fix 5); (c) ADT undercount correction (v0.1 claimed 0 readers, actually 4 spec sites); (d) explicit per-case policy matrix (Fix 6); (e) dispatcher API surface documented |
| v1.0 (Proposed) | — | All 5 findings folded | — |
| v1.0 → v1.1 | data-eng (best-coding) | workflow-stage mismatch (ADR is design-only; implementation deferred to subagent per v0.4-wave pattern) | 2 substantive cache-correctness findings that survive into implementation: (a) case-class `name` shape change at `EngineContextSpec.scala:109-115` — engine-side `CachePolicy.ReadThrough` is a case object, model-side is a case class with required `name: String`; (b) missing integration spec for acceptance #4 — `EngineServiceRunQueryWithHooksSpec` constructs `NoCache` models only |
| v1.1 (Proposed) | — | Both findings folded | — |
| v1.1 → v3.0 | self (pre-implementation) | non-review | v3.0 docs tightening: 2 P3 prose nits folded in `9eabd41` (stale `WriteThrough` inline comment, stale line-number refs) |
| Implementation (4 commits) | subagent `sweADR009gImpl` | success | `7f3ffcf` core ADT collapse; `8fb0639` core test migration + integration spec; `b087dae` platform fold; `9eabd41` plugin both-hook gate |
| v3.0 → accepted | architect (best-reasoning) | **APPROVED** 0.94 | 3 P3 prose nits (spec site migration glosses case-class shape change; cache-plugin import not explicitly named; `PortableQueryCompiler` import migration claim slightly imprecise) |
| v3.0 → accepted | data-eng (best-coding) | **APPROVED** 0.97 | 2 P3 prose/gap nits (stale inline comment; pre-existing cache-region routing gap documented in §Out-of-scope) |
| Pre-merge author-gated | 3 reviewers (cache-plugin / core / platform) | all **APPROVED** | cache-plugin 0.95; core 0.95; platform 0.93 with 2 P3 whitespace nits folded in `8763d0a` (indentation drift + double-blank-line) |
| v3.1 (closeout) | self | Implemented via PR-182 | this retrospective |

---

## 4. Key decisions (rationale + ADR anchors)

1. **Pair in `engineExecutor` was wrong; pair in `initialCtx.meta` BEFORE `dispatcher.run`** — `EngineHookDispatcher.run` fires `firePre` (lines 106-108) before `execute`. A fold in `engineExecutor` is unreachable from any PreExecute hook. The v0.1 audit missed this; architect caught it. The fold is one line in `initialCtx` construction. **v1.0 P1 #1 fold site correction.**

2. **Both hooks (read + write) gate on the same `ctx.meta`** — `CacheWritePostHook.run` was unconditional (incremented `writeFires` + wrote `cache.putJournaled` on every successful engine result). The pre-fix `NoCache` model: read hook skips → engine runs → post hook fires → cache populated. Violates acceptance #1. The post-hook must consult the same fold as the read hook; the per-case matrix distinguishes `WriteThrough` (write) from `ReadThrough` (no write). **v1.0 P1 #2 + Fix 5.**

3. **The dual `CachePolicy` ADT is a drift hazard** — `io.sm8.core.engine.CachePolicy` (4 cases, no `name` on `ReadThrough`/`WriteThrough`) coexists with `io.sm8.core.model.CachePolicy` (3 cases, `name: String`). A contributor who picks the wrong import gets a silent no-op. Same drift hazard as ADR-009-f Gap 5 (dual `MaterializePolicy`). **Collapse to the model-side ADT; delete the engine-side entirely.**

4. **`EngineContext.cachePolicy` repurposed, not deleted (inversion of ADR-009-f Fix 4)** — `materializePolicy` had no consumer after ADR-009-f; `cachePolicy` HAS a consumer (the cache hook, after Fix 1 + Fix 5). YAGNI for `materializePolicy`, real consumer for `cachePolicy`. The field is still unread at runtime (the fold goes through `ctx.meta`, not the field) but it documents the folded value for the spec seam.

5. **`ReadOnly` case deleted; `ReadThrough`/`WriteThrough` case-class invocations carry `name`** — `ReadOnly` was a future-design residue with no current production consumer. The engine-side `ReadThrough` / `WriteThrough` were case objects (no `name`); the model-side ones are case classes. Migrating spec sites required adding `("default")` literals — a migration-time signal that the case-class shape is the canonical one.

6. **The fold is one line in `initialCtx`** — `meta = Map("sm8.cache.policy" -> model.defaultPolicies.cache)`. The existing `DecisionHints` fold inside `engineExecutor` is unchanged (ADR-009-d's pattern: pre-hooks write to `ctx.meta`; post-hooks consult `ctx.meta`; the executor extracts the typed `DecisionHints` from `ctx.meta`). The cache-policy fold is ADDITIVE — it doesn't disturb the `DecisionHints` extraction.

7. **Per-case counter discipline** — `readFires` increments ONLY for `ReadThrough`/`WriteThrough` (not `NoCache`); `writeFires` increments ONLY for `WriteThrough` (not `NoCache`, not `ReadThrough`); `hits` / `misses` increment ONLY for the read-hook's read branch. The counter `incrementAndGet()` calls were MOVED inside the active branches so they cannot fire for `NoCache`. This is the user-visible "cache metrics for the model that declared caching" fix.

8. **Cite-by-topic, not version-specific ADR IDs** — in the local-only `.omp/WATCHDOG.yml`, the watchdog instructions reference ADRs by topic ("the persist-lifecycle ADR", "the hook-fold ADR"), not by version number (`ADR-009-f`, `ADR-009-d`). Future renumbering doesn't break the references.

---

## 5. Skill alignment

- **`karpathy-app-design`** (single source of truth, typed boundaries) — one `CachePolicy` ADT lives in `sm8-core/model`; the cache-plugin enforces the policy via the ADR-009-d fold pattern (engine-portable `ctx.meta` channel); deployment stays outside core + transport; transport does not import adapter types.

- **`karpathy-guidelines`** (surgical edits, smallest correct change, dead code is a smell) — engine-side ADT deleted; `ReadOnly` case deleted; 4 spec sites migrated (no more, no less); 2 P3 prose nits folded in `8763d0a`.

- **`scala-bug-hunting`** (silent no-ops are bugs) — the entire ADR is a silent-no-op closure: `CacheReadPreHook` + `CacheWritePostHook` fired unconditionally regardless of `model.cache`; counter metrics were wrong; the dual `CachePolicy` ADT was a silent-no-op drift hazard. The runtime gate is the user-visible fix.

- **`scala-impact-analysis`** (blast radius) — every caller of the engine-side `CachePolicy` ADT must be enumerated + migrated; `PortableQueryCompiler.scala:534` rejection message references the ADT name; `EngineContextSpec.scala` has 4 sites; the spark-connector tests may reference it transitively. The 4 spec sites + the rejection message were enumerated in Fix 2.

- **`scala-error-handling`** (typed errors over `Throwable` swallow) — the fold is an Either-or-pass-through, no exception-based gate. The hook does not throw on `NoCache`; it early-returns.

- **`scala-jvm-safety`** (resource lifecycle, hot-path semantics) — the gate must NOT change the hot-path semantics for the `NoCache` default. Both hooks early-return without touching the cache. No allocation beyond the existing `Map.get` + pattern match (O(1)).

- **`scala-data-driven-refactor`** (sealed-trait dispatch over Map-based rule tables) — `CachePolicy.NoCache` / `ReadThrough(name)` / `WriteThrough(name)` dispatch is the typed rule. No Map-based rule table. The fold carries the typed ADT value through `ctx.meta`.

- **`scala2-scaladoc`** (every fix carries a scaladoc anchor) — every fix in the §Decision section has an ADR anchor + a file:line + a code snippet showing the production shape. The ADR vs implementation drift is real and corrected (e.g., the `initCtx.meta` snippet in Fix 4 is byte-accurate to the merged code).

- **`debug-mantra`** (reproduce → trace → falsify → cross-reference → verify) — the 6 falsifiable acceptance criteria map 1-to-1 to the test fixtures. The integration spec constructs the same `ReadThrough("region-a")` / `WriteThrough("region-a")` models that the ADR claims; the counter assertions verify the per-case matrix; the parent reactor re-run independently confirms BUILD SUCCESS.

---

## 6. What this ADR does NOT do (deferred to future ADRs)

- **`Multi-region cache routing`** — `ReadThrough(name)` carries a cache-region `name`; the current `InMemoryResultCache` ignores the `name` and stores everything in one map. A future multi-region cache (`InMemoryResultCache` per region, keyed by `name`) is a separate ADR when a real consumer surfaces. Already documented in ADR-009-f and ADR-009-g §Out-of-scope.

- **TTL / eviction policies on `CachePolicy.ReadThrough`** — the current `CachePolicy` is a routing-only shape. A future ADR adding `ReadThrough(name: String, ttl: Duration, evictOn: EvictTrigger)` is independent of this ADR.

- **Typed `Materialize.Persist(StorageLevel)` ADT** — `Persist(level: String)` is engine-specific (Spark `StorageLevel.fromString`) leaking through a model-attached ADT. A future Trino / DuckDB adapter would need either to reject `Persist` entirely or to maintain a parallel level-to-engine-native mapping. A typed `Persist(StorageLevel)` ADT (engine-portable enum) is the proper fix; deferred to a future ADR when a non-Spark adapter with a real `Persist` semantic surfaces.

- **`MaterializePolicy` as a typed engine-portable shape in `EngineContext`** — the original design intent. Restored only if a real engine-portable materialize concern surfaces. The current ADR removes the dead field; reintroducing it requires a real consumer + a typed `MemoryAndDisk` mapping.

---

## 7. Hygiene /

- **Memory:** 7.5 Gi total, 3.7 Gi used (~50%)
- **Disk:** 75G of 100G used (~75%)
- **Codegraph:** 6 procs active (2-tier: session MCP + singleton daemon + replica pair; not duplicates)
- **Metals / Bloop:** 0 / 0 (cleaned in PR-178 era)
- **Branch hygiene:** local `adr/009-g-cache-policy-contract` deleted; remote-tracking retained (GitHub default for merged PRs)
- **`.omp/WATCHDOG.yml`:** local-only, gitignored, project-scoped advisor roster (Code + Verify) with disjoint failure classes; persists across `git pull` and `reset --hard` (untracked + ignored)

---

## 8. References

- ADR-009-g (`docs/adr/0009-g-cache-policy-contract.md`) — this ADR, promoted to Implemented
- ADR-009-f (`docs/adr/0009-f-paired-persist-lifecycle.md`) — the prior wave that closed the sibling `MaterializePolicy` shape; PR-180; PR-181 closeout
- ADR-009-e (`docs/adr/0009-e-driver-materialization-bounds.md`) — the v0.4 wave's other half; driver-materialization bounds + `truncated` field
- ADR-009-d (`docs/adr/0009-d-broadcast-skew-decision-via-context-meta.md`) — the `DecisionHints` fold pattern at `EngineService.scala:430-441`; the SDK meta channel boundary; PR-178 honor-or-UnsupportedCapability discipline
- ADR-008-P — the upstream "Open Items" tracker; CROSS-P0-B closed by ADR-009-f
- PR-176 (`d55c1d2`) — the 8 P1 fix wave that established NonFatal discipline + production-style typing
- PR-178 (`62317a5`) — cross-engine DecisionHints contract closure (honor-or-UnsupportedCapability)
- PR-179 (`7b82362`) — ADR-009-e driver-materialization bounds (Option A; cap+1 + truncated)
- PR-180 (`821d270`) — ADR-009-f paired persist lifecycle (4 atomic commits)
- PR-181 (`23d4398`) — ADR-009-f closeout
- PR-182 (`8d5e4cb`) — **this ADR's implementation** (4 atomic commits; squash-merged)
- RFC §3 Core Boundary (`docs/rfcs/2026-08-12_v1_architecture-spec/semantic-layer-engine-architecture.md`) — typed engine-portable ADTs in `sm8-core`; config in deployment layer
- RFC §11a Deployment Module — deployment lives outside core AND outside transport; transport does not import adapter types
- `/tmp/cross-engine-dechints-audit.md` — the PR-178 cross-engine audit notes (PR-178 closed the `DecisionHints` honor-or-error gap; the same fold pattern is reused for `cachePolicy`)
- `/tmp/advisor-watchdog.md` (fetched from `can1357/oh-my-pi/docs/advisor-watchdog.md`) — the WATCHDOG.yml schema reference used to design the local-only `.omp/WATCHDOG.yml`

---

## 9. Final state

- **main HEAD:** `8d5e4cb` (PR-182 merged)
- **Local branch:** `adr/009-g-cache-policy-contract` (preserved, can be deleted when convenient)
- **`.omp/WATCHDOG.yml`:** local-only, gitignored, project-scoped advisor roster
- **All 6 gaps closed** (unconditional read fire, unconditional write fire, dual ADT, dead field, missing fold, fold site)
- **All 7 falsifiable acceptance criteria verified by tests** (1 NoCache no-op; 2 per-case matrix via 3 tests; 3 dual ADT grep ZERO; 4 fold reaches hook via 4 integration tests; 5 NoCache backwards-compat; 6 per-case counter discipline; 7 no regression — full reactor green)
- **No follow-ups from this ADR** — `Multi-region cache routing` is the only documented deferral