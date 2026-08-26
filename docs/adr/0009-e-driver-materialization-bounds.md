# ADR-009-e: Driver-materialization bounds — server-side cap + typed `truncated` on the wire

| Field | Value |
|---|---|
| **Status** | **Accepted** — Option A (no escape hatch; `Unbounded` deferred to a future ADR if a real consumer surfaces). Dual senior review (architect `best-reasoning` + data-eng `best-coding`) both rated the draft "incorrect as written"; all 4 review findings are folded into this revision (see Revision history). |
| **Date** | 2026-08-26 |
| **Module** | `sm8-core/engine/PortableQueryResult.scala` (+`truncated` field) + `sm8-core/cache/RestateCachedRow.scala` (+`truncated` journal field) + `sm8-core/cache/CachedRowDecoder.scala` (round-trip) + `connectors/spark-connector/.../SparkEngineProvider.scala` (cap at `applyPostCompilePipeline`) + `sm8-platform/query/EngineService.scala` (cap config + `toQueryResultFromPortable` forwarding) + `connectors/spark-connector/.../PortableQueryCompiler.scala` (SM-08 gate) |
| **Supersedes scope** | The unbounded-`collect()` driver-OOM risk (P2 SM-07) + the unpartitioned-Window executor-OOM risk (P2 SM-08) left open by the PR-176 fix wave. Also closes the `wasPersisted`-vs-`.limit()` unpersist leak the reviewers found (new P2). |
| **Skill alignment** | `karpathy-app-design` (typed boundaries, closed core, config in deployment layer), `scala-spark-batch-bugs` (driver memory + executor scan shape + `.limit()` resets storage level), `scala-bug-hunting` (silent defaults are bugs — the row-cap-plugin stub was one), `scala-jvm-safety` (persisted-frame lifecycle), `scala-error-handling` (typed errors never swallow real Spark failures), `scala-data-driven-refactor` (closed ADT, defaulted additive field), `scala-impact-analysis` (the 4 threading sites enumerated), `scala-perf-testing` (cap+1 probe, no `df.count()` on the hot path), `scala2-scaladoc` (WHY prose), `debug-mantra` (falsifiable acceptance per site) |
| **Architecture alignment** | RFC §3 Core Boundary: `truncated` is an engine-portable wire field in core; the cap VALUE is deployment config (server-side), not a per-query `EngineContext` field. PR-178's discipline extends: silent drops are contract violations — a capped result MUST arrive flagged, including through the cache journal. |

---

## Revision history

| Version | Date | Change |
|---|---|---|
| v0.1 (Proposed) | 2026-08-26 | Initial draft; 3 options (A/B/C); investigation recommended C. |
| v1.0 (Accepted) | 2026-08-26 | Dual senior review rejected the draft as written. Folded in: **architect P1** — `EngineService.toQueryResultFromPortable:283-287` hardcodes `truncated=false`; **data-eng P1** — cache journal `RestateCachedRow` has no `truncated`, so cache HITs serve truncated results as complete; **data-eng P2** — `wasPersisted` derived from `withLimit.storageLevel` is wrong because `.limit()` resets it (persisted aggregate frame leaks); **both P2** — detection must probe `cap+1` (`truncated = pulled > cap`), not `actual_count >= applied_cap` (off-by-one + invites a full-scan `count()`). Decision resolved to **Option A** (no escape hatch): the escape is a named footgun with no current consumer; `MaxRowsPolicy`/`Unbounded` can be added later as an additive sealed trait if real demand surfaces. Blast-radius wording corrected to the precise 4-site list (data-eng: 9 constructor sites all default-safe; ZERO pattern matches exist). |

---

## Context and problem statement

Every `EngineProvider.query()` ends, on the happy path, in driver-side materialization of the result set into `PortableQueryResult`. In `connectors/spark-connector` that materialization is `withLimit.collect()` (`SparkEngineProvider.scala:513`). When the caller passes no `request.limit` — the default — `withLimit` is the unbounded DataFrame and `collect()` materializes every row into the driver heap. A 100M-row table OOMs the driver.

Existing mitigations are partial:

| Layer | Mitigates? | Notes |
|---|---|---|
| Caller passing `request.limit = Some(N)` | yes | Opt-in; the platform builds `QueryRequest.empty` with `limit = None` |
| `plugins/row-cap-plugin` (PostExecute, priority 200) | **no — it's a stub** | Header: "shape-correct (counter only); real capping lands with the typed Result shape". It increments a counter; it does not truncate |
| Connector-layer cap | **this ADR** | Not yet implemented |

The user-facing promise ("caller sees `truncated=true` and can act") additionally requires the flag to survive THREE boundaries, not one — the draft missed two of them and the reviews caught it:

1. **Engine → platform**: `EngineService.toQueryResultFromPortable` (`EngineService.scala:283-287`) hardcodes `truncated = false`. Its scaladoc literally defers this: "PR-C5b-extension can add truncated ... when the engine reports a cap". The CLI already renders `(TRUNCATED)` from this platform JSON (`sm8-cli/Main.scala:397-399`) — the consumer exists; the producer forwards nothing.
2. **Platform → cache journal → platform**: the write-through cache-plugin stores engine output as `RestateCachedRow` (fields: `fieldNames`/`fieldTypes`/`rows` — no `truncated`) via `CachedRowDecoder.toRestateCachedRowFromPortable` and rehydrates via `fromRestateCachedRowAsPortable` which constructs `truncated=false` (field default). The cache key is model+version only (`CachePlugin.scala:153`), so a capped result is served verbatim to a later caller regardless of its requested limit — **the silent-truncation class this ADR eliminates, resurfacing on the cache-HIT path**.
3. **Engine → engine (persist lifecycle)**: `wasPersisted` is derived from `withLimit.storageLevel` (`SparkEngineProvider.scala:508-510`), but `.limit()`/`.filter()` build a NEW uncached logical plan, so `withLimit.storageLevel == StorageLevel.NONE` even when the upstream frame was persisted by `applyAggregations` (ADR-008-P paired persist). The `finally`'s `unpersist()` is skipped on exactly the capped path this ADR makes the default — the persisted aggregate frame leaks until `provider.close()`.

---

## Decision

**Option A — server-side default cap + typed `truncated` threaded through all three boundaries. No escape hatch.**

Options B (typed reject on unbounded queries) and C (A + `Unbounded` opt-in) were rejected:
- **B** hinges on "source-side stats > threshold", which Spark does not expose reliably pre-scan (metastore stats absent/stale; counting is itself a full scan — a hot-path liability), and forces a behavioral break on every no-limit query.
- **C**'s escape hatch is a named footgun (setting it reintroduces the exact silent-OOM this ADR closes), owned per-query on `EngineContext` — the wrong layer per RFC §3 (config belongs to the deployment layer). No current consumer needs it. `MaxRowsPolicy`/`Unbounded` can be added later as an additive sealed trait if real demand surfaces.

### The cap

- The cap value is **deployment config** (server-side), threaded to the engine by the platform — NOT a per-query `EngineContext` field (the draft's Option C placement, corrected per the architect finding). Default: `1_000_000` rows.
- Engine-side enforcement in `applyPostCompilePipeline`: apply `limit(min(cap, request.limit.getOrElse(cap)) + 1)` — the **cap+1 probe** — then `collect()`, then `truncated = (collected.length > effectiveCap)`, discarding the extra row. `collected.length` is computed once from the materialized array (O(1)); **no `df.count()`** on the hot path. The probe makes detection truthful: a source returning exactly `cap` rows reports `truncated=false`.

### The three threading sites (all mandatory; the draft's "constructor sites compile unchanged" understated the work)

| # | Site | Change |
|---|---|---|
| 1 | `connectors/spark-connector/.../SparkEngineProvider.scala` `applyPostCompilePipeline:488-529` | cap+1 probe; set `truncated` on the constructed `PortableQueryResult`; **fix `wasPersisted`**: derive it from the passed-in `df` BEFORE applying `limit`/`filter` (the `.limit()` resets `storageLevel`), so the `finally`'s `unpersist()` runs on the capped path |
| 2 | `sm8-platform/.../EngineService.scala` `toQueryResultFromPortable:283-287` | forward `truncated = portable.truncated` (delete the hardcoded `false` + the stale deferral comment) |
| 3 | `sm8-core/cache/RestateCachedRow.scala` + `CachedRowDecoder.scala:128-148, 239-262` | add `truncated: Boolean = false` to `RestateCachedRow`; `toRestateCachedRowFromPortable` writes it; `fromRestateCachedRowAsPortable` rehydrates it — cache HITs preserve the flag (`RestateCachedRowSerializationSpec` gains a round-trip case) |

Blast radius (grounded by the data-eng): **9** `PortableQueryResult(...)` constructor sites, **all compile unchanged** with the defaulted field; **zero** case-pattern matches on the ADT exist in the codebase. The Consequences-section claim in the draft ("every pattern-match breaks") was overstated and is corrected here.

### SM-08 — unpartitioned Window gate (independent of the cap)

`PortableQueryCompiler.scala:586`: `if (dimCols.isEmpty) Window.partitionBy()` is reached when a calculated measure references `Expr.All` and `model.dimensions.isEmpty`. A zero-partition window is a single-window whole-scan: one executor touches every row during execution, BEFORE any driver-side `limit` can run — truncation cannot protect the executor, and the AQE skew factor is irrelevant with no partition to balance.

Fix: return `Left(EngineError.UnsupportedCapability(engine="spark-3.5", capability="Window.UnpartitionedPercentOfTotal", message=...))` at the `applyWithWindows` entry — compile-time typed rejection. Returning early is strictly better than truncating: there is no valid "truncated global percent-of-total". (Method name note: the entry point is `applyWithWindows` at `:581`; the draft's `applyWhenWindows` label was wrong.)

### row-cap-plugin — explicitly deferred

The stub stays a stub. This ADR's cap is engine-portable policy owned by the connector + deployment config, making a real plugin implementation unnecessary for closure. Whether to implement-or-delete the stub is a separate follow-up (original review B2/SM-14); this ADR does not close it.

### Error-handling guardrail

The cap path must not wrap `collect()` in a catch. Real Spark failures (`SparkException`, executor OOM) propagate per the PR-176 `NonFatal` discipline: the dispatcher/engine wraps only `NonFatal` + re-interrupts on `InterruptedException`; `Error` subtypes propagate. The typed `truncated` result is a VALUE, not an exception path.

---

## Falsifiable acceptance

```
1. SM-07 happy: a query with no request.limit over a fixture larger than the cap
   returns Right(PortableQueryResult) with rows.size == cap and truncated == true.
2. Off-by-one: a fixture of EXACTLY cap rows returns truncated == false
   (the cap+1 probe found nothing to drop).
3. Platform forwarding: toQueryResultFromPortable maps the engine flag through —
   the QueryResult JSON carries truncated: true (assert at the EngineService spec).
4. Cache round-trip: write a truncated PQR through the cache-plugin journal,
   re-query for a HIT, assert truncated survives the RestateCachedRow round-trip
   (assert false on the pre-fix decoder).
5. Persist lifecycle: materialize == Persist + a capped query — assert the
   upstream persisted frame is unpersisted after collect (storage-level /
   Spark-listener assertion), not lingering until provider.close().
6. SM-08: a calc measure referencing Expr.All with zero dimensions returns
   Left(UnsupportedCapability(capability="Window.UnpartitionedPercentOfTotal"))
   and NO Spark job runs (the plan is never built).
7. Non-swallow: a SparkException from collect is NOT reclassified as
   UnsupportedCapability (assert the error type propagates).
```

---

## Consequences

**Positive**
- Closes the silent driver-OOM (SM-07) and the unpartitioned-window executor-OOM (SM-08) with typed, observable outcomes
- The `truncated` flag survives engine → platform → CLI AND the cache journal — the "no silent drop" invariant holds end-to-end (PR-178 discipline extended to the materialization boundary)
- Fixes the pre-existing `wasPersisted`/`.limit()` unpersist leak (a reviewer-found P2 that predates this ADR)
- The cap value is deployment policy per RFC §3; callers cannot trip the guard off

**Negative**
- `RestateCachedRow` journal shape changes (additive defaulted field; `RestateCachedRowSerializationSpec` + Jackson round-trip update required)
- Legitimately-large results require explicit `request.limit` — no escape hatch (deliberate; revisit with a real consumer)
- The stub row-cap-plugin remains misleading dead weight until the separate implement-or-delete follow-up
