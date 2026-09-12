# Wayfinder decision ticket — `QueryService/validate` (execute-free query compilation)

**Ticket status**: DRAFT r2 (post grilling round 1: clover + sunflower; see revision log at bottom)
**Date**: 2026-09-12
**Wayfinder map**: #378 (Tier 2+ refinements — user-facing query tooling axis)
**Related**: ADR-0012-a (additive-service precedent), ADR-0026 (rollup routing invocation), `io.sm8.core.Pipeline`, `io.sm8.core.rel.RollupRewriter`

## Destination

A spec for a **`QueryValidationService/validate`** handler in sm8-platform: an execute-free query pass that answers "will this query work against this model, and what would it do?" — running parse → resolve → rollup-routing decision with **no execute, no format, zero writes, zero cache effects** — returning either typed per-stage errors or a `ResolveStageOutcome` (rollup hit/miss + reason, join path, decision hints). sm8 today has no execute-free validation surface: the only way to learn a query is broken is to run it, and the production run path can WRITE (Tier 0/1 rollup refresh, cascade fan-out).

## Context — why now, why sm8-specific

sm8's in-tree pipeline (`io.sm8.core.Pipeline`, `Stage.All = Parse → Resolve → Execute → Format`) is foldLeft-pure, but **production queries do not run through it** — production orchestration is `EngineService.runQueryWithHooks → EngineHookDispatcher → engineExecutor → SparkEngineProvider.query`, a for-comprehension that fuses parse + resolve + rollup-routing + execute into one method. There is no "stop after plan" point to switch on.

The sm8-specific footgun: **the execute path writes**. Tier 0 rollup refresh is a whole-table CTAS/overwrite; Tier 1 does scoped partition overwrite; cascade fans out. An operator exploring "would this query route to the rollup or hit the base table?" has exactly one way to find out today: run it and accept the side effects.

## Competitive fact-check (verified 2026-09-12)

| capability | dbt | MetricFlow | Cube | sm8 |
|---|---|---|---|---|
| execute-free query compilation | ✅ `Compile` + `Parse` top-level commands (verified: `dbt-clap-core/src/commands.rs`, 338 lines, main branch) | ✅ `query --explain`, `--show-dataflow-plan`, `--show-sql-descriptions` (verified: `dbt-metricflow/cli/main.py`, main branch) | ❌ no `/v1/dry-run`, no `/v1/preview`, no `validate` (verified: full endpoint list is `/v1/load`, `/v1/meta`, `/v1/sql`, `/v1/cubesql`, `/v1/pre-aggregations`, `/v1/pre-aggregations/jobs`, `/v1/convert-query`, `/v1/data-sources`, `/v1/entities`, `/v1/public`, `/v1/running-query`, `/graphql`, `/livez`, `/readyz`) | ❌ |
| semantic validation without query execution | ✅ compile-time + `dbt test` | ✅ `health_checks` → `validate_semantic_models` / `validate_dimensions` / `validate_entities` / `validate_simple_metrics` (verified in CLI source) | ✅ at schema-compile time | ⚠ partial (ModelValidator runs inside execute; no standalone surface) |

dbt and MetricFlow both ship it; Cube does not (its model is compile-time validation + pre-aggregation transparency). sm8 is in the "none" column on all rows. Note: MetricFlow's `--explain`/plan flags were verified by direct fetch of `dbt-metricflow/dbt_metricflow/cli/main.py`; the sunflower griller could not independently re-verify the same paths (its fetches 404'd on a different path guess) but confirmed the file layout via the GitHub tree API.

## Decision to resolve (revised per grilling round 1)

**D1 — Validate implementation path (the load-bearing decision).**

The in-tree `Pipeline` foldLeft is DORMANT in production; adding a Plan stage would be a breaking SDK change (`PipelineStage` is frozen per `Context.scala`). The correct v1 shape is **a direct-call composition in the platform handler**:

```
QueryValidationService/validate:
  1. parse the request against the captured Model   (existing core parse path)
  2. resolve dimension/measure refs                 (existing core resolve path)
  3. call io.sm8.core.rel.RollupRewriter.rewrite    (ALREADY IN CORE — no extraction needed;
                                                     grilling round 1: sunflower finding 2 —
                                                     the connector's routeThroughRollup is a thin
                                                     fold that just invokes this core function and
                                                     records telemetry)
  4. STOP. No execute. No format. No hooks. No cache.
```

This is option B reframed: not "extract routing from Execute" but "call the core function the connector already calls." Zero core change, zero connector change; the handler lives entirely in sm8-platform.

**D2 — Response shape.**

Reuse existing typed ADTs (per clover S1 — do not invent a "plan summary"):

```
ResolveStageOutcome {
  rollupDecision: RollupRewriteResult        // sealed: Rewritten(plan, rollupName) | Unchanged(refusal)
  decisionHints: Option[DecisionHints]        // broadcast/skew if plugin installed
  engineSelection: EngineSelection            // which provider would serve
  errors: List[EngineError]                   // per-stage failures, empty on success
}
CompiledSQLPreview: Option[String]            // deferred to v2 (see open questions)
```

**D3 — Same service vs separate service.**

**Separate `QueryValidationService`** (revised from the original same-service recommendation, per clover B4: ADR-0012-a's precedent is a separate additive ServiceDefinition, not a handler added to an existing service — ModelService was its own `object ... definition(model)`). Binding alongside `QueryService` in `HttpTransport.endpoint` matches the additive pattern and avoids touching the existing service's wire contract.

**D4 — Write-safety contract.**

The v1 contract is structural, not runtime: **validate never invokes Execute, never fires pipeline hooks, never touches the cache.** It is a direct composition of pure core functions — there is no hook wiring to get wrong.

- Grilling round 1 rejected the original `Context.stop = true` mechanism: post-hooks with `runsOnStop = true` (the default) fire even when stop is set (`Pipeline.runPostHooks`), so a third-party plugin at PostResolve/PostExecute would still write. By not running hooks at all, validate sidesteps the entire class of bugs.
- Trade-off, documented honestly: validate bypasses plugin behavior by design. A plugin that would have mutated the query before execute makes validate's answer wrong for the plugin-modified query. That is acceptable for v1 (validate answers "what does THIS model + query do"), and the Scaladoc must state it.
- Cache effects are structurally excluded: no `CachePlugin` code runs because no hooks run. The cache PreExecute counter cannot increment on a validate path (verified: CachePlugin registers at PreExecute/PostExecute; validate runs neither).

**D5 — MCP exposure: deferred to v2** (per clover S2 — MCP wiring is ~30 LOC but a real scope add; HTTP-first via the existing Restate ingress is the canonical operator/UI surface).

**D6 — Rate limiting: v1 inherits runQuery's posture (none); operators rate-limit at the ingress** (per clover S3).

## Open questions for the build ticket

- [ ] **Q1**: Exact response JSON wire shape for `ResolveStageOutcome` (serde for `RollupRewriteResult` sealed hierarchy).
- [ ] **Q2**: Should v1 include `CompiledSQLPreview` (dbt-compile parity)? Requires lifting the SQL-compile step out of the connector's execute path — deferred unless cheap.
- [ ] **Q3**: Does validate need its own metrics counters (validation success/failure counts) or reuse MetricsService? (Recommend: new counters, additive.)
- [ ] **Q4**: Test fixture: reuse `AuditStub` counter pattern (verified to exist: `AuditPostStubHookFiresSpec`) + a synthetic write-stub plugin (PreResolve increments counter + PostResolve writes temp file) → assert both zero after validate.

## Skills every session should consult

- **grilling** for the open questions
- **scala-error-handling** for the per-stage typed error aggregation (reuse `EngineError`; may add `ValidationOutcome` wrapper)
- **karpathy-guidelines** for the thin-handler shape

## Standing preferences (per the sm8 Execution Rules Checklist)

- Dual review + PR + RULE 9 stop on the implementation PR.
- Layer discipline: handler in sm8-platform; all called functions (`parse`, `resolve`, `RollupRewriter.rewrite`) already in sm8-core; **zero core change, zero connector change** (verified by sunflower: `RollupRewriter.rewrite` at RollupRewriter.scala:370-398 is engine-portable core; the connector's `routeThroughRollup` is a thin fold invoking it).
- No new persistence, no schema changes; additive service only (ADR-0012-a pattern).

## LOC estimate (committed, per clover S4)

**~665 LOC across 6 files**:
- `QueryValidationService.scala` (new, platform): ~180 LOC (handler + serde + response types)
- `HttpTransport.scala` (bind): ~10 LOC
- `ResolveStageOutcome.scala` (new response type): ~60 LOC
- tests: `QueryValidationServiceSpec` ~250 LOC + `WriteSafetySpec` ~100 LOC (AuditStub pattern + write-stub plugin)
- serde wiring in existing Restate definition machinery: ~65 LOC

## Grilling log

- **Round 1** (2026-09-12): clover (decision completeness) + sunflower (architecture/seam).
  - clover: 4 BLOCKING (production path doesn't use in-tree Pipeline — Context.stop premise wrong; D4 mechanism unsafe; cache PreExecute effects; ADR-012-a precedent points to separate service) + 4 SHOULD-FIX (response-shape rename, MCP demotion, rate-limit question, LOC estimate).
  - sunflower: 3 BLOCKING/SHOULD-FIX (no Plan stage exists; routing decision ALREADY in core — nothing to extract; Context.stop doesn't short-circuit default hooks) + verification notes (Cube endpoint list audit-proofed; AuditStub test pattern confirmed).
  - All BLOCKINGs addressed in this r2: D1 reframed to direct-call composition (no Plan stage, no pipeline run); D4 contract restructured to structural no-hook-invocation; D3 reversed to separate service per ADR-0012-a; response shape renamed to `ResolveStageOutcome` reusing existing ADTs; MCP demoted; rate-limit added as D6; LOC committed.

## Notes

- This ticket is a **decision** ticket, not a build ticket. Resolving D1–D6 + Q1–Q4 produces the implementation spec.
- The competitive motivation is secondary: even with zero competitor pressure, the Execute-stage write side effects justify validate. Both arguments hold independently.
- Layer map: **zero core change, zero connector change; one new platform service + bind line.** Verified by sunflower against RollupRewriter.scala, Pipeline.scala, Context.scala, QueryService.scala, ModelService.scala, SparkEngineProvider.scala.

## Decisions so far

- 2026-09-12: Ticket opened; r1 grilled; r2 revision addresses all round-1 BLOCKING findings. Awaiting Q1–Q4 resolution + go/no-go on the build ticket.
