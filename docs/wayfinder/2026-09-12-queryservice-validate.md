# Wayfinder decision ticket — `QueryService/validate` (execute-free query compilation)

**Ticket status**: DRAFT r4 (post grilling round 3: clover READY-TO-OPEN, sunflower NEEDS-REVISION with β-internal-contradiction BLOCKING; β' resolution applied — drop both QueryBuilder.build and rollupDecision from v1)
**Date**: 2026-09-12
**Wayfinder map**: #378 (Tier 2+ refinements — user-facing query tooling axis)
**Related**: ADR-0012-a (additive-service precedent), ADR-0026 (rollup routing invocation), `io.sm8.core.Pipeline`, `io.sm8.core.rel.RollupRewriter`

## Destination

A spec for a **`QueryValidationService/validate`** handler in sm8-platform: an execute-free query pass that answers "is this query well-formed against this Model?" — running **field-existence + cross-reference + calc-DAG + status checks** with **no execute, no format, no plan, no DataFrame, no Spark IO, no cache effects, no hooks** — returning a `ValidationOutcome { modelVersion, errors: List[EngineError] }`. This matches the competitive baseline: dbt compile / MetricFlow --explain do not show routing either — routing is a runtime concern. v1 answers "would this be refused at execute time for a *well-formedness* reason?" Routing preview, plan-tree enumeration, and physical-table-touch list are deferred to v2 (they all require a `RelOp`, whose only producer needs a `SourceResolver` that only Spark implements today).

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
  1. ModelValidator.validate(model)                — cross-refs, duplicate names, calc-DAG
  2. resolve dimension/measure refs                — against the Model's declared fields
     (NO SourceResolver; NO RollupRewriter.rewrite; see R3-1 resolution below)
  3. STOP. No execute. No format. No hooks. No cache. No Spark. No SourceResolver.
```

**R2-1 resolution (clover B5 + sunflower R2-1, both BLOCKING): option β — drop `QueryBuilder.build` from v1.**

Both grillers flagged the same contradiction: `QueryBuilder.build(model, resolver, identity)` requires a `SourceResolver`; the only impl is `SparkSourceResolver` (Spark IO + DataFrame); the docstring-referenced `NoopSourceResolver` does not exist; and option α would need a second core change (`Model.declaredSchema` doesn't exist either).

**R3-1 resolution (sunflower R3-1, BLOCKING): option β' — also drop `rollupDecision` from v1's response.**

Sunflower caught what r2/r3 missed: `RollupRewriter.rewrite` requires a `RelOp`; the ONLY `(Model, QueryRequest) → RelOp` producer is `QueryBuilder.build`, which still needs the SourceResolver. So β had an internal contradiction — D1 says "no QueryBuilder.build" but D2 says "include rollupDecision."

β' resolves cleanly: v1 is **well-formedness only**. The service answers "is this query well-formed against this Model?" — not "what would it touch." That matches the competitive baseline (dbt compile and MetricFlow --explain don't show routing either — routing is a runtime concern). Routing preview moves to v2 via a named deferred core change: a no-resolver `QueryRequest → RelOp` builder in sm8-core, OR a new `NoopSourceResolver`, OR a schema-only resolver wired at definition-time. The v2 fork is documented; not picked now.

Errors surfaced in v1: unknown dimension, unknown measure, unparsable filter, calc-measure cycle, duplicate names, missing model. NOT surfaced in v1: rollup routing decision, join-key type mismatch against source schema (needs resolver), source-column existence (needs resolver), what would be executed. (needs resolver).

**D2 — Response shape.**

Reuse existing typed ADTs (per clover S1 — do not invent a "plan summary"):

```
ValidationOutcome {
  modelVersion: Int                            // the version that was validated
  errors: List[EngineError]                    // per-stage failures, empty on success
  // tablesTouched, rollupDecision, decisionHints, engineSelection:
  //   deferred to v2 (per β' resolution — all require a RelOp, which
  //   requires SourceResolver, which would force Spark IO into v1).
}
```

The "tables touched" claim from the r1 motivation is **deferred to v2** as well — without `RollupRewriter.rewrite`, we cannot enumerate the physical tables the execute WOULD read. v1 is honest: well-formedness-against-Model-only.

Serde: Jackson + `DefaultScalaModule` (per the QueryService.scala:174 pattern) covers `ValidationOutcome` — plain fields + `List[EngineError]` (a sealed trait with existing serde wiring).

Metrics: new additive counters on the existing `MetricsSink` seam (clover S10): `recordValidationSuccess(outcome)` + `recordValidationFailure(stage)`, observed by the rollup-refusal-observer pattern (MetricsSink.scala:46-51).

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

- [ ] **Q1**: Exact response JSON wire shape for `ValidationOutcome{modelVersion: Int, errors: List[EngineError]}` — trivial (`Int` + `List[EngineError]`, the latter sealed-trait serde already wired in QueryService.scala:174). No `RollupRewriteResult` or other sealed hierarchy is needed in v1 (deferred to v2 with the routing preview).
- [ ] **Q2**: (Was CompiledSQLPreview in r3.) Now moot — the v2 RelOp fork governs whether SQL preview is reachable at all. Tracked under v2 promotion path in the r4 R3-1 resolution paragraph.
- [ ] **Q3**: Metrics counters — confirmed (clover S10): `recordValidationSuccess(outcome)` + `recordValidationFailure(stage)` on the existing `MetricsSink` seam (MetricsSink.scala:46-51).
- [ ] **Q4**: Test fixture: `AuditStub` + `AuditStubNoOpContractSpec` pattern. Validate runs no hooks, so the safety pin is structural: a test that registers the stub plugin at every Pre/Post hook slot and asserts each `fires.get` is zero after validate. No temp-file write-side hook is needed (validate doesn't reach those points).

## Skills every session should consult

- **grilling** for the open questions
- **scala-error-handling** for the per-stage typed error aggregation (reuse `EngineError`; may add `ValidationOutcome` wrapper)
- **karpathy-guidelines** for the thin-handler shape

## Standing preferences (per the sm8 Execution Rules Checklist)

- Dual review + PR + RULE 9 stop on the implementation PR.
- Layer discipline: handler in sm8-platform; the single called function in v1 is `ModelValidator.validate` (already in sm8-core); **zero core change, zero connector change**. `RollupRewriter.rewrite` and `QueryBuilder.build` are NOT called in v1 — the β' resolution: their `RelOp`/`SourceResolver` dependencies would force Spark IO or a deferred core change (both incompatible with the v1 "no core change" commitment). They move to v2 alongside the routing-preview response fields.
- No new persistence, no schema changes; additive service only (ADR-0012-a pattern).

## LOC estimate (revised per clover S7 + r3/r4 nits)

**~350–450 LOC, consolidated into 3 files** (single-handler service ≈ MetaInspectorService at 240 LOC for 2 handlers):
- `QueryValidationService.scala` — service + handler + `ValidationOutcome` response ADT (~150 LOC; the response type lives inside the service object, matching the MetaInspectorService pattern)
- `HttpTransport.scala` — additive `.bind(...)` (~5 LOC)
- `QueryValidationServiceSpec.scala` — write-safety pin (AuditStub pattern, stub plugins at every Pre/Post slot asserted zero) + well-formedness/error-aggregation tests (~200 LOC)

(v1 carries no serde for `RollupRewriteResult`/`ResolveStageOutcome` — both deferred to v2 with the routing preview, per the β' resolution.)

## Grilling log

- **Round 1** (2026-09-12): clover (decision completeness) + sunflower (architecture/seam).
  - clover: 4 BLOCKING (production path doesn't use in-tree Pipeline — Context.stop premise wrong; D4 mechanism unsafe; cache PreExecute effects; ADR-012-a precedent points to separate service) + 4 SHOULD-FIX (response-shape rename, MCP demotion, rate-limit question, LOC estimate).
  - sunflower: 3 BLOCKING/SHOULD-FIX (no Plan stage exists; routing decision ALREADY in core — nothing to extract; Context.stop doesn't short-circuit default hooks) + verification notes (Cube endpoint list audit-proofed; AuditStub test pattern confirmed).
  - All BLOCKINGs addressed in r2: D1 reframed to direct-call composition; D4 contract restructured; D3 reversed to separate service per ADR-0012-a; MCP demoted; rate-limit added; LOC committed.
- **Round 2** (same pair): clover NEEDS-REVISION (B5: QueryBuilder.build requires SourceResolver whose only impl is SparkSourceResolver — Spark IO breaks the by-construction guarantee; NoopSourceResolver referenced but absent; option α needs a second core change) + 5 SHOULD-FIX (Option[DecisionHints], engineSelection type, LOC 350-450/3 files, call-chain naming, serde pattern, MetricsSink counters). sunflower NEEDS-REVISION (R2-1: same fork, independently verified NoopSourceResolver absent + Model.declaredSchema absent; R2-3: ADR-0032 gloss too thin).
  - All addressed in r3 (option β: drop QueryBuilder.build, keep rollupDecision).
- **Round 3** (same pair): clover READY-TO-OPEN (13/13 verifications, 2 nits: LOC body inconsistency + wrong spec name). sunflower NEEDS-REVISION (R3-1 BLOCKING: β has an internal contradiction — RollupRewriter.rewrite requires a RelOp; the only (Model, QueryRequest) → RelOp producer is QueryBuilder.build; so dropping QueryBuilder but keeping rollupDecision was incoherent). R3-2 NICE: dbt-metricflow path unverifiable from that session.
- **Round 4 resolution**: β' applied — QueryBuilder.build AND rollupDecision both dropped from v1. v1 = well-formedness only (ModelValidator.validate + field-existence vs declared fields). tablesTouched/rollupDecision/decisionHints/engineSelection/CompiledSQLPreview all deferred to v2 with the three-way RelOp fork documented (NoopSourceResolver / new QueryRequest→RelOp builder / schema-only resolver — fork unpicked). Matches competitive baseline: dbt compile and mf --explain do not show routing either. Consistency sweep applied (Destination, standing prefs, Q1-Q4 rewritten for β').

## Notes

- This ticket is a **decision** ticket, not a build ticket. Resolving D1–D6 + Q1–Q4 produces the implementation spec.
- The competitive motivation is secondary: even with zero competitor pressure, the Execute-stage write side effects justify validate. Both arguments hold independently.
- Layer map: **zero core change, zero connector change; one new platform service + bind line.** Verified by sunflower against RollupRewriter.scala, Pipeline.scala, Context.scala, QueryService.scala, ModelService.scala, SparkEngineProvider.scala.

## Decisions so far

- 2026-09-12: Ticket opened; r1 grilled (4+3 BLOCKINGs) → r2 resolved all round-1 findings → r2 grilled (1 BLOCKING: SourceResolver fork) → **r3 resolved via option β** (drop `QueryBuilder.build` from v1) → r3 grilled (sunflower introduced R3-1 BLOCKING: β had an internal contradiction — `RollupRewriter.rewrite` requires a `RelOp`, whose only producer is `QueryBuilder.build`) → **r4 resolved via option β'** (drop both `QueryBuilder.build` AND `rollupDecision` from v1; v2 routing preview deferred to a named core change; v1 = well-formedness only). Awaiting round-4 confirmation + go/no-go on the build ticket.
