# Wayfinder decision ticket — `QueryService/validate` (execute-free query compilation)

**Ticket status**: DRAFT r6 (post grilling round 4: maple + seedling both READY-TO-OPEN; their fixes applied — resolveModel policy pinned, drift-backstop claim corrected, wording fixes)
**Date**: 2026-09-12
**Wayfinder map**: #378 (Tier 2+ refinements — user-facing query tooling axis)
**Related**: ADR-0012-a (additive-service precedent), ADR-0026 (rollup routing invocation), `io.sm8.core.Pipeline`, `io.sm8.core.rel.RollupRewriter`

## Destination

A spec for a **`QueryValidationService/validate`** handler in sm8-platform: an execute-free query pass that answers "would this run cleanly against this Model, and what would it touch?" — running **field-existence + cross-reference + calc-DAG + status + routing decision** with **no execute, no format, no DataFrame, no Spark IO, no cache effects, no hooks** — returning a `ValidationOutcome` with rollup decision + tables touched + errors (Path 1 / fork 1a: routing preview IS in v1, via `DeclaredSchemaResolver`).

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

**D1 — Validate implementation path.**

The in-tree `Pipeline` foldLeft is DORMANT in production; production orchestration is `EngineService.runQueryWithHooks → EngineHookDispatcher → engineExecutor → SparkEngineProvider.query` (fused for-comprehension). The correct v1 shape — now that the RelOp fork is committed — is **a direct-call composition**:

```
QueryValidationService/validate:
  1. ModelValidator.validate(model)                                     — cross-refs, duplicate names, calc-DAG
  2. QueryBuilder.build(model, new DeclaredSchemaResolver(model), id)    — pure relop build; resolver
                                                                       synthesises Field(name, dataType,
                                                                       nullable=true) from the Model's
                                                                       declared dimensions (dims carry
                                                                       SealedDataType; measures default
                                                                       to Varchar — verified seedling r5 #1:
                                                                       RollupRewriter.scala:1095 falls back
                                                                       to Varchar for untyped measure refs)
  3. RollupRewriter.rewrite(plan, model.rollups)                       — core function; the rewrite may
                                                                       collapse to Unchanged when the
                                                                       query's dimension set is not fully
                                                                       covered by rollups (verified:
                                                                       RollupRewriter.scala:534, 396) —
                                                                       this is a VALID outcome, not a
                                                                       failure
  4. STOP. No execute. No format. No hooks. No cache. No Spark. No warehouse reads.
```

Layer-map: **zero core change.** The new `DeclaredSchemaResolver` lives in `sm8-platform` (deployment wiring); the `SourceResolver` trait and `ResolvedSource` ADT already exist in `sm8-core/.../engine/SourceResolver.scala`. The handler lives in `sm8-platform` and binds additively in `HttpTransport` per the ADR-0012-a pattern.

**R2-1 / R3-1 / Path-1 resolutions**: see the Grilling log. Key insight from rounds 2–3: `QueryBuilder.build` requires a `SourceResolver`; the only existing impl is `SparkSourceResolver` (Spark IO); both β (drop `QueryBuilder.build`) and β' (drop it AND `rollupDecision`) weakened the user story. Path 1 / 1a restores the full r1 motivation by adding `DeclaredSchemaResolver` to the platform — a zero-core-change, zero-Spark solution that unblocks `QueryBuilder.build` + `RollupRewriter.rewrite` inside validate.

**Drift caveat, stated honestly (seedling r5 #2)**: validate does NOT catch warehouse-side schema drift (dropped column, widened type) — and neither does today's execute path in a typed way. `EngineError.SourceSchemaChanged` exists (EngineError.scala:109) and `ModelValidator.validateAgainstSchema` exists (ModelValidator.scala:323) but has ZERO production callers — a warehouse-side column drop currently surfaces as an untyped Spark `AnalysisException` at execute. This is the pre-existing DocumentedButNotWired pattern, not something validate introduces or fixes. A separate follow-up should wire `validateAgainstSchema` into the connector's execute path (tracked as Q8 in the build-ticket questions).

**D2 — Response shape.**

Reuse existing typed ADTs (per clover S1 — do not invent a "plan summary"):

```
ValidationOutcome {
  modelVersion: Int                             // the Model version that was validated
  rollupDecision: RollupRewriteResult           // sealed: Rewritten(plan, rollupName) | Unchanged(refusal)
  decisionHints: Option[DecisionHints]         // broadcast/skew if plugins installed (None by default)
  engineSelection: String                      // the engine that WOULD serve
  errors: List[EngineError]                    // per-stage failures (empty on success)
  tablesTouched: List[String]                  // physical tables the execute WOULD read
                                               // (= model.source + model.joins.map(_.rightModel))
}
```

Serde: Jackson + `DefaultScalaModule` (per QueryService.scala:174) covers the sealed `RollupRewriteResult` + `RollupRewriteRefusal` hierarchies, plus plain fields.

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

- [ ] **Q1**: Exact response JSON wire shape for `ValidationOutcome{modelVersion, rollupDecision: RollupRewriteResult, decisionHints: Option[DecisionHints], engineSelection: String, errors: List[EngineError], tablesTouched: List[String]}` — sealed-trait serde for `RollupRewriteResult`/`RollupRewriteRefusal` via Jackson + `DefaultScalaModule` (QueryService.scala:174 pattern), per clover S9.
- [ ] **Q2**: DeclaredSchemaResolver details — does it synthesize `ResolvedSource.Scan(source, schema)` from `Model.dimensions`/`measures` field names alone, or does it also need data types? (Dimensions carry `SealedDataType`; measures don't. Proposal: synthesize from dimensions + whatever the relop needs.) Also: `EngineIdentity` parameter — validate has no real identity; pass a synthetic constant?
- [ ] **Q3**: Metrics counters — confirmed (clover S10): `recordValidationSuccess(outcome)` + `recordValidationFailure(stage)` on the existing `MetricsSink` seam (MetricsSink.scala:46-51).
- [ ] **Q4**: Test fixture: `AuditStub` + `AuditStubNoOpContractSpec` pattern. Validate runs no hooks, so the safety pin is structural: a test that registers the stub plugin at every Pre/Post hook slot and asserts each `fires.get` is zero after validate. Plus: QueryBuilder-build-integration-trap guard — the fixture model must declare only rollup-covered dimensions, AND the spec must assert the Unchanged-collapse case explicitly (rollupDecision collapses to `Unchanged`, never silently returns zero rows — verified RollupRewriter.scala:534, 396).
- [ ] **Q5 (new)**: DeclaredSchemaResolver placement — `sm8-platform/.../query/` beside the service, or `connectors/spark-connector/`? (Argument for platform: it's deployment wiring independent of Spark; argument for connector: SourceResolver impls have historically lived connector-side. Recommend platform per the zero-Spark property.)
- [ ] **Q6 (new, seedling r5 #4)**: `EngineIdentity` synthetic constant for validate — pin a value (proposal: `("validate", "0.0", "sm8-validate/0.1.0")`) so validate invocations are identifiable in production logs and any future `describe_model` output.
- [ ] **Q7 (new, maple r5 #4)**: `DeclaredSchemaResolver.resolveModel` policy — v1 must choose: (a) override with `Right(SourceRef.ByName(name))` to support multi-model joins in validate, or (b) document the default `UnsupportedCapability` refusal as v1 scope (single-model validate only). Either is recoverable; the ticket must pick before implementation.
- [ ] **Q8 (new, seedling r5 #2)**: the drift-backstop gap — `validateAgainstSchema` has zero production callers. NOT validate's job to fix, but the build ticket must NOT claim drift coverage; a separate follow-up issue should wire `validateAgainstSchema` into the connector execute path.

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
- `QueryValidationServiceSpec.scala` — write-safety pin (AuditStub pattern, stub plugins at every Pre/Post slot asserted zero) + rollup-routing tests (Rewritten AND Unchanged-collapse cases — the latter is a valid outcome, not a failure; per seedling r5 #3) + error-aggregation tests (~200 LOC)

(v1 carries serde for `RollupRewriteResult` + `DecisionHints` + `EngineError` — Jackson + `DefaultScalaModule` per QueryService.scala:174, restored per Path 1/1a; routing-preview fields ARE in the v1 response again.)

## Grilling log

- **Round 1** (2026-09-12): clover (decision completeness) + sunflower (architecture/seam).
  - clover: 4 BLOCKING (production path doesn't use in-tree Pipeline — Context.stop premise wrong; D4 mechanism unsafe; cache PreExecute effects; ADR-012-a precedent points to separate service) + 4 SHOULD-FIX (response-shape rename, MCP demotion, rate-limit question, LOC estimate).
  - sunflower: 3 BLOCKING/SHOULD-FIX (no Plan stage exists; routing decision ALREADY in core — nothing to extract; Context.stop doesn't short-circuit default hooks) + verification notes (Cube endpoint list audit-proofed; AuditStub test pattern confirmed).
  - All BLOCKINGs addressed in r2: D1 reframed to direct-call composition; D4 contract restructured; D3 reversed to separate service per ADR-0012-a; MCP demoted; rate-limit added; LOC committed.
- **Round 2** (same pair): clover NEEDS-REVISION (B5: QueryBuilder.build requires SourceResolver whose only impl is SparkSourceResolver — Spark IO breaks the by-construction guarantee; NoopSourceResolver referenced but absent; option α needs a second core change) + 5 SHOULD-FIX (Option[DecisionHints], engineSelection type, LOC 350-450/3 files, call-chain naming, serde pattern, MetricsSink counters). sunflower NEEDS-REVISION (R2-1: same fork, independently verified NoopSourceResolver absent + Model.declaredSchema absent; R2-3: ADR-0032 gloss too thin).
  - All addressed in r3 (option β: drop QueryBuilder.build, keep rollupDecision).
- **Round 3** (same pair): clover READY-TO-OPEN (13/13 verifications, 2 nits: LOC body inconsistency + wrong spec name). sunflower NEEDS-REVISION (R3-1 BLOCKING: β has an internal contradiction — RollupRewriter.rewrite requires a RelOp; the only (Model, QueryRequest) → RelOp producer is QueryBuilder.build; so dropping QueryBuilder but keeping rollupDecision was incoherent). R3-2 NICE: dbt-metricflow path unverifiable from that session.
- **Round 4 resolution (out-of-band, user)**: user closed the loop with two strategic questions from clover r4 — "is v1 well-formedness-only acceptable, or does r1 motivation require committing to one of the v2 RelOp-fork candidates now?" + "is the new endpoint distinct enough from existing `ManifestValidator`?". User picked **Path 1: commit the RelOp fork now; routing preview ships in v1**. Among the three forks (1a/1b/1c), user picked **1a (`DeclaredSchemaResolver`)**: new platform-side class implementing the existing core `SourceResolver` trait, returning the Model's own declared field set as the resolved schema. Zero core change, zero Spark. Drift detection stays at execute time. Grilling log + standing prefs + D2 + Q-list all rewritten for the 1a resolution (r5).

## Notes

- This ticket is a **decision** ticket, not a build ticket. Resolving D1–D6 + Q1–Q4 produces the implementation spec.
- The competitive motivation is secondary: even with zero competitor pressure, the Execute-stage write side effects justify validate. Both arguments hold independently.
- Layer map: **zero core change, zero connector change; one new platform service + bind line.** Verified by sunflower against RollupRewriter.scala, Pipeline.scala, Context.scala, QueryService.scala, ModelService.scala, SparkEngineProvider.scala.

## Decisions so far

- 2026-09-12: Ticket opened; r1 grilled (4+3 BLOCKINGs) → r2 resolved all round-1 findings → r2 grilled (1 BLOCKING: SourceResolver fork) → **r3 resolved via option β** (drop `QueryBuilder.build` from v1) → r3 grilled (sunflower introduced R3-1 BLOCKING: β had an internal contradiction — `RollupRewriter.rewrite` requires a `RelOp`, whose only producer is `QueryBuilder.build`) → **r4 resolved via option β'** (drop both `QueryBuilder.build` AND `rollupDecision` from v1; v2 routing preview deferred to a named core change; v1 = well-formedness only). Awaiting round-4 confirmation + go/no-go on the build ticket.
