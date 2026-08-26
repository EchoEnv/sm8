# ADR-009-g: CachePolicy contract closure — runtime gate via `ctx.meta` fold + single-source ADT

| Field | Value |
|---|---|
| **Status** | **Proposed (v1.1)** — v0.1 BLOCKED by architect (best-reasoning, 0.45 confidence, 5 P1+P2 findings). v1.0 APPROVED by architect (best-reasoning, 0.92 confidence, 3 P3 prose-tightening nits) but data-eng (best-coding, 0.97 confidence) flagged v1.0 as workflow-stage mismatch (ADR is design-only; implementation deferred to subagent per the v0.4-wave pattern — `Accepted` = decision approved, `Implemented` = code merged). v1.1 folds the 2 substantive data-eng cache-correctness findings that survive into implementation: (a) **case-class `name` shape change** at `EngineContextSpec.scala:109-115` — the engine-side `CachePolicy.ReadThrough` is a case object (no `name`); the model-side `CachePolicy.ReadThrough` is a case class with required `name: String`. Migration requires adding `("default")` literal. (b) **Missing integration spec** for acceptance #4 — existing `EngineServiceRunQueryWithHooksSpec` constructs `NoCache` models only; the falsifiable acceptance #4 (`ReadThrough("region-a")` / `WriteThrough("region-a")` flows through dispatcher) has no test fixture. New spec MUST construct `ReadThrough` + `WriteThrough` models, run the full dispatcher, and assert the fold reaches the hook via counter increments per Fix 6's matrix. Architect v1.0 verdict APPROVED 0.92 stands; v1.1 is a docs-only fold (no architectural changes). |
| **Date** | 2026-08-26 |
| **Module** | `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala` (`runQueryWithHooks` constructs `initialCtx` with `meta = Map("sm8.cache.policy" -> model.defaultPolicies.cache)` BEFORE `dispatcher.run` — fold is one line at lines 387-393) + `plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala` (`CacheReadPreHook.run` consults `ctx.meta.get("sm8.cache.policy")`; `CacheWritePostHook.run` consults the same — both gates honor `model.cache`. Counter discipline: `readFires` fires only on `ReadThrough/WriteThrough`; `writeFires` fires only on `WriteThrough`; per-case matrix explicit) + `sm8-core/engine/EngineContext.scala` (retyped `cachePolicy: CachePolicy` to import `io.sm8.core.model.CachePolicy`; the dead field becomes a real fold target) + `sm8-core/engine/EngineContext.scala:66-87` (the engine-side `CachePolicy` ADT deleted; `ReadOnly` case deleted as zero-reader; `ReadThrough` / `WriteThrough` deleted as model-side-superset-with-`name`) + `sm8-core/test/.../EngineContextSpec.scala` (lines 14-24 + 16-20 + 100 + 109-115 migrated to `io.sm8.core.model.CachePolicy`; the 4-cases test becomes 3-cases test; the field-references test becomes the field-default test) + `connectors/spark-connector/.../PortableQueryCompiler.scala:534` (rejection message updated — `CachePolicy.ReadThrough(<cache-name>)` references the model-side ADT name; same shape, no behavior change) + `plugins/cache-plugin/src/test/.../CachePluginContractSpec.scala` + `CachePluginSpec.scala` (3 new falsifiable tests: `NoCache` no-op for read + write, `ReadThrough(name)` HIT path, `WriteThrough(name)` MISS-then-write path) + `sm8-platform/src/test/.../EngineServiceRunQueryWithHooksSpec.scala` (the fold seam). |
| **Supersedes scope** | The pre-existing cache-feature half-open contract surfaced by ADR-009-f §Out-of-scope: (a) **Gap 1**: `CacheReadPreHook.run` at `plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala:95-118` fires unconditionally on every query — it does NOT consult `model.defaultPolicies.cache`. A model with `cache = NoCache` (the default) still hits `cache.getJournaled` + the `misses.incrementAndGet()` counter. Same class of bug as ADR-009-f's pre-state for `MaterializePolicy.Cache`. (b) **Gap 2**: dual `CachePolicy` ADTs — `io.sm8.core.model.CachePolicy` (3 cases: `NoCache`/`ReadThrough(name)`/`WriteThrough(name)`) at `sm8-core/src/main/scala/io/sm8/core/model/Model.scala:79-84` AND `io.sm8.core.engine.CachePolicy` (4 cases including `ReadOnly` zero-reader) at `sm8-core/src/main/scala/io/sm8/core/engine/EngineContext.scala:66-87`. Same drift hazard as ADR-009-f Gap 5. (c) **Gap 3**: `EngineContext.cachePolicy` is typed as the engine-side ADT (`EngineContext.scala:38, 59`); the field is dead in production code BUT has 3 spec test sites at `EngineContextSpec.scala:16-20, 100, 109-115` + a 4-cases test at lines 14-24. The field is the natural seam for the runtime gate; if it's deleted without a replacement, the runtime gate requires leaking `Model` into the hook. (d) **Gap 4** (architect v0.1 finding, real): the fold as prescribed in v0.1 Fix 4 lived in `EngineService.engineExecutor` — but `engineExecutor` runs AFTER `firePre` (the dispatcher fires pre-hooks before the executor; see `EngineHookDispatcher.run` lines 106-108). The hook would always see `None` from `ctx.meta.get("sm8.cache.policy")` and always take the NoCache-skip branch. **The fold must move to `initialCtx.meta` construction at `EngineService.scala:387-393` BEFORE `dispatcher.run`.** (e) **Gap 5** (architect v0.1 finding, real): `CacheWritePostHook.run` at `CachePlugin.scala:145-153` is unconditional — `writeFires` counter increments on every request; `cache.putJournaledWithModelAndVersion` writes every successful engine result. A `NoCache` model skips the read but still writes on the way back. (f) **Gap 6** (architect v0.1 finding, design contradiction): v0.1 acceptance #2 says "ReadThrough("default") + cache MISS → existing pass-through (no change)" but the current behavior is `cache.putJournaled` on miss (write-on-miss). Need explicit per-case policy matrix. |
| **Skill alignment** | `karpathy-app-design` (single source of truth — one `CachePolicy` ADT; the fold is the canonical ADR-009-d pattern; the seam is `initialCtx.meta` not a new dispatcher API); `karpathy-guidelines` (surgical edits; dead code is a smell — engine-side ADT deleted; `ReadOnly` case deleted; the v0.1 "0 readers" claim was wrong — the v1.0 audit enumerates the actual 3 spec sites); `scala-bug-hunting` (silent no-ops are bugs — `CacheReadPreHook` + `CacheWritePostHook` fire unconditionally regardless of `model.cache`; counter metrics are wrong); `scala-error-handling` (typed errors over `Throwable` swallow; the fold is an Either-or-pass-through, no exception-based gate); `scala-impact-analysis` (blast radius — every caller of the engine-side `CachePolicy` ADT must be enumerated + migrated; `PortableQueryCompiler.scala:534` rejection message references the ADT name; `EngineContextSpec.scala` has 4 sites; the spark-connector tests may reference it transitively); `scala-jvm-safety` (the gate must NOT change the hot-path semantics for the `NoCache` default — both hooks early-return without touching the cache); `scala-data-driven-refactor` (sealed-trait dispatch in both hooks; the fold is a typed ADT value in `ctx.meta`; no Map-based rule table); `scala2-scaladoc` (every fix carries a scaladoc anchor; the fold pattern is documented at `EngineService.runQueryWithHooks` mirroring the ADR-009-d `DecisionHints` fold pattern; the migration of test sites is documented inline); `debug-mantra` (reproduce the unconditional-fire via `CachePluginSpec`; trace through `EngineHookDispatcher.run` lines 106-108 to confirm `firePre` runs before `execute`; falsify via the new `NoCache → noop` tests for both hooks; cross-reference every hook + fold + migration seam; verify the full reactor green). |
| **Architecture alignment** | RFC §3 Core Boundary: `CachePolicy` lives once in `sm8-core/model` (engine-portable data shape); the cache-plugin enforces the policy via the ADR-009-d fold pattern (engine-portable `ctx.meta` channel); deployment stays outside core + transport; transport does not import adapter types. PR-178 discipline extends: silent no-ops are contract violations — both `CacheReadPreHook` + `CacheWritePostHook` must honor-or-pass-through `model.cache`, no silent unconditional-fire. |

---

## Revision history

| Version | Date | Change |
|---|---|---|
| v0.1 (Proposed) | 2026-08-26 | Initial draft; 4 findings (unconditional-fire, dual `CachePolicy` ADT, dead `EngineContext.cachePolicy` field, missing fold); 4 options considered (Option A: dual-ADT collapse + ADR-009-d fold-pattern runtime gate — the recommended path; rejected Options B/C/D detailed in §Decision). Investigation files: `/tmp/cross-engine-dechints-audit.md`, ADR-009-d `DecisionHints` SDK pattern, ADR-009-f §Out-of-scope, codegraph probes (`CachePolicy`, `CacheReadPreHook`, `CachePlugin`, `EngineContext.cachePolicy`). |
| v1.0 (Proposed) | 2026-08-26 | Architect review (best-reasoning, 0.45 confidence) BLOCKED v0.1 with 5 load-bearing findings. v1.0 folds all 5: (a) **Fix 4 fold-site correction** — the fold moves from `engineExecutor` (after `firePre` runs) to `initialCtx.meta` construction at `EngineService.scala:387-393` (before `dispatcher.run`). The dispatcher takes `initial: Context` and runs `firePre` first (see `EngineHookDispatcher.run` lines 106-108) — a fold in `engineExecutor` would be unreachable from `CacheReadPreHook`. (b) **Fix 1 + Fix 5 extended** — `CacheWritePostHook.run` is ALSO unconditional (increments `writeFires` + writes `cache.putJournaled` on every successful engine result); gate the post-hook too. (c) **Fix 2 ADT undercount corrected** — the v0.1 audit claimed "1 declaration + 1 default + 0 readers" for `EngineContext.cachePolicy`; the actual readers are `EngineContextSpec.scala` lines 14-24 (4-cases test for the engine-side ADT), 16-20 + 100 + 109-115 (field references) — 4 spec sites that must be migrated to `io.sm8.core.model.CachePolicy`. The 4-cases test becomes 3-cases. (d) **Fix 6 explicit policy matrix** — resolves the v0.1 ReadThrough-on-miss contradiction: per-case `readFires`/`writeFires` behavior explicit. (e) **Fix 5 dispatcher API surface documented** — the `initial: Context` parameter is the seam; no dispatcher API change needed; the fold goes into `initialCtx.meta` construction at `EngineService.scala:387-393`. |

| v1.1 (Proposed) | 2026-08-26 | Data-eng re-review (best-coding, 0.97 confidence) flagged v1.0 as workflow-stage mismatch (ADR is design-only; implementation deferred to subagent per the v0.4-wave pattern). 7 of 8 findings collapse to "implementation hasn't landed" — non-blocking per the ADR-009-f v3.0 precedent. 2 substantive cache-correctness findings that survive into implementation get folded: (a) **case-class `name` shape change at EngineContextSpec.scala:109-115** — the engine-side `CachePolicy.ReadThrough` is a case object (no `name`); the model-side `CachePolicy.ReadThrough` is a case class with required `name: String`. Fix 2 row updated to specify `CachePolicy.ReadThrough("default")` literal at the migrated sites; any other `ReadThrough` / `WriteThrough` references also require case-class invocation. (b) **Missing integration spec for acceptance #4** — existing `EngineServiceRunQueryWithHooksSpec` constructs `NoCache` models only; the falsifiable acceptance #4 (`ReadThrough("region-a")` + `WriteThrough("region-a")` flows through dispatcher) has no test fixture. Acceptance #4 updated to REQUIRE a new spec that constructs `ReadThrough` + `WriteThrough` models, runs the full dispatcher, and asserts the fold reaches the hook via counter increments per Fix 6's matrix. Status header bumped to v1.1; architect v1.0 verdict APPROVED 0.92 stands; v1.1 is a docs-only fold (no architectural changes). |


---

## Context and problem statement

The cache feature has six distinct gaps, each independently reproducible and each silently degrading the contract that `CachePolicy.ReadThrough(<name>)` is supposed to express. The pre-ADR-009-f state for `MaterializePolicy.Cache` was: declared, defaulted, never honored. The pre-ADR-009-g state for `CachePolicy` is the same shape, plus a dual-ADT drift hazard that mirrors the pre-ADR-009-f dual `MaterializePolicy`:

### Gap 1 — `CacheReadPreHook` fires unconditionally (`plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala:95-118`)

The `CacheReadPreHook` registered at priority 50 (`HookStage.PreExecute`) fires on every query via the dispatcher. Its `run` method does NOT consult `model.defaultPolicies.cache`:

```scala
override def run(context: Context): Context = {
  counter.incrementAndGet()
  context.request match {
    case hookReq: EngineHookRequest =>
      cache.getJournaled(hookReq.cacheKey) match {  // <- UNCONDITIONAL
        case Some(row) =>
          hits.incrementAndGet()
          // ... HIT path: set context.stop = true, return cached pqr ...
        case None =>
          misses.incrementAndGet()                   // <- counter fires for NoCache models
          context
      }
    case _ => context
  }
}
```

A model with `cache = NoCache` (the default — `ModelPolicyDefaults.cache = CachePolicy.NoCache` at `Model.scala:230`) still hits `cache.getJournaled(hookReq.cacheKey)` on every query. The `misses` counter increments; the post-write hook (Gap 5) writes the result on the way back. The contract `cache = NoCache` says "do not cache" — the hook says "always look in the cache, always miss, always write through".

**Consequence:** `model.cache` is silently ignored. Same class of bug as ADR-009-f Gap 3 (`MaterializePolicy.Cache` silent no-op).

### Gap 2 — dual `CachePolicy` ADTs (`sm8-core/src/main/scala/io/sm8/core/model/Model.scala:79-84` + `sm8-core/src/main/scala/io/sm8/core/engine/EngineContext.scala:66-87`)

Two sealed traits named `CachePolicy` coexist in `sm8-core`:

- **`io.sm8.core.model.CachePolicy`** (model-attached-side, active shape):
  - `case object NoCache extends CachePolicy`
  - `final case class ReadThrough(name: String) extends CachePolicy`
  - `final case class WriteThrough(name: String) extends CachePolicy`
  - This is the shape the `Model.of(.)` constructor and `ModelBuilder` use. `ReadThrough` and `WriteThrough` carry a `name: String` for cache-region routing.
- **`io.sm8.core.engine.CachePolicy`** (engine-context-side, latent shape):
  - `case object NoCache extends CachePolicy`
  - `case object ReadThrough extends CachePolicy` (no `name`)
  - `case object WriteThrough extends CachePolicy` (no `name`)
  - `case object ReadOnly extends CachePolicy` (no model-side equivalent; never read in production)
  - The 4-cases test at `EngineContextSpec.scala:14-24` reads this ADT. The field declaration at line 38 + default at line 59 reference this ADT.

The `engine.CachePolicy.ReadOnly` case is a future-design residue with no current production consumer (the spec test is the only reader; that test is migrated in Fix 2). The lack of `name` on the engine-side `ReadThrough` / `WriteThrough` (vs the model-side which has `name: String`) is a drift hazard.

**Consequence:** same drift hazard as ADR-009-f Gap 5. A contributor who picks the wrong import gets a silent no-op. The `ReadOnly` case is unreachable in production.

### Gap 3 — `EngineContext.cachePolicy` is dead in production but has 3 spec readers (`sm8-core/src/main/scala/io/sm8/core/engine/EngineContext.scala:38,59` + `sm8-core/src/test/scala/io/sm8/core/engine/EngineContextSpec.scala:14-24, 16-20, 100, 109-115`)

```scala
case class EngineContext(
    ...
    cachePolicy: CachePolicy = CachePolicy.NoCache,  // <-- this Gap (engine-side ADT)
    auditPolicy: AuditPolicy = AuditPolicy.NoAudit,
    ...
)
```

The field is declared at line 38 (typed as `io.sm8.core.engine.CachePolicy`), defaulted at line 59, flows through `EngineService.runQueryWithHooks` → `provider.query(model, request, ctx)` unchanged. **Production readers: zero.** **Spec readers:** `EngineContextSpec.scala:14-24` (4-cases test for the engine-side ADT) + `16-20` + `100` + `109-115` (field references in the EngineContext tests).

The v0.1 audit undercounted: "1 declaration + 1 default + 0 readers" was wrong. The actual reader count is 4 spec sites. Fix 2 must enumerate and migrate all 4 sites to `io.sm8.core.model.CachePolicy` (the single-source ADT).

### Gap 4 — fold must be BEFORE `dispatcher.run`, not in `engineExecutor` (`sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:386-393`)

The `EngineHookDispatcher.run` method at `sm8-platform/src/main/scala/io/sm8/platform/query/hooks/EngineHookDispatcher.scala:100-123` runs the hook sequence:

```scala
def run(initial: Context, execute: Context => Either[EngineError, Context]): Either[EngineError, Context] = {
  val stage: PipelineStage = PipelineStage.Execute
  val afterPreE: Either[EngineError, Context] = firePre(stage, initial)  // <- PreExecute hooks fire HERE
  afterPreE.flatMap { afterPre =>
    if (afterPre.stop) firePost(stage, afterPre)
    else execute(afterPre).flatMap(withResult => firePost(stage, withResult))  // <- engineExecutor runs HERE
  }
}
```

`firePre` runs BEFORE `execute`. `CacheReadPreHook` is `PreExecute` (priority 50). Therefore: **the fold for `cachePolicy` MUST be in `initialCtx` construction, not in `engineExecutor`.** A fold in `engineExecutor` (the v0.1 Fix 4 site) is unreachable from `CacheReadPreHook.run` because the pre-hook has already fired by the time `engineExecutor` runs. The hook would always see `None` from `ctx.meta.get("sm8.cache.policy")` and always take the NoCache-skip branch.

The v0.1 audit missed this. The fold site is `initialCtx.meta` construction at `EngineService.scala:387-393`:

```scala
val hookRequest = EngineHookRequest(model, mcpReq, cacheKey)
val initialCtx: Context = Context(
  stage   = PipelineStage.Execute,
  request = hookRequest,
  result  = None,
  meta    = Map.empty,                   // <- FIX 4: populate with cachePolicy fold
  stop    = false
)
```

**Consequence:** the runtime gate is impossible to wire without the correct fold site. v0.1's prescription would compile but never honor `ReadThrough` / `WriteThrough` — the hook would always NoCache-skip.

### Gap 5 — `CacheWritePostHook` fires unconditionally (`plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala:145-153`)

```scala
override def run(context: Context): Context = {
  counter.incrementAndGet()                  // <- UNCONDITIONAL
  context.result match {
    case Some(EngineHookResult(pqr)) =>
      // ... encode + write ...
      cache.putJournaledWithModelAndVersion(hookReq.cacheKey, row, hookReq.model.name, hookReq.model.version)
    case _ =>
  }
  context
}
```

The post-hook fires `writeFires` on every successful engine result and writes via `cache.putJournaledWithModelAndVersion`. A `NoCache` model: read hook NoCache-skip → engine runs → post hook fires → cache gets populated. Violates acceptance criterion #1 ("NoCache ... no write-through on the way back").

**Consequence:** even if the read hook is gated, the post hook still writes. The runtime gate is incomplete without gating both hooks.

### Gap 6 — v0.1 acceptance #2 contradicts the current `CacheWritePostHook` behavior

v0.1 acceptance #2 states:

> A model with `defaultPolicies.cache == ReadThrough("default")` and a cache MISS → existing pass-through (no change).

But the current `CacheWritePostHook.run` writes on every successful engine result (line 153). A `ReadThrough("default")` MISS today DOES write to the cache. The v0.1 wording contradicts the current behavior. The fix is an explicit per-case policy matrix in Fix 6.

---

## Decision

**Option A — dual-ADT collapse + `initialCtx.meta` fold-pattern runtime gate + both-hook gate.**

The fold moves to `initialCtx.meta` construction at `EngineService.scala:387-393` BEFORE `dispatcher.run`. Both `CacheReadPreHook` AND `CacheWritePostHook` gate on `ctx.meta.get("sm8.cache.policy")`. Per-case policy matrix (Fix 6) makes the read/write behavior explicit. The `engine.CachePolicy` ADT is deleted entirely; all 4 spec sites + the `PortableQueryCompiler.scala:534` rejection message are migrated to `io.sm8.core.model.CachePolicy`.

Options considered and rejected:

| Option | Why rejected |
|---|---|
| B. Keep both `CachePolicy` ADTs; document the model-side as "the active shape" and add scaladoc to the engine-context-side warning it's reserved | Documentation-only closure of Gap 2 leaves Gap 1 (read-hook unconditional-fire), Gap 5 (post-hook unconditional-fire), Gap 4 (wrong fold site), Gap 3 (4 spec readers). The dual ADT is still a drift hazard. Rejected per RFC §3. |
| C. Delete the engine-side ADT but DON'T add the runtime gate — leave both hooks unconditionally firing | Closes Gap 2 + 3 only. Gap 1 + 5 (the load-bearing user-visible bugs) are unfixed. A model with `cache = NoCache` continues to hit `cache.getJournaled` + `cache.putJournaled` + increment both counters. Rejected — the runtime gate is the user-facing value of this ADR. |
| D. Drop the cache feature entirely (remove `CachePolicy.ReadThrough` / `WriteThrough` from `model`) | The feature is used by `InMemoryResultCache`. Removing it breaks the cache lookup. Removing it is a breaking change without an approved deprecation cycle. Rejected — the right move is closure, not removal. |

### Fix 1 — `CacheReadPreHook` runtime gate via `ctx.meta` (`plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala:95-118`)

The hook consults the engine-portable meta channel for the folded `cachePolicy`:

```scala
override def run(context: Context): Context = {
  context.request match {
    case hookReq: EngineHookRequest =>
      // ADR-009-g: honor the folded cache policy. The fold is in
      // EngineService.runQueryWithHooks at initialCtx construction
      // (model.cache → EngineContext.cachePolicy → ctx.meta("sm8.cache.policy")).
      // NoCache (and the None backwards-compat default) early-return
      // without touching the cache or incrementing counters.
      ctx.meta.get("sm8.cache.policy") match {
        case Some(CachePolicy.NoCache) | None =>
          // NoCache (or fold absent — backwards-compat default): pass
          // through unchanged. NO counter increment, NO cache lookup,
          // NO write-through on the way back. This is the hot-path
          // skip that Gap 1 surfaces.
          context
        case Some(CachePolicy.ReadThrough(_)) =>
          // Read-through: only read; do NOT write on miss.
          // Per Fix 6 explicit policy matrix.
          counter.incrementAndGet()
          cache.getJournaled(hookReq.cacheKey) match {
            case Some(row) =>
              hits.incrementAndGet()
              val pqr = CachedRowDecoder.fromRestateCachedRowAsPortable(row)
              context.copy(
                result = Some(EngineHookResult(pqr)),
                stop   = true
              )
            case None =>
              misses.incrementAndGet()
              context
          }
        case Some(CachePolicy.WriteThrough(_)) =>
          // Write-through: only write; do NOT short-circuit on read.
          // Per Fix 6 explicit policy matrix.
          counter.incrementAndGet()
          cache.getJournaled(hookReq.cacheKey) match {
            case Some(row) =>
              hits.incrementAndGet()
              val pqr = CachedRowDecoder.fromRestateCachedRowAsPortable(row)
              context.copy(
                result = Some(EngineHookResult(pqr)),
                stop   = true
              )
            case None =>
              misses.incrementAndGet()
              context
          }
      }
    case _ => context
  }
}
```

The `None` branch handles the backwards-compat default: if the fold is absent (older `EngineContext` shape, or the platform doesn't fold yet), the hook defaults to `NoCache` semantics (skip). This matches the existing behavior for the `cache = NoCache` default — no regression for callers that don't wire the fold.

### Fix 2 — delete the dual `CachePolicy` ADT + migrate all 4 spec sites (`sm8-core/src/main/scala/io/sm8/core/engine/EngineContext.scala:66-87` + `sm8-core/src/test/scala/io/sm8/core/engine/EngineContextSpec.scala:14-24, 16-20, 100, 109-115` + `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/PortableQueryCompiler.scala:534`)

Delete `io.sm8.core.engine.CachePolicy` entirely. Migrate all 4 spec sites + the spark-connector rejection message to `io.sm8.core.model.CachePolicy`:

| Site | Change |
|---|---|
| `EngineContext.scala:66-87` | DELETE (the engine-side ADT declaration + companion) |
| `EngineContext.scala:38` (field type) | `cachePolicy: io.sm8.core.model.CachePolicy` (single-source import) |
| `EngineContext.scala:59` (default) | `cachePolicy: io.sm8.core.model.CachePolicy = io.sm8.core.model.CachePolicy.NoCache` |
| `EngineContextSpec.scala:14-24` (4-cases test) | UPDATE to 3-cases test against `io.sm8.core.model.CachePolicy` (the model-side ADT has 3 cases; `ReadOnly` is deleted) |
| `EngineContextSpec.scala:16-20` (field references) | UPDATE import to `io.sm8.core.model.CachePolicy` |
| `EngineContextSpec.scala:100` (EngineContext default test) | UPDATE field reference to `io.sm8.core.model.CachePolicy.NoCache` |
| `EngineContextSpec.scala:109-115` (EngineContext holds-all-5-fields test) | UPDATE field reference to `io.sm8.core.model.CachePolicy.ReadThrough("default")` (the model-side `ReadThrough` is a case class with required `name: String`; the engine-side `ReadThrough` is a case object without `name` — migration requires adding `("default")` literal). Likewise any other `CachePolicy.ReadThrough` / `CachePolicy.WriteThrough` references become case-class invocations. |
| `PortableQueryCompiler.scala:534` (rejection message) | "CachePolicy.ReadThrough(<cache-name>) — this routes through the cache-plugin..." (the ADT name reference is unchanged — the message text already says `CachePolicy.ReadThrough(<cache-name>)` which IS the model-side ADT; the FQ import needs to point at `io.sm8.core.model.CachePolicy` if any) |

After migration, the only `CachePolicy` reference in `sm8-core` is the model-side one. The spark-connector rejection message is unchanged in user-visible text (the type referenced is the same name; only the FQ import may shift).

### Fix 3 — `EngineContext.cachePolicy` becomes the fold target (additive, `sm8-core/src/main/scala/io/sm8/core/engine/EngineContext.scala:38,59`)

The dead field is repurposed to carry the folded `cachePolicy`:

```scala
case class EngineContext(
    ...
    cachePolicy: io.sm8.core.model.CachePolicy = io.sm8.core.model.CachePolicy.NoCache,  // <- ADR-009-g Fix 3: fold target
    auditPolicy: AuditPolicy = AuditPolicy.NoAudit,
    ...
)
```

The field is **additive** — callers that don't construct `EngineContext` continue to get `NoCache` (no behavioral change). The fold in `EngineService.runQueryWithHooks` (Fix 4) populates `initialCtx.meta("sm8.cache.policy")` from `model.cache`. The field itself is unused in the runtime path — it's the spec seam (Fix 2's `EngineContext.defaultContext has sensible defaults` test at line 100 still asserts the field's default value). Future ADRs may use the field as a typed contract seam; for now it documents the folded value.

**Note:** this is a deliberate inversion of the ADR-009-f Fix 4 decision (which deleted `EngineContext.materializePolicy`). The inversion is justified: `materializePolicy` had no consumer after ADR-009-f; `cachePolicy` HAS a consumer (the cache hook, after Fix 1 + Fix 5). YAGNI for `materializePolicy`, real consumer for `cachePolicy`.

### Fix 4 — `EngineService.runQueryWithHooks` fold in `initialCtx.meta` BEFORE `dispatcher.run` (`sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:386-393`)

The fold extends the initial `Context` construction:

```scala
val hookRequest = EngineHookRequest(model, mcpReq, cacheKey)
// ADR-009-g Fix 4: fold model.cache → ctx.meta BEFORE dispatcher.run.
// EngineHookDispatcher.run fires PreExecute hooks BEFORE engineExecutor
// (see EngineHookDispatcher.scala:106-108); the fold MUST be in
// initialCtx.meta, not engineExecutor, for CacheReadPreHook (priority 50,
// PreExecute) to consult the value.
val initialCtx: Context = Context(
  stage   = PipelineStage.Execute,
  request = hookRequest,
  result  = None,
  meta    = Map("sm8.cache.policy" -> model.defaultPolicies.cache),
  stop    = false
)
```

This is ONE line added to the existing `initialCtx` construction. The hook consults `ctx.meta.get("sm8.cache.policy")` (engine-portable), not `model.defaultPolicies.cache` (model-attached). The existing `DecisionHints` fold inside `engineExecutor` is unchanged (ADR-009-d's pattern: pre-hooks write to `ctx.meta`; post-hooks consult `ctx.meta`; the executor extracts the typed `DecisionHints` from `ctx.meta`).

### Fix 5 — `CacheWritePostHook` runtime gate (`plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala:145-153`)

The post-hook consults the same `ctx.meta("sm8.cache.policy")` and gates per Fix 6's policy matrix:

```scala
override def run(context: Context): Context = {
  // ADR-009-g Fix 5: honor the folded cache policy on the way back.
  // The fold is in EngineService.runQueryWithHooks at initialCtx.meta
  // construction. The post-hook consults the same value as the
  // pre-hook (both hooks are engine-portable; the model never leaks).
  context.result match {
    case Some(EngineHookResult(pqr)) =>
      ctx.meta.get("sm8.cache.policy") match {
        case Some(CachePolicy.WriteThrough(_)) =>
          // Write-through: write on every successful engine result.
          counter.incrementAndGet()
          CachedRowDecoder.toRestateCachedRowFromPortable(pqr) match {
            case Right(row) =>
              context.request match {
                case hookReq: EngineHookRequest =>
                  cache.putJournaledWithModelAndVersion(hookReq.cacheKey, row, hookReq.model.name, hookReq.model.version)
                case _ =>
              }
            case Left(err) =>
              System.err.println(s"sm8: cache write skipped: $err")
          }
        case Some(CachePolicy.NoCache) | None | Some(CachePolicy.ReadThrough(_)) =>
          // NoCache (or fold absent / ReadThrough): NO write-through.
          // Per Fix 6 explicit policy matrix: ReadThrough is
          // read-only-by-default; the post-hook is a no-op.
          ()
      }
    case _ =>
  }
  context
}
```

The `runsOnStop = false` setting is preserved (cache HIT short-circuits the executor and PostExecute; per RFC `hooks.md` and PR-9 ADR-008-P §T1-D2). For the `WriteThrough(name)` MISS case: the post-hook fires (the executor ran; `runsOnStop = false` doesn't apply when there's no stop); the post-hook consults the fold and writes (per Fix 6). For the `ReadThrough(name)` MISS case: the post-hook fires, consults the fold, no-ops.

### Fix 6 — explicit per-case policy matrix

| `model.cache` | PreExecute (`CacheReadPreHook`) | PostExecute (`CacheWritePostHook`) | Rationale |
|---|---|---|---|
| `NoCache` | NO cache lookup; `readFires` NOT incremented | NO write-through; `writeFires` NOT incremented | The user said "don't cache". Both hooks are silent no-ops. |
| `ReadThrough(name)` | cache lookup; `readFires` incremented; HIT short-circuits via `stop = true`; MISS continues to executor | NO write-through; `writeFires` NOT incremented | Read-only caching; the cache is populated externally (cache-warming, batch job, etc.). The hook reads, never writes. |
| `WriteThrough(name)` | cache lookup; `readFires` incremented; HIT short-circuits via `stop = true`; MISS continues to executor | write-through; `writeFires` incremented on every successful executor result | Write-through caching; the cache is populated by the post-hook on every successful executor result (including the post-write for a HIT if the executor ran, but the HIT short-circuits so the post-hook fires but the executor result is the cached result — see `runsOnStop = false`) |

The matrix is explicit. Acceptance criterion #2 is updated to match the actual behavior:

> A model with `defaultPolicies.cache == ReadThrough("default")` and a cache MISS → existing read path runs (cache lookup + miss counter); the post-hook is a no-op (no write-through); the executor result flows back to the caller uncommitted to cache.

The previous v0.1 acceptance #2 was incorrect — the post-hook currently writes on miss, and `ReadThrough` semantics explicitly forbid write-on-miss.

---

## Falsifiable acceptance (v1.0)

```
1. CacheReadPreHook honors model.cache: a model with
   defaultPolicies.cache == NoCache → the cache hook's
   run is a no-op (no `cache.getJournaled` call, no
   `readFires` counter increment, no write-through on the
   way back). The existing pre-fix tests that assert
   `readFires.incrementAndGet()` is called unconditionally
   MUST be updated to assert it's NOT called when the
   model declares `NoCache`.

2. CacheReadPreHook honors ReadThrough / WriteThrough:
   a model with defaultPolicies.cache == ReadThrough("default")
   and a cache HIT → context.stop = true and the cached
   PortableQueryResult is returned (existing behavior).
   A model with defaultPolicies.cache == ReadThrough("default")
   and a cache MISS → readFires incremented, miss counter
   incremented, executor runs, PostExecute hook is a
   no-op (NO write-through). A model with
   defaultPolicies.cache == WriteThrough("default") and a
   cache MISS → readFires incremented, miss counter
   incremented, executor runs, PostExecute hook writes
   to cache (writeFires incremented, putJournaled
   called). The per-case policy matrix in Fix 6 is
   the falsifiable artifact.

3. Dual CachePolicy ADT deleted + 4 spec sites migrated:
   grep -rn 'io.sm8.core.engine.CachePolicy' in
   src/main and src/test → ZERO matches. The only
   `CachePolicy` reference is `io.sm8.core.model.CachePolicy`.
   `EngineContextSpec.scala` lines 14-24, 16-20, 100,
   109-115 are migrated to `io.sm8.core.model.CachePolicy`.
   `PortableQueryCompiler.scala:534` rejection message
   references the model-side ADT (text unchanged).

4. EngineContext.cachePolicy fold target: a model with
   defaultPolicies.cache == ReadThrough("region-a") flows
   through `EngineService.runQueryWithHooks` → initialCtx.meta
   contains "sm8.cache.policy" -> ReadThrough("region-a") →
   CacheReadPreHook consults and honors. Assert via the
   **new dispatcher integration spec** added to
   `sm8-platform/src/test/scala/io/sm8/platform/query/EngineServiceRunQueryWithHooksSpec.scala`:
   the spec MUST construct a model with `defaultPolicies.cache
   == ReadThrough("region-a")` AND `== WriteThrough("region-a")`,
   run the full dispatcher with the cache plugin registered,
   and assert the fold reaches the hook (introspect `ctx.meta`
   in a test-only hook OR assert via the existing `misses` /
   `writeFires` counter increments per Fix 6's per-case matrix).
   Existing `EngineServiceRunQueryWithHooksSpec` tests construct
   `NoCache` models only — the v1.0 acceptance #4 has no test
   fixture to run against without this spec addition. **This
   is a v1.1 fold from data-eng P1 #8: acceptance #4 is
   unfalsifiable without the integration spec.**

5. NoCache default preserved: a model with defaultPolicies.cache
   == NoCache (the default) and a query with no fold wired →
   both hooks are no-ops (backwards-compat for callers that
   don't fold yet). This is the existing behavior for NoCache
   + the absence of the fold. The hooks do NOT default to
   unconditional-fire — they default to NoCache-skip.

6. Per-case counter discipline: `readFires` increments
   ONLY for `ReadThrough` / `WriteThrough` (not NoCache).
   `writeFires` increments ONLY for `WriteThrough` (not
   NoCache, not ReadThrough). The `hits` / `misses` counters
   increment ONLY for the read-hook's read branch (not
   NoCache, not the post-hook). This is the user-visible
   "cache metrics for the model that declared caching" fix.

7. No regression on the existing cache HIT/MISS paths:
   the existing 6 falsifiable tests in `CachePluginSpec` +
   `CachePluginContractSpec` + the platform's
   `EngineServiceRunQueryWithHooksSpec` cache wiring all
   remain green after the gate (UPDATED to reflect the new
   per-case matrix). The full reactor build + the 241
   spark-connector tests + the 634 core tests are the
   assertion. This ADR touches the cache hook (read +
   write) + the engine-context-side field (retyped) + the
   dispatcher's initialCtx construction (one line) + 4
   spec sites; the regression risk is concentrated in the
   cache plugin tests + the EngineContextSpec migration.
```

---

## Consequences (v1.0)

**Positive**

- `model.cache` is honored end-to-end on both the read AND write path. The most common configuration (`NoCache`) does the right amount of work (zero cache interactions).
- The dual-ADT drift hazard is closed (one `CachePolicy` ADT lives in `sm8-core/model`).
- The dead `EngineContext.cachePolicy` field is repurposed (additive, backwards-compat default `NoCache`).
- The runtime gate is engine-portable (the fold mirrors the ADR-009-d `DecisionHints` pattern — `model → ctx.meta → hook`; the fold lives in `initialCtx` construction BEFORE `dispatcher.run` per the canonical hook order).
- The cache hook's counter metrics become truthful (`readFires` / `writeFires` / `hits` / `misses` count only the queries that actually touched the cache).
- The per-case policy matrix (Fix 6) makes the read/write behavior explicit, falsifiable, and documented in the ADR.

**Negative**

- `EngineContext.cachePolicy` is retyped to `io.sm8.core.model.CachePolicy` (was `io.sm8.core.engine.CachePolicy`). 4 spec sites in `EngineContextSpec.scala` must be migrated (lines 14-24, 16-20, 100, 109-115) — enumerated in Fix 2.
- `EngineService.runQueryWithHooks` adds one line to `initialCtx.meta` construction (Fix 4).
- `CacheReadPreHook.run` gains one match (Fix 1). The hot-path NoCache-skip is a single `ctx.meta.get("sm8.cache.policy") match { case Some(NoCache) | None => context; ... }` — O(1), no allocation.
- `CacheWritePostHook.run` gains one match (Fix 5). Same hot-path shape.

**Neutral**

- The model-side `CachePolicy.ReadThrough(name)` + `WriteThrough(name)` shape is unchanged (the fold carries the full ADT value, including the `name`).
- The `InMemoryResultCache` implementation is unchanged (the gate is in the hook, not the cache).
- The deployment module is unchanged (`sm8.plugins.allowed` allowlist + `META-INF/services/io.sm8.sdk.Plugin` discovery — already correct).
- The `EngineHookDispatcher` API is unchanged (no new parameter; the fold goes into `initialCtx.meta` construction).

---

## Out of scope (deferred, named for future ADRs)

- **`MaterializePolicy.Cache` → cache-plugin handoff**: closed by ADR-009-f §Out-of-scope; the typed rejection at `PortableQueryCompiler.scala:534` names `CachePolicy.ReadThrough(<cache-name>)` as the alternative. ADR-009-g closes the alternative half: `model.cache = ReadThrough("default")` is now honored end-to-end (the hook does the cache lookup, the dispatcher short-circuits on HIT). The two halves together mean a model declaring `cache = ReadThrough("default")` AND `materialize = Cache` is fully wired.
- **Multi-region cache routing**: `ReadThrough(name: String)` carries a cache-region `name`; the current `InMemoryResultCache` ignores the `name` and stores everything in one map. A future multi-region cache (`InMemoryResultCache` per region, keyed by `name`) is a separate ADR when a real consumer surfaces.
- **TTL / eviction policies on `CachePolicy.ReadThrough`**: the current `CachePolicy` is a routing-only shape. A future ADR adding `ReadThrough(name: String, ttl: Duration, evictOn: EvictTrigger)` is independent of this ADR.
- **`EngineContext.cachePolicy` field semantics as a typed contract** (vs `DecisionHints`-style `Option[CachePolicy]`): the field is currently a documentation seam (default `NoCache`); the fold goes through `ctx.meta`, not the field. An `Option[CachePolicy]` shape would distinguish "fold absent" from "fold says NoCache" — useful for diagnostics. Deferred.

---

## References

- ADR-009-f (`docs/adr/0009-f-paired-persist-lifecycle.md`) — the prior wave that closed `MaterializePolicy`; PR-180 implementation pattern.
- ADR-009-d (`docs/adr/0009-d-broadcast-skew-decision-via-context-meta.md`) — the `DecisionHints` fold pattern at `EngineService.scala:430-441`; the SDK meta channel boundary; PR-178 honor-or-UnsupportedCapability discipline.
- ADR-009-e (`docs/adr/0009-e-driver-materialization-bounds.md`) — the v0.4 wave's other half.
- RFC §3 Core Boundary (`docs/rfcs/2026-08-12_v1_architecture-spec/semantic-layer-engine-architecture.md`) — typed engine-portable ADTs in `sm8-core`; config in deployment layer.
- RFC §11a Deployment Module — deployment lives outside core AND outside transport; transport does not import adapter-specific types.
- `EngineHookDispatcher.run` at `sm8-platform/src/main/scala/io/sm8/platform/query/hooks/EngineHookDispatcher.scala:100-123` — the canonical hook order: `firePre` runs BEFORE `execute`; the fold must be in `initialCtx.meta` construction.
- `/tmp/cross-engine-dechints-audit.md` — PR-178 cross-engine audit notes; the same fold pattern is reused for `cachePolicy`.
- Codegraph probes (this audit): `CachePolicy` (3 model-side cases + 4 engine-side cases, dual ADT); `CacheReadPreHook` (no `case NoCache` / `case ReadThrough` matches in the body); `CacheWritePostHook` (unconditional `writeFires` increment + unconditional `putJournaled` on every successful engine result); `EngineContext.cachePolicy` (typed as engine-side ADT; 4 spec readers at `EngineContextSpec.scala:14-24, 16-20, 100, 109-115`); `EngineHookDispatcher.run` (fold must be in `initialCtx.meta`, not `engineExecutor`).
