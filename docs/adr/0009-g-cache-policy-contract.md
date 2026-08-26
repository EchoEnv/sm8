# ADR-009-g: CachePolicy contract closure — runtime gate via `EngineContext.cachePolicy` + single-source ADT

| Field | Value |
|---|---|
| **Status** | **Proposed (v0.1)** — initial draft; closing the deferred cache-feature half that ADR-009-f named as out-of-scope ("`MaterializePolicy.Cache` → cache-plugin handoff") + closing the dual-`CachePolicy` ADT drift hazard that mirrors the pre-ADR-009-f `MaterializePolicy` state. Investigation files: `/tmp/cross-engine-dechints-audit.md` (PR-178), ADR-009-d (`DecisionHints` SDK fold pattern), ADR-009-f §Out-of-scope. |
| **Date** | 2026-08-26 |
| **Module** | `plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala` (`CacheReadPreHook.run` consults the folded `cachePolicy` via the `ctx.meta` channel from PR-178 / ADR-009-d; the `cache = NoCache` model passes through unchanged — no cache read attempt, no miss counter increment, no write attempt on the way back) + `sm8-core/engine/EngineContext.scala` (additive `cachePolicy: CachePolicy = CachePolicy.NoCache` field for engine-portable propagation; the engine-context-side `CachePolicy.ReadOnly` case + the dead `EngineContext.cachePolicy` field get folded into the model-side ADT) + `sm8-core/model/Model.scala` (the single-source `CachePolicy` ADT; the `ReadOnly` case is dropped as zero-reader per Option A) + `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala` (the `engineExecutor` thunk folds `model.cache` into `decisionCtx.cachePolicy` via the established ADR-009-d `DecisionHints` fold pattern, so the hook consults `ctx.meta.get("sm8.cache.policy")` and is engine-portable) + `sm8-platform/src/main/scala/io/sm8/platform/query/hooks/EngineHookDispatcher.scala` (propagates `decisionCtx.cachePolicy` into `ctx.meta` so the hook can consult) + `sm8-core/test/.../EngineContextSpec.scala` (the dead-field references deleted) + `plugins/cache-plugin/src/test/.../CachePluginContractSpec.scala` + `CachePluginSpec.scala` (3 new falsifiable tests for `NoCache` no-op, `ReadThrough(name)` HIT path, `WriteThrough(name)` MISS-then-write path) + `sm8-platform/src/test/.../EngineServiceRunQueryWithHooksSpec.scala` (the fold seam). |
| **Supersedes scope** | The pre-existing cache-feature half-open contract surfaced by ADR-009-f §Out-of-scope: (a) **Gap 1**: `CacheReadPreHook.run` at `plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala:95-118` fires unconditionally on every query — it does NOT consult `model.defaultPolicies.cache` or `EngineContext.cachePolicy`. A model with `cache = NoCache` (the default) still hits `cache.getJournaled` + the `misses.incrementAndGet()` counter + the post-write hook. Same class of bug as ADR-009-f's pre-state for `MaterializePolicy.Cache` (declared, defaulted, never honored). (b) **Gap 2**: dual `CachePolicy` ADTs — `io.sm8.core.model.CachePolicy` (the active shape: `NoCache`/`ReadThrough(name)`/`WriteThrough(name)`) at `sm8-core/src/main/scala/io/sm8/core/model/Model.scala:79-84` AND `io.sm8.core.engine.CachePolicy` (the engine-context-side, with an extra `ReadOnly` case that's never read) at `sm8-core/src/main/scala/io/sm8/core/engine/EngineContext.scala:66-87`. Same drift hazard as ADR-009-f Gap 5 (dual `MaterializePolicy`). (c) **Gap 3**: `EngineContext.cachePolicy: io.sm8.core.engine.CachePolicy` field is dead — declared at `EngineContext.scala:38`, defaulted at `EngineContext.scala:59` (`CachePolicy.NoCache`), zero readers. Same dead-field pattern as `EngineContext.materializePolicy` was (closed by ADR-009-f Fix 4). The field is the natural seam for the runtime gate; if it's deleted without a replacement, the runtime gate becomes impossible without leaking the `Model` into the hook (violates the engine-portable claim — the hook IS in production but `Model.cache` is a model-attached ADT). The fold pattern from ADR-009-d (`EngineService.engineExecutor` folds `model.cache` into `ctx.meta` as `sm8.cache.policy`; the hook consults `ctx.meta`) preserves engine-portable propagation without the `Model` leaking. |
| **Skill alignment** | `karpathy-app-design` (single source of truth — one `CachePolicy` ADT; the fold is the canonical ADR-009-d pattern); `karpathy-guidelines` (surgical edits; dead code is a smell — `EngineContext.cachePolicy` deleted or folded; `ReadOnly` case deleted or kept as real consumer); `scala-bug-hunting` (silent no-ops are bugs — `CacheReadPreHook` fires unconditionally regardless of `model.cache`); `scala-error-handling` (typed errors over `Throwable` swallow; the fold is an Either-or-pass-through, no exception-based gate); `scala-impact-analysis` (dual ADTs are a blast-radius hazard — every caller of one ADT risks a silent-no-op on the other); `scala-jvm-safety` (the hook fires on every query in production today; the gate must NOT change the hot-path semantics for the `NoCache` default — the hook must early-return without `cache.getJournaled`); `scala-data-driven-refactor` (sealed-trait `CachePolicy` dispatch in the hook; no Map-based rule table; the fold is a typed ADT value in `ctx.meta`); `scala2-scaladoc` (each fix carries a scaladoc anchor; the fold pattern is documented at `EngineService.engineExecutor` mirroring the ADR-009-d `DecisionHints` fold); `debug-mantra` (reproduce the unconditional-fire via the existing `CachePluginSpec`; trace through `EngineService.runQueryWithHooks`; falsify via the new `NoCache → noop` test; cross-reference every hook + fold seam; verify the full reactor green). |
| **Architecture alignment** | RFC §3 Core Boundary: `CachePolicy` lives once in `sm8-core/model` (engine-portable data shape); the cache-plugin enforces the policy via the ADR-009-d fold pattern (engine-portable `ctx.meta` channel); the deployment module wires the cache via `META-INF/services/io.sm8.sdk.Plugin` discovery + `sm8.plugins.allowed` allowlist (no change — already correct). PR-178 discipline extends: silent no-ops are contract violations — `CacheReadPreHook` must honor-or-pass-through `model.cache`, no silent unconditional-fire. RFC §11a: deployment stays outside core AND outside transport; transport does not import adapter-specific types — the fold is engine-portable, no transport-side change. |

---

## Revision history

| Version | Date | Change |
|---|---|---|
| v0.1 (Proposed) | 2026-08-26 | Initial draft; 4 findings (unconditional-fire, dual `CachePolicy` ADT, dead `EngineContext.cachePolicy` field, missing fold); 4 options considered (Option A: dual-ADT collapse + ADR-009-d fold-pattern runtime gate — the recommended path; rejected Options B/C/D detailed in §Decision). Investigation files: `/tmp/cross-engine-dechints-audit.md`, ADR-009-d `DecisionHints` SDK pattern, ADR-009-f §Out-of-scope, codegraph probes (`CachePolicy`, `CacheReadPreHook`, `CachePlugin`, `EngineContext.cachePolicy`). |

---

## Context and problem statement

The cache feature has four distinct gaps, each independently reproducible and each silently degrading the contract that `CachePolicy.ReadThrough(<name>)` is supposed to express. The pre-ADR-009-f state for `MaterializePolicy.Cache` was: declared, defaulted, never honored. The pre-ADR-009-g state for `CachePolicy` is the same shape, plus a dual-ADT drift hazard that mirrors the pre-ADR-009-f dual `MaterializePolicy`:

### Gap 1 — `CacheReadPreHook` fires unconditionally (`plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala:95-118`)

The `CacheReadPreHook` registered at priority 50 (`HookStage.PreExecute`) fires on every query via the dispatcher. Its `run` method does NOT consult `model.defaultPolicies.cache` or `EngineContext.cachePolicy`:

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

A model with `cache = NoCache` (the default — `ModelPolicyDefaults.cache = CachePolicy.NoCache` at `Model.scala:230`) still hits `cache.getJournaled(hookReq.cacheKey)` on every query. The `misses` counter increments; if a write-through path is also configured, the post-execute hook writes a result that was supposed to be uncached. The contract `cache = NoCache` says "do not cache" — the hook says "always look in the cache, always miss, always write through". The counter metrics are wrong (inflated misses), and any cache-key-collision risk surfaces for a `NoCache` model that should never have entered the cache at all.

**Consequence:** `model.cache` is silently ignored. The most common configuration (`NoCache`) does the most wasted work. The `ReadThrough(name)` case is reachable but its sibling `NoCache` is silently no-op'd on the "skip" path. Same class of bug as ADR-009-f Gap 3 (`MaterializePolicy.Cache` silent no-op).

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
  - `case object ReadOnly extends CachePolicy` (no model-side equivalent; never read)
  - This shape lives only in `EngineContext.scala` lines 66-87. Zero readers in the runtime path (verified via repo-wide `grep` — only the ADT declaration + the field declaration + the default appear; no `case _ : engine.CachePolicy` matches anywhere).

The `engine.CachePolicy.ReadOnly` case is a future-design residue with no current consumer. The lack of `name` on the engine-side `ReadThrough` / `WriteThrough` (vs the model-side which has `name: String`) is a drift hazard: if a contributor wires the engine-side field, they lose the cache-region name.

**Consequence:** same drift hazard as ADR-009-f Gap 5. A contributor who picks the wrong import gets a silent no-op. The `ReadOnly` case is unreachable code.

### Gap 3 — `EngineContext.cachePolicy` is dead (`sm8-core/src/main/scala/io/sm8/core/engine/EngineContext.scala:38,59`)

```scala
case class EngineContext(
    ...
    materializePolicy: MaterializePolicy = MaterializePolicy.None,  // dead per ADR-009-f Fix 4
    cachePolicy:       CachePolicy      = CachePolicy.NoCache,    // <-- this Gap
    auditPolicy:       AuditPolicy      = AuditPolicy.NoAudit,
    ...
)
```

The field is declared at line 38, defaulted at line 59, flows through `EngineService.runQueryWithHooks` → `provider.query(model, request, ctx)` unchanged. Zero readers in production or test code. Same dead-field pattern as `EngineContext.materializePolicy` was before ADR-009-f Fix 4. The field is the natural seam for the runtime gate — it would carry the `cache` policy across the engine-portable boundary, the way `materializePolicy` was supposed to carry the materialize policy. ADR-009-f removed the dead `materializePolicy` because there was no consumer; ADR-009-g keeps the field BECAUSE there IS a consumer (the cache hook, after Fix 1 lands).

**Consequence:** without the field, the runtime gate requires leaking `Model.cache` into the cache hook (breaks engine-portable). With the field, the fold pattern from ADR-009-d (`EngineService.engineExecutor` folds `model.cache` into `EngineContext.cachePolicy`, then into `ctx.meta` for the hook to consult) preserves the boundary.

### Gap 4 — missing fold from `model.cache` to `EngineContext.cachePolicy` (`sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:430-441`)

The `engineExecutor` thunk at `EngineService.scala:430-441` already folds `model`-side decision values into `EngineContext`:

```scala
val decisionCtx: io.sm8.core.engine.EngineContext =
  io.sm8.core.engine.EngineContext.defaultContext.copy(
    decisionHints = Some(io.sm8.core.engine.DecisionHints(
      broadcastArmed          = ctx.meta.get("sm8.broadcast.arm").collect { case b: Boolean => b },
      skewArmed               = ctx.meta.get("sm8.skew.arm").collect { case b: Boolean => b },
      broadcastThresholdBytes = ctx.meta.get("sm8.broadcast.thresholdBytes").collect { case l: Long => l }
    ))
  )
```

The fold is the canonical ADR-009-d pattern: the engine-side context carries typed decision values; the hooks consult `ctx.meta` (the engine-portable channel); the platform wires the fold; the model is never leaked into the hook. **The fold for `cachePolicy` is missing.** `cachePolicy` should be folded from `model.cache` into `decisionCtx.cachePolicy` in the same line.

**Consequence:** without the fold, the runtime gate is impossible to wire engine-portably. The fold is the missing plumbing.

---

## Decision

**Option A — dual-ADT collapse + ADR-009-d fold-pattern runtime gate.**

The `engine.CachePolicy.ReadOnly` case is **deleted** (zero readers — `karpathy-guidelines` "dead code is a smell"). The `engine.CachePolicy.ReadThrough` / `WriteThrough` are **deleted** (the model-side carries the `name: String`; the engine-side shape is a strict subset that would silently lose the region name if anyone ever wired it — `karpathy-app-design` "single source of truth"). The `io.sm8.core.engine.CachePolicy` ADT is **deleted entirely**. The dead `EngineContext.cachePolicy` field is **repurposed** to carry the fold from `model.cache` (additive — defaults to `NoCache`, no behavioral change for callers that don't consult it). The runtime gate is wired via the ADR-009-d fold pattern: `EngineService.engineExecutor` folds `model.cache` into `decisionCtx.cachePolicy`; `EngineHookDispatcher` propagates `decisionCtx.cachePolicy` into `ctx.meta("sm8.cache.policy")`; `CacheReadPreHook.run` consults `ctx.meta.get("sm8.cache.policy")` and early-returns on `NoCache` (preserving the hot-path skip semantics).

Options considered and rejected:

| Option | Why rejected |
|---|---|
| B. Keep both `CachePolicy` ADTs; document the model-side as "the active shape" and add scaladoc to the engine-context-side warning it's reserved | Documentation-only closure of Gap 2 leaves the dead `EngineContext.cachePolicy` field (Gap 3) and the unconditional-fire (Gap 1). The dual ADT is still a drift hazard. Rejected per RFC §3. |
| C. Delete the engine-side ADT but DON'T add the runtime gate — leave the hook unconditionally firing | Closes Gap 2 only. Gap 1 (the load-bearing user-visible bug) is unfixed. A model with `cache = NoCache` continues to hit `cache.getJournaled` + increment `misses`. Rejected — the runtime gate is the user-facing value of this ADR. |
| D. Drop the cache feature entirely (remove `CachePolicy.ReadThrough` / `WriteThrough` from `model`) | The feature is used by `InMemoryResultCache` (the cache hook fires on every query in production — removing the cases breaks the cache lookup). Removing it is a breaking change without an approved deprecation cycle. Rejected — the right move is closure, not removal. |

### Fix 1 — `CacheReadPreHook` runtime gate via `ctx.meta` (`plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala:95-118`)

The hook consults the engine-portable meta channel for the folded `cachePolicy`:

```scala
override def run(context: Context): Context = {
  context.request match {
    case hookReq: EngineHookRequest =>
      // ADR-009-g: honor the folded cache policy. The fold is in
      // EngineService.engineExecutor (model.cache → EngineContext.cachePolicy
      // → ctx.meta.get("sm8.cache.policy")). NoCache early-returns
      // without touching the cache or incrementing counters.
      ctx.meta.get("sm8.cache.policy") match {
        case Some(CachePolicy.NoCache) | None =>
          // NoCache (or fold absent — backwards-compat default): pass
          // through unchanged. NO counter increment, NO cache lookup,
          // NO write-through on the way back. This is the hot-path
          // skip that Gap 1 surfaces.
          context
        case Some(CachePolicy.ReadThrough(_)) | Some(CachePolicy.WriteThrough(_)) =>
          // Read-through / write-through: existing behavior.
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

### Fix 2 — delete the dual `CachePolicy` ADT (`sm8-core/src/main/scala/io/sm8/core/engine/EngineContext.scala:66-87`)

Delete `io.sm8.core.engine.CachePolicy` entirely. The engine-context-side shape has zero readers; the model-side shape carries the cache-region `name: String` (which the engine-side would silently lose). After deletion, the only `CachePolicy` reference in `sm8-core` is the model-side one. All `cachePolicy` references in `EngineContextSpec.scala` (the dead-field references — see Fix 3) are deleted.

### Fix 3 — `EngineContext.cachePolicy` becomes the fold target (additive, `sm8-core/src/main/scala/io/sm8/core/engine/EngineContext.scala:38,59`)

The dead field is repurposed to carry the folded `cachePolicy`:

```scala
case class EngineContext(
    ...
    materializePolicy: MaterializePolicy = MaterializePolicy.None,  // dead per ADR-009-f Fix 4
    cachePolicy:       CachePolicy      = CachePolicy.NoCache,    // <- ADR-009-g Fix 3: fold target
    auditPolicy:       AuditPolicy      = AuditPolicy.NoAudit,
    ...
)
```

The field is **additive** — callers that don't construct `EngineContext` continue to get `NoCache` (no behavioral change). The fold in `EngineService.engineExecutor` (Fix 4) populates it from `model.cache`. The default `NoCache` makes the existing dead-field references in `EngineContextSpec.scala` (which currently construct `EngineContext(materializePolicy = …)` with the dead field) still compile — the references are deleted in Fix 2's spec cleanup.

**Note:** this is a deliberate inversion of the ADR-009-f Fix 4 decision (which deleted `EngineContext.materializePolicy`). The inversion is justified: `materializePolicy` had no consumer after ADR-009-f; `cachePolicy` HAS a consumer (the cache hook, after Fix 1). YAGNI for `materializePolicy`, real consumer for `cachePolicy`.

### Fix 4 — `EngineService.engineExecutor` fold for `cachePolicy` (`sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:430-441`)

The fold extends the existing `DecisionHints` fold at lines 430-441:

```scala
val decisionCtx: io.sm8.core.engine.EngineContext =
  io.sm8.core.engine.EngineContext.defaultContext.copy(
    decisionHints = Some(io.sm8.core.engine.DecisionHints(
      broadcastArmed          = ctx.meta.get("sm8.broadcast.arm").collect { case b: Boolean => b },
      skewArmed               = ctx.meta.get("sm8.skew.arm").collect { case b: Boolean => b },
      broadcastThresholdBytes = ctx.meta.get("sm8.broadcast.thresholdBytes").collect { case l: Long => l }
    )),
    cachePolicy   = model.defaultPolicies.cache  // <-- ADR-009-g Fix 4 fold
  )
```

The hook consults `ctx.meta` (engine-portable). The fold is the plumbing that gets `model.cache` (model-attached) into `ctx.meta` (engine-portable). Without the fold, the hook cannot honor the policy without leaking the `Model` reference (breaks the engine-portable claim — the `Context` is the SDK boundary).

### Fix 5 — `ctx.meta` propagation for `cachePolicy` (`sm8-platform/src/main/scala/io/sm8/platform/query/hooks/EngineHookDispatcher.scala`)

The `EngineHookDispatcher` must propagate `decisionCtx.cachePolicy` into `ctx.meta` so the `CacheReadPreHook` (in production, engine-portable) can consult it. The propagation follows the same pattern as the existing `DecisionHints` meta writes:

```scala
// In EngineHookDispatcher.run, after building decisionCtx:
ctx.copy(meta = ctx.meta ++ Map("sm8.cache.policy" -> decisionCtx.cachePolicy))
```

This is one line added to the existing dispatcher. The `ctx.meta` channel is the SDK boundary (per the ADR-009-d `DecisionHints` pattern). The cache hook reads `ctx.meta.get("sm8.cache.policy")` (engine-portable), not `model.defaultPolicies.cache` (model-attached).

---

## Falsifiable acceptance (v0.1)

```
1. CacheReadPreHook honors model.cache: a model with
   defaultPolicies.cache == NoCache → the cache hook's
   run is a no-op (no `cache.getJournaled` call, no
   `misses` counter increment, no write-through on the
   way back). The existing pre-fix test that asserts
   `misses.incrementAndGet()` is called on cache-MISS
   MUST be updated to assert it's NOT called when the
   model declares `NoCache`.

2. CacheReadPreHook honors ReadThrough: a model with
   defaultPolicies.cache == ReadThrough("default") and
   a cache HIT → context.stop = true and the cached
   PortableQueryResult is returned (existing behavior).
   A model with defaultPolicies.cache == ReadThrough("default")
   and a cache MISS → existing pass-through (no change).
   The `misses` counter increments ONLY for ReadThrough MISS,
   not for NoCache.

3. Dual CachePolicy ADT deleted: grep -rn 'io.sm8.core.engine.CachePolicy'
   in src/main and src/test → ZERO matches. The only
   `CachePolicy` reference is `io.sm8.core.model.CachePolicy`.
   `EngineContextSpec.scala` has no remaining references to
   `cachePolicy` (the deleted field's spec sites).

4. EngineContext.cachePolicy fold target: a model with
   defaultPolicies.cache == ReadThrough("region-a") flows
   through `EngineService.engineExecutor` → decisionCtx.cachePolicy
   == ReadThrough("region-a") → ctx.meta("sm8.cache.policy") ==
   ReadThrough("region-a") → CacheReadPreHook consults and
   honors. Assert via the dispatcher integration spec:
   build a hook request with model.cache = ReadThrough("region-a"),
   run through the full dispatcher, observe the hook received
   the right value in ctx.meta.

5. NoCache default preserved: a model with defaultPolicies.cache
   == NoCache (the default) and a query with no fold wired →
   the hook is a no-op (backwards-compat for callers that
   don't fold yet). This is the existing behavior for NoCache
   + the absence of the fold. The hook does NOT default to
   unconditional-fire — it defaults to NoCache-skip.

6. CachePlugin counter discipline: the `readFires` counter
   (currently increments unconditionally at the start of run)
   is moved INSIDE the `Some(ReadThrough(_)) | Some(WriteThrough(_))`
   branch — it does NOT fire for NoCache. The `misses` counter
   is moved to the `Some(ReadThrough(_))` MISS sub-branch.
   This is the user-visible "cache metrics for the model that
   declared caching" fix.

7. No regression on the existing cache HIT/MISS paths:
   the existing 6 falsifiable tests in `CachePluginSpec` +
   `CachePluginContractSpec` + the platform's
   `EngineServiceRunQueryWithHooksSpec` cache wiring all
   remain green after the gate. The full reactor build +
   the 241 spark-connector tests + the 634 core tests are
   the assertion. This ADR touches the cache hook + the
   engine-context-side field + the dispatcher's meta propagation;
   the regression risk is concentrated in the cache plugin tests.
```

---

## Consequences (v0.1)

**Positive**

- `model.cache` is honored end-to-end. The most common configuration (`NoCache`) does the right amount of work (zero cache interactions).
- The dual-ADT drift hazard is closed (one `CachePolicy` ADT lives in `sm8-core/model`).
- The dead `EngineContext.cachePolicy` field is repurposed (additive, backwards-compat default `NoCache`).
- The runtime gate is engine-portable (the fold mirrors the ADR-009-d `DecisionHints` pattern — `model → EngineContext → ctx.meta → hook`).
- The cache hook's counter metrics become truthful (`readFires` + `misses` count only the queries that actually touched the cache).

**Negative**

- `EngineContext.cachePolicy` is added as a real field (was dead). One site in `EngineContextSpec.scala` referencing the field needs to be updated or deleted (Fix 2 + 3).
- The `EngineHookDispatcher` gains one line (Fix 5).
- `CacheReadPreHook.run` gains one match (Fix 1). The hot-path NoCache-skip is a single `ctx.meta.get("sm8.cache.policy") match { case Some(NoCache) | None => context; ... }` — O(1), no allocation.

**Neutral**

- The model-side `CachePolicy.ReadThrough(name)` + `WriteThrough(name)` shape is unchanged (the fold carries the full ADT value, including the `name`).
- The `InMemoryResultCache` implementation is unchanged (the gate is in the hook, not the cache).
- The deployment module is unchanged (`sm8.plugins.allowed` allowlist + `META-INF/services/io.sm8.sdk.Plugin` discovery — already correct).

---

## Out of scope (deferred, named for future ADRs)

- **`MaterializePolicy.Cache` → cache-plugin handoff**: closed by ADR-009-f §Out-of-scope; the typed rejection at `PortableQueryCompiler.scala:534` names `CachePolicy.ReadThrough(<cache-name>)` as the alternative. ADR-009-g closes the alternative half: `model.cache = ReadThrough("default")` is now honored end-to-end (the hook does the cache lookup, the dispatcher short-circuits on HIT). The two halves together mean a model declaring `cache = ReadThrough("default")` AND `materialize = Cache` is fully wired (cache + persist are complementary — cache stores the post-decode PQR, persist caches the upstream DataFrame).
- **Multi-region cache routing**: `ReadThrough(name: String)` carries a cache-region `name`; the current `InMemoryResultCache` ignores the `name` and stores everything in one map. A future multi-region cache (`InMemoryResultCache` per region, keyed by `name`) is a separate ADR when a real consumer surfaces. The model-side ADT shape is already correct; only the cache implementation is single-region today.
- **TTL / eviction policies on `CachePolicy.ReadThrough`**: the current `CachePolicy` is a routing-only shape. A future ADR adding `ReadThrough(name: String, ttl: Duration, evictOn: EvictTrigger)` is independent of this ADR (the fold carries whatever shape `model.cache` has; the cache implementation honors it).
- **`EngineContext.cachePolicy` field semantics as a typed contract** (vs `DecisionHints`-style `Option[CachePolicy]`): the fold puts `model.cache` directly into the field (always-present, defaulted to `NoCache`). An `Option[CachePolicy]` shape would distinguish "fold absent" from "fold says NoCache" — useful for diagnostics. Deferred until a real diagnostic need surfaces.

---

## References

- ADR-009-f (`docs/adr/0009-f-paired-persist-lifecycle.md`) — the prior wave that closed `MaterializePolicy` (the sibling shape of `CachePolicy`); v3.6 docs tightening; PR-180 implementation pattern (4 atomic commits: Core+Platform atomicity / Core deletion / Adapter rejection / Spark connector pair).
- ADR-009-d (`docs/adr/0009-d-broadcast-skew-decision-via-context-meta.md`) — the `DecisionHints` fold pattern at `EngineService.scala:430-441`; the SDK meta channel boundary; PR-178 honor-or-UnsupportedCapability discipline.
- ADR-009-e (`docs/adr/0009-e-driver-materialization-bounds.md`) — the v0.4 wave's other half; the driver-materialization bounds + `truncated` field.
- RFC §3 Core Boundary (`docs/rfcs/2026-08-12_v1_architecture-spec/semantic-layer-engine-architecture.md`) — typed engine-portable ADTs in `sm8-core`; config in deployment layer; the `connectors/spark-connector` is the only module with Spark imports.
- RFC §11a Deployment Module — deployment lives outside core AND outside transport; transport does not import adapter-specific types.
- `/tmp/cross-engine-dechints-audit.md` — the PR-178 cross-engine audit notes (PR-178 closed the `DecisionHints` honor-or-error gap; the same pattern is reused for `cachePolicy`).
- Codegraph probes (this audit): `CachePolicy` (3 model-side cases + 4 engine-side cases, dual ADT); `CacheReadPreHook` (no `case NoCache` / `case ReadThrough` matches in the body); `EngineContext.cachePolicy` (1 declaration + 1 default + 0 readers).
