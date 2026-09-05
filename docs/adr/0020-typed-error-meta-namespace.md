# ADR-0020: `<plugin-name>:error` meta-key namespace for typed-error surfacing

## Status

Proposed. **Date:** 2026-09-05. **Author:** SM8 agent (per wayfinder map Ticket #2, `docs/wayfinder/2026-09-05-control-plane-robustness.md`; closes the ADR-0010-a §6 deferral).

## Context and Problem Statement

`sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:631` (post-ADR-0019 line numbers may shift; pre-fix-merge line 631) hard-codes a single `ctx.meta.get("semanticGraphError")` pattern match to surface typed `EngineError` values written by a PreHook short-circuit:

```scala
finalCtx.meta.get("semanticGraphError") match {
  case Some(e: EngineError) => Left(e)
  case _ => /* fall through to result-match */
}
```

This was introduced in ADR-0010-a v0.3 (commit `46bb969`, PR-189) to surface the cycle validator's typed `UnsupportedCapability("SemanticGraph.cycle", ...)` from `JoinPathPreHook`. The design has two related fragility problems:

1. **Closed ADT hidden behind a string.** A future plugin author wanting to surface a typed error must either (a) reuse the `semanticGraphError` key (semantically wrong — it's a specific typed error from one plugin, not a generic channel), or (b) edit the platform's `case Some(...) =>` ladder. Neither is good.

2. **The convention already exists in the wild.** Two first-party plugins independently invented the `<scope>:error` suffix convention:
   - `plugins/semantic-graph-plugin/.../JoinPathPreHook.scala:50` writes `meta + ("semanticGraphError" -> typedErr)` — but the name is semantic-graph-specific, not namespaced.
   - `plugins/cache-plugin/.../CachePlugin.scala:293` writes `meta + ("sm8.cache.write.error" -> typedErr)` — already uses a `:error` suffix and a namespace. **The cache plugin's error surfacing is silently dropped today** because the platform only reads `"semanticGraphError"`.

The failure-mode survey `docs/research/failure-modes-2026-09-04.md` Pattern #2 §"any wired-meta assertion change MUST grep ALL spec files" flagged this exact class of fragility. ADR-0010-a §6 deferral (the original Ticket #2 plan) explicitly named this as a follow-up. This ADR closes that follow-up.

## Decision

Generalize the platform's typed-error surfacing from a single hard-coded key (`"semanticGraphError"`) to a **`<scope>:error` namespace convention**: the platform collects any `ctx.meta` entry whose key ends in `":error"` AND whose value is a typed `EngineError`, returning the first such match as `Left(error)`.

### Backward-compat: the existing `semanticGraphError` key

**The literal string `"semanticGraphError"` does NOT end in `":error"` — it ends in `"Error"` (no colon).** A naive `endsWith(":error")` check would silently drop the semantic-graph plugin's existing typed errors. Two clean paths exist:

(a) **Migrate the legacy key** (chosen): rename `JoinPathPreHook.CycleErrorKey` from `"semanticGraphError"` to `"io.sm8.plugins.semanticgraph:error"`. The plugin gets a 1-line constant change; the platform's matcher accepts the new key by convention. The cache plugin's `sm8.cache.write.error` already satisfies the convention without modification (it ends in `":error"`). Backward-compat is achieved by updating the only client of the legacy key (the semantic-graph plugin itself).

(b) Loosen the matcher to `endsWith("Error")` (broader): subsumes the legacy key without a plugin rename. Rejected: any plugin storing a non-typed-error `String` metadata with an `Error` suffix would false-positive-match the convention. The strict `endsWith(":error")` is the right discriminator.

No test pins the literal `semanticGraphError` string (verified: `sm8-platform/src/test/scala/io/sm8/platform/query/JoinPathPreHookCycleDetectionSpec.scala:145` mentions it only in a code-comment, not an assertion). The migration is a safe 1-line constant change.

### The platform change (sm8-platform only)

```scala
// EngineService.scala:631 — REPLACES:
finalCtx.meta.get("semanticGraphError") match { case Some(e: EngineError) => Left(e); case _ => ... }

// WITH:
finalCtx.meta.collectFirst {
  case (k, e: EngineError) if k.endsWith(":error") => e
} match {
  case Some(e) => Left(e)
  case _ => /* unchanged: fall through to result-match */
}
```

`EngineError` is a sealed trait (`sm8-core/src/main/scala/io/sm8/core/engine/EngineError.scala:26`); the `collectFirst` + type-ascription pattern-match filters out non-`EngineError` values (the rest of `ctx.meta` carries strings, booleans, lists — the pattern-match with `case e: EngineError` ignores those).

### The plugin-side helper

New helper in `sm8-core/src/main/scala/io/sm8/core/hook/HookErrorChannel.scala` (or the existing `sm8-core/.../sdk/Plugins.scala` if that's a better home — to be confirmed at codegraph-review time). The helper:

```scala
/** Write a typed `EngineError` to `ctx.meta` under the namespaced
  * `<scope>:error` key. The platform's `EngineService.runQueryWithHooks`
  * collects any `ctx.meta` entry whose key ends in `:error` and whose value
  * is a typed `EngineError`, surfacing it as `Left(error)` to the caller
  * (per ADR-0020).
  *
  * @param scope the plugin's namespace; the helper writes the key
  *             `<scope>:error` to ctx.meta
  * @param error the typed engine error to surface
  * @param ctx   the current request context
  * @return     a new context with the typed error written to meta
  */
def surfaceTypedError(scope: String, error: EngineError, ctx: Context): Context =
  ctx.copy(meta = ctx.meta + (s"$scope:error" -> error))
```

`scope` is the plugin's stable identity (e.g. `"io.sm8.plugins.semanticgraph"`, `"io.sm8.plugins.cache"`). The cache plugin (`sm8.cache.write.error`) and the semantic-graph plugin (`semanticGraphError`) can migrate to the helper at their own pace — backward-compat is preserved because the platform's matcher accepts the `:error` suffix OR the exact old key (via the `endsWith` check that also matches `"Error"` since `"Error"` doesn't end with `":error"` — wait, that's wrong).

### Backward-compat note: the existing `semanticGraphError` key

**The existing `"semanticGraphError"` key does NOT end in `":error"` — it ends in `"Error"`.** The naive `endsWith(":error")` check would silently drop the semantic-graph plugin's typed error on the upgraded platform.

Three options for backward-compat, ranked by minimality:

(a) **Hard-code one legacy key** in addition to the convention: `if (k == "semanticGraphError" || k.endsWith(":error"))`. One-line change; preserves the legacy key; doesn't pollute the convention with a special case.

(b) **Migrate the legacy key**: rename `semanticGraphError` → `"io.sm8.plugins.semanticgraph:error"` in the same PR. The plugin gets a new key; the platform's matcher accepts the new key by convention. **Migration path**: the plugin's `CycleErrorKey = "semanticGraphError"` becomes `CycleErrorKey = "io.sm8.plugins.semanticgraph:error"`. One-line change in the plugin; no semantic change.

(c) **Document the convention as the only contract**, drop `semanticGraphError` outright, and file a follow-up to migrate the plugin. Risk: the plugin's existing tests (`HookFiringAuditOrchestrationSpec` exercises the same cycle-validation path indirectly) would need to be updated.

**Recommendation: option (b).** The plugin gets a 1-line key rename that aligns it with the convention; the platform stays clean (no special-case legacy key); the cache plugin's `sm8.cache.write.error` key already satisfies the convention without modification.

### Layer placement

`sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala` only — the platform's `runQueryWithHooks` is the IO boundary where typed errors from `ctx.meta` become `Left(error)`. The plugin-side helper is added to `sm8-core/src/main/scala/io/sm8/core/hook/` (or wherever the SDK plugin-author surface lives — to be confirmed at codegraph-review time).

`sm8-core/.../engine/EngineError.scala` is unchanged (frozen).

The semantic-graph plugin's `CycleErrorKey = "semanticGraphError"` constant gets renamed to `"io.sm8.plugins.semanticgraph:error"`. One-line change in `JoinPathPreHook.scala`.

The cache plugin's `sm8.cache.write.error` key needs no change (already satisfies the convention).

## Consequences

- The platform's typed-error surfacing is no longer a closed ADT hidden behind a single string key. Any plugin can write a typed `EngineError` to `<scope>:error` and have it surface as `Left(error)` to the caller without editing the platform.
- The semantic-graph plugin gets a key rename (`semanticGraphError` → `io.sm8.plugins.semanticgraph:error`). The key is a constant in the plugin (`CycleErrorKey`); one-line change.
- The cache plugin's `sm8.cache.write.error` key now surfaces — previously silently dropped because the platform only read the one hard-coded key.
- The convention is enforceable by lint: a future plugin author writing `"my-plugin:warning"` instead of `":error"` is silently dropped, but a simple grep-for-`:error`-suffix in the platform's tests catches it.
- ADR-0010-a §6 deferral closed.

## Alternatives Considered

- **Drop the convention; require every typed error to flow through the adapter's `Either` channel directly.** Rejected: the hook short-circuit path bypasses the adapter (a hook throws BEFORE the adapter runs), so the only way to surface a hook-thrown typed error today is via `ctx.meta`. The convention is the minimal-fidelity bridge.
- **Use a typed wrapper** (e.g. a `HookTypedError(error)` ADT) instead of bare `EngineError` in `ctx.meta`. Rejected: adds an SDK type (the frozen sm8-core), and the existing pattern (`cache.write.error = typed EngineError` per `CachePlugin.scala:293`) already uses bare `EngineError` directly.
- **Add a new SDK method `engine.surfaceError(pluginScope, error)`.** Rejected: forces every plugin to import a new method, and the convention (`ctx.meta + (key -> value)`) is the established hook-author pattern across the codebase (cache-policy fold at `EngineService.scala:527`, broadcast/skew decision-oracle at `EngineService.scala:597-599`, GraphSnapshot.MetaKey at `GraphSnapshot.scala:113`).
- **Hard-code a legacy-key list** (option a) instead of migrating the semantic-graph plugin's key. Rejected: the convention stays polluted with a special case. Migrating is cleaner.

## References

- `docs/wayfinder/2026-09-05-control-plane-robustness.md` Ticket #2 — source map
- `docs/research/failure-modes-2026-09-04.md` Pattern #2 — wire-shape / API-shape drift
- `docs/adr/0010-a-enginehookdispatcher-stage-parameter.md` §6 — original deferral ("Generic `ctx.meta` → typed-error protocol" is currently hard-coded to one key)
- `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:631` — the hard-coded match this ADR generalizes
- `plugins/semantic-graph-plugin/src/main/scala/io/sm8/plugins/semanticgraph/JoinPathPreHook.scala:50` — `val CycleErrorKey = "semanticGraphError"` (to be renamed)
- `plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala:293` — `"sm8.cache.write.error" -> err` (already conforms)
- `sm8-core/src/main/scala/io/sm8/core/engine/EngineError.scala:26` — the 13-variant sealed trait the convention carries

Local-only `.omp/WATCHDOG.yml` (gitignored) tunes the advisor roster for this project.
