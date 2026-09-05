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

The literal string `"semanticGraphError"` does NOT end in `":error"` — it ends in `"Error"` (no colon). A naive `endsWith(":error")` check would silently drop the semantic-graph plugin's existing typed errors.

**Choice: migrate the legacy key** (the simpler of two clean paths; the alternative is to loosen the matcher to `endsWith("Error")` (broader), but that risks false positives for any plugin storing a non-typed-error `String` metadata with an `Error` suffix — rejected).

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
- `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:627-628` — the new `collectFirst` matcher (this ADR)
- `plugins/semantic-graph-plugin/src/main/scala/io/sm8/plugins/semanticgraph/JoinPathPreHook.scala:58` — `val CycleErrorKey = "io.sm8.plugins.semanticgraph:error"` (renamed)
- `sm8-core/src/main/scala/io/sm8/core/engine/EngineError.scala:26` — the 13-variant sealed trait the convention carries

Local-only `.omp/WATCHDOG.yml` (gitignored) tunes the advisor roster for this project.
