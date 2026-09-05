# ADR-0018: hook-firing-audit plugin — self-probing every stage attachment point to detect registered-but-never-fired hooks

## Status

Proposed.

## Context and Problem Statement

The pipeline's hook dispatch spans two layers: the in-tree `Pipeline` in
`sm8-core` (dormant in production) and the production path,
`HookRunnerOrchestration` driving `EngineHookDispatcher` per stage in
`sm8-platform`. The 2026-08-26 ADR-010-a v0.3 fix documented the worst
realized failure of this seam: the dispatcher drove only the Execute
stage, so hooks registered at the other three stages never fired — while
the full test suite stayed green, because the specs exercised the helper
in isolation rather than the production entry point.

That defect class — a hook is registered, the registry lists it, but no
request ever dispatches it — is structural: any future wiring change on
the dispatch seam can reintroduce it without a compile error and without
a spec failure, unless something on the request path itself notices that
a registered attachment point went silent.

## Decision

Add a first-party hook-only plugin, `hook-firing-audit-plugin`, that
detects the silent-inertness class at request time with zero Core or
Platform changes:

1. **Eight probes, one per attachment point.** Each of the 8 stage
   attachment points (pre/post × parse/resolve/execute/format) gets a
   probe hook at first-party-floor priority 100 that stamps its stage
   wireName into a shared accumulator on `context.meta` (key
   `io.sm8.plugins.hookfiringaudit:stamps`).

2. **One reporter at PostFormat (priority 898).** A Post-hook with
   Observer semantics (`runsOnStop = true`) stamps its own stage, then
   diffs the accumulated stamps against all 8 expected wireNames. The
   report (`fired` / `skipped` / `missing` / `stopped`) is written to
   `context.meta` under `io.sm8.plugins.hookfiringaudit:report` for any
   transport to surface via the meta-inspector.

3. **Typed anomaly on a real gap.** A missing stamp at a stage the
   pipeline actually reached is surfaced as
   `EngineError.UnsupportedCapability(engine = "hook-firing-audit-plugin",
   capability = "HookNotFired", message = "hooks registered but never
   fired: …")` on `context.meta` under
   `io.sm8.plugins.hookfiringaudit:anomaly`.

4. **Short-circuit-aware classification.** When a pre-hook sets
   `Context.stop = true`, the orchestrator skips subsequent stages
   legitimately. The reporter reads `Context.stage` (the stage where the
   halt was stamped) and classifies every expected stamp at-or-after
   that stage as `skipped` rather than `missing`, so the short-circuit
   path is audit-clean while the reached-but-silent path is not.

All per-request state lives on the `Context`; probes and reporter are
stateless and `Serializable`. No Spark types, no captured engine state,
no new Core API.

## Layer placement

`plugins/hook-firing-audit-plugin/` — hook-only plugin, first-party
priority range. Depends on `sm8-core` only. Registered via
`META-INF/services/io.sm8.sdk.Plugin`; Maven coordinates declared in
`META-INF/sm8/plugin.properties` for the allowlist.

## Consequences

- A future dispatch-seam regression that silences a stage's hooks is
  detected on the first request after the regression lands, not at
  code-review time.
- The per-request report gives operators a query-correlated record of
  which attachment points actually fired.
- The probes add 8 meta-map writes per request — constant overhead,
  no I/O, no synchronization.
- Priority interaction: probes at 100 run after Core-range hooks
  (0-99) and before later first-party hooks at each stage; the reporter
  at 898 runs after all other first-party PostFormat hooks. The plugin
  observes dispatch; it does not reorder or gate it.

## Alternatives Considered

- **Core-side assertion in `HookRunnerOrchestration`** (compare
  registered vs dispatched per stage): rejected — puts audit policy in
  the Core seam it is auditing, and a bug in the auditor would be
  invisible to itself. The plugin layer keeps the audit externally
  verifiable.
- **Compile-time scalafix rule** asserting every `HookStage` value has
  at least one production dispatch site: complementary, not a
  substitute — it cannot catch runtime-only regressions such as a
  stage conditionally skipped by a future orchestrator change.
- **Log-based detection** (warn when a stage's hook list is non-empty
  but its dispatch count is zero at shutdown): delayed, process-scoped,
  and invisible to per-request correlation.

## References

- `docs/adr/0010-a-enginehookdispatcher-stage-parameter.md` — the stage
  parameter contract whose regression this plugin guards.
- `docs/project_status/2026-08-26-adr-010-a-orchestration-layer-retrospective.md`
  — the realized silent-inertness defect and its fix.
- `docs/rfcs/2026-08-12_v1_architecture-spec/hooks.md` — the 8
  attachment points and priority ranges.
- `docs/research/failure-modes-2026-09-04.md` — the failure-mode survey
  ranking silent no-ops as the most-recurring high-severity pattern.
