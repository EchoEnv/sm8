# ADR-0027: Rollup refusal telemetry reader (`sm8 rollup-report`)

**Status:** Proposed.
**Date:** 2026-09-08.

## Context

The rollup lane shipped end-to-end in four PRs (#345 staleness, #346 grain,
#347 observability, #349 routing invocation). Operators can now see the
counters — `sm8_rollup_rewrites_total`, `sm8_rollup_refusals_total`,
`sm8_rollup_refusals_permanent_total`, plus per-reason counters like
`sm8_rollup_refusals_noGroupSetMatch` — but only as raw Prometheus text on
the `/metrics` endpoint (port 9090 by default).

A measurement ticket was the explicit recommended next pick (orangutan session
2026-09-08): read the counters in their first weeks of production, rank
refusal reasons by frequency, and turn "which refusal dominates?" from a
hypothesis into a measurement. The arch review of #349 called this out
verbatim as the data point needed to pick the next follow-up
(`AlgebraicStateNotWired` → algebraic state migration; `NoGroupSetMatch`
→ more rollups needed; `GrainMismatch` → coarsening default).

## Decision

**Add a new CLI command `sm8 rollup-report` (CLI side only — no server
or core change).** The command:

1. Fetches `GET /metrics` from the metrics port (default 9090; overrides
   via `--metrics-url`).
2. Parses the Prometheus text exposition format with a hand-rolled
   zero-dependency parser (~80 LOC; no regex-on-Prometheus-output to
   avoid the `version`/`HELP`/`TYPE` block edge cases).
3. Filters to the rollup counter set:
   - `sm8_rollup_rewrites_total`
   - `sm8_rollup_refusals_total`
   - `sm8_rollup_refusals_permanent_total`
   - `sm8_rollup_refusals_<reason>` for every observed reason.
4. Renders a ranked report (default):
   - **Header** with the three totals (rewrites, refusals, permanent).
   - **Per-reason ranking** (refusals desc) with each row showing
     reason label, count, and share of total refusals.
   - **Bottom line**: the dominant reason (if any) plus a one-line
     recommended next pick (data-driven, not a hard recommendation).
5. Supports `--json` for machine consumption (per the existing CLI
   convention).
6. Supports `--metrics-url <url>` for the metrics endpoint base
   (default `http://localhost:9090`, env override
   `SM8_METRICS_URL`).

**Zero new server code.** The counters already exist on the wire via
`MetricsHttpRoute.start` (port 9090, `/metrics`). The report is a pure
client-side read.

**Zero new core code.** No engine-portable changes; the report is
specific to the rollup refusal taxonomy, which lives in `sm8-core` but
needs no augmentation.

**Layer discipline (RFC §3)**: a new CLI command in `sm8-cli`. The CLI
already depends on `sm8-core` (for shared types) and `sm8-platform`
(only via the `sm8 metrics` and `sm8 rollup-refresh` patterns). Adding
`sm8 rollup-report` follows the same pattern as `sm8 rollup-refresh`
(ADR-0022 Ticket 6) and `sm8 inspect` (generic meta-inspector).

## Alternatives considered

1. **Wire the report as a new MCP tool (`get_rollup_report`).** Rejected
   for this ticket — the read is operational (cron + on-call), not
   conversational. Operators want a single command they can paste in a
   postmortem timeline, not an MCP round-trip.
2. **A new platform service (`RollupReportService/measure`).**
   Rejected — adds a Restate handler and a wire DTO for a 5-line
   computation. The data is already on the wire via `/metrics`.
3. **Tail the existing `audit-tail` command.** Rejected — audit logs
   are per-request events, not aggregated counters. The counters live
   in `QueryMetrics`; a separate reader is the right shape.

## File-level change list

- `sm8-cli/src/main/scala/io/sm8/cli/Main.scala` — new command dispatch
  + `cmdRollupReport` (~120 LOC) + `--metrics-url` config field +
  usage line.
- `sm8-cli/src/test/scala/io/sm8/cli/...` — unit tests for the
  Prometheus text parser (golden text → parsed counter map).
- This ADR.

## Tests

- **Parser correctness.** Golden-text regression: a hand-crafted
  `/metrics` body with the rollup counter set + a non-rollup metric
  + a comment line + a `# TYPE` block parses to the expected
  counter map.
- **Ranking correctness.** A fixture with rewrites=10,
  refusals={noGroupSetMatch=5, grainMismatch=3, unsplittableAggregate=1,
  filtersNotEvaluable=1} ranks in the right order with correct
  percentages.
- **Empty-state.** A zero-counter body renders "no rollup traffic
  yet — the routing fold has not been exercised since startup".
- **JSON output.** A `--json` run prints the same data as a flat
  `Map[String, Long]`.
- **HTTP failure modes.** Metrics endpoint unreachable (exit 3, per
  the existing CLI transport convention); 404 / non-Prometheus body
  (exit 1, with a clear stderr message).

## Consequences

- Operators gain a single-command view of "where the rollup lane is
  bleeding" without reading raw Prometheus.
- The next ticket can be data-driven: read the report, pick the
  dominant reason, fix that.
- The report is a small, isolated CLI change — no platform, no
  connector, no core, no plugin work. No feature flag, no gate.

## Non-consequences

- **No change to `MetricsSink` or `QueryMetrics`.** Counters stay
  where they are.
- **No new wire surface.** The report consumes the existing
  Prometheus text exposition.
- **No new dependency.** The Prometheus text parser is hand-rolled;
  the spec is 30 years old and the format used in this project
  has a stable shape.
- **No operational runbook change.** `docs/runbooks/rollup-refresh.md`
  already documents the `/metrics` endpoint.
