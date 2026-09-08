# Rollup routing — the operator's observation handbook

**Status:** Active.
**Scope:** Reads from the counters shipped across PRs #347 (refusal
observability) and #349 (routing invocation), consumed via the
`sm8 rollup-report` CLI shipped in #350 (ADR-0027).

The rollup lane is live end-to-end: every `SparkEngineProvider.query()`
on a model with declared rollups invokes `RollupRewriter.rewrite`,
fails open, and increments one of three counter families on the
process-wide `QueryMetrics` singleton. The counters flow to:

- `MetricsHttpRoute` → `GET /metrics` (port 9090, Prometheus text)
- `MetricsService/snapshot` → JSON snapshot for the MCP `get_metrics` tool
- `rollup-refusal-observer` plugin → `context.meta` for `sm8 inspect`

This handbook is what an operator runs when the report shows something
unexpected — and the rubric for interpreting the result.

## 1. How to read the report

```sh
sm8 rollup-report                          # default: http://localhost:9090
sm8 rollup-report --metrics-url http://staging-sm8:9090
SM8_METRICS_URL=http://prod-sm8:9090 sm8 rollup-report
sm8 rollup-report --json | jq '.["sm8_rollup_refusals_noGroupSetMatch"]'
```

The human-readable output (default) shows three scalars and a per-reason
ranking. The `--json` output is the same data as a flat sorted map for
cron / dashboard consumers.

### 1.1 What every number means

| Counter | Increments when | Expected steady state |
| --- | --- | --- |
| `sm8_rollup_rewrites_total` | The routing fold rewrote a canonical plan to a rollup scan | Positive, growing with rollup-served traffic |
| `sm8_rollup_refusals_total` | The fold returned `Unchanged(reason)` — query fell through to the base path | Positive if some queries miss the rollup surface; the per-reason breakdown tells you why |
| `sm8_rollup_refusals_permanent_total` | A subset of refusals whose reason is `UnsplittableAggregate` (holistic / positional / approximable aggregates have no re-aggregation algebra) | LOWEST: any non-zero number is a hot signal. Each one is a query the rollup lane can never serve — but the query still runs (base path) and is correct. |

### 1.2 The per-reason ranking

The 8 refusal reasons come from the sealed `RollupRewriteRefusal` ADT
(`sm8-core/src/main/scala/io/sm8/core/rel/RollupRewriter.scala:105-149`).
They map 1:1 to the hint table the report uses; the next pick
falls out of which one dominates.

| Reason (label) | Meaning | Recommended action if it dominates |
| --- | --- | --- |
| `noGroupSetMatch` | The query's group set isn't a subset of the rollup's declared dimensions | Declare a rollup that covers this shape; or add the missing dim to an existing rollup (pure YAML, no code) |
| `grainMismatch` | The query requests a grain the rollup doesn't store | Either add a coarser-grain rollup, or accept that the query misses the rollup (coarsen manually) |
| `unsplittableAggregate` | A `Count(expr)`, `Sum(distinct)`, or holistic/positional/approximable aggregate — v1 has no re-aggregation algebra for these. **Fixable subset:** `Count(expr)` and `Sum(distinct)` are caller-side fixes (rewrite the measure). **Non-fixable:** holistic / positional / approximable (the query class is unsuited to rollups). | This is a permanent refusal (counts toward `refusals_permanent_total`). If high, investigate the dominant `unsplittableAggregate` shape: per-measure rewrite for the fixable subset; pre-aggregate server-side or split the query for the non-fixable subset. The rollup lane itself is doing the right thing |
| `filterNotEvaluable` | A `where` filter references a column the rollup table doesn't carry | Add the column to the rollup's declared dimensions (must be a `RollupSpec.dimensions` member — see `RollupRewriter.scala:613,936`) |
| `sourceKindUnsupported` | The model source is `ByPath` or `ByProvider` — v1 only routes `ByName` | This is structural — the source kind must change, not the rollup |
| `nonCanonicalShape` | The plan isn't the canonical `Scan → Filter* → Aggregate` shape (e.g. projects the wrong place) | Caller-side — the query needs to be reshaped; the report can't help |
| `algebraicStateNotWired` | An Avg / Stddev / Variance request routed, but the chosen rollup table does not carry the named partial-state columns (`count__F`, `sum__F`, `m2__F`). The rewriter + two-phase `Aggregate→Project` are already wired (the algebraic routing shipped with ADR-0023); this refusal is purely a *materializer* gap on the chosen rollup table. | Recover by re-materializing: `sm8 rollup-refresh <model>`, or add the measure to the rollup's `RollupSpec.measures` so the rewriter picks the right partials |
| `rollupSchemaStale` | The connector detects at `lowerScan` that the chosen rollup's physical table is missing a column the rewriter routed against (e.g. `m2__F` absent on a rollup that was materialized before the Welford migration shipped). Surfaces as a typed `UnsupportedCapability` with the `"RollupSchemaStale"` capability tag — LOUD, not silent | Run `sm8 rollup-refresh <model>` — the rewriter is doing the right thing; the table is stale |

## 2. The interpretation rubric

The report is a diagnostic, not a dashboard. Three readings:

### 2.1 Baseline (zero counters)
```
rollup routing report
  rewrites:            0
  refusals:            0
  refusals permanent:  0
  no rollup traffic yet — the routing fold has not been exercised since startup
```

Expected before any query has been served. **Not actionable** — the
fold has not had a chance to run. If you see this after steady traffic,
the routing fold is not being invoked; check that `SparkEngineProvider`
is the engine actually serving requests.

### 2.2 Healthy reading
```
rollup routing report
  rewrites:            1247
  refusals:            13
  refusals permanent:  0
  refusals by reason (ranked):
    noGroupSetMatch              8  (61%)
    grainMismatch                3  (23%)
    filterNotEvaluable           2  (15%)
  dominant reason: noGroupSetMatch (8) — more rollup declarations likely needed
```

`rewrites` large, `refusals` small, `permanent` zero, no single reason
> 30%. The rollup lane is doing its job; the residual refusals are
individually addressable (each one is a 1-YAML-line change). Pick the
biggest reason and act.

### 2.3 Saturated reading
```
rollup routing report
  rewrites:            892
  refusals:            4410
  refusals permanent:  3184
  refusals by reason (ranked):
    unsplittableAggregate      3184  (72%)
    noGroupSetMatch             803  (18%)
    grainMismatch               423  (9%)
  dominant reason: unsplittableAggregate (3184) — permanent refusals (holistic/positional aggregates)
```

> *Note on the example shares*: 72 + 18 + 9 = 99% (and 61 + 23 + 15 = 99%
> in §2.2). The CLI uses integer truncation for display
> (`sm8-cli/src/main/scala/io/sm8/cli/Main.scala`), so the per-reason
> shares may sum to 99% or 100% depending on the data — the exact
> value lives in the JSON output. Operationally the rounding is
> off-by-one-point at most, well within diagnostic noise.

`refusals_permanent` dominates AND `unsplittableAggregate` ranks #1.
Two things to know: (a) the dominant reason is permanent — these
queries will never route; (b) `refusals_total` is large but it's the
right shape — the fold is being called and fail-open is working. The
action is *not* "fix the routing" — it's "either accept the base
path for this query class, or pre-aggregate it server-side". The
fold is doing the right thing.

### 2.4 Caller-shape alert
```
rollup routing report
  rewrites:            0
  refusals:            1200
  refusals permanent:  0
  refusals by reason (ranked):
    nonCanonicalShape         1200  (100%)
  dominant reason: nonCanonicalShape (1200) — queries bypass the canonical shape — inspect callers
```

`rewrites=0` AND `nonCanonicalShape=100%` means EVERY query has a
non-canonical plan. The fold is being called but the query plumbing
is producing plans the rewriter can't recognize. This is a bug in
the caller (likely a non-model query or a missing dim) — the report
will pin it, but the fix lives outside the rollup lane.

## 3. Workflow

The intended operating loop:

1. **Baseline at deploy.** Run `sm8 rollup-report` immediately after a
   fresh deploy. Expect the baseline reading (zero counters). If you
   see non-zero counters, traffic is flowing from a previous process
   (or a different host) — the metrics port is per-process; check the
   `--metrics-url`.

2. **Hourly cron in production.** Once-per-hour `sm8 rollup-report
   --json` feeds the counter-delta stream to your observability
   stack. Read the three scalar counters directly (avoid the
   `sm8_rollup_refusals_*` glob — it also matches the scalar
   `_total` keys). Three thresholds to alert on (all implicitly
   assume `rewrites_total > 0` in the window — on a cold start with
   zero queries, rates are undefined and the alert should stay
   silent):

   | Metric | Alert at | Why |
   | --- | --- | --- |
   | `sm8_rollup_refusals_total` rate | sustained > 50% of `sm8_rollup_rewrites_total` rate (both non-zero) | The fold is being invoked but most calls fall through — likely a coverage gap |
   | `sm8_rollup_refusals_permanent_total` rate | any non-zero over 1h | Permanent refusals never recover; investigate on the caller side |
   | `sm8_rollup_refusals_algebraicStateNotWired` rate | sustained > 100/h | The dominant model's rollup table is missing partial-state columns — re-materialize it, or add the measure to the rollup declaration |

3. **Dominant-reason decision tree.** When the daily report shows a
   single reason > 30% of refusals, that reason is the next pick.
   The hint table in §1.2 maps reason → action.

## 4. What this is NOT

- **Not a dashboard.** The CLI is for one-shot diagnostics. The
  `--json` output feeds a dashboard if you need one; build a
  Grafana / Datadog panel from the counter-delta stream and use this
  CLI to investigate the cause when the panel alerts.
- **Not authoritative.** Counter values reset on process restart
  (the `QueryMetrics` singleton is JVM-global; not persistent).
  Cross-process comparison requires cross-process aggregation (use
  a metrics scraper that records the values).
- **Not a profiler.** The counters do not measure latency, resource
  use, or plan quality. They measure routing decisions. Latency
  and plan-cost live in `MetricsService/snapshot`'s other sections
  (`sm8_invocation_*` and the `MetricsHttpRoute` Prometheus line
  timings). Read those for those questions.

## 5. The first reading

Captured at the moment of this PR's merge (no production deployment
exists yet — this handbook ships the rubric, not a reading). When a
real server runs `sm8 rollup-report` for the first time, the expected
reading is:

```
rollup routing report
  rewrites:            0
  refusals:            0
  refusals permanent:  0
  no rollup traffic yet — the routing fold has not been exercised since startup
```

That reading is the seed. Every subsequent reading is a delta.
