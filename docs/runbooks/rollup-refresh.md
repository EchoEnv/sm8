# Runbook — rollup refresh + query-frequency observability

Ticket 6 of `docs/wayfinder/2026-09-06-pre-aggregation.md` · Design: `docs/adr/0022-pre-aggregation-sm8-native.md`

## What exists after this ticket

| Piece | Layer | Purpose |
|---|---|---|
| `sm8 rollup-refresh <model>` | adapter (sm8-cli) | one command rebuilds a model's rollups |
| `RollupRefreshService/refresh` | adapter (sm8-platform) | server-side trigger surface (Restate SERVICE+SHARED) |
| `RollupRefresher.refreshModel` | connector (spark-connector) | eager re-materialization of every declared rollup |
| `RollupMaterializer.persistCatalog` | connector (spark-connector) | durable `saveAsTable` write (job runs before return) |
| Tier 2 merge refresh (`RollupRefresher.mergeRefreshModel`) | connector (spark-connector) | row-level MERGE refresh for scoped buckets + watermark advance (PR #370); CLI/REST surface added in #376 (`--tier 2 --scope <hour-buckets>`) |
| Cascade rollups (`RollupCascadeRefresher.cascadeRefresh`) | connector (spark-connector) | build a coarser rollup from a FINER rollup's partial states (e.g. weekly from daily); ADR-0031; CLI: `--tier 2 --scope <day-buckets>` when the target declares `cascade_source` |
| `query-frequency-observer` plugin | plugin | counts query shapes per model (PostExecute observer) |
| `QueryShapeCounters.snapshot()` | plugin | programmatic counts read (hottest shapes first) |

## One-command refresh

```bash
sm8 rollup-refresh flights
```

Behavior:
- POSTs `{"model":"flights"}` to `RollupRefreshService/refresh`.
- The server resolves the model's `rollups:` declarations and eagerly rebuilds each one (`saveAsTable`, overwrite mode — the aggregation job RUNS; a returned result means the table is durable).
- Prints one line per rollup: `refreshed -> <table>` or `FAILED: <reason>`; exit 0 only if every rollup refreshed. Per-rollup isolation: one failure never blocks the others.

### Cron wiring

Refresh cadence should match the base table's ingest cadence. Example, nightly 02:30, with staleness alerts:

```cron
# m h dom mon dow  command
30 2 * * * $HOME/bin/sm8 rollup-refresh flights >> $HOME/logs/rollup-refresh.log 2>&1
```

Operational notes:
- `rollup-refresh` is **idempotent**: re-running overwrites the same `<model>__<rollup>` table. A failed run leaves the previous table intact for rollups that did not start or failed before write; rollups are rebuilt independently.
- **Do not overlap refreshes for the same model**: two concurrent `saveAsTable` overwrites on one table can race. Schedule cron so runs cannot overlap (the run duration is bounded by the base-table size).
- Refreshes are **not transactional across rollups** — a model with several rollups can end a run half-refreshed (each table individually consistent). If cross-rollup consistency matters, refresh all rollups *before* any consumer is pointed at them, or order consumers after the cron window.
- **Within ONE rollup**, `saveAsTable` overwrite is delete-then-write (non-atomic): readers querying during the write window can see an empty/partial table, and a crash mid-write loses the previous version. Schedule inside the quiet window; if reader-facing atomicity matters, stage to a temp table and swap (future work).
- **Single-writer discipline**: do not overlap refreshes for the same model (nightly cron + a manual `rollup-refresh` during the window can race concurrent overwrites of the same table). The refresh handler is SHARED (concurrent invocations allowed by design).
- Tier 0/1 refreshes are full re-aggregations of the base table (or of the scoped partitions, for Tier 1). For very large bases, schedule inside the warehouse's quiet window. Tier 2 (row-level MERGE, incremental by scope buckets) shipped in PR #370 — see the "Tier 2 merge refresh" section below.
- The command targets the server's configured Spark session; the CLI only needs network access to the server.
- **Timeout**: the CLI's HTTP timeout is 30s (exit 3 = transport). An eager refresh of a large base runs the aggregation job server-side BEFORE the response — raise the server-side response budget or the CLI timeout for large models. Note the job also runs on the server's shared Spark session concurrent with query traffic: schedule big refreshes in the quiet window (memory pressure on small hosts).
- The CLI's HTTP timeout is 30s (client default). A refresh of a very large base can run LONGER than that server-side: the CLI will report a transport-style failure (exit 3) while the server-side job continues to completion. For large models, raise the client timeout or check the server logs for the authoritative outcome. Do not re-invoke in a tight loop — refresh is idempotent but each invocation re-runs the aggregation.

## Query-frequency observability

Two complementary surfaces feed rollup-selection decisions:

1. **Per-invocation event stream** (Ticket 2): logger `io.sm8.platform.query.QueryShape` at DEBUG — one line per query with the normalized shape. Enable selectively:

```bash
# logback example
<logger name="io.sm8.platform.query.QueryShape" level="DEBUG"/>
```

2. **Aggregate counts** (this ticket): the `query-frequency-observer` plugin keeps JVM-global counters keyed by canonical shape (`model|v<version>|m=measures|d=dimensions`, sorted + deduped so set-equal shapes collide). Read them:

```bash
sm8 inspect io.sm8.plugins.queryfreqobs:counts
```

Each query's post-execute state carries the hottest-first snapshot under that key; the observer also publishes it into `context.meta` for the meta-inspector transport.

Note: `snapshot()` reads each counter atomically but the set of reads is not a single atomic instant — a snapshot taken mid-traffic may mix counts from slightly different moments. Acceptable for hot-shape discovery; do not use as an accounting ledger.

### Reading the counts for rollup selection

- Hottest shapes at the top — check whether a declared rollup's (dims, measures) covers any hot shape's group set (`dims ⊆`) and aggregates (Additive set materializes in v1).
- Shapes that never appear do not justify a rollup — drop or don't declare it.
- Cardinality guard: beyond 10,000 distinct shapes, new shapes count under `__overflow__` (telemetry degrades gracefully, never leaks).
- Shape keys use measure ALIASES: renaming an alias fragments its counts across the rename boundary. Treat alias renames as telemetry resets.
- Shape keys are transport-dependent: REST queries key on the request's measure strings, MCP/DSL queries on the declared measure aliases. If the two transports normalize names differently, counts split across two keys for the same logical shape.

## File hygiene (small-file compaction)

Scoped Tier 1 refreshes (one partition per refresh) add a new data
file to the touched partition on every run. File count is CUMULATIVE
per partition: a partition refreshed hourly for a month accumulates
~720 small files in total, not per refresh. Scan latency degrades as
cumulative file count grows. This is the standard Iceberg small-file
problem — handled by periodic compaction, not by the refresh path.

### Table properties (set at rollup creation)

```sql
ALTER TABLE iceberg_cat.<model>__<rollup> SET TBLPROPERTIES (
  'write.target-file-size-bytes'='134217728'  -- 128 MB
);
```

Set the property once per table — before or after the first refresh
(the property governs subsequent writes only). The materializer does
not set it automatically.

### Scheduled compaction

Run `rewrite_data_files` on each rollup table on a schedule slower
than the refresh cadence (e.g. daily compaction for hourly refreshes):

```sql
CALL iceberg_cat.system.rewrite_data_files(
  table => '<model>__<rollup>',
  options => map(
    'min-input-files', '5',
    'target-file-size-bytes', '134217728'
  )
);
```

For a HadoopCatalog the `table` argument is the table name WITHOUT the
catalog prefix (the CALL's `system` namespace is resolved against
`iceberg_cat` already). Note `target-file-size-bytes` as a procedure
option governs the compaction OUTPUT; the table property governs
subsequent writes — set both.

**Serialization with refreshes** (ADR-0030 §D1): compaction and
refresh must not run concurrently on the same table. A
`rewrite_data_files` job typically takes 10-45 minutes — for hourly
refresh on tables with more than a few partitions, there is no
realistic gap between refresh windows. Options:

- **Tables with few partitions (≤ 8) and small data**: time-window
  gating works (compaction at 04:00; refreshes at :15/:45).
- **Larger tables**: switch compaction to a slower cadence (every 6h
  or daily) at a known quiet period, OR use the per-rollup advisory
  lock ADR-0030 §D1 specifies (compaction and refresh each acquire it
  before reading the table's snapshot; whichever runs first completes
  before the other starts). NOTE: the advisory lock is ADR-specified
  but NOT yet implemented in sm8-platform — until it ships, the
  time-window gating is the only available mechanism.

### When to compact (checkable query)

Run this against the rollup's metadata to get per-partition file
counts and average sizes:

```sql
SELECT
  data_file.partition,
  count(*) AS files,
  sum(data_file.file_size_in_bytes) / count(*) AS avg_bytes
FROM iceberg_cat.<model>__<rollup>.files
GROUP BY data_file.partition
ORDER BY files DESC;
```

Compact when: any partition shows `files > ~100`, or `avg_bytes` < 10
MB. Latency regression is measurable with the observation harness
(`RollupObservationHarness`, see PR #351) and the Gate B probe
(`RollupRefreshCostProbe`, prints per-partition byte/file counts).

## Tier 2 merge refresh (row-level MERGE, PR #370)

> **CLI/REST surface (shipped):** `sm8 rollup-refresh <model>
> --tier 2 --scope <hour-bucket-1>,<hour-bucket-2>,...` triggers the
> cascade refresh via `RollupRefreshService`. Without `--tier`, the
> legacy `{model}` shape drives Tier 0/1 (pre-cascade behavior,
> unchanged).

Tier 2 refreshes a rollup by **merging only the recomputed rows for
the declared scope's buckets** into the existing Iceberg rollup table
(`RollupRefresher.mergeRefreshModel`), instead of swapping whole
partitions (Tier 1) or the whole table (Tier 0). The win it targets:
intra-partition rows that did NOT change are no longer rewritten
(the Gate B "rewritten-but-unchanged bytes" signal).

### When to enable Tier 2 for a rollup

Per-deployment decision, measured not guessed — see ADR-0029 §Gate B
(amended 2026-09-09) and
`docs/runbooks/gate-b-trace-collection.md`: enable when refresh
wall-clock (Tier 1 scoped) > 5 min AND rewritten-but-unchanged bytes
> 30% of the rollup, measured over a representative window at
production refresh cadence. Below either threshold, stay at Tier 1 —
that is the ladder working as designed.

### What Tier 2 refresh does

- Recomputes the scoped buckets' aggregate rows from the base table
  (same aggregation shape as Tier 0/1 — no cross-tier schema drift).
- **MERGE** into `iceberg_cat.<model>__<rollup>`: matched keys
  `UPDATE` only when values actually changed (`IS DISTINCT FROM`
  guard — unchanged rows are not rewritten); new keys `INSERT`.
  Never `+=` accumulation (ADR-0030 D2-5: not idempotent).
- After the data commit succeeds, advances the **watermark table**
  `iceberg_cat.<model>__<rollup>__watermark` (one row per
  `(model, rollup, bucket)`: `is_final`, `last_refreshed_at`,
  `last_commit_snapshot_id`). Day-grain buckets strictly before
  today latch `is_final = true` and it never regresses
  (monotone OR-latch). A merge failure never advances the watermark.
- Typed refusals (fail-loud, never silent): grain-less rollup
  (`RollupMergeRefresher.grain`), missing rollup table (`table` —
  Tier 2 refreshes, it never creates; run a Tier 1 refresh first),
  unsafe identifiers (`identifiers`), duplicate merge keys
  (`RollupMergeRefresher.duplicateKeys`), scope naming no source
  buckets (`scopeEmpty`), recomputed source containing buckets
  outside the scope (`scopeUncovered`), and the MERGE step itself
  failing (`merge` — Iceberg optimistic-concurrency conflicts surface
  here; retry is the caller's policy).

### Freshness policy (optional, per rollup)

A rollup may declare `freshness: final_required` (with
`time_grain` + `grain_dimension`). With the policy, any queried
bucket whose watermark row is non-final (or absent) refuses routing
with `RollupBucketStale` — the freshness gate. Without the policy
(the default), routing is unchanged: buckets route even when
non-final. Choose the policy when consumers must never see a bucket
that late data could still change.

### Operational notes

- **Prerequisites**: the rollup table must already exist as an
  **Iceberg** table — which means **one Tier 1 refresh first (a
  declared scope; Tier 0 alone writes Parquet and does NOT satisfy
  this)** — and the rollup must be grain-bucketed (`time_grain` +
  `grain_dimension`; a grain-less rollup refuses with the `grain`
  capability).
- **Scope = the delta declaration**: v1 Tier 2 takes the scope as a
  plain `scopeValues: List[String]` of canonical bucket strings
  (NOT the Tier 1 `RefreshScope` ADT) and refreshes every bucket the
  list names (the hot-window pattern: re-merge today's partition
  every run). Base-table snapshot-diff change detection is a future
  refinement.
- **Concurrency**: MERGE is optimistic-concurrent; a concurrent
  commit fails the merge loudly (no internal retry by design — the
  caller owns the retry policy, same single-writer discipline as
  Tier 0/1).
- **Cross-table atomicity is NOT claimed** (Iceberg design): the
  watermark commit follows the data commit. A crash between them
  leaves the watermark stale-but-valid; it never claims more final
  than the data (ADR-0030 D3).

## Cascade rollups (ADR-0031, Tier 2)

A **cascade rollup** is built from another rollup's partial states
(e.g. weekly from daily) instead of from base. When the finer rollup
is Tier 1/Tier 2 (Iceberg), cascading the coarser rollup is much
cheaper than a base re-scan — this is exactly where Tier 2's
row-level MERGE pays off.

### Declaring a cascade

In the model YAML, the COARSER rollup declares which FINER rollup
it builds from:

```yaml
rollups:
  - name: daily
    dimensions: [order_date_day, region]
    measures: [total, avg_price]
    time_grain: day
    grain_dimension: order_date_day
  - name: weekly
    dimensions: [order_date_week, region]
    measures: [total, avg_price]
    time_grain: week
    grain_dimension: order_date_week
    cascade_source: daily    # the finer rollup's name
```

Both rollups must be on the SAME model, the target must be grain-
bucketed, and the target's grain must strictly coarsen the source's
(hour → day, day → week, etc. — the truncation semantics come from
ADR-0024's date_trunc contract). The validator refuses at deployment:
self-cycles, two-node cycles, longer cycles, unknown source names,
dimension mismatches, and same-or-finer grain declarations.

### Measure eligibility

Only Additive (Sum, Count) and Algebraic (Avg, Stddev, Variance)
measures cascade. Min/Max also cascade (binary re-application).
Positional (First/Last), Holistic (Median/Percentile), and
Approximable (CountDistinct, ApproxPercentile) NEVER cascade —
declare those on a non-cascaded rollup (built from base) instead.

### Triggering a cascade refresh

```bash
# Tier 2 cascade: weekly from daily's day buckets
sm8 rollup-refresh mymodel --tier 2 \
  --scope '2026-09-07,2026-09-08,2026-09-09,2026-09-10,2026-09-11,2026-09-12,2026-09-13'
```

The `--scope` values are the SOURCE rollup's day buckets whose
deltas to cascade. The connector:

1. Verifies the source's watermark `is_final=true` for every
   coarsened bucket (any non-final source bucket refuses with
   `CascadeSourceNotFinal` — the daily rollup cannot be more final
   than its source).
2. Pins the source's snapshot at build start (concurrent source
   refreshes don't contaminate the build).
3. Aggregates the source's PARTIAL STATES (not raw base rows):
   Sum/Count sum their partials; Min/Max re-apply the binary op;
   Avg/Stddev/Variance use the Welford cross-group merge
   (`m2_ab = m2_a + m2_b + δ²·n_a·n_b/n_ab` — verified against the
   ADR-0023 1e8±1.0 cancellation fixture).
4. MERGEs into the weekly Iceberg table + advances the weekly
   watermark with the PINNED SOURCE snapshot id (not the weekly's
   own — the diagnostic contract per D3 rule 2).

### Prerequisites

- Both rollup tables must already exist as Iceberg (run a Tier 1
  refresh on each first).
- Both rollups must be grain-bucketed (`time_grain` +
  `grain_dimension`).
- The target must declare `cascade_source` pointing at the source.
- The source's watermark must be `is_final=true` for the scope's
  buckets (past day-grain buckets latch automatically after their
  refresh; today's bucket stays open until the day closes). NB: the
  finality heuristic only latches DAY-grain sources — cascading from
  a sub-day source requires the operator to explicitly advance the
  source's watermark per bucket (a documented v1 boundary).

### When to cascade vs build from base

- **Cascade**: the source rollup is already refreshed (its watermark
  is final), the target's grain strictly coarsens the source's, and
  the measures are Additive/Algebraic. The scan cost is the source
  rollup's size (much smaller than base).
- **Build from base** (drop `cascade_source`, run without
  `--tier 2`): when the source isn't fresh enough, when you need
  Positional/Holistic/Approximable measures, or when the base scan
  is cheap enough that the cascade indirection isn't worth it.

### Staleness propagation

The cascade inherits the source's staleness — a weekly rollup built
from a stale daily rollup is stale by definition. The connector
refuses (`CascadeSourceNotFinal`) rather than silently propagating.
If you need the weekly bucket NOW despite a non-final source day,
the honest options on the current surface are:

1. **Drop `cascade_source` from the model YAML** (edit + redeploy)
   and run the legacy eager rebuild: `sm8 rollup-refresh mymodel`.
   This rebuilds daily from base (slower, but always available).
2. **Wait for the source's window to close** (the daily watermark
   latches final on the next daily refresh after the lateness
   window), then re-run the cascade.
3. **Programmatic**: call `RollupRefresher.mergeRefreshModel` from a
   Spark job with the daily's own scope (no CLI/REST surface for
   this yet — the Tier 2 v1 boundary note above applies).

Note: `--tier 1 --scope <buckets>` is NOT a valid fallback on the
current CLI — `--scope` without `--tier 2` is silently dropped (the
legacy `{model}` shape has no scope field to carry it), and tier 1
runs the whole-model eager rebuild, not a scoped refresh.

## Known limits (v1)

- **Refresh tiers** (ADR-0029/0030/0031): a rollup refreshed WITHOUT a
  declared scope writes Tier 0 — a Parquet table
  `<model>__<rollup>` in the session catalog. A rollup refreshed WITH
  a declared scope writes Tier 1 — an Iceberg table
  `iceberg_cat.<model>__<rollup>`. Same rollup name, DIFFERENT
  catalogs and formats: when investigating a missing or stale rollup,
  check both locations. See ADR-0029 for the strategy ladder.
- **Upgrading from Parquet-only refreshes**: on the first refresh with
  a declared scope, a NEW Iceberg table `iceberg_cat.<model>__<rollup>`
  is created; the pre-existing Parquet `<model>__<rollup>` in the
  session catalog is NOT touched or removed. Operators must (a)
  explicitly drop or archive the old Parquet table after verifying
  the Iceberg table serves correct results, and (b) confirm the
  routing lane resolves the Iceberg table (it wins by ADR-0028's
  format-standard resolution).
- Time-grain rollups (`time_grain:` + `grain_dimension:`) materialize and route: the grain dimension must be `Date`/`Timestamp` (declared or resolved), and a query at a coarser grain re-buckets a finer rollup for Additive measures and Avg. Truncation is session-timezone. Week-bucket boundaries are whatever the engine's `date_trunc('week')` emits (pinned by test in `RollupMaterializerSpec`); re-verify the pin on a Spark upgrade.
- Dispersion measures (Stddev/Variance) are refused on the coarsening arm at QUERY time (the rewriter cannot re-bucket a finer dispersion rollup to a coarser grain). Cascade CONSTRUCTION of dispersion measures is supported (the Welford merge is the correct cross-group reduction); the refusal applies to serving a query at a coarser grain than the rollup's own grain — declare the rollup at the coarser grain instead.
- The refresh surface is session-catalog scoped in tests; point `saveAsTable` at your production catalog via the server's Spark session configuration.
- **Tier 2 v1 boundaries** (ADR-0030 D2-6; D5 governs the FUTURE
  snapshot-diff path, disclosed): the delta is scope-declared, not
  base-snapshot-diffed; sub-day grains report non-final until a
  lateness model exists. (The design-level "non-decomposable measure
  columns fall back to Tier 1 recompute" clause is for that future
  path — today such measures are refused at materialization time,
  capability `RollupMaterializer.measureState`, so a Tier 2 rollup
  never carries them.)
