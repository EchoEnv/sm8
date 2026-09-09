# Runbook — rollup refresh + query-frequency observability

Ticket 6 of `docs/wayfinder/2026-09-06-pre-aggregation.md` · Design: `docs/adr/0022-pre-aggregation-sm8-native.md`

## What exists after this ticket

| Piece | Layer | Purpose |
|---|---|---|
| `sm8 rollup-refresh <model>` | adapter (sm8-cli) | one command rebuilds a model's rollups |
| `RollupRefreshService/refresh` | adapter (sm8-platform) | server-side trigger surface (Restate SERVICE+SHARED) |
| `RollupRefresher.refreshModel` | connector (spark-connector) | eager re-materialization of every declared rollup |
| `RollupMaterializer.persistCatalog` | connector (spark-connector) | durable `saveAsTable` write (job runs before return) |
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
- The write is a full re-aggregation of the base table (no incremental/watermark support in v1). For very large bases, schedule inside the warehouse's quiet window.
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
- Dispersion measures (Stddev/Variance) are refused on the coarsening arm — declare the rollup at the coarser grain instead.
- The refresh surface is session-catalog scoped in tests; point `saveAsTable` at your production catalog via the server's Spark session configuration.
