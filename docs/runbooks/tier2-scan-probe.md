# Tier2ScanProbe — runbook

AFK research artifact for [ticket #380 — MOR-hybrid design research](https://github.com/EchoEnv/sm8/issues/380).
Companion to `docs/runbooks/gate-b-trace-collection.md`; same host, same tables, same fixture.

## What it is

A synthetic probe that measures **scan latency on tables production routing excludes**.
ADR-0030 §D1 (ll. 105–108, wren H3) names this gap: the observation harness only drives
queries through the production `SparkEngineProvider.query()` path, which routes rollup-shaped
queries to rollups — so a base-table scan number was never collectable. This probe talks to
the table **directly**, bypassing `RollupRewriter`.

It is a dry-run rehearsal for the live-model Gate B study (ADR-0030 §D1) and for the
`Tier2ScanProbe` implementation named in §D5 and ticket [#382](https://github.com/EchoEnv/sm8/issues/382).

## Pre-flight (all values from `docs/runbooks/gate-b-trace-collection.md`)

```bash
# 1. Spark connect + JVM opens (Java 17+)
export SM8_SPARK_MASTER=local[2]
export SM8_SPARK_SUBMIT_ARGS="--add-opens java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens java.base/java.net=ALL-UNNAMED"

# 2. Fixture tables (same 4 as gate-b-trace-collection)
TABLES="sm8db.base_events sm8db.base_sessions sm8db.rollup_events_hourly sm8db.rollup_sessions_daily"

# 3. Spark connect server must be up
check-spark-connect   # or: nc -z localhost 15002
```

## Invocation (matches ADR §D5 parameters)

```bash
# --tables: which tables to probe
# --query:  parameterized scan. NOTE: the template filters on a DATA column
#           (event_ts) only — Spark SQL on this Iceberg pin cannot filter by
#           snapshot time inline. Snapshot context is captured by the probe
#           itself (see Stratification below), not by this query.
# --rounds: repetitions per table (default 3)
mvn -q test -pl connectors/spark-connector \
  -Dtest=Tier2ScanProbeMain \
  -Dprobe.tables="$TABLES" \
  -Dprobe.query="SELECT count(*) FROM {table} WHERE event_ts >= date_sub(current_date(), 30)" \
  -Dprobe.rounds=3
```

## Stratification (where `snapshots_since_last_compaction` comes from)

The query above measures scan latency **of the table's current snapshot**;
it cannot and does not select snapshots. The probe reads the Iceberg
metadata at probe time — current snapshot id, parent lineage depth since
the last compaction commit — and records `snapshots_since_last_compaction`
per round alongside `latency_ms`. D6's per-cycle stratification groups
rounds by that captured value; it is never derived from the SQL text.

To replay latency against a HISTORICAL snapshot (delete-file accumulation
without compaction replay), use the VERSION AS OF variant — the probe must
first resolve the target snapshot id from the metadata table:

```sql
SELECT count(*) FROM {table} VERSION AS OF {snapshot_id}
```

Use this variant only for the D6 appendix measurements; the primary study
rows use live snapshots so latency reflects what production scans see.

## Output fields (per table, per round)

| Field | Meaning | Feeds |
|---|---|---|
| `table` | probed identifier | study rows |
| `latency_ms` | wall time of one scan | scan-latency p95, stratified |
| `snapshots_since_last_compaction` | Iceberg metadata at probe time | freshness-cost column |
| `records_scanned` | rows the scan touched | sanity / cache-hit detection |
| `route` | always `direct` (bypasses rewriter) | distinguishes from routed-surface rows |

## Interpretation

- **v1 ceiling**: 3 rounds × 4 tables ≈ 12 scans; wall ≈ 40s on the 7.5 GiB host. Enough to
  shape the D5 study design, not enough for p95 — repeat at production scale for real numbers.
- If `records_scanned` is implausibly low, a Spark cache is serving the scan; rerun with a
  fresh session (`SM8_SPARK_MASTER=local[1]` restart) before trusting `latency_ms`.
- Compare `rollup_*` vs `base_*` rows: the gap between them is the "routing-excluded scan
  cost" D5 wants to bound.

## Status

- **Not implemented.** This runbook pins the interface so ticket #382's spec and the Gate B
  study can reference it. Implementation follows the `RollupRefreshCostProbe` pattern
  (same package, `probes` subpackage), stays in `connector` layer (Spark I/O is
  out of `core` per RFC §3).
