# Gate B trace collection — Tier 2 evidence procedure

Operational procedure for ADR-0029 §Gate B item 3: collecting the ≥
2 weeks of production traces that decide whether Tier 2
(row-level delta MERGE) is justified to build.

## What ships

`GateBTraceRunner` (spark-connector) — a cron-friendly wrapper around
`RollupRefreshCostProbe` that persists one JSON report per run.

## Two modes: live model vs synthetic fixture (read first)

The runner supports TWO modes:

**Live-model mode** (decision-grade metrics — but SHADOW WRITES):

```bash
spark-submit --class io.sm8.connectors.spark.GateBTraceRunner \
  <connector-jar> \
  --model <label-for-logs> \
  --out-path /var/lib/sm8/gate-b/traces/$(date +%Y-%m-%d).json \
  --model-path /path/to/model.yaml \
  --rollup <rollup-name> \
  --scope-date <yyyy-MM-dd>
```

Loads the model YAML manifest via `ModelLoader.fromStream`, resolves
the named rollup, and measures a scoped refresh + whole-table refresh
against the rollup's declared base table.

> **SHADOW-WRITE WARNING**: the runner builds an EMBEDDED HadoopCatalog
> in a temp warehouse (or `--bootstrap-warehouse` if supplied) — NOT
> your production Iceberg catalog. The base table is read via
> `spark.table(<model>.source.table)`, which resolves against THIS
> embedded catalog. If your production base table lives in a
> production catalog namespace, the probe will fail with a
> table-not-found error — **this is safe** (the probe never writes
> to production). For production-data measurement, configure the
> embedded catalog to point at the production warehouse, or stage a
> copy of the base data. See ADR-0030 §D1 for the production-catalog
> wiring (future work).
>
> **TIER 0 OVERWRITES THE FULL ROLLUP TABLE** (not just the scoped
> partition). The `--scope-date` flag scopes Tier 1 ONLY; Tier 0
> ignores it and does a full-table overwrite. Plan accordingly.
>
> **M3 (R1 loon)**: the embedded catalog is a SHADOW of production —
> same schema, same table names, but a different physical warehouse.
> The probe reads the base table via `spark.table(...)` against this
> shadow; if the base table doesn't exist in the shadow warehouse,
> the probe errors with a table-not-found message (safe: it never
> writes to production). This is by design — it prevents accidental
> production writes during evidence collection.

**Synthetic-fixture mode** (procedure exercise — NOT decision-grade):

```bash
spark-submit --class io.sm8.connectors.spark.GateBTraceRunner \
  <connector-jar> \
  --model <label-for-logs> \
  --out-path /var/lib/sm8/gate-b/traces/synthetic-$(date +%Y-%m-%d).json
```

Measures the documented 1-partition synthetic fixture. Use this to
validate the cron wiring and the output shape before pointing the
runner at a production model.

The output JSON's `measuredSource` field records which mode ran, and
the `rendered` text is banner-prefixed (`[LIVE MODEL]` vs
`[SYNTHETIC FIXTURE — not production model]`) so log greps cannot
confuse the two.

## Invocation

```bash
# The connector jar does NOT bundle Spark/Iceberg classes — run via
# spark-submit, or reuse the classpath scripts/rollup-observe.sh
# builds (it is the classpath-authoritative reference):
spark-submit --class io.sm8.connectors.spark.GateBTraceRunner \
  <connector-jar> \
  --model <model-name> \
  --out-path /var/lib/sm8/gate-b/traces/$(date +%Y-%m-%d).json
```

Accepted flag: `--bootstrap-warehouse <dir>` (offline fixtures;
production uses the cluster-side catalog config and omits it).

Both flags are required. `--model` is recorded in the JSON output
(`requestedModel` field) even though the current fixture is synthetic
— forward-compat when live-model measurement lands.

Exit codes: 0 = trace written; 1 = probe failed; 2 = bad arguments.

## Cron wiring

Daily alongside the existing refresh cron (example):

```cron
# Gate B trace collection — Tier 2 evidence (ADR-0029 §Gate B item 3)
30 3 * * * java -cp $HOME/bin/sm8-connector.jar \
  io.sm8.connectors.spark.GateBTraceRunner \
  --model <representative-model> \
  --out-path $HOME/logs/gate-b/$(date +\%Y-\%m-\%d).json \
  >> $HOME/logs/gate-b-trace.log 2>&1
```

Time it AFTER the refresh cron (the probe boots its own Spark
session; it does not race the refresh path, but staggering avoids
memory contention on the same box — the runner has an 85% /proc/meminfo
guard that aborts instead of OOMing, same pattern as
`RollupObservationHarness`).

NOTE for live-model mode (L1): the 85% guard assumes ~1-1.5 GB for a
local[1] Spark session over the synthetic fixture. A live production
model may pull in much more (the base table scan + the rollup write).
For production measurement, run on a machine with adequate headroom or
reduce the scope to a smaller partition subset.

## Output shape

Re-running on the same calendar day OVERWRITES that day's file
(last-write-wins). Use a suffix (e.g.
`.retry-$(date +%H%M).json`) if you want to preserve multiple
runs per day.

### Retention

Files accumulate one-per-day for the 2-week window. After the Gate B
decision lands, archive or delete: `rm $HOME/logs/gate-b/*.json`.
Files are owned by the cron user with the default umask; no logrotate
hook is provided.

Each JSON file is self-describing:

```json
{
  "requestedModel": "<model-name>",
  "collectedAtEpochMs": 1757400000000,
  "collectedAtIso": "2026-09-09T03:30:00Z",
  "measuredSource": "RollupRefreshCostProbe synthetic fixture (...)",
  "report": { "...GateBReport fields..." },
  "rendered": "...human-readable text for grep..."
}
```

## Evaluation at week 2+

> **Only LIVE-MODEL runs are Gate B decision-grade.** Synthetic-
> fixture runs validate the procedure and the instrumentation; their
> numbers do NOT open or close Gate B. Check each JSON's
> `measuredSource` field before evaluating: live-model runs
> (`[LIVE MODEL]` banner) count; synthetic runs do not.

Apply the ADR-0029 §Gate B thresholds to the accumulated JSON files:

| Metric | Gate B opens when |
|---|---|
| refresh wall-clock (Tier 1 run) | > 5 minutes |
| rewritten-but-unchanged bytes | > 30% of table |

Both must hold on the representative model. Below either threshold,
Gate B stays closed — Tier 1 is sufficient, re-measure next quarter.

Gate B item 3 thresholds above are the gate-opening criteria. The
**ADR-0030 §D1 experiment metrics** (scan-latency p95 stratified by
snapshots-since-compaction, ambiguity rate) are NOT collected by this
probe — they require the dual-table scheduled experiment that runs
only after Gate B opens. This trace collection is the prerequisite
procedure exercise, not the D1 measurement itself.

See `docs/adr/0029-rollup-refresh-strategy-ladder.md` §Gate B for the
full criteria, and `docs/adr/0030-tier2-merge-posture.md` §D1 for the
posture Tier 2 adopts if the gate opens.
