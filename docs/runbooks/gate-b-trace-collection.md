# Gate B trace collection — Tier 2 operator enablement evidence

Operational procedure for ADR-0029 §Gate B (amended 2026-09-09):
collecting the production traces that tell an **operator** whether to
**enable** Tier 2 (row-level delta MERGE) for their deployment. The
numeric criteria govern per-deployment enablement, not whether Tier 2
is built (the build proceeds per the funded roadmap; see the amended
ADR-0029 §Gate B for the open-source rationale).

## What ships

`GateBTraceRunner` (spark-connector) — a cron-friendly wrapper around
`RollupRefreshCostProbe` that persists one JSON report per run.

## Two modes: live model vs synthetic fixture (read first)

The runner supports TWO modes:

**Live-model mode** (enablement-evaluation-grade metrics — but SHADOW WRITES):

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

**Synthetic-fixture mode** (procedure exercise — NOT evaluation-grade):

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

**Java 17+ requires Spark's `--add-opens` JVM flags** — without them,
Spark 3.5.8 throws `IllegalAccessError: cannot access class
sun.nio.ch.DirectBuffer`. Spark's `spark-submit` passes them
automatically; bare `java -cp` does not.

```bash
# The connector jar does NOT bundle Spark/Iceberg classes — run via
# spark-submit (passes --add-opens automatically), or via bare java
# with the flags added manually (see the ADD_OPENS variable below),
# or reuse the classpath scripts/rollup-observe.sh builds (it is the
# classpath-authoritative reference):

# Via spark-submit (Java 17+ flags handled automatically):
spark-submit --class io.sm8.connectors.spark.GateBTraceRunner \
  <connector-jar> \
  --model <model-name> \
  --out-path /var/lib/sm8/gate-b/traces/$(date +%Y-%m-%d).json

# Via bare java (add the flags explicitly):
ADD_OPENS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
java $ADD_OPENS -cp <connector-jar>:<scala-library>:<dep-classpath> \
  io.sm8.connectors.spark.GateBTraceRunner \
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
# Gate B trace collection — Tier 2 operator enablement evidence (ADR-0029 §Gate B)
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
enablement evaluation completes, archive or delete: `rm $HOME/logs/gate-b/*.json`.
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

> **The Gate B enablement-evaluation metric is a DISTRIBUTION over many runs, not
> a single daily-cron sample.** Schedule live-model traces at the
> production refresh cadence (or denser) for the 2-week window;
> one file per day is a minimum, not the target. This cadence
> requirement applies equally to enablement evaluation (amended
> §Gate B): 14 daily single samples are NOT evaluation-grade.
>
> **Only LIVE-MODEL runs are Gate B enablement-evaluation-grade.** Synthetic-
> fixture runs validate the procedure and the instrumentation; their
> numbers do NOT open or close Gate B. Check each JSON's
> `measuredSource` field before evaluating: live-model runs
> (`[LIVE MODEL]` banner) count; synthetic runs do not.

Apply the ADR-0029 §Gate B thresholds to the accumulated JSON files:

| Metric | Tier 2 enablement fires when |
|---|---|
| refresh wall-clock (Tier 1 run) | > 5 minutes |
| rewritten-but-unchanged bytes | > 30% of table |

Both must hold on the representative model. Below either threshold,
keep Tier 1 for that rollup — Tier 1 is sufficient for that
deployment; re-measure next quarter. Above both, enable Tier 2 for
that rollup (the enablement path is the Tier 2 refresh strategy
configuration; the D1 experiment below informs the MOR-vs-COW
posture choice).

The thresholds above are the Tier 2 operator enablement criteria
(amended §Gate B). The **ADR-0030 §D1 experiment metrics**
(scan-latency p95 stratified by snapshots-since-compaction, ambiguity
rate) are NOT collected by this probe — they require the dual-table
scheduled experiment that runs when a site enables Tier 2. This trace
collection is the enablement-evidence procedure, not the D1
measurement itself.

See `docs/adr/0029-rollup-refresh-strategy-ladder.md` §Gate B for the
full criteria, and `docs/adr/0030-tier2-merge-posture.md` §D1 for the
posture Tier 2 adopts once enabled.
