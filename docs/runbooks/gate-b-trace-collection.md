# Gate B trace collection — Tier 2 evidence procedure

Operational procedure for ADR-0029 §Gate B item 3: collecting the ≥
2 weeks of production traces that decide whether Tier 2
(row-level delta MERGE) is justified to build.

## What ships

`GateBTraceRunner` (spark-connector) — a cron-friendly wrapper around
`RollupRefreshCostProbe` that persists one JSON report per run.

## Honest limitation (read first)

The probe currently measures its **documented synthetic fixture**
(a 1-partition, 3-row seed exercising scoped Tier 0 + Tier 1). It does
NOT yet measure a live production model. The instrumentation shape,
the metric definitions, and the JSON persistence are production-ready;
the data source is synthetic. Wiring the probe against a live
registered model is future work (tracked under ADR-0029 §Gate B item
3's "representative model" clause). Until then, running the trace
clock exercises the procedure and produces comparable-shape data — it
does not yet produce Gate B decision-grade numbers.

## Invocation

```bash
java -cp <connector-jar> io.sm8.connectors.spark.GateBTraceRunner \
  --model <model-name> \
  --out-path /var/lib/sm8/gate-b/traces/$(date +%Y-%m-%d).json
```

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
memory contention on the same box — see the memory guard in
`RollupObservationHarness`).

## Output shape

Each JSON file is self-describing:

```json
{
  "requestedModel": "<model-name>",
  "collectedAtEpochMs": 1757400000000,
  "collectedAtIso": "2026-09-09T03:30:00Z",
  "measuredFixture": "RollupRefreshCostProbe synthetic fixture (...)",
  "report": { "...GateBReport fields..." },
  "rendered": "...human-readable text for grep..."
}
```

## Evaluation at week 2+

Apply the ADR-0029 §Gate B thresholds to the accumulated JSON files:

| Metric | Gate B opens when |
|---|---|
| refresh wall-clock (Tier 1 run) | > 5 minutes |
| rewritten-but-unchanged bytes | > 30% of table |

Both must hold on the representative model. Below either threshold,
Gate B stays closed — Tier 1 is sufficient, re-measure next quarter.

See `docs/adr/0029-rollup-refresh-strategy-ladder.md` §Gate B for the
full criteria, and `docs/adr/0030-tier2-merge-posture.md` §D1 for the
posture Tier 2 adopts if the gate opens.
