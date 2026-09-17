# sm8 flight-delays example

Daily flight delays on top of **sm8** — the **time-series rollup freshness lifecycle** (ingest → declare → materialize → seal → query → late batch → stale refusal → refresh → route).

This is the **second end-to-end example** in the sm8 repo (after `examples/hospital-cleaning`). It complements the hospital example by showing the **freshness story** — most templates assume one big batch; this one shows what happens when reality doesn't match the schedule (a late bucket arrives, the freshness gate refuses routing, the operator refreshes, routing resumes).

## What you get

```
examples/flight-delays/
├── README.md                          ← you are here
├── pom.xml                            ← standalone Maven project (mirrors hospital-cleaning)
├── data/
│   ├── flights_batch1.csv             ← 14 rows, flights Sep 7-9 (closed days)
│   └── flights_batch2.csv             ← 5 rows, flights Sep 10 (late-arriving day)
├── models/
│   └── flights.yml                    ← target model shape (doc; the ModelBuilder DSL is the source of truth)
└── src/main/scala/com/example/flight/
    └── Main.scala                     ← full lifecycle: ingest -> model -> materialize -> seal -> query -> late batch -> stale refusal -> refresh -> query
```

## Run it (5 minutes)

### Prerequisites

- JDK 17, Maven 3.9+
- Spark 3.5.x (not required to be pre-installed — the `spark-sql` dep brings it in)
- The Iceberg runtime (`iceberg-spark-runtime-3.5_2.13:1.5.x`) is **transitive via `spark-connector_2.13`** (compile-scope) — no extra config needed.

### Step 1: install sm8 locally

```bash
cd /path/to/sm8
mvn -B -ntp -DskipTests install
```

This installs `sm8-core_2.13` + `spark-connector_2.13` (which carries the Iceberg runtime) into `~/.m2/repository` at version `0.1.0-SNAPSHOT`.

### Step 2: run the example

```bash
cd examples/flight-delays
mvn -B -ntp scala:run -DmainClass=com.example.flight.Main
```

You'll see all 7 steps run in sequence:

1. **INGEST batch 1** — read `flights_batch1.csv` (Sep 7-9), register `flights_clean_csv` temp view
2. **DECLARE** — build the `flights` `Model` via `ModelBuilder.withName.withSource.withRollup...build`, with a daily rollup `daily_delay_by_airline` declaring `FreshnessPolicy.FinalRequired`, `timeGrain=day`, `grainDimension=flight_date`. ModelValidator passes.
3. **MATERIALIZE + SEAL** — Iceberg `saveAsTable` materializes the rollup, then `RollupWatermark.advance(...)` latches final for past days (the v1 finality heuristic: buckets strictly before today latch final; today/future stay open)
4. **QUERY** — first query runs against the model
5. **LATE BATCH → STALE BUCKET** — append Sep 10 (a day whose bucket is NOT latched final). The example then reads the freshness gate DIRECTLY via `RollupWatermark.stalenessRefusal`, printing the typed `RollupBucketStale(buckets=BucketKey(2026-09-10))` refusal — the same refusal the MCP `validate_query` tool and `sm8 rollup-status` surface. (The plain `provider.query` path falls back to the base table on the calculated-measure peel refusal BEFORE the freshness gate — see "Honest limitations".)
6. **REFRESH** — `RollupMergeRefresher.mergeRefresh` + `RollupWatermark.advance` close out the Sep 10 bucket; subsequent queries route cleanly again.
7. **METRICS SUMMARY** — Prometheus-format rollup counter summary at end of run (same events a real `sm8-server --metrics-port` exposes)

## Sample output (actual, captured 2026-09-17 from this example)

```
======================================================================
sm8 flight-delays example — rollup freshness lifecycle
======================================================================
STEP 1: INGEST batch 1 (Sep 7-9)
  rows: 14
STEP 2: DECLARE model (ModelBuilder DSL + FinalRequired day-grain rollup)
  model: flights v1, rollups: daily_delay_by_airline
  model validation: ok
STEP 3: MATERIALIZE rollup (Iceberg) + seal past buckets final
  rollup 'daily_delay_by_airline' materialized as Iceberg table: flights__daily_delay_by_airline
  watermark sealed final: 2026-09-09, 2026-09-07, 2026-09-08
STEP 4: QUERY daily avg delay (all past buckets final)
--- Q1: avg delay by airline/day, Sep 7-9 ---
  rows: 14
STEP 5: INGEST batch 2 (Sep 10, late-arriving) — bucket NOT sealed
  rows now: 19 (batch2 adds 5)
STEP 5a: QUERY again (Sep 10 bucket open -> RollupBucketStale, base-table fallback)
--- Q2: avg delay by airline/day WITH the open Sep 10 bucket ---
  rows: 19
  typed refusal for bucket 2026-09-10: RollupBucketStale(buckets=BucketKey(2026-09-10))
STEP 6: REFRESH Sep 10 bucket (mergeRefresh + watermark advance)
  mergeRefresh 'daily_delay_by_airline' scope [2026-09-10]: iceberg_cat.flights__daily_delay_by_airline
  watermark advanced final for [2026-09-10]
STEP 6a: QUERY again (Sep 10 now sealed)
--- Q3: avg delay by airline/day AFTER refresh ---
  rows: 19
======================================================================
Metrics summary (Prometheus text format; the same counters a
real deployment exposes via `sm8-server --metrics-port`):
# HELP flight_rollup_rewrite_total queries routed to a rollup
# TYPE flight_rollup_rewrite_total counter
flight_rollup_rewrite_total 0
# HELP flight_rollup_refusal_total queries NOT routed, broken out by typed reason
# TYPE flight_rollup_refusal_total counter
flight_rollup_refusal_total{reason="nonCanonicalShape"} 3
flight_rollup_refusal_total 3
======================================================================
Lifecycle complete: seal -> route -> stale refusal -> refresh -> route.
```

## The freshness story (in 4 sentences)

`RollupSpec(name=daily_delay_by_airline, freshness=FinalRequired, timeGrain=day, grainDimension=flight_date)` declares a rollup that only serves queries once its `flight_date` buckets are sealed final. The connector's routing fold reads the per-bucket watermark on every query; a query that touches a non-final bucket gets `RollupBucketStale(buckets: Set[BucketKey])` (typed refusal, NOT a silent stale read). The operator's fix is `RollupMergeRefresher.mergeRefresh` + `RollupWatermark.advance(spark, model, spec, Set(bucket), isFinal=true)` — the D3 ordering (watermark follows the data commit) is enforced by code, not by caller discipline, and `is_final` never regresses (monotone OR-merge on the watermark row).

## What this exercises (a checklist for the reader)

| Concept | Where it shows up |
|---|---|
| Time-series rollup with `timeGrain=day` + `grainDimension=flight_date` | `Main.scala` STEP 2 — the rollup declaration |
| `FreshnessPolicy.FinalRequired` | STEP 2 declaration; the freshness gate fires only when the policy is set |
| Iceberg rollup materialization (Tier-2 path) | STEP 3 — `RollupMaterializer.materialize(... eager=true, TableFormat.Iceberg)` |
| Per-bucket watermark latching | STEP 3 — `RollupWatermark.advance(spark, model, spec, {pastBuckets}, isFinal=true)` |
| Late-batch arrival triggering `RollupBucketStale` | STEP 5 — `RollupWatermark.stalenessRefusal(spark, model, spec, Set("2026-09-10"))` produces the typed refusal |
| Tier-2 refresh path (merge + watermark) | STEP 6 — `RollupMergeRefresher.mergeRefresh` + `RollupWatermark.advance` |
| Metrics seam (rollup routing counters) | End-of-run Prometheus summary |

## Architecture: where this example fits in the sm8 RFC §3 stack

```
┌──────────────────────────────────────────────────────┐
│ THIS EXAMPLE (examples/flight-delays)                │  Consumer layer
│   - reads CSVs                                      │  (per RFC §3)
│   - builds the sm8 Model via ModelBuilder DSL        │  Imports:
│   - declares + materializes the rollup (Iceberg)    │  - sm8-core (SDK)
│   - advances the watermark                           │  - spark-connector
│   - demonstrates RollupBucketStale + refresh         │
└───────────────────┬──────────────────────────────────┘
                    │ ModelBuilder, Iceberg, watermark
┌───────────────────▼──────────────────────────────────┐
│ spark-connector   (the engine adapter for Spark)     │  Adapter layer
│   - RollupMaterializer (Iceberg saveAsTable)         │  Imports:
│   - RollupWatermark (the freshness table)            │  - sm8-core
│   - RollupMergeRefresher (Tier-2 partial refresh)    │  - spark
│   - routeThroughRollup (the rewrites + sink events)  │  - iceberg (transitive)
└───────────────────┬──────────────────────────────────┘
                    │ RollupSpec, FreshnessPolicy, BucketKey
┌───────────────────▼──────────────────────────────────┐
│ sm8-core   (the FROZEN Core — engine-portable SDK)    │  Core layer
│   - Model, RollupSpec, FreshnessPolicy               │  Spark-free
│   - RollupRewriter (typed refusals)                  │  Public Maven coord
│   - ModelValidator (grain + freshness co-presence)   │
└──────────────────────────────────────────────────────┘
```

This example does **NOT** import `sm8-platform` or `sm8-server` (no Restate, no Restate ingress). The plain `provider.query` path through the spark-connector exposes the same `RollupBucketStale` refusal the platform's `QueryService` and the MCP `validate_query` tool see.

## Related

- **[`sm8-core/.../model/RollupSpec.scala`](../../sm8-core/src/main/scala/io/sm8/core/model/RollupSpec.scala)** — the rollup declaration ADT (timeGrain + grainDimension + freshness)
- **[`sm8-core/.../rel/RollupRewriter.scala`](../../sm8-core/src/main/scala/io/sm8/core/rel/RollupRewriter.scala)** — the typed refusal vocabulary including `RollupBucketStale`
- **[`connectors/spark-connector/.../RollupWatermark.scala`](../../connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/RollupWatermark.scala)** — the freshness table + the `advance()` / `stalenessRefusal()` seams
- **[`connectors/spark-connector/.../RollupMaterializer.scala`](../../connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/RollupMaterializer.scala)** — Iceberg materialization
- **[`connectors/spark-connector/.../RollupMergeRefresher.scala`](../../connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/RollupMergeRefresher.scala)** — Tier-2 scoped refresh
- **[`docs/runbooks/rollup-refresh.md`](../../docs/runbooks/rollup-refresh.md)** — the operator runbook that goes with this example

## Honest limitations

- **The query rows you see come from the BASE table, not from the rollup.** The flight model has a calculated measure (`avg_delay = total_delay / flight_count`); `QueryBuilder.projectExpressions` wraps that in `Expr.Alias("avg_delay", <Div expr>)`, which fails `RollupRewriter.decomposeCanonical`'s Project-arm pass-through check (only `FieldRef` or `Alias(_, FieldRef)` are peelable). So the planner still emits a canonical Scan -> Filter* -> Aggregate -> Project plan AND the rewriter refuses to peel through the calculated projection — the same shape the hospital example observes. The freshness gate STILL fires (and the typed `RollupBucketStale` is the headline of this example); only the rollup-table path itself doesn't route for queries with calculated-measure projections. In a model without a calculated measure, `provider.query` would land on the rollup table; this is documented behavior, not a bug.
- **Local Spark + a temp-dir iceberg_cat catalog.** No external metastore, no production Iceberg S3 wiring. For production, replace the `iceberg_cat` hadoop catalog wiring in `Main.main` with your real catalog (REST, Nessie, Glue, ...) and deploy `sm8-server` to expose the `RollupRefreshService` REST verb, `sm8 rollup-status`, and the `/metrics` endpoint.
- **Watermark sealing is a one-shot call here.** In production, a cron scheduler (the `CronService` sm8-platform surface) calls `RollupWatermark.advanceForScope` for each day that closes — see `docs/runbooks/rollup-refresh.md` for the cron expression + the source-bucket finality heuristic.
