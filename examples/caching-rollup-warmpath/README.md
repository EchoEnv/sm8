# sm8 caching-rollup-warmpath example

The "my dashboard re-issues the same aggregate every 5 seconds" story. The example demonstrates the **read/write-through cache lifecycle** with a model-version-driven invalidation contract — the consumer wiring (Plugin → HookManager → pre/post hooks → HookRunner → engine) that NO other example currently shows.

The example does three things, in order:

1. **COLD PATH** — N identical queries against an engine with **no cache** wired: every call pays the full engine cost.
2. **WARM PATH** — the same N queries against an engine wired with `InMemoryResultCache` + `CachePlugin` through a consumer-side `HookRunner`: query 1 MISSES + runs (and writes through); queries 2..N HIT and short-circuit.
3. **INVALIDATION** — bump `Model.version`; the cache key domain changes; the next query MISSES again (residents of the old version stay but are unreachable for the new model version).

## What you get

```
examples/caching-rollup-warmpath/
├── README.md                          ← you are here
├── pom.xml                            ← standalone Maven (sm8-core + spark-connector + cache-plugin)
├── data/
│   └── events.csv                     ← 12 rows: event_id/region/amount
└── src/main/scala/com/example/caching/
    └── Main.scala                     ← cold -> warm -> invalidate lifecycle + counters summary
```

## Run it (5 minutes)

### Prerequisites

- JDK 17, Maven 3.9+
- Spark 3.5.x (not required to be pre-installed — the `spark-sql` dep brings it in)

### Step 1: install sm8 locally

```bash
cd /path/to/sm8
mvn -B -ntp -DskipTests install
```

This installs `sm8-core_2.13`, `spark-connector_2.13`, **and** the `cache-plugin_2.13` artifact (under group `io.sm8.plugins`). The example declares the cache-plugin as a regular dep — plugins/ artifacts are the documented consumer-facing use case (RFC §11).

### Step 2: run the example

```bash
cd examples/caching-rollup-warmpath
mvn -B -ntp scala:run -DmainClass=com.example.caching.Main
```

You'll see all 5 steps run in sequence:

1. **INGEST + DECLARE** — read `events.csv`; build the `sales` `Model` with `CachePolicy.WriteThrough("dashboard")`.
2. **COLD PATH** — 20 identical queries, no cache → 20 full engine runs.
3. **WARM PATH** — the SAME 20 queries WITH the cache wired → 1 MISS + 19 HITs.
4. **INVALIDATION** — bump `Model.version` (1 → 2) → next query MISSES again.
5. **METRICS SUMMARY** — `CachePlugin.reads/writes/hits/misses` counters + hit ratio.

## Sample output (actual, captured 2026-09-18 from this example)

```
======================================================================
sm8 caching-rollup-warmpath example — read-through cache lifecycle
======================================================================
STEP 1: INGEST events.csv -> Spark temp view; DECLARE the sales Model (WriteThrough)
  spark view 'sales_spark' rows: 12
  model v1 validation: ok
STEP 2: COLD PATH — 20 identical queries, NO cache
  cold[0..19]: 4 rows each (region-grouped aggregate)
  cold path: 20 queries in 4776ms (~238ms/query)
STEP 3: WARM PATH — the SAME 20 queries WITH the read-through cache
  warm[0]..warm[19]: 4 rows each (20 separate log lines; range shown)
  warm path: 20 queries in 138ms (~6ms/query)
  value parity: cold and warm normalized row multisets are identical (4 rows)
STEP 4: INVALIDATION — bump Model.version 1 -> 2; the next query MISSES
  v2 query after bump: 4 rows (MISSED — new key domain)
STEP 5: METRICS SUMMARY
======================================================================
Cache + plugin counters (the same counters a real deployment
exposes on `sm8-server --metrics-port`):
  CachePlugin.reads  = 21
  CachePlugin.writes = 2
  CachePlugin.hits   = 19
  CachePlugin.misses = 2
  hit ratio          = 0.90
======================================================================
Caching lifecycle complete: cold -> warm -> invalidate.
```

The warm path runs **~40× faster** than cold (138ms vs 4776ms — same 20 queries); the cache's hit ratio is **90%** (19 HITs / 21 reads); `writes` is 2 (one per MISS — the write-through hook is a mutator and correctly skips the 19 HITs via the SDK `runsOnStop` contract); the version bump forces the next query back to MISS (the old entries are untouched but unreachable).

## What this exercises (a checklist for the reader)

| Concept | Where it shows up |
|---|---|
| `CachePolicy.WriteThrough` declaration on the `Model` | STEP 1 — the policy fold into `ctx.meta("sm8.cache.policy")` |
| `InMemoryResultCache` constructed inline (no sm8-platform) | STEP 3 |
| `CachePlugin` (`readFires/writes/hits/misses` AtomicInts) wired through `EngineFactory.create(plugins)` | STEP 3 |
| `HookManagerImpl` from the engine — what `CachePlugin.setup(engine)` registered against | STEP 3 — `engine.hooks.asInstanceOf[HookManagerImpl]` |
| Consumer-side `MinimalHookRunner` (~30 lines) — the SDK Protocol implementation the platform's `HookRunnerOrchestration` is the heavier version of | STEP 3 |
| `Context` carry (stage, request, result, meta, stop) — the single shared data object threads through every hook | the warm-loop block |
| `EngineHookRequest(model, mcpRequest, cacheKey)` — the typed request that the hooks read | warm-loop + v2 loop |
| `EngineHookResult(pqr)` — the typed result that the write-through hook journals | warm-loop |
| `ctx.stop = true` short-circuit from `CacheReadPreHook` | (subtle — observable as the ~40× speedup) |
| `Model.version` participates in the cache key domain → version bump invalidates | STEP 4 — 19 HITs → next MISS |

## Architecture: where this example fits in the sm8 RFC §3 stack

```
┌───────────────────────────────────────────────────────────────┐
│ THIS EXAMPLE (examples/caching-rollup-warmpath)              │  Consumer layer
│   - reads CSV                                                │  (per RFC §3)
│   - constructs InMemoryResultCache + CachePlugin inline     │  Imports:
│   - ships its own MinimalHookRunner (~30 lines)              │  - sm8-core
│   - wires EngineFactory.create(Seq(plugin)) +                │  - spark-connector
│     engine.hooks.preHooksFor(...) -> fire-then-engine path  │  - plugins/cache-plugin
│   - prints plugin counters                                  │
└─────────────────────────┬─────────────────────────────────────┘
                          │ EngineProvider.query -> PortableQueryResult
                          │ Plugin hooks (registered via EngineFactory)
┌─────────────────────────▼─────────────────────────────────────┐
│ spark-connector  │  cache-plugin  │  sm8-core               │  Adapter + plugin
│   (engine adapter) │  (Pre/Post hooks)│ (Engine, Model, etc)  │  layers (each
│                   │                 │                         │  separately importable)
└─────────────────────────┬─────────────────────────────────────┘
                          │ Model, QueryRequest, PortableQueryResult, Context
┌─────────────────────────▼─────────────────────────────────────┐
│ sm8-core   (the FROZEN Core — engine-portable SDK)            │  Core layer
│   - Model, CachePolicy ADT, EngineHookRequest/Result       │  Public Maven coord
│   - EngineFactory, EngineImpl, HookManager                 │
│   - HookRunner SDK Protocol (the example's MinimalHookRunner
│     satisfies it; sm8-platform's HookRunnerOrchestration is
│     the bigger sibling)                                     │
└─────────────────────────────────────────────────────────────┘
```

No `sm8-platform` import — the example's `MinimalHookRunner` is exactly what `HookRunnerOrchestration` does for the full 4-stage pipeline, but for bare consumers wiring the cache the ~30-line version is enough.

## Honest limitations

- **`CachePolicy.ReadThrough` would not write** — the plugin's per-case matrix documents: ReadThrough = read-only-by-default, no write-through. The example uses `WriteThrough` so the warm path stays warm. For real deployments the policy choice is "do you want write-through or not"; the cache key domain stays the same either way.
- **The example's `MinimalHookRunner` is single-stage (Execute only)** — `HookRunnerOrchestration` in sm8-platform fires all 4 pipeline stages (Parse, Resolve, Execute, Format). Bare consumers can ship the single-stage runner; production deployments use the platform's orchestration. Same code shape, different coverage.
- **`PostHook.runsOnStop` matters.** `CacheWritePostHook` declares `runsOnStop = false` ("a mutator must not re-journal on a HIT"); the platform dispatcher skips such hooks when a pre-hook set `ctx.stop`. The example's `MinimalHookRunner` honors the same gate — without it, every cache HIT would redundantly re-journal the entry (writes=21 instead of writes=2 in this run). If you copy the runner, keep the gate.
- **The cache key in the example is the consumer-side derivation** (`"sales|v$version|region=product"`) — the actual `sm8-platform/CacheBridge.platformCacheKey` produces a canonical length-prefixed SHA-256 key (`CacheBridge.scala`). Both produce the same HIT/MISS differentiation for a given model + version + request shape; the canonical form is portable across plugin + provider.
- **Wall-clock ~40× speedup at this scale is dominated by Spark job submission overhead**, not by data work. At production scale (multi-second aggregations on millions of rows), the speedup is much larger; at microbenchmark scale the ratio shrinks.

## Related

- **`sm8-core/.../cache/ResultCache.scala`** — the `ResultCache` trait (the contract every cache impl satisfies; `getJournaled` / `putJournaledWithModelAndVersion` / `getOrComputeJournaled`)
- **`sm8-core/.../model/Model.scala`** — `CachePolicy` sealed trait (`NoCache`, `ReadThrough(name)`, `WriteThrough(name)`)
- **`sm8-core/.../sdk/Hooks.scala`** — `HookRunner` SDK Protocol + `Context` (the single shared data object)
- **`plugins/cache-plugin/.../CachePlugin.scala`** — the read-through Pre@50 / write-through Post@60 plugin + `InMemoryResultCache`
- **`sm8-platform/.../EngineService.scala`** — the platform's `runQueryWithHooks` (the same wiring this example ships, plus the 4-stage orchestration, plus the cache-policy fold)
- **`sm8-platform/.../hooks/HookRunnerOrchestration.scala`** — the platform's full 4-stage `HookRunner` implementation (the heavier sibling of this example's `MinimalHookRunner`)
- **`examples/hospital-cleaning`** — consumer-layer cleansing + query on one engine
- **`examples/flight-delays`** — consumer-layer rollup freshness lifecycle on one engine
- **`examples/multi-engine-portability`** — consumer-layer same Model through Spark + DuckDB
