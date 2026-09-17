# sm8 rollup-cache-under-contention example

The "my dashboard auto-refreshes from 4 workers and all 4 fire the same aggregate at the same instant" story. The example demonstrates the **single-flight property** of `InMemoryResultCache.getOrComputeJournaled` under a concurrent query storm: 20 concurrent queries against a cold cache collapse to exactly **1 engine execution**.

## What you get

```
examples/rollup-cache-under-contention/
├── README.md                          ← you are here
├── pom.xml                            ← standalone Maven (sm8-core + spark-connector + cache-plugin)
├── data/
│   └── orders.csv                     ← 12 rows: order_id/region/amount
└── src/main/scala/com/example/contention/
    └── Main.scala                     ← herd -> steady -> version roll; ~430 lines
```

## What "single-flight" means (and why it matters)

`InMemoryResultCache.getOrComputeJournaled(key, model, version, compute)` is the concurrency primitive. When N threads ask for the same key at the same instant, exactly ONE of them runs the `compute` supplier; the other N-1 block on that leader's `CompletableFuture` and receive the same value when it completes.

**Why `putIfAbsent` and not `computeIfAbsent`?** `computeIfAbsent` holds the `ConcurrentHashMap` bin lock for the duration of the compute function — a long-running engine query would block every other operation on that bin for its whole duration. `putIfAbsent` registers an empty future, releases the lock immediately, then runs compute outside the lock; followers join the future. The legacy implementation had the `computeIfAbsent` bug; this one does not.

**Terminology**:
- **leader** — the single thread that actually runs the compute (registers its future in the inflight map first).
- **follower** — the other N-1 threads that found an inflight future and blocked on it.

## Run it (5 minutes)

### Prerequisites

- JDK 17, Maven 3.9+
- Spark 3.5.x (not required to be pre-installed)

### Step 1: install sm8 locally

```bash
cd /path/to/sm8
mvn -B -ntp -DskipTests install
```

### Step 2: run the example

```bash
cd examples/rollup-cache-under-contention
mvn -B -ntp scala:run -DmainClass=com.example.contention.Main
```

You'll see 5 steps run in sequence:

1. **INGEST + DECLARE** — read `orders.csv`; declare the `sales` Model with `CachePolicy.WriteThrough("contention")`.
2. **THUNDERING HERD** — 4 workers × 5 rounds = 20 concurrent queries against a cold cache. Verified: **exactly 1 engine execution** (not 20, not 4); all workers agree on the result.
3. **STEADY STATE** — the same 20-query storm against a warm cache. Verified: **0 engine executions** (all HIT).
4. **VERSION ROLL** — bump `Model.version` 1 → 2; the storm continues. Verified: **exactly 1 engine execution on the new key domain**; all workers agree.
5. **METRICS SUMMARY** — `CachePlugin` counters + hit ratio.

## Sample output (actual, captured 2026-09-18)

```
STEP 2: THUNDERING HERD — 4 workers x 5 rounds = 20 concurrent queries against a COLD cache
  [engine-exec #1] sales v1 MISS -> running engine
  engine executions: 1 (single-flight expected: 1)
  distinct result shapes: 1 (all workers must agree)
  single-flight contract holds: 1 execution, all workers agree
STEP 3: STEADY STATE — the same 20-query storm against a WARM cache
  engine executions: 0 (expected 0 — all HIT)
  steady state holds: 0 executions, all workers agree
STEP 4: VERSION ROLL — bump Model.version 1 -> 2; storm continues
  [engine-exec #1] sales v2 MISS -> running engine
  engine executions: 1 (single-flight expected: 1 — new key domain)
  version roll holds: 1 execution on the new key domain, all workers agree
STEP 5: METRICS SUMMARY
  CachePlugin.reads  = 60
  CachePlugin.writes = 8
  CachePlugin.hits   = 52
  CachePlugin.misses = 8
  hit ratio          = 0.87
```

## Why `writes = 8`, not 2

The `PreExecute` read-hook and the executor's `getOrComputeJournaled` are **two independent gates**:

- **Round 1 of each cold phase** (herd, then v2): the cache is empty. The Pre-hook lets **all 4 workers** through to the executor. `getOrComputeJournaled`'s single-flight collapses them to **1 actual engine run** — but the Post-hook (`CacheWritePostHook`) fires **once per worker that reaches PostExecute**, not once per unique compute. 4 workers × 1 cold round = 4 writes.
- **Round 2 onward**: the cache is warm. The Pre-hook sets `ctx.stop = true`; the engine never runs; the mutator Post-hook is skipped via `runsOnStop = false`. 0 writes.
- **Steady phase**: pure HIT, 0 writes.

So: 1 cold round × 4 workers × 2 cold phases (herd + v2) = **8 hook-fire writes**.

The counter is a **hook-fire count, not a unique-persisted count**. The underlying `putJournaledWithModelAndVersion` is an upsert (same key overwrites), so functional behavior is unaffected; only the counter semantics differ from what a naive `writes == unique-keys` reading would suggest. A production deployment reading the `/metrics` counters must interpret `writes` as hook-fire volume.

## What this exercises (a checklist for the reader)

| Concept | Where it shows up |
|---|---|
| `InMemoryResultCache.getOrComputeJournaled` — single-flight (4-arg form threading model + version) | `Main.clientQuery` — inside the engine compute thunk |
| Concurrent query storm (4 workers × 5 rounds) | STEP 2/3/4 — `ExecutorService` with `Workers` threads |
| Thundering-herd collapse (1 engine execution, N workers) | STEP 2 — `engineExecs.get() == 1` assertion |
| Steady-state all-HIT | STEP 3 — `engineExecs.get() == 0` assertion |
| Model-version invalidation mid-storm | STEP 4 — new key domain, 1 execution on v2 |
| Worker-agreement (all threads return the same normalized result) | STEP 2/3/4 — `distinct.size == 1` assertion |
| `PostHook.runsOnStop` honored by the consumer-side runner | `MinimalHookRunner.run` — post-hook skip gate |
| `CachePlugin.regionKey` namespaced keys (the executor-direct write and the plugin's read use the SAME key) | `clientQuery` cacheKey construction |

## Architecture: where this example fits in the sm8 RFC §3 stack

```
┌───────────────────────────────────────────────────────────────┐
│ THIS EXAMPLE (examples/rollup-cache-under-contention)         │  Consumer layer
│   - reads CSV                                                 │  (per RFC §3)
│   - builds 1 shared Model, 1 shared provider, 1 shared cache  │  Imports:
│   - spawns 4 worker threads issuing concurrent queries        │  - sm8-core
│   - asserts single-flight + worker-agreement invariants       │  - spark-connector
│   - bumps Model.version mid-storm                             │  - plugins/cache-plugin
└──────────────────────────┬────────────────────────────────────┘
                           │ EngineProvider.query -> PortableQueryResult
                           │ Plugin hooks (via EngineFactory)
┌──────────────────────────▼────────────────────────────────────┐
│ spark-connector  │  cache-plugin  │  sm8-core                 │  Adapter + plugin
│  (engine adapter)│  (Pre/Post)    │  (Engine, Model, Cache)   │  layers
└──────────────────────────┬────────────────────────────────────┘
                           │ Model, QueryRequest, PortableQueryResult, Context
┌──────────────────────────▼────────────────────────────────────┐
│ sm8-core   (the FROZEN Core — engine-portable SDK)            │  Core layer
│   - Model, CachePolicy, EngineHookRequest/Result              │  Public Maven coord
│   - HookRunner SDK Protocol, Context                          │
└──────────────────────────────────────────────────────────────┘
```

No `sm8-platform` import — the `MinimalHookRunner` is the same shape proven in `examples/caching-rollup-warmpath` (single-stage Execute; the platform's `HookRunnerOrchestration` fires all 4 stages).

## Honest limitations

- **`PostHook.runsOnStop` is honored but not by the hook itself** — the gate lives in the consumer-side runner (`MinimalHookRunner.run`). The platform dispatcher has the same gate; a production deployment using `HookRunnerOrchestration` gets it for free.
- **`cache.getOrComputeJournaled` is called with the 4-arg overload** (threading `model.name` + `model.version`) so entries are invalidateable via `cache.invalidateModel(model.name)` — the 2-arg overload would tag entries with `model=""` and leak past model-scoped invalidation.
- **The concurrency caveat for real deployments**: this example uses a JVM-local `ExecutorService` — the "4 workers" are 4 threads in ONE process. A distributed deployment (multiple JVMs) needs a distributed coordination layer; the cache's single-flight works per-process, not cross-process. The read-through semantics still apply (each JVM caches independently), but the "exactly 1 engine execution" guarantee only holds within a single JVM.
- **`CachePlugin.writes` is a hook-fire count, not a unique-persisted count** — see the "Why writes = 8, not 2" section above. `putJournaledWithModelAndVersion` is an upsert so redundant writes are functionally harmless, but a metrics consumer must not equate `writes` with unique keys.
- **The example uses `Model.version` bump for invalidation.** Real deployments bump version via a pipeline-run job (or a new `Model` build in code); the example does it inline in `main` for clarity.

## Related

- **`plugins/cache-plugin/.../InMemoryResultCache.scala`** — the `getOrComputeJournaled` single-flight primitive + `putIfAbsent` vs `computeIfAbsent` contract
- **`plugins/cache-plugin/.../CachePlugin.scala`** — the read/write hooks + `regionKey` namespacing
- **`sm8-core/.../cache/CachedRowDecoder.scala`** — `PortableQueryResult ↔ RestateCachedRow` round-trip
- **`sm8-core/.../sdk/Hooks.scala`** — the `HookRunner` SDK Protocol + `Context` (stage, request, result, meta, stop)
- **`examples/caching-rollup-warmpath`** — the single-threaded version of the same cache wiring
- **`examples/multi-engine-portability`** — the same wire format across Spark + DuckDB
- **`examples/flight-delays`** — the rollup freshness lifecycle (a different concurrency axis)
