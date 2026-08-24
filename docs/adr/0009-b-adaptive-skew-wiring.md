# ADR-009-b: AQE skew wiring — deferred (operator-precedence is non-negotiable on a shared session; per-query factor not expressible)

| Field | Value |
| **Status** | **v0.4 — revised after 4 dual review rounds; round 4 fixes the contradictory operator-precedence by deferring the per-query wiring to a per-session-deployment follow-up (the skew half of RFC §2 'Feeding broadcast/skew' is NOT delivered by this ADR)** |
| **Date** | 2026-08-24 |
| **Module** | `connectors/spark-connector` (no code change) + `sm8-core` (`JoinHints.skewFactor` seam preserved for future per-session work) |
| **Supersedes scope** | ADR-009-a "AQE skew follow-up"; the prior draft of this ADR (v0.1–v0.3 were rejected in successive rounds for: false Spark-3.5 defaults premise; `getOption` returning `Some(5.0)` never `None`; self-contradictory precedence contract; execution-time read races; unwired ctor param with no operator ingress) |
| **Skill alignment** | scala-spark-batch-bugs, scala-impact-analysis, scala-bug-hunting, scala-error-handling, scala-jvm-safety, scala-perf-testing, scala-data-driven-refactor, karpathy-app-design, karpathy-guidelines, scala2-scaladoc, debug-mantra |

## Decision-at-a-glance

Four review rounds converged on the same architectural truth, which this v0.4 makes explicit and the central one of the design:

**Per-query factor binding is not expressible in Spark's session-config model on a shared session.** The provider's one `SparkSession` + `OptimizeSkewedJoin` reading the factor via `SQLConf.getConf` at **rule-application (execution)** time means a concurrent query's later `set` can change an in-flight query's effective factor; "operator-set always wins" and "re-seed on every query" are **mutually exclusive** (both cannot hold after the first seed) when implemented over the same session key. This ADR does not deliver the skew half of RFC §2 in this codebase's lifecycle (the broadcast half IS delivered — ADR-009-a).

Concrete, single-paragraph decision:

1. **The skew half is DEFERRED** — not "partly fulfilled." A `JoinHints.skewFactor` carrier exists, but a per-query seed is unwired in this shared-session design.
2. **Operator-facing lever today**: set the key on the existing `SparkSession` (`spark.sql.adaptive.skewJoin.skewedPartitionFactor`) before any `EngineService` query — the standard Spark `SparkConf` path, the same way the operator already customizes `autoBroadcastJoinThreshold`. The provider does not write this key. Trino: no equivalent; ignored.
3. **Per-query `JoinHints.skewFactor`**: preserved as a dead-on-arrival-but-documented seam; the *real* future path is **per-query SparkSession** (one session per query, factored out of the provider) — that makes per-query factor binding expressible, and the seam is ready when that migration happens. Not in this PR.
4. **No code change** in this PR; the v0.1–v0.3 implementation arm is DROPPED.

## Revision history

| Version | Date | Change |
|---|---|---|
| v0.1 | 2026-08-23 | Draft — enabled-flags arm (REJECTED: false premise; both flags default true in 3.5.8) |
| v0.2 | 2026-08-23 | Per-query factor seed (REJECTED: getOption-None false; precedence contradictory; execution-time read) |
| v0.3 | 2026-08-24 | Provider-lifecycle factor policy (REJECTED: ctor param dead wiring; operator-precedence self-contradictory; "model-declared skew join" predicate nonexistent) |
| v0.4 | 2026-08-24 | DEFERRED — single operative decision: operator sets on the existing `SparkSession`; provider writes nothing; `JoinHints.skewFactor` preserved as the seam for a future per-session follow-up. No code change. |

---

## Context

### Verified facts (multiple reviewer rounds, jar bytecode)

- `spark.sql.adaptive.enabled` and `spark.sql.adaptive.skewJoin.enabled` BOTH default `true` in spark-3.5.8 (verified in `SQLConf.createWithDefault(true)`).
- `spark.sql.adaptive.skewJoin.skewedPartitionFactor` defaults `5.0`; `skewedPartitionThresholdInBytes` defaults `256MB`; split rule = `size > factor × median AND > 256MB`.
- `OptimizeSkewedJoin` reads both via `SQLConf.getConf` at execution time (rule-apply) — not bound per plan.
- `JoinHints.skewFactor: Option[Int]` exists in `JoinHints` (`EngineContext.scala:136`), never consumed (zero adapter refs).
- The provider holds ONE `SparkSession` (one-session lifetime); concurrent queries are normal; deployment path is `Main → EngineLoader.discoverAndRealize → descriptor.realize(url) → SparkEngineProvider`; only `engineName + rawUrl` flow on the CLI (no provider-config type).
- `SkewStub.consult(model, threshold)` is a counter-only stub (decision-only); nothing writes the hint to a `JoinHints`.

### The 3 things the reviews kept proving

1. **No Spark API expresses per-query factor binding on a shared session.** Even with the cleanest seed, a concurrent query's seed landing between this query's compile and its AQE read changes the effective factor.
2. **Operator-precedence is un-implementable as a self-contradictory clause.** "Never override operator" + "re-seed when differs" together fail after the first seed; bytecode proves the unset detector cannot distinguish "operator-set to default" from "not set" (both return `Some("5.0")`).
3. **There is no operator ingress to a ctor param.** All three `realize`/`realizeTyped`/ctor sites hardcode their args; only `(url)` reaches the provider in deployment; no `--skew-factor` CLI flag exists.

## Decision

### 1. The skew half of RFC §2 'Feeding broadcast/skew' is DEFERRED (not delivered)

This ADR does not deliver per-query factor binding. The broadcast half IS delivered (ADR-009-a); the skew half is not. The honest restatement replaces "partly fulfilled via the provider config" with "deferred to a per-session-deployment follow-up."

### 2. Operator-facing lever today = `SparkSession` config (the standard path)

Operators configure AQE skew via the same `SparkSession`/`SparkConf` they already use for `autoBroadcastJoinThreshold`, `adaptive.enabled`, etc. The provider does not write any of these keys. Concretely:

- `SparkSession.builder().master(url).config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "8.0").getOrCreate()` — operator-set at session creation.
- `EngineLoader` / `descriptor.realize(url)` is unchanged; no new ctor param; no new flag.
- `JoinHints.skewFactor: Option[Int]` is preserved in the type (no breaking change); explicitly **NOT** read in the provider in this lifecycle. Documented as the seam for the per-session follow-up.

### 3. Trino: no AQE / skew equivalent — ignored (portable seam preserved).

### 4. The future per-session-deployment follow-up (named, not in this ADR)

The real fix to per-query factor binding is **per-query SparkSession** (one session per `query()` call, not one shared session). That is a non-trivial refactor of `SparkEngineProvider` (the entire provider is built around the one-session assumption), recorded as a separate future ADR. Once that exists, the existing `JoinHints.skewFactor` seam becomes honor-able per-query; this ADR's decision is correct under both the current and the future designs (the future design changes the ingress, not the per-query value path).

## Consequences

- **Positive**: the contradictory operator-precedence contract is gone (the provider writes nothing; operator sets on the session as today). The seam is preserved for the future per-session migration. No code change in this PR.
- **Negative**: the skew half of RFC §2 'Feeding broadcast/skew' is deferred, not delivered. Documented honestly. Trino: unchanged.
- **No risk** (no code change, no run-time alteration). The broadcast half is unaffected.

## Alternatives considered

- v0.1 enabled-flags arm — rejected (false premise).
- v0.2 per-query factor seed — rejected (race + self-contradictory precedence + execution-time read).
- v0.3 provider-lifecycle ctor param — rejected (no operator ingress; dead wiring).
- Nothing (leave skewFactor dead) — superseded by v0.4 (deferred but seam preserved).

## References

- RFC §2 Feeding broadcast/skew
- ADR-009-a (broadcast seed — the sibling; this is its deferred counterpart)
- `SparkEngineProvider` (one-session lifecycle); `EngineContext.scala JoinHints.skewFactor`; the three `realize` sites
- spark-catalyst_2.13-3.5.8.jar `SQLConf$` defaults (verified across 4 review rounds)