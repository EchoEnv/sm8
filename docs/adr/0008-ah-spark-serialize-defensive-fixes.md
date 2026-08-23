# ADR-008-AH: Spark/serialization defensive fixes (L1 + L2 from audit)

| Field | Value |
|---|---|
| **Status** | **v1.0 — approved (per spark/serialize hard audit 2026-08-23)** |
| **Date** | 2026-08-23 |
| **Module** | `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/` |
| **Closes** | The user's 2026-08-22 directive ("periodically monitor memory and disk and spark serialize issues and perf concern") — Spark/serialization hard audit second pass |
| **Skill alignment** | `scala-spark-batch-bugs-mindset`, `karpathy-guidelines-mindset` (smallest correct change), `karpathy-impact-analysis-mindset` |

## Decision-at-a-glance

Apply 2 defensive fixes from the 2026-08-23 Spark/serialization audit. Both are LOW severity — no current runtime hazard, but they prevent future regression if anyone captures the affected class in a closure or journals it through Restate.

## Revision history

| Version | Date | Change |
|---|---|---|
| v1.0 | 2026-08-23 | Initial draft — 2 LOW defensive fixes from audit |

---

## Context

The 2026-08-23 Spark/serialization hard audit on main HEAD `46d5480` (post Wave 3 merge) returned:

- **BLOCKER**: 0
- **HIGH**: 0
- **MEDIUM**: 0
- **LOW (defensive)**: 2 — applied in this PR
- **INFO**: 3 — about plugin `Refs.scala` coverage (intentional stubs per ADR scope)

The full audit report (9.3KB) lives at `history://spark-serialize-audit`. The verdict: the spark connector is **closure-clean by construction**. ZERO executor-side closures exist (no `mapPartitions`, `foreachPartition`, `rdd.map`, `dataset.flatMap`, `udf[`, `functions.udf`, or `UDFRegistration.register`). Every class in the compile path either explicitly or transitively extends `Serializable`. All 3 plugin `Refs.scala` objects are JVM singletons with typed-dimension/measure witnesses. The `TypedDimension → TypedMeasure → ResultCache → RestateCachedRow → ResultValue` chain is Serializable end-to-end.

---

## Decision (the 2 LOW fixes)

### L1: `TypedQueryCompiler` — add `extends java.io.Serializable`

**File**: `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/TypedQueryCompiler.scala:96`

**Current**:
```scala
final class TypedQueryCompiler(...) extends ... {
```

**Target**:
```scala
final class TypedQueryCompiler(...) extends ... with java.io.Serializable {
```

**Why** (per `karpathy-guidelines-mindset` "smallest correct change"): the 3 sibling compiler classes (`PortableQueryCompiler`, `MinimalRelOpLowerer`, `SparkEngineProvider`) all explicitly extend `java.io.Serializable`. `TypedQueryCompiler` currently relies on the same transitive extension via the `EngineProvider` trait, but the audit noted the explicit declaration is missing. Per `scala-spark-batch-bugs-mindset` "trust the compiler but not the runtime": an explicit `extends Serializable` matches the sibling pattern and prevents future regression if the trait chain changes.

**Not a current hazard** because `TypedQueryCompiler` instances are factory-created via `apply(spark)` and consumed within the same statement via `Either`-fold (no closure or field capture).

### L2: `persistedFrames` — mark `@transient`

**File**: `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/SparkEngineProvider.scala:111`

**Current**:
```scala
private val persistedFrames: ConcurrentHashMap[Long, Dataset[_]] = new ConcurrentHashMap()
```

**Target**:
```scala
@transient
private val persistedFrames: ConcurrentHashMap[Long, Dataset[_]] = new ConcurrentHashMap()
```

**Why** (per `karpathy-impact-analysis-mindset` "distinguish source from binary compatibility"): the field is used only inside `close()` (JVM-shutdown driver-side iteration, lines 129-135). Per-JVM unpersist tracking is not meaningful across serialization (e.g. Restate journal capture of an `EngineProvider`). Marking `@transient` is defensive — if anyone ever serializes an `EngineProvider` (e.g. via Restate's journal), the `ConcurrentHashMap[Long, Dataset[_]]` will not attempt to serialize Spark `Dataset` references, which are not `Serializable`.

**Not a current hazard** because the field is never captured in a closure that crosses the executor boundary. Defensive future-proof.

---

## Files NOT touched (audit INFO — out of scope)

| File | Reason |
|---|---|
| `plugins/broadcast-plugin/src/main/scala/io/sm8/plugins/broadcast/BroadcastStub.scala` | No `Refs.scala` exists — the broadcast stub per its ADR scope does not need phantom-typed dimension/measure witnesses. No hazard. |
| `plugins/materialize-plugin/src/main/scala/io/sm8/plugins/materialize/MaterializeStub.scala` | Same — materialize stub ships without phantom-typed dimension witnesses per ADR scope. No hazard. |
| `plugins/skew-plugin/src/main/scala/io/sm8/plugins/skew/SkewStub.scala` | Same — skew stub ships without phantom-typed dimension witnesses. No hazard. |

---

## Skill alignment

- `scala-spark-batch-bugs-mindset` — "trust the compiler but not the runtime": the L1 explicit `extends Serializable` matches the sibling compiler pattern. L2 `@transient` annotation prevents a future runtime `NotSerializableException` if anyone serializes an `EngineProvider`.
- `karpathy-guidelines-mindset` — "smallest correct change": 2 sub-1-line defensive annotations. Zero behavior change.
- `karpathy-impact-analysis-mindset` — distinguish source from binary compat: the L1 + L2 changes are source-compatible (no callers reference these classes' Serializable status) and binary-compatible (no new methods).
- `scala-jvm-safety-mindset` — `@transient` is the standard JVM idiom for non-persistent fields.

---

## Acceptance criteria

1. `TypedQueryCompiler` class declaration explicitly extends `java.io.Serializable` (matching the 3 sibling compiler classes).
2. `SparkEngineProvider.persistedFrames` is marked `@transient`.
3. The 911 existing tests pass (zero regression; this is an annotation-only change).
4. Memory + disk baseline under 90% (no new artifacts).

## Verification plan

```bash
mvn -B -ntp -pl sm8-core,connectors/spark-connector,connectors/in-memory-connector,connectors/trino-connector,plugins/audit-plugin,plugins/cache-plugin,sm8-cli,sm8-server,sm8-platform test 2>&1 | grep -E 'Tests: succeeded|BUILD' | tail -10
# Expected: 623 + 6 + 7 + 201 + 14 + 33 + 27 = 911 tests pass
```

## Risks

| Risk | Mitigation |
|---|---|
| Adding `@transient` could break a future use case that relies on `persistedFrames` being serialized | Per the audit, the field is only used in `close()` — driver-side JVM-shutdown unpersist. No serialization use case exists. |
| Adding `Serializable` could conflict with an existing trait chain | Per the audit, the sibling compilers already extend `Serializable` directly. No conflict. |

---

## ADR

`docs/adr/0008-ah-spark-serialize-defensive-fixes.md` v1.0 (full ADR; ~110 lines).