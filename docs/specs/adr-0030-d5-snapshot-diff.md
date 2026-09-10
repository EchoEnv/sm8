# ADR-0030 §D5 amendment — row-level snapshot-diff extraction: implementation spec

Resolution of [Write the ADR-0030 D5 snapshot-diff spec (unblocked: Gate B probe verified)](https://github.com/EchoEnv/sm8/issues/382),
a `wayfinder:task` child of [the Tier 2+ refinements map](https://github.com/EchoEnv/sm8/issues/378).
Gate B grounding: probe wiring verified on a synthetic fixture (blocker ticket closed 2026-09);
production enablement remains gated on ≥ 2 weeks of live-model traces — this spec is written
against the verified wiring so it can ship without waiting on that data.

## Verdict (Question step 2)

**Yes — the spec earns its place, scoped to partial-extraction only.** The Gate B run showed
rewritten-but-unchanged bytes ≈ total bytes for every refresh mode (Tier 0/1, rewritten-unchanged
paths all landed within 2 bytes of each other): the bucket-granularity rewrite writes the whole
bucket even when a handful of rows changed. The scope-declared delta path (shipped, #370) can
only skip whole buckets. Row-level snapshot-diff extraction attacks exactly the gap the probe
measured. It stays behind the same production gate as the probe itself (live traces before
enabling on a real deployment); the spec, like the probe, is buildable now.

## 1. Where it lives (RFC §3 layering)

- `core` (`sm8-core`): `SnapshotDelta` ADT + `SnapshotDeltaPolicy` — pure data + pure
  functions, IO-free. No Iceberg imports, no `java.nio`, no Spark types.
- `connector` (`connectors/spark-connector`): `RollupSnapshotDiffExtractor` — the only place
  Iceberg's API is touched; produces `SnapshotDelta` values for core to consume; plus
  `RollupMergeRefresher` wiring (it already owns the Tier 2 MERGE path).
- `adapter`: untouched. `plugin`: untouched. `hook`: untouched.

## 2. Core ADT (new file `sm8-core/.../rollup/SnapshotDelta.scala`)

```scala
sealed trait SnapshotDelta extends Product with Serializable
object SnapshotDelta {
  /** Clean append: every added file's rows are new facts. */
  final case class Appended(rows: Long, files: Seq[DataFileRef])            extends SnapshotDelta
  /** Deletes present but confined to the current (open) window. */
  final case class DeletesInOpenWindow(deletedRows: Long, files: Seq[DataFileRef]) extends SnapshotDelta
  /** Equality deletes or rewrites outside the open window: not row-attributable. */
  final case class Ambiguous(reason: AmbiguityReason)                       extends SnapshotDelta
  /** Snapshot touches no data files (metadata-only, e.g. expire/rewrite decisions logged elsewhere). */
  final case object NoDataChange                                            extends SnapshotDelta
}
sealed trait AmbiguityReason extends Product with Serializable
object AmbiguityReason {
  final case class EqualityDeletes(count: Int)  extends AmbiguityReason
  final case class OutOfWindowRewrite(snap: Long) extends AmbiguityReason
  final case class SchemaOrPartitionEvolution(snap: Long) extends AmbiguityReason
}
/** IO-free mirror of Iceberg's DataFile identity: path + pos. */
final case class DataFileRef(path: String, pos: Long, rowCount: Long)
```

Policy (same file): `SnapshotDeltaPolicy.isRowExtractable: SnapshotDelta => Boolean` —
`Appended` and `DeletesInOpenWindow` are extractable; `Ambiguous` is not (fallback,
§5); `NoDataChange` short-circuits to a no-op refresh.

## 3. Iceberg 1.5.2 API surface (connector)

Consumed per **snapshot** (`table.currentSnapshot()` / iteration over
`table.snapshots()` between watermarks), NOT via the incremental scan
operator (`SparkActions.incrementalScan` is scan-oriented, not
delta-oriented — wrong shape for MERGE feeding):

| Iceberg member | Used for |
|---|---|
| `Snapshot.snapshotId`, `parentSnapshotId` | lineage walk between last-refresh watermark and head |
| `Snapshot.addedDataFiles()` / `removedDataFiles()` | `DataFileRef` construction (path, pos, rowCount) |
| `Snapshot.addedRows()` / `removedRows()` | row totals for the ADT (post-filter summary) |
| `Snapshot.deleteFiles()` | presence of any delete file → `DeletesInOpenWindow` candidate |
| `DataFile.pos`, `DataFile.recordCount` | file identity + row attribution |
| `DeleteFile.referencedDataFiles()` | equality-delete containment test (§4 b2) |

Checked against Iceberg **1.5.2** (root `pom.xml` `<iceberg.version>`; the
1.7.1 override sits under an inactive profile). `addedRows()`/`removedRows()`
are summary methods — correct for the ADT's totals, not for row attribution.

## 4. Delta → MERGE mapping per Decomposability class

Input: `Seq[SnapshotDelta]` between watermarks, joined to each bucket by
partition transform of the touched files. Per bucket, per
`DecomposabilityAuditRow` class:

| Class | `Appended` | `DeletesInOpenWindow` | `Ambiguous` / `NoDataChange` |
|---|---|---|---|
| **Distributive + decomposable** (count/sum/min/max) | delta MERGE: source = added files' rows projected to rollup schema; `whenMatched` aggregate-merge, `whenNotMatched` insert | same, with removed files' rows subtract-aggregated in the source CTE | fallback (§5) / no-op |
| **Avg/mean-carrying** | delta MERGE on (sum, count) carriers, never on the mean itself | same, carriers subtract | fallback / no-op |
| **Distinct-carrying** (count distinct, approx) | NOT extractable — distinct state is not mergeable from partials | NOT extractable | fallback / no-op — buckets containing any distinct measure always take the scope-declared full path |

Frame rule: a bucket is row-extractable only if **every** measure in the
rollup's measure list for that bucket is extractable for the observed
delta kinds. One ambiguous measure → whole bucket falls back.

## 5. Ambiguity boundary cases (D2-4 sibling vocabulary)

Fallback to the shipped scope-declared full-bucket re-aggregation when:

1. **Equality deletes** touch any file feeding the bucket
   (`DeleteFile.referencedDataFiles` intersects bucket files) — row
   attribution for "which rows died" is not recoverable from file
   metadata.
2. **OutOfWindowRewrite**: a `removedDataFiles` entry whose partition
   transform lands outside the freshness policy's open window.
3. **SchemaOrPartitionEvolution**: any snapshot in the lineage walk
   carries a schema/partition change (`Snapshot.schemaId` drift,
   spec change) — the diff is not row-comparable across it (extends
   ADR-0030 D5's existing unreliable-for list; the amendment formally
   encodes it as a refusal reason, not just prose).
4. **Delete-file-only snapshot** with no added files: cannot
   distinguish row-level delete from whole-file rewrite at ADT
   resolution; treated as `DeletesInOpenWindow` only if every removed
   file's replacement is present in `addedDataFiles` of the SAME
   snapshot (COW rewrite signature); else `Ambiguous`.
5. **Lineage gap**: watermark snapshot id not found walking `parentSnapshotId`
   chain (expired snapshots) — `Ambiguous(OutOfWindowRewrite)` + watermark
   reset to head; never guess across a gap (matches D3's stale-watermark rule).

Every fallback emits the existing Tier 2 refusal-vocabulary entry with
the `AmbiguityReason` attached, feeding D2-5's ambiguity-rate metric.

## 6. Fallback behavior (wiring into shipped code)

`RollupMergeRefresher` refresh sequence, insertion point after eligibility
(D5 step 1-2 of the existing 7-step sequence):

```
eligibility → finality → [NEW: snapshot-diff extraction] → snapshot pin
    → if all buckets row-extractable: partial-state aggregate over delta rows only
    → else: existing scope-declared full re-aggregation (unchanged path)
    → MERGE → watermark advance (unchanged)
```

The extraction step adds one Iceberg metadata walk (snapshot lineage +
file lists; no data scan). Cost contract (matches the probe precedent):
the walk must be cheaper than the aggregation it replaces — asserted by
contract test, enforced by a row-count guard (skip extraction when
`addedRows + removedRows > bucket_rows / 2`: past half, full re-agg is
cheaper anyway).

## 7. Contract tests (connector, `AnyFlatSpec with Matchers`)

`RollupSnapshotDiffExtractorSpec`:
1. append-only lineage → `Appended` with correct file count + row totals
2. COW rewrite (remove+add same snapshot) → `DeletesInOpenWindow` per §5.4
3. equality delete present → `Ambiguous(EqualityDeletes)`
4. schema evolution mid-lineage → `Ambiguous(SchemaOrPartitionEvolution)`
5. expired-watermark lineage gap → `Ambiguous` + watermark reset
6. `NoDataChange` → no-op, watermark advances
7. half-row-count guard: extraction skipped, full path taken
8. core `SnapshotDeltaPolicy.isRowExtractable` truth table (core test, no Spark)
9. idempotency: re-running extraction over an already-consumed lineage →
   empty delta, content-identical table (the D2 idempotency contract, at diff level)

## 8. What this spec deliberately does NOT decide

- Whether extraction ships enabled-by-default (live-trace gate, per blocker
  ticket's closing comment — unchanged).
- The `Tier2ScanProbe` and D6-D8 study-gate work — belongs to the graduated
  ADR-0030 D1 amendment ticket, not this one.
- Iceberg version bump decisions (1.5.2 pinned as-read).
