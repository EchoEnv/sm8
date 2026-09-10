/*
 * SM8 Core — SnapshotDelta (ADR-0030 §D5 amendment: row-level
 * snapshot-diff extraction, implementation spec at
 * docs/specs/adr-0030-d5-snapshot-diff.md).
 *
 * The IO-free ADT that RollupSnapshotDiffExtractor (connector) emits
 * and that the Tier 2 MERGE materializer consumes to decide between
 * row-extractable and full-bucket recompute. Pure data + pure functions,
 * no Iceberg imports, no java.nio, no Spark types (RFC §3).
 *
 * ADT shape encodes the spec's row-extraction vocabulary:
 *   - Appended            : clean inserts, every row's a new fact
 *   - DeletesInOpenWindow : removes within the freshness window
 *                           (in this ADT, classified but NOT extracted
 *                           in v1: subtract-carriers collide with
 *                           D2-3's overwrite-only rule)
 *   - Ambiguous(reason)   : cannot row-attribute, fallback to full
 *   - NoDataChange        : metadata-only or empty-file-list snapshot,
 *                           short-circuits to no-op refresh
 *
 * Five AmbiguityReason cases mirror the spec's §5 boundary list and
 * feed the D2-5 ambiguity-rate metric on the connector side.
 *
 * DataFileRef is the IO-free mirror of Iceberg's ContentFile identity
 * (path + pos + rowCount); translation notes live in the spec.
 */
package io.sm8.core.rollup

/** IO-free mirror of an Iceberg data-file identity reference.
  *
  * Translation notes (connector-side, when building from
  * `org.apache.iceberg.ContentFile`):
  *   - `path` is a `String`; Iceberg returns `CharSequence` —
  *     convert via `String.valueOf` at the boundary.
  *   - `pos` is a primitive `Long`; Iceberg's `pos()` is a boxed
  *     `java.lang.Long` that may be null — a null `pos` maps to `0L`
  *     only when the file occurs once in the manifest, else the file
  *     is treated as ambiguous (spec §5.4 rewrite signature).
  *
  * @param path the file's full data path within the table location
  * @param pos the file's ordinal position in its manifest (0L when
  *           singleton; see translation note above)
  * @param rowCount the file's record count (parsed from
  *           `ContentFile.recordCount()`; must be ≥ 0)
  */
final case class DataFileRef(path: String, pos: Long, rowCount: Long)

/** Row-level delta derived from the Iceberg snapshot lineage between
  * the last-refresh watermark and the current head snapshot.
  *
  * Produced by `RollupSnapshotDiffExtractor` (connector), consumed by
  * `SnapshotDeltaPolicy` (core) and the Tier 2 MERGE materializer.
  *
  * Each variant carries the data file references that bound the
  * partial state — when a variant's carrier list is empty, the
  * extraction is a true no-op (regardless of which variant). */
sealed trait SnapshotDelta extends Product with Serializable

/** Companion for [[SnapshotDelta]]. The [[SnapshotDelta$]] object
  * owns the policy that classifies a delta as row-extractable. */
object SnapshotDelta {

  /** A snapshot whose summary records only clean inserts: every
    * added data file's rows are new facts. No removes, no delete
    * files, no schema drift in the lineage walk.
    *
    * @param rows total rows added across `files` (sum of
    *           `DataFileRef.rowCount`)
    * @param files the added data file identities in this lineage
    *           segment (non-empty by invariant; an empty Appended
    *           with no rows collapses to `NoDataChange`) */
  final case class Appended(
    rows: Long,
    files: Seq[DataFileRef]
  ) extends SnapshotDelta

  /** Snapshot lineage that includes removes OR delete files
    * restricted to the freshness-policy open window. ADT-only:
    * the spec's v1 extraction is append-only (subtract-carrier
    * merges are `+=`, forbidden by D2-3's overwrite-only rule), so
    * `SnapshotDeltaPolicy.isRowExtractable` returns false for this
    * variant and the MERGE path falls back to full recompute.
    *
    * Retained in the ADT because MOR-only delete classification
    * still needs to record the event for the ambiguity-rate
    * metric (D2-5 / D8); it is a FIRST-CLASS outcome, not a
    * shorthand for "fallback".
    *
    * @param deletedRows total rows removed across `files` (or
    *           covered by delete files; per-file counts when
    *           available, else −1)
    * @param files the touched file identities (removed data files,
    *           OR delete-file targets the lineage walk attributed
    *           to a window-bounded partition)
    */
  final case class DeletesInOpenWindow(
    deletedRows: Long,
    files: Seq[DataFileRef]
  ) extends SnapshotDelta

  /** Snapshot lineage the extractor cannot row-attribute to a
    * partial-state merge. Always falls back to the shipped
    * scope-declared full re-aggregation (spec §5). The
    * `AmbiguityReason` is recorded for the D2-5/D8 ambiguity-rate
    * metric and surfaces in the runbook's refusal-debugging table.
    */
  final case class Ambiguous(
    reason: AmbiguityReason
  ) extends SnapshotDelta

  /** Snapshot lineage with NO data file additions or removals and
    * NO delete files (metadata-only snapshot, e.g. expire/rewrite
    * decisions logged elsewhere). The refresher short-circuits to
    * a no-op MERGE (empty source view; the MERGE statement itself
    * still runs and commits a snapshot, but touches no rows), and
    * the watermark advances as usual. */
  final case object NoDataChange extends SnapshotDelta

  /** Whether a delta is row-extractable in v1 (spec §4).
    *
    * Truth table:
    *   - `Appended`           → true (caller filters out buckets whose
    *                             measures are Positional / Holistic /
    *                             Approximable via `DecomposabilityAudit`;
    *                             v1 extracts ADDS only — append-only
    *                             rule below)
    *   - `DeletesInOpenWindow`→ false (v1 append-only; D2-3
    *                             overwrite-only rule forbids
    *                             subtract-carrier MERGEs)
    *   - `Ambiguous`          → false (always)
    *   - `NoDataChange`       → true (trivially: no rows to apply;
    *                             the refresher issues a no-op MERGE)
    *
    * @param delta the snapshot delta to classify
    * @return true iff the Tier 2 MERGE materializer may attempt a
    *         row-extractable partial merge; false forces the
    *         scope-declared full-bucket recompute path
    */
  def isRowExtractable(delta: SnapshotDelta): Boolean = delta match {
    case _: Appended       => true
    case NoDataChange      => true
    case _: DeletesInOpenWindow => false
    case _: Ambiguous      => false
  }

  /** Whether a delta is a true no-op (no MERGE job at all).
    *
    * Used by the refresher to short-circuit before the partial-state
    * aggregate: only `NoDataChange` qualifies. `Appended` with zero
    * rows is folded to `NoDataChange` at the extractor level so
    * this predicate never sees it.
    *
    * @param delta the snapshot delta to classify
    * @return true iff the refresher should advance the watermark
    *         without running any merge job
    */
  def isNoOp(delta: SnapshotDelta): Boolean = delta match {
    case NoDataChange => true
    case _            => false
  }
}

/** Why the extractor could not row-attribute a delta (spec §5). */
sealed trait AmbiguityReason extends Product with Serializable

/** Companion for [[AmbiguityReason]]. The sealed hierarchy is
  * intentionally small and closed: each variant maps to a
  * documented refusal reason in the runbook's refusal-debugging
  * table, and feeds the D8 ambiguity-rate counter on the
  * connector side. */
object AmbiguityReason {

  /** One or more equality-delete files touch any file feeding the
    * bucket (`DeleteFile.referencedDataFiles` intersects bucket
    * files). Row attribution for "which rows died" is not
    * recoverable from file metadata. */
  final case class EqualityDeletes(count: Int) extends AmbiguityReason

  /** A `removedDataFiles` entry whose partition transform lands
    * outside the freshness policy's open window. PER-BUCKET
    * verdict: the affected bucket falls back, untouched buckets
    * may stay row-extractable. */
  final case class OutOfWindowRewrite(snap: Long) extends AmbiguityReason

  /** Any snapshot in the lineage walk carries a schema/partition
    * change (`Snapshot.schemaId` drift, spec change) — the diff
    * is not row-comparable across it. Extends ADR-0030 D5's
    * unreliable-for list (prose) as a typed refusal reason.
    *
    * Also used when a snapshot's summary map lacks
    * `added-records`/`deleted-records` keys AND the snapshot has
    * data-file entries (metadata-only snapshots with no files
    * fold to `NoDataChange` instead). */
  final case class SchemaOrPartitionEvolution(snap: Long) extends AmbiguityReason
}
