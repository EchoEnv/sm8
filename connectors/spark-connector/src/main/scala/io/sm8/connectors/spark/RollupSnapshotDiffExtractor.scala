/*
 * SM8 Spark Connector — RollupSnapshotDiffExtractor (ADR-0030 D5
 * amendment: row-level snapshot-diff extraction; implementation spec
 * at docs/specs/adr-0030-d5-snapshot-diff.md).
 *
 * The ONLY connector file that binds to Iceberg's Java API (Snapshot /
 * DataFile / DeleteFile). It walks the snapshot lineage between the
 * watermark's last-commit snapshot id and the table head, classifies
 * each lineage segment into the core SnapshotDelta ADT
 * (io.sm8.core.rollup.SnapshotDelta), and hands the delta to the Tier 2
 * MERGE materializer, which decides between a row-extractable partial
 * merge and the shipped scope-declared full-bucket recompute.
 *
 * v1 extraction is APPEND-ONLY (spec §4): a DeletesInOpenWindow or
 * Ambiguous verdict forces the scope-declared fallback. Subtract-carrier
 * merges are `+=` under D2-3's overwrite-only rule and stay forbidden.
 *
 * Layer discipline (RFC §3): every Iceberg binding lives HERE, in the
 * connector. Core sees only the SnapshotDelta ADT values; adapters and
 * plugins never import this class.
 *
 * Closure safety (spark-batch mantra 1): driver-side only. No user
 * code ships to executors — the extractor reads Iceberg metadata
 * (snapshot summary, manifest file lists) on the driver via the Java
 * API; no Spark jobs are launched for the classification itself.
 *
 * Cost contract (spec §6): the lineage walk must be cheaper than the
 * aggregation it replaces. The half-row-count guard lives one level up,
 * in RollupMergeRefresher, which owns `bucket_rows`; the extractor
 * returns deltas + totals only.
 */
package io.sm8.connectors.spark

import scala.jdk.CollectionConverters._

import io.sm8.core.engine.EngineError
import io.sm8.core.rollup.{AmbiguityReason, DataFileRef, SnapshotDelta}

import org.apache.iceberg.{Snapshot, Table}
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession

/** Snapshot-diff extractor: walks Iceberg lineage, emits core
  * [[SnapshotDelta]] values. See the header for layering + safety
  * contract. */
object RollupSnapshotDiffExtractor {

  /** Parse the snapshot summary's `added-records` /
    * `deleted-records` keys into Long totals. Iceberg 1.5.2 exposes
    * row totals ONLY through the summary map (string-typed); the
    * `addedRows()` / `removedRows()` accessors are 1.7+ and MUST NOT
    * be used on this pin.
    *
    * A missing summary key is NOT parsed as zero: per spec §5.3, a
    * snapshot with file entries but missing keys is
    * `SchemaOrPartitionEvolution`; only a snapshot with empty file
    * lists AND missing keys folds to `NoDataChange`.
    *
    * @param snapshot the Iceberg snapshot to summarize
    * @return Right of (addedRecords, deletedRecords) when both keys
    *         parse; Left of the ambiguity reason when a key is
    *         missing or unparseable
    */
  private[spark] def summaryRowTotals(
    io: org.apache.iceberg.io.FileIO,
    snapshot: Snapshot
  ): Either[AmbiguityReason, (Long, Long)] = {
    val summary = snapshot.summary()
    val added = Option(summary.get("added-records"))
    val deleted = Option(summary.get("deleted-records"))
    (added, deleted) match {
      // Pure append: Iceberg writes added-records but OMITS
      // deleted-records when nothing was removed (ground-truthed via
      // the D5-PROBE lineage dump: `files=100/null`). Missing delete
      // key = zero deletions.
      case (Some(a), None) =>
        try Right((java.lang.Long.parseLong(a), 0L))
        catch {
          case _: NumberFormatException =>
            Left(AmbiguityReason.SchemaOrPartitionEvolution(snapshot.snapshotId()))
        }
      case (Some(a), Some(d)) =>
        try Right((java.lang.Long.parseLong(a), java.lang.Long.parseLong(d)))
        catch {
          case _: NumberFormatException =>
            Left(AmbiguityReason.SchemaOrPartitionEvolution(snapshot.snapshotId()))
        }
      case _ =>
        // Metadata-only snapshot: absent ADDED key is legal ONLY when
        // the snapshot also has no data-file activity (a schema-create
        // or a pure metadata commit). Checked via FileIO here.
        val noFiles =
          !snapshot.addedDataFiles(io).iterator().hasNext &&
          !snapshot.removedDataFiles(io).iterator().hasNext &&
          !snapshot.addedDeleteFiles(io).iterator().hasNext
        if (noFiles) Right((0L, 0L))
        else Left(AmbiguityReason.SchemaOrPartitionEvolution(snapshot.snapshotId()))
    }
  }

  /** Whether the snapshot's operation signals a rewrite/replace that
    * hides row attribution (spec §5.4): Iceberg operations "overwrite"
    * and "replace" rewrite file content in place (COW signature);
    * "delete" carries delete files; "append" is the clean case.
    *
    * A rewrite/replace snapshot with NO file activity (files=null in
    * the summary, e.g. the initial empty-table create, or a metadata-
    * only replace) is harmless: there are no rows to attribute. Only
    * file-bearing rewrites poison the walk.
    *
    * @param snapshot the snapshot to test
    * @return Some(reason) when the operation forbids clean row
    *         extraction AND the snapshot carries file activity; None
    *         when the operation is append/none or the rewrite is
    *         file-less (the summary checks below still guard it)
    */
  private[spark] def operationAmbiguity(
    snapshot: Snapshot
  ): Option[AmbiguityReason] = {
    val op = Option(snapshot.operation()).getOrElse("none")
    val summary = snapshot.summary()
    val hasFileActivity =
      Option(summary.get("added-data-files")).exists(_.toLong > 0L) ||
      Option(summary.get("deleted-data-files")).exists(_.toLong > 0L) ||
      Option(summary.get("added-delete-files")).exists(_.toLong > 0L) ||
      Option(summary.get("removed-delete-files")).exists(_.toLong > 0L)
    op match {
      case "append" | "none" => None
      case _ if !hasFileActivity => None
      case _ =>
        // overwrite / replace / delete WITH file activity:
        // rewrite-or-delete signature.
        Some(AmbiguityReason.OutOfWindowRewrite(snapshot.snapshotId()))
    }
  }

  /** Walk the lineage from `fromSnapshotId` (inclusive of its first
    * child) up to the table head, classifying each step.
    *
    * Lineage-gap rule (spec §5.5): if `fromSnapshotId` is not on the
    * `parentSnapshotId` chain from head (expired snapshots), the walk
    * stops at the gap and returns `Ambiguous(OutOfWindowRewrite(head))`
    * — never guess across a gap.
    *
    * @param spark the active SparkSession (used only to resolve the
    *        Iceberg Table handle; the walk itself is driver-local)
    * @param qualifiedName the table in `catalog.table` form, e.g.
    *        "iceberg_cat.sm8db_rollup_events_hourly"
    * @param fromSnapshotId the watermark's last-commit snapshot id
    * @return Right of the classified lineage delta (aggregated across
    *         the whole walk), or Left of a typed EngineError when the
    *         table cannot be resolved
    */
  def extract(
    spark: SparkSession,
    qualifiedName: String,
    fromSnapshotId: Long
  ): Either[EngineError, SnapshotDelta] = {
    try {
      val table: Table = Spark3Util.loadIcebergTable(spark, qualifiedName)
      val head = table.currentSnapshot()
      if (head == null) {
        // Fresh table: no snapshots at all — nothing to extract.
        Right(SnapshotDelta.NoDataChange)
      } else {
        val delta = classifyLineage(table, head, fromSnapshotId)
        Right(delta)
      }
    } catch {
      case e: org.apache.spark.sql.AnalysisException =>
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupSnapshotDiffExtractor.tableResolve",
          message = s"snapshot-diff: table '$qualifiedName' is not an " +
            s"Iceberg table resolvable by this catalog: ${e.getMessage}"))
      case e: IllegalArgumentException =>
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupSnapshotDiffExtractor.tableResolve",
          message = s"snapshot-diff: table '$qualifiedName' could not be " +
            s"loaded: ${e.getMessage}"))
    }
  }

  /** Classify the full lineage walk from `from` (exclusive) to `head`
    * (inclusive) into a single aggregated SnapshotDelta.
    *
    * Aggregation rule: ANY ambiguous step in the walk poisons the
    * whole segment (conservative, per spec §5's frame rule — one
    * ambiguous step means row attribution is broken across the walk).
    * Appends aggregate additively; any delete presence forces
    * `DeletesInOpenWindow` (append-only v1 — even a windowed delete
    * forces the fallback per spec §4).
    */
  private def classifyLineage(
    table: Table,
    head: Snapshot,
    from: Long
  ): SnapshotDelta = {
    // Walk head → root via parentSnapshotId, collecting steps until we
    // reach `from` (exclusive) or run out of parents (gap).
    val steps: Seq[Snapshot] = collectLineageTo(table, head, from)
    if (steps.isEmpty) {
      // Either from == head (already consumed) or head IS from's child
      // chain with nothing new. No new data.
      SnapshotDelta.NoDataChange
    } else {
      // Gap detection: the OLDEST step's parent must be either `from`
    // (normal continuation) or null (the table's very first snapshot —
    // from=0L is the watermark's "never refreshed" sentinel, so walking
    // to the beginning of history is a COMPLETE walk, not a gap).
    val oldestParent = steps.last.parentId()
    val gapFree =
      (oldestParent != null && oldestParent.longValue() == from) ||
      (oldestParent == null && from == 0L)
      if (!gapFree) {
        SnapshotDelta.Ambiguous(AmbiguityReason.OutOfWindowRewrite(head.snapshotId()))
      } else {
        aggregateSteps(table, steps, head)
      }
    }
  }

  /** Collect the lineage steps from `head` back to (but not including)
    * `from` by walking `parentId()` through the TABLE's snapshot
    * resolver (Iceberg ancestors are addressable by id from the Table,
    * not by chaining Snapshot objects — 1.5.2 has no `Snapshot.parent()`).
    * Empty when `from` is absent (gap or already-consumed). Ordered
    * oldest → newest. `parentId()` is a boxed `java.lang.Long`, null at
    * the table's first snapshot (jvm-safety mantra 1: null at the
    * boundary). */
  private def collectLineageTo(
    table: Table,
    head: Snapshot,
    from: Long
  ): Seq[Snapshot] = {
    // from == head (or from is in the future): zero new steps.
    if (head.snapshotId() == from) return Nil
    var cur: Snapshot = head
    var acc = List.empty[Snapshot]
    var guard = 0
    val maxWalk = 10000 // lineage-walk bound (jvm-safety mantra 3: a
    // runaway loop over unbounded history is a heap risk — bounded).
    while (cur != null && cur.snapshotId() != from && guard < maxWalk) {
      acc = cur :: acc
      val p: java.lang.Long = cur.parentId()
      cur = if (p == null) null else table.snapshot(p.longValue())
      guard += 1
    }
    acc.reverse
  }

  /** Aggregate the collected lineage steps into one SnapshotDelta.
    * Conservative: any ambiguous step poisons the whole walk (spec §5
    * frame rule). Delete presence forces DeletesInOpenWindow (v1
    * append-only). */
  private def aggregateSteps(
    table: Table,
    steps: Seq[Snapshot],
    head: Snapshot
  ): SnapshotDelta = {
    var addedRows = 0L
    var removedRows = 0L
    val addedFiles = scala.collection.mutable.ArrayBuffer.empty[DataFileRef]
    var sawDeleteFiles = false
    var sawRemovedFiles = false
    var ambiguity: Option[AmbiguityReason] = None

    val io = table.io()
    val it = steps.iterator
    while (it.hasNext && ambiguity.isEmpty) {
      val snap = it.next()
      // (a) operation-level check: overwrite/replace/delete are the
      // rewrite-or-delete signatures (spec §5.4) — clean extraction
      // only trusts "append".
      ambiguity = operationAmbiguity(snap).orElse {
        // (b) summary-row-totals presence + parse check (missing keys
        // are legal for metadata-only snapshots — verified via io)
        summaryRowTotals(io, snap) match {
          case Left(reason) => Some(reason)
          case Right((a, d)) =>
            addedRows += a
            removedRows += d
            // (c) file-level classification via FileIO. The IO is
            // table.io() — metadata manifests only, no data scans
            // (cost contract, spec §6).
            val adds = snap.addedDataFiles(io).asScala.toSeq
            val removes = snap.removedDataFiles(io).asScala.toSeq
            val delAdds = snap.addedDeleteFiles(io).asScala.toSeq
            if (delAdds.nonEmpty) {
              sawDeleteFiles = true
              // Delete files make row attribution impossible in v1
              // (spec §5.1). Equality vs positional is not
              // distinguished here: both force the fallback under the
              // append-only rule.
              ambiguity = Some(AmbiguityReason.EqualityDeletes(delAdds.size))
            }
            if (removes.nonEmpty) {
              sawRemovedFiles = true
              addedFiles ++= removes.map(toRef)
            }
            if (adds.nonEmpty) {
              addedFiles ++= adds.map(toRef)
            }
            None
        }
      }
    }

    ambiguity match {
      case Some(reason) => SnapshotDelta.Ambiguous(reason)
      case None =>
        if (addedRows == 0L && removedRows == 0L && addedFiles.isEmpty) {
          SnapshotDelta.NoDataChange
        } else if (sawDeleteFiles || (sawRemovedFiles && removedRows > 0)) {
          // Removed files WITH a matching add in the SAME snapshot is
          // the COW-rewrite signature (spec §5.4): the file content
          // was rewritten, not deleted. We keep it in the ADT as
          // DeletesInOpenWindow so the refresher's fallback path
          // handles it uniformly with MOR deletes.
          val files = addedFiles.toList
          SnapshotDelta.DeletesInOpenWindow(removedRows, files)
        } else {
          SnapshotDelta.Appended(addedRows, addedFiles.toList)
        }
    }
  }

  /** IO-free mirror construction from Iceberg's ContentFile.
    * `path()` is `CharSequence` → `String.valueOf`; `pos()` is a boxed
    * `java.lang.Long` that may be null (singleton-manifest files only
    * → 0L per the spec's translation notes); `recordCount()` is
    * primitive long (never null). */
  private def toRef(f: org.apache.iceberg.ContentFile[_]): DataFileRef = {
    val path = String.valueOf(f.path())
    val pos: Long = if (f.pos() == null) 0L else f.pos().longValue()
    DataFileRef(path, pos, f.recordCount())
  }
}
