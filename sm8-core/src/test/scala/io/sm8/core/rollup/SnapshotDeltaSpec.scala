/*
 * SM8 Core — SnapshotDeltaSpec (D5 snapshot-diff spec §7 test 8).
 *
 * Pure core-side truth-table test: no Spark, no Iceberg, no IO
 * (the test itself must respect the RFC §3 boundary it verifies).
 */
package io.sm8.core.rollup

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SnapshotDeltaSpec extends AnyFlatSpec with Matchers {

  private val f1 = DataFileRef("s3://w/t/data/f1.parquet", 0L, 100L)
  private val f2 = DataFileRef("s3://w/t/data/f2.parquet", 1L, 40L)

  "isRowExtractable" should "be true for Appended (clean inserts)" in {
    SnapshotDelta.isRowExtractable(SnapshotDelta.Appended(140L, Seq(f1, f2))) shouldBe true
  }

  it should "be true for NoDataChange (trivially extractable)" in {
    SnapshotDelta.isRowExtractable(SnapshotDelta.NoDataChange) shouldBe true
  }

  it should "be false for DeletesInOpenWindow (v1 append-only, D2-3 no-+=)" in {
    SnapshotDelta.isRowExtractable(SnapshotDelta.DeletesInOpenWindow(10L, Seq(f1))) shouldBe false
  }

  it should "be false for every AmbiguityReason variant" in {
    val reasons: Seq[AmbiguityReason] = Seq(
      AmbiguityReason.EqualityDeletes(2),
      AmbiguityReason.OutOfWindowRewrite(99L),
      AmbiguityReason.SchemaOrPartitionEvolution(99L)
    )
    reasons.foreach { r =>
      withClue(s"reason $r: ") {
        SnapshotDelta.isRowExtractable(SnapshotDelta.Ambiguous(r)) shouldBe false
      }
    }
  }

  "isNoOp" should "be true only for NoDataChange" in {
    SnapshotDelta.isNoOp(SnapshotDelta.NoDataChange) shouldBe true
    SnapshotDelta.isNoOp(SnapshotDelta.Appended(1L, Seq(f1))) shouldBe false
    SnapshotDelta.isNoOp(SnapshotDelta.DeletesInOpenWindow(1L, Seq(f1))) shouldBe false
    SnapshotDelta.isNoOp(SnapshotDelta.Ambiguous(AmbiguityReason.EqualityDeletes(1))) shouldBe false
  }

  "Appended" should "carry its file refs so the refresher can narrow" in {
    val d = SnapshotDelta.Appended(140L, Seq(f1, f2))
    d.files should have size 2
    d.rows shouldBe 140L
  }

  "DataFileRef" should "stay a pure value (IO-free mirror)" in {
    val r = DataFileRef("/warehouse/t/data/a.parquet", 3L, 7L)
    r.path should include("a.parquet")
    r.pos shouldBe 3L
    r.rowCount shouldBe 7L
  }
}
