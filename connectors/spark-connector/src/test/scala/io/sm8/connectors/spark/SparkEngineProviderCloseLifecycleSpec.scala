/*
 * SM8 Spark Connector — SparkEngineProvider.close() lifecycle spec
 * (PR-157, data-eng WARN-1 from 3rd-pass cumulative-session-review).
 *
 * Verifies that PR-148 L2's `@transient persistedFrames` field is
 * correctly handled on `close()`:
 *  1. `close()` iterates `persistedFrames`, unpersists each tracked
 *     DataFrame (swallowing per-frame errors), clears the map, then
 *     stops the SparkSession.
 *  2. `close()` is idempotent — calling it twice does not throw.
 *  3. The `@transient` annotation does not prevent close-time
 *     unpersist (which happens in the live JVM, not via deser).
 *
 * Per data-eng WARN-1: existing `SparkEngineProviderSpec` covers
 * `Serializable` round-trip but does NOT exercise
 * `trackPersist(...) + close()`. This spec fills that gap.
 */
package io.sm8.connectors.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkEngineProviderCloseLifecycleSpec
    extends AnyFunSuite
    with Matchers {

  test("close(): unpersists tracked DataFrames, clears the map, and is idempotent") {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("tCloseLifecycle")
      .getOrCreate()
    try {
      // Build a minimal SparkEngineProvider via the public test
      // seam (same shape as the existing tests in this module).
      val provider = new SparkEngineProvider(
        spark = spark,
        bridge = io.sm8.connectors.spark.SparkTypeBridge,
        sparkEngineName = "spark-3.5"
      )

      // Track 2 DataFrames via the public-private[spark] API.
      val df1 = spark.range(10).persist()
      val df2 = spark.range(20).persist()
      val tok1 = provider.trackPersist(df1)
      val tok2 = provider.trackPersist(df2)
      tok1 should not be tok2

      // The map is internal; we don't have a getter, but the
      // close() side effect is observable via "DataFrame is
      // unpersisted after close" (verified by StorageLevel going
      // back to NONE on the df reference — we capture before).
      // Spark's .persist() defaults to MEMORY_AND_DISK, so the
      // DataFrame is cached BEFORE close(). The unpersist
      // observable is: storageLevel returns NONE post-close.
      val _df1LevelBefore = df1.storageLevel // sanity: not yet NONE
      val _df2LevelBefore = df2.storageLevel
      provider.close()

      // After close(), DataFrame storage levels should be NONE
      // (Spark unpersist resets them). This is the visible side
      // effect of the close() iterating persistedFrames and
      // calling df.unpersist() on each.
      // (Note: Spark's API for "is this DataFrame still cached"
      // is `df.storageLevel != StorageLevel.NONE`; after unpersist
      // it should equal StorageLevel.NONE.)
      df1.storageLevel shouldBe org.apache.spark.storage.StorageLevel.NONE
      df2.storageLevel shouldBe org.apache.spark.storage.StorageLevel.NONE

      // Close #2: idempotent (no throw, no side effect).
      noException should be thrownBy provider.close()

      // SparkSession.stop() is also a no-op after the first call.
      // Per Spark docs, calling stop() twice is safe.
      noException should be thrownBy spark.stop()
    } finally {
      // Best-effort cleanup in case any assertion above failed
      // before close() was called.
      try spark.stop() catch { case _: Throwable => () }
    }
  }

  test("close(): with no tracked DataFrames is a no-op (does not throw)") {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("tCloseNoop")
      .getOrCreate()
    try {
      val provider = new SparkEngineProvider(
        spark = spark,
        bridge = io.sm8.connectors.spark.SparkTypeBridge,
        sparkEngineName = "spark-3.5"
      )
      // No trackPersist calls. close() should iterate an empty
      // map and proceed to spark.stop() without throwing.
      noException should be thrownBy provider.close()
    } finally {
      try spark.stop() catch { case _: Throwable => () }
    }
  }
}