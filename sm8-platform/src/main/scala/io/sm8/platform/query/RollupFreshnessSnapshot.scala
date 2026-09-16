/*
 * SM8 Platform — RollupFreshnessSnapshot.
 *
 * Engine-portable shape that the metrics exporter renders as
 * gauges. Populated by the deployment's freshness reader (the
 * connector reads the ADR-0030 watermark Iceberg table; the
 * platform never touches Spark).
 *
 * Layer placement: defined in sm8-platform because that's where the
 * Prometheus exporter lives; the connector's RollupFreshnessReader
 * produces it via structural-matching (same case class shape, no
 * connector import on the platform — the reflective bridge in
 * sm8-server adapts at runtime).
 */
package io.sm8.platform.query

/** Engine-portable freshness snapshot (no Spark types). */
object RollupFreshnessSnapshot {

  /** One rollup's freshness verdict at scrape time. */
  final case class Entry(
      /** The rollup's name (matches `RollupSpec.name`). */
      rollupName:      String,
      /** ISO-8601 wall-clock of the most recent bucket refresh, or
        * "" when the watermark table has no rows yet (the
        * rollup's bucket counter is the authoritative "has
        * anything been refreshed yet" signal). */
      lastRefreshedAt: String,
      /** True iff every bucket row is `is_final=true`. A rollup with
        * zero buckets reports false — nothing proven final. */
      allFinal:        Boolean,
      /** Number of bucket rows in the watermark table. */
      bucketCount:     Long
  )
}
