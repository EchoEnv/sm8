/*
 * SM8 Platform — MetricsService.
 *
 * Per ADR-012-b (`docs/adr/0012-b-metricsservice-restate-handler.md`):
 * exposure of invocation metrics via a Restate handler so the
 * web UI's Services / Operations views can surface counters (and a
 * future UI dashboard can graph them).
 *
 * Handlers:
 *   - `snapshot` → returns `MetricsSnapshot(invocations, cache, errors,
 *                    startedAt, uptimeSeconds)` — all counters are
 *                    PLACEHOLDER ZEROS for this ADR. Real instrumentation
 *                    (per-event counter increments at query/cache paths)
 *                    is deferred to ADR-012-b-followup.
 *
 * Per karpathy-guidelines-mindset "smallest correct change": the
 * wire surface is real; the values are 0. Honest disclosure at every
 * touchpoint (Scaladoc, README, smoke script) makes the placeholder
 * status explicit so operators don't mistake 0s for "metrics broken."
 *
 * Per [[scala-data-driven-refactor-mindset]]: all wire DTOs are pure
 * data; no behavior in the DTOs themselves. The `MetricsService`
 * object holds the handler logic and the captured `startedAt: Instant`.
 *
 * Per [[scala-jvm-safety-mindset]] (resource lifecycle): no resources
 * are acquired. `startedAt` is captured once at object initialization
 * (Scala object lazy val semantics) and read on each `snapshot` call.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1 (closure
 * serialization): the handler closure captures only `startedAt`
 * (`Instant extends Serializable`). All wire DTOs are `Product with
 * Serializable`. Safe for future journaled execution.
 *
 * Per building-restate-services skill skill (verifying before
 * finishing):
 *   - [x] All wire DTOs are Product with Serializable
 *   - [x] No side effects (pure read of in-process `Instant`)
 *   - [x] No random/time/sleep — `Instant.now()` is deterministic
 *       per call (clock-driven), no sleeps
 *   - [x] Handler is stateless, registered via ServiceDefinition
 *
 * Per the same skill's "When to use" guidance (also cited in
 * MetaInspectorService.scala): `ServiceType.SERVICE` + `HandlerType.SHARED`
 * is correct here. The state (counters) will live in sm8-platform's
 * own counters — NOT in a Restate journal. Same rationale as
 * MetaInspectorService.
 */
package io.sm8.platform.query

import java.time.Instant

import dev.restate.sdk.HandlerRunner
import dev.restate.sdk.endpoint.definition.{
  HandlerDefinition,
  HandlerType,
  ServiceDefinition,
  ServiceType
}
import dev.restate.serde.jackson.JacksonSerdeFactory

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

// ===========================================================================
//  Wire DTOs
// ===========================================================================

/**
 * Empty request body for the `snapshot` handler.
 *
 * Per ADR-012-b §Wire DTOs: required by the Restate SDK's
 * `HandlerRunner` signature (every handler takes one input).
 * The empty-body form keeps the handler round-trip unambiguous —
 * clients always POST `{}` rather than guessing "no body".
 */
final case class SnapshotRequest() extends Product with Serializable

/**
 * Invocation counters surfaced by the `snapshot` handler.
 *
 * Per ADR-012-b: PLACEHOLDER ZEROS for this ADR. Future
 * ADR-012-b-followup will replace with real per-event counters at the
 * query/cache paths.
 *
 * @param total      the total number of `QueryService.runQuery` calls
 *                   attempted (succeeded + failed)
 * @param succeeded  the number that completed without error
 * @param failed     the number that threw (typed-error or otherwise)
 */
final case class InvocationCounters(
    total: Long,
    succeeded: Long,
    failed: Long
) extends Product with Serializable

/**
 * Cache counters surfaced by the `snapshot` handler.
 *
 * Per ADR-012-b: PLACEHOLDER ZEROS for this ADR.
 *
 * @param hits   the number of cache hits (cache-plugin read returned
 *               a value)
 * @param misses the number of cache misses (cache-plugin read returned
 *               None)
 */
final case class CacheCounters(
    hits: Long,
    misses: Long
) extends Product with Serializable

/**
 * Error counters surfaced by the `snapshot` handler.
 *
 * Per ADR-012-b: PLACEHOLDER ZEROS for this ADR. The two error
 * types covered are:
 *   - `auditSinkUnavailable` — `EngineError.AuditSinkUnavailable` was
 *     thrown (sm8-core's typed error when the audit-plugin sink is
 *     down). Counter increments when the typed error is raised.
 *   - `timedOut` — query execution exceeded the engine-portable
 *     timeout (sm8-core raises a typed timeout error). Counter
 *     increments on timeout.
 *
 * @param auditSinkUnavailable the count of `EngineError.AuditSinkUnavailable`
 *                              raised since process start
 * @param timedOut               the count of execution timeouts since
 *                              process start
 */
final case class ErrorCounters(
    auditSinkUnavailable: Long,
    timedOut: Long
) extends Product with Serializable

/**
 * Response payload for the `snapshot` handler.
 *
 * Per ADR-012-b: the wire surface is real; the counter values
 * are placeholders (zeros) until ADR-012-b-followup instruments the
 * call sites. `startedAt` + `uptimeSeconds` are computed real-time and
 * give operators a stable reference frame to distinguish
 *   "server up, no activity yet" (zero counters, positive uptime)
 *   from "server crashed" (HTTP unreachable).
 *
 * @param startedAt     ISO-8601 timestamp of process start
 * @param uptimeSeconds seconds since `startedAt`, computed at each
 *                      call to `Instant.now()`
 * @param invocations   the `InvocationCounters` projection
 * @param cache         the `CacheCounters` projection
 * @param errors        the `ErrorCounters` projection
 */
final case class MetricsSnapshot(
    startedAt: String,
    uptimeSeconds: Long,
    invocations: InvocationCounters,
    cache: CacheCounters,
    errors: ErrorCounters
) extends Product with Serializable

// ===========================================================================
//  Service object — `MetricsService.definition()`
// ===========================================================================

object MetricsService {

  /**
   * Captured ONCE at service initialization (Scala object lazy
   * semantics). The placeholder counter implementation captures
   * `Instant.now()` exactly once; ADR-012-b-followup will replace
   * this with real per-event counters without changing the wire
   * shape.
   *
   * Per [[scala-spark-batch-bugs-mindset]]: `Instant extends Serializable`
   * so the closure capture is journal-safe.
   */
  private val startedAt: Instant = Instant.now()

  /**
   * Build the Restate `ServiceDefinition` for the `snapshot`
   * handler.
   *
   * Per ADR-012-b: the `definition` mirrors the existing
   * `MetaInspectorService.definition` (single-object factory) so
   * callers can compose MetricsService via
   * `HttpTransport.endpoint.bind(...)` like the other services.
   *
   * `ServiceType.SERVICE` + `HandlerType.SHARED` is correct here:
   *   - SERVICE: state will live in sm8-platform's counters, NOT
   *     in a Restate journal. Same rationale as MetaInspectorService
   *     (see MetaInspectorService.scala header for the full
   *     SERVICE-vs-VIRTUAL_OBJECT argument).
   *   - SHARED: read-only query. Concurrent invocations across all
   *     calls — what you want for a metrics endpoint.
   *
   * @return the `ServiceDefinition` exposing `snapshot`
   */
  def definition(): ServiceDefinition = {
    // Per the convention from QueryService.scala: explicit
    // `DefaultScalaModule` registration — the SDK's
    // `JacksonSerdeFactory.DEFAULT` mapper doesn't reliably auto-load
    // `jackson-module-scala` via SPI.
    val scalaMapper: ObjectMapper =
      new ObjectMapper()
        .registerModule(DefaultScalaModule)
    val jacksonSerdeFactory = new JacksonSerdeFactory(scalaMapper)

    val snapshotRequestSerde = jacksonSerdeFactory.create(classOf[SnapshotRequest])
    val snapshotResponseSerde = jacksonSerdeFactory.create(classOf[MetricsSnapshot])

    val snapshotRunner: HandlerRunner[SnapshotRequest, MetricsSnapshot] =
      HandlerRunner.of(
        (ctx: dev.restate.sdk.Context, _: SnapshotRequest) => {
          val now: Instant = Instant.now()
          // Per ADR-012-b-followup (= PR-255): delegate to the
          // QueryMetrics singleton which holds the live counters.
          // The snapshot reader reads 6 AtomicLongs (non-atomic
          // across the read; documented acceptable for diagnostic use
          // per QueryMetrics.snapshot Scaladoc).
          QueryMetrics.snapshot(
            uptimeSeconds = (now.toEpochMilli - startedAt.toEpochMilli) / 1000L,
            startedAtIso  = startedAt.toString
          )
        },
        jacksonSerdeFactory,
        HandlerRunner.Options.DEFAULT
      )

    ServiceDefinition.of(
      "MetricsService",
      ServiceType.SERVICE,
      java.util.List.of(
        HandlerDefinition.of(
          "snapshot",
          HandlerType.SHARED,
          snapshotRequestSerde,
          snapshotResponseSerde,
          snapshotRunner
        )
      )
    )
  }
}