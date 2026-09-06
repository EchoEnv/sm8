/*
 * SM8 Platform — RollupRefreshService (Ticket 6 of
 * docs/wayfinder/2026-09-06-pre-aggregation.md).
 *
 * The server-side trigger surface for "one command rebuilds a
 * model's rollups": a Restate SERVICE exposing `refresh` that takes
 * a model name, resolves the model's rollups, and eagerly
 * re-materializes each one (via the caller-supplied refresh
 * function — the spark-connector `RollupRefresher.refreshModel` is
 * the wired implementation; the platform stays connector-agnostic
 * per the layer discipline in
 * docs/rfcs/2026-08-12_v1_architecture-spec/semantic-layer-engine-architecture.md §3).
 *
 * ==Why a function parameter (not a direct connector import)==
 *
 * sm8-platform is an ADAPTER layer module and must not depend on
 * the spark-connector. The refresh MECHANISM (Spark aggregation +
 * saveAsTable) lives in the connector; the platform defines the
 * TRIGGER SEAM (this service) and the deployment wires them
 * together (`RollupRefresher.refreshModel(spark, name, modelOf)`).
 *
 * ==Restate semantics==
 *
 * SERVICE + SHARED (read/write but idempotent-by-nature: refresh
 * overwrites rollup tables): the same rationale as
 * MetricsService/RegistryInspectorService — state lives in the
 * catalog, not a Restate journal.
 *
 * ==Responses==
 *
 * Per-rollup results (refreshed / failed + reason) so cron and the
 * CLI report exactly what happened — never a silent partial
 * refresh.
 */
package io.sm8.platform.query

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import dev.restate.sdk.HandlerRunner
import dev.restate.sdk.endpoint.definition.{
  HandlerDefinition,
  HandlerType,
  ServiceDefinition,
  ServiceType
}
import dev.restate.serde.jackson.JacksonSerdeFactory

/** Request: which model to refresh. */
final case class RollupRefreshRequest(model: String)

/** Per-rollup outcome. */
final case class RollupRefreshOutcome(
    rollup: String,
    table: String,
    refreshed: Boolean,
    error: Option[String]
)

/** Response: every rollup's outcome + an overall ok flag
  * (ok = all refreshed). */
final case class RollupRefreshResponse(
    model: String,
    ok: Boolean,
    results: List[RollupRefreshOutcome]
)

object RollupRefreshService {

  /** The refresh function the deployment wires (connector side:
    * `RollupRefresher.refreshModel(spark, name, modelOf)` returns
    * its own richer result type; adapt it here). */
  type RefreshFn =
    String => Either[String, List[(String, String, Option[String])]]

  /** Build the Restate `ServiceDefinition` exposing `refresh`.
    *
    * @param refreshFn per-call refresh function: model name ->
    *                  Right(list of (rollup, table, None)) on
    *                  success, or per-rollup (rollup, table,
    *                  Some(error)) for partial failures; Left =
    *                  model-level failure (e.g. not found)
    * @return the `ServiceDefinition` exposing `refresh`
    */
  def definition(refreshFn: RefreshFn): ServiceDefinition = {
    // Per the convention from QueryService.scala + MetricsService:
    // explicit DefaultScalaModule registration.
    val scalaMapper: ObjectMapper =
      new ObjectMapper().registerModule(DefaultScalaModule)
    val jacksonSerdeFactory = new JacksonSerdeFactory(scalaMapper)

    val requestSerde = jacksonSerdeFactory.create(classOf[RollupRefreshRequest])
    val responseSerde = jacksonSerdeFactory.create(classOf[RollupRefreshResponse])

    val refreshRunner: HandlerRunner[RollupRefreshRequest, RollupRefreshResponse] =
      HandlerRunner.of(
        (_: dev.restate.sdk.Context, req: RollupRefreshRequest) => {
          refreshFn(req.model) match {
            case Right(results) =>
              val outcomes = results.map { case (rollup, table, err) =>
                RollupRefreshOutcome(
                  rollup = rollup,
                  table = table,
                  refreshed = err.isEmpty,
                  error = err)
              }
              RollupRefreshResponse(
                model = req.model,
                ok = outcomes.forall(_.refreshed),
                results = outcomes)
            case Left(modelErr) =>
              RollupRefreshResponse(
                model = req.model,
                ok = false,
                results = List(RollupRefreshOutcome(
                  rollup = "(model)",
                  table = "",
                  refreshed = false,
                  error = Some(modelErr)))
              )
          }
        },
        jacksonSerdeFactory,
        HandlerRunner.Options.DEFAULT
      )

    ServiceDefinition.of(
      "RollupRefreshService",
      ServiceType.SERVICE,
      java.util.List.of(
        HandlerDefinition.of(
          "refresh",
          HandlerType.SHARED,
          requestSerde,
          responseSerde,
          refreshRunner
        )
      )
    )
  }
}
