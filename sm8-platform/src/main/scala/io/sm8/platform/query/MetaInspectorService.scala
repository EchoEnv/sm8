/*
 * SM8 Platform — MetaInspectorService (PR-150, ADR-008-AI follow-up).
 *
 * Per the architect's 2026-08-23 design review
 * (`docs/review/graph-display-design-review.md`): the transport
 * layer exposes a GENERIC `MetaInspectorService` that reads any
 * `context.meta` key. The transport knows NOTHING about graphs,
 * dangling edges, or any other plugin's domain — it just reads
 * `context.meta.get(key)` and returns the value.
 *
 * Per  SS1 (transport layer owns
 * the wire shape): the wire DTOs (`MetaRequest` + `MetaResponse`)
 * are minimal — no plugin-specific knowledge. The plugin OWNS the
 * value schema; the transport only commits to "round-trip via
 * Jackson with `DefaultScalaModule`".
 *
 * Per  SS1 ("one purpose, one
 * service"): this service's purpose is "diagnostic inspector for
 * `context.meta`". It does NOT know about graphs, cycles, or any
 * other domain.
 */
package io.sm8.platform.query

import dev.restate.sdk.HandlerRunner
import dev.restate.sdk.endpoint.definition.{
  HandlerDefinition,
  HandlerType,
  ServiceDefinition,
  ServiceType
}
import dev.restate.serde.jackson.JacksonSerdeFactory

import io.sm8.core.engine.EngineRegistry
import io.sm8.core.model.Model

import scala.jdk.CollectionConverters._

/**
 * Service definition for the `getMeta` handler.
 *
 * The handler is a closure over `(model, registry, engineFn)` that
 * invokes `engineFn()` to read the most recent request's
 * `context.meta` map. The transport does not know what the values
 * are — it just serves them as `Map[String, Any]`.
 *
 * Per  SS1 (the handler body is a
 * pure function of `(request, model, registry, engineFn)`): no
 * `HandlerContext` capture, no driver-side state.
 *
 * @param model the engine-portable model whose most recent
 *        request's `context.meta` to inspect
 * @param registry the engine registry
 * @param engineFn function that runs a single request through the
 *        engine's hook pipeline and returns the resulting
 *        `Map[String, Any]`. Typically wired by `sm8-server` as
 *        `EngineImpl.use(...)` + a synthetic request.
 */
object MetaInspectorService {

  def definition(
      model: Model,
      registry: EngineRegistry,
      engineFn: () => Map[String, Any]
  ): ServiceDefinition = {
    val scalaMapper: com.fasterxml.jackson.databind.ObjectMapper =
      new com.fasterxml.jackson.databind.ObjectMapper()
        .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    val jacksonSerdeFactory = new JacksonSerdeFactory(scalaMapper)

    val requestSerde = jacksonSerdeFactory.create(classOf[MetaRequest])
    val responseSerde = jacksonSerdeFactory.create(classOf[MetaResponse])

    val handlerRunner: HandlerRunner[MetaRequest, MetaResponse] =
      HandlerRunner.of(
        (ctx: dev.restate.sdk.Context, req: MetaRequest) => {
          val meta = engineFn()
          MetaResponse(
            key = req.key,
            present = meta.contains(req.key),
            value = meta.get(req.key).map { v =>
              // The plugin may have written a typed value
              // (GraphSnapshot) or a String. We project to
              // Map[String, Any] for the wire. Jackson handles
              // either shape with `DefaultScalaModule`.
              v match {
                case m: Map[_, _] =>
                  m.asInstanceOf[Map[String, Any]]
                case other =>
                  Map("value" -> other)
              }
            }
          )
        },
        jacksonSerdeFactory,
        HandlerRunner.Options.DEFAULT
      )

    val handlerDefinition: HandlerDefinition[MetaRequest, MetaResponse] =
      HandlerDefinition.of(
        "getMeta",
        HandlerType.SHARED,
        requestSerde,
        responseSerde,
        handlerRunner
      )

    ServiceDefinition.of(
      "MetaInspectorService",
      ServiceType.SERVICE,
      java.util.List.of(handlerDefinition)
    )
  }
}