/*
 * SM8 Platform — MetaInspectorService.
 *
 * The transport layer exposes a generic `MetaInspectorService`
 * that reads any `context.meta` key. The transport knows NOTHING
 * about graphs, dangling edges, or any other plugin's domain —
 * it just reads `context.meta.get(key)` and returns the value.
 *
 * The wire DTOs (`MetaRequest` + `MetaResponse`) are minimal —
 * no plugin-specific knowledge. The plugin owns the value
 * schema; the transport only commits to "round-trip via Jackson
 * with `DefaultScalaModule`".
 *
 * This service's purpose is "diagnostic inspector for
 * `context.meta`". It does NOT know about graphs, cycles, or any
 * other domain.
 *
 * == Why `ServiceType.SERVICE` (and not `VIRTUAL_OBJECT`) ==
 *
 * Per the `building-restate-services` skill
 * (`references/design-and-architecture.md`, "When to use"):
 *   - **Service**: stateless handlers; API endpoints; ETL; each call
 *     independent.
 *   - **Virtual Object**: stateful entities; persistent K/V per key;
 *     one writer per key + concurrent readers.
 *
 * MetaInspectorService reads from an in-process
 * `AtomicReference[Map[String, Any]]` (the `engineFn()` closure
 * passed by `sm8-server` from `MetaCaptureObserver`). The state is
 * sm8-server memory, NOT a Restate journal. Restate is used purely
 * as a wire protocol + invocation routing + UI surface.
 *
 * If we keyed this as a `VIRTUAL_OBJECT` keyed by `key`:
 *   1. **Every call would be its own key** — there is no single
 *      canonical key (each `context.meta` entry is a different
 *      key). The URL shape would be `MetaInspector/getMeta/{key}`,
 *      forcing callers to enumerate keys up-front.
 *   2. **Restate would dedupe repeat calls** — calling
 *      `getMeta("sm8.cache.policy")` twice with the same key and
 *      no state change would replay the cached result. That is the
 *      WRONG semantics for a debug/audit endpoint: each call must
 *      return the current in-process state, not a stale journaled
 *      result.
 *
 * Therefore `SERVICE` is the correct type. The `key` field in
 * `MetaRequest` is a *handler parameter*, not a Restate-level
 * routing key. The closure captures `engineFn()` (stateless
 * closure over sm8-server's `AtomicReference`); no per-key state
 * exists on the Restate side.
 *
 * Future extension (`getMetaByPrefix`) follows the same pattern:
 * one `SERVICE` handler that takes `prefix: String` and returns
 * all matching `context.meta` keys + values. Still no per-key state.
 *
 * == Why `HandlerType.SHARED` (and not `EXCLUSIVE`) ==
 *
 * `getMeta` is a read-only query — no state mutation. SHARED
 * handlers allow concurrent execution across all invocations,
 * which is what you want for diagnostic reads. EXCLUSIVE would
 * queue every read against every other read, which is wasteful.
 *
 * (Both choices — `SERVICE` + `SHARED` — are correct for our
 * usage. If a future requirement adds mutating handlers (e.g.
 * "set meta entry"), the same service would split: reads remain
 * SHARED, writes would be EXCLUSIVE on the same keying strategy.
 * But that's a future ADR.)
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
 * @param model    the engine-portable model whose most recent
 *                 request's `context.meta` to inspect
 * @param registry the engine registry
 * @param engineFn function that runs a single request through
 *                 the engine's hook pipeline and returns the
 *                 resulting `Map[String, Any]`. Typically wired
 *                 by the deployment module.
 */
object MetaInspectorService {

  /**
   * Build the Restate `ServiceDefinition` for the `getMeta`
   * handler.
   *
   * @param model    the model whose `context.meta` to read
   * @param registry the engine registry
   * @param engineFn the hook-pipeline runner
   * @return         the `ServiceDefinition` exposing `getMeta`
   */
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
              // The plugin may have written a typed value or a
              // String. We project to `Map[String, Any]` for the
              // wire; Jackson handles either shape with
              // `DefaultScalaModule`.
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