/*
 * SM8 Platform — EngineServiceRest (Restate handler factory for the
 * engine registry). ADR-013 (PR-259) requires an MCP tool that exposes
 * the engine registry so agents can pick a value for `query.engine`
 * without out-of-band knowledge.
 *
 * This file adds `EngineService.listEngines` to mirror the existing
 * `ModelService.definition` pattern (single-object factory + the same
 * Jackson Serde setup) WITHOUT touching the existing EngineService
 * `runQueryWithHooks` path. Two `object`s coexist: `EngineService`
 * (engine-portable runQuery path, untouched) and `EngineServiceRest`
 * (Restate handler factory for the listEngines endpoint).
 *
 * Handlers:
 *   - `listEngines` → returns `EngineListResponse(names: Seq[String])`
 *                     (sorted names of providers that successfully
 *                     realized — `EngineRegistry.availableProviders`,
 *                     NOT every discovered provider)
 *
 * Per [[scala-error-handling-mindset]]: no errors on this path. The
 * registry either has available providers or it doesn't. An empty
 * list is the correct (typed) result for "no engines ready," not a
 * thrown exception.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1 (closure
 * serialization): the handler closure captures `registry:
 * EngineRegistry` which is `Product with Serializable` (per
 * sm8-core). Safe for any future journaled execution.
 *
 * Per [[scala-data-driven-refactor-mindset]] (sealed-trait dispatch +
 * MatchError-free): no pattern match here; the handler body is a
 * single method call on `registry.availableProviders`.
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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Wire DTO for `EngineService.listEngines` request. Empty-body 0-field
 * case class so Jackson's `DefaultScalaModule` emits `{}` deterministically
 * rather than `null` or omitting the body (matches the
 * `ListModelsRequest` / `DescribeRequest` pattern at
 * `ModelService.scala`).
 */
final case class ListEnginesRequest() extends Product with Serializable

/**
 * Wire DTO for `EngineService.listEngines` response. `names` is a
 * sorted list of provider names that successfully realized at boot
 * (`EngineRegistry.availableProviders`). Empty list is the correct
 * result for "no engines ready" — not a failure.
 */
final case class EngineListResponse(names: Seq[String])
    extends Product with Serializable

/**
 * EngineServiceRest — single-object factory that builds the Restate
 * `ServiceDefinition` for the `listEngines` handler.
 *
 * Per [[karpathy-guidelines-mindset]] "smallest correct change":
 * the `definition` mirrors the existing `ModelService.definition`
 * (single-object factory) so callers compose it via
 * `HttpTransport.endpoint.bind(...)` like the other services.
 *
 * @param registry the discovered engine registry (from
 *                 `Main.discoverProviders(...)._2` in sm8-server)
 * @return         the `ServiceDefinition` exposing `listEngines`
 */
object EngineServiceRest {

  def definition(registry: EngineRegistry): ServiceDefinition = {
    // Per review pass #2 (DE-reviewer #3, captured in ModelService.scala):
    // the SDK's `JacksonSerdeFactory.DEFAULT` mapper doesn't reliably
    // auto-load `jackson-module-scala` via SPI. Construct the ObjectMapper
    // explicitly with `DefaultScalaModule` so Scala case classes serialize
    // correctly.
    val scalaMapper: ObjectMapper =
      new ObjectMapper().registerModule(DefaultScalaModule)
    val jacksonSerdeFactory = new JacksonSerdeFactory(scalaMapper)

    val listRequestSerde = jacksonSerdeFactory.create(classOf[ListEnginesRequest])
    val listResponseSerde = jacksonSerdeFactory.create(classOf[EngineListResponse])

    val listRunner = HandlerRunner.of(
      (ctx: dev.restate.sdk.Context, _: ListEnginesRequest) => {
        EngineListResponse(registry.availableProviders)
      },
      jacksonSerdeFactory,
      HandlerRunner.Options.DEFAULT
    )

    ServiceDefinition.of(
      "EngineService",
      ServiceType.SERVICE,
      java.util.List.of(
        HandlerDefinition.of(
          "listEngines",
          HandlerType.SHARED,
          listRequestSerde,
          listResponseSerde,
          listRunner
        )
      )
    )
  }
}