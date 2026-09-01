/*
 * SM8 Platform — ModelService wire DTOs.
 *
 * Per [[ADR-012-a]] (`docs/adr/0012-a-modelservice-restate-handler.md`):
 * the wire-stable request + response case classes surfaced via the
 * `ModelService` Restate handler. Lives next to
 * `QueryService.scala` / `MetaInspectorService.scala` per the
 * `sm8-platform.query` package convention.
 *
 * Per [[scala-data-driven-refactor-mindset]]: pure data; no behavior
 * in the DTOs themselves (the `ModelService.definition` object holds
 * the handler logic).
 *
 * Per [[scala-jvm-safety-mindset]] (null is a liar): the
 * empty-body request DTOs (`ListModelsRequest`, `DescribeRequest`) are
 * kept as 0-field case classes so Jackson's `DefaultScalaModule`
 * serializer emits `{}` deterministically rather than `null` or
 * omitting the body.
 */
package io.sm8.platform.query

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import dev.restate.sdk.HandlerRunner
import dev.restate.sdk.common.TerminalException
import dev.restate.sdk.endpoint.definition.{
  HandlerDefinition,
  HandlerType,
  ServiceDefinition,
  ServiceType
}
import dev.restate.serde.jackson.JacksonSerdeFactory

import io.sm8.core.model.{Model, ModelSummary}

/**
 * Empty request body for `ModelService.listModels`. Required by the
 * Restate SDK's `HandlerRunner` signature (every handler takes one
 * input).
 */
final case class ListModelsRequest() extends Product with Serializable

/**
 * Response for `ModelService.listModels`.
 *
 * @param models the list of `ModelSummary` (today: always exactly 1,
 *               since sm8-server boots with one model; future
 *               multi-model serving would expand this)
 */
final case class ListModelsResponse(models: Seq[ModelSummary])
    extends Product with Serializable

/**
 * Request body for `ModelService.getModel`.
 *
 * @param name the model name to look up
 */
final case class GetModelRequest(name: String) extends Product with Serializable

/**
 * Response for `ModelService.getModel`. Contains the summary only;
 * the YAML source is **not** included (per ADR-012-a §Alternatives —
 * `PlatformModelLoader.toYaml` does not exist and adding a YAML
 * round-trip is deferred to a follow-up ADR).
 *
 * @param summary the `ModelSummary` projection of the matched model
 */
final case class GetModelResponse(summary: ModelSummary)
    extends Product with Serializable

/**
 * Empty request body for `ModelService.describe`. Convenience
 * handler for the single-primary-model case (today: always 1).
 */
final case class DescribeRequest() extends Product with Serializable

/**
 * Response for `ModelService.describe`.
 *
 * @param model the `ModelSummary` projection of the primary model
 */
final case class DescribeResponse(model: ModelSummary)
    extends Product with Serializable
/**
 * SM8 Platform — ModelService handler.
 *
 * Per [[ADR-012-a]] (`docs/adr/0012-a-modelservice-restate-handler.md`):
 * the 3rd Restate service in `sm8-platform`, exposing 3 read-only
 * handlers that let the Restate web UI (Services + Invocations pages)
 * and external clients discover what model a deployment is serving.
 *
 * Handlers:
 *   - `listModels`  → returns a `Seq[ModelSummary]` (today: always 1)
 *   - `getModel`    → looks up by name; throws `TerminalException(404)`
 *                     if not found (preserves the typed-error contract
 *                     per [[scala-error-handling-mindset]] + matches
 *                     the existing `QueryService` pattern at
 *                     `QueryService.scala:248`)
 *   - `describe`    → convenience no-arg shorthand for the single
 *                     primary model
 *
 * Per [[scala-jvm-safety-mindset]] (resource lifecycle): the
 * service is stateless — `ModelSummary.fromModel(model)` is computed
 * on every call from the captured `model` reference. No I/O, no
 * mutable caches, no per-request state.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1 (closure
 * serialization): the closure captures `model: Model`, which is a
 * case-class-derived `Product with Serializable` (per
 * `sm8-core/.../Model.scala`). Safe for any future journaled
 * execution.
 */
object ModelService {

  /**
   * Build the Restate `ServiceDefinition` for the 3 ModelService
   * handlers.
   *
   * Per [[karpathy-guidelines-mindset]] "smallest correct change":
   * the `definition` mirrors the existing `MetaInspectorService.definition`
   * (single-object factory) so callers can compose ModelService via
   * `HttpTransport.endpoint.bind(...)` like the other services.
   *
   * @param model the the single primary model currently loaded by
   *              `sm8-server` (today: always one model; future
   *              multi-model serving would change this signature to
   *              take a `Seq[Model]` or a `ModelRegistry`)
   * @return      the `ServiceDefinition` exposing
   *              `listModels` / `getModel` / `describe`
   */
  def definition(model: Model): ServiceDefinition = {
    // Per review pass #2 (DE-reviewer #3): the SDK's
    // `JacksonSerdeFactory.DEFAULT` mapper doesn't reliably auto-load
    // `jackson-module-scala` via SPI. Construct the ObjectMapper
    // explicitly with `DefaultScalaModule` so Scala case classes
    // (the wire DTOs) serialize correctly. Mirrors QueryService.scala.
    val scalaMapper: ObjectMapper =
      new ObjectMapper()
        .registerModule(DefaultScalaModule)
    val jacksonSerdeFactory = new JacksonSerdeFactory(scalaMapper)

    // Per-handler serdes (NOT per-Endpoint — see QueryService.scala).
    // Each `Serde[T]` encodes/decodes one type via Jackson.
    val listRequestSerde = jacksonSerdeFactory.create(classOf[ListModelsRequest])
    val listResponseSerde = jacksonSerdeFactory.create(classOf[ListModelsResponse])
    val getRequestSerde = jacksonSerdeFactory.create(classOf[GetModelRequest])
    val getResponseSerde = jacksonSerdeFactory.create(classOf[GetModelResponse])
    val describeRequestSerde = jacksonSerdeFactory.create(classOf[DescribeRequest])
    val describeResponseSerde = jacksonSerdeFactory.create(classOf[DescribeResponse])

    // The captured `model` reference is `Product with Serializable`,
    // so the handler closures are journal-safe per [[ADR-006]].
    val listRunner = HandlerRunner.of(
      (ctx: dev.restate.sdk.Context, _: ListModelsRequest) => {
        ListModelsResponse(Seq(ModelSummary.fromModel(model)))
      },
      jacksonSerdeFactory,
      HandlerRunner.Options.DEFAULT
    )

    // Per [[scala-error-handling-mindset]]: a not-found model is a
    // typed, non-retryable failure. `TerminalException(404, msg)` matches
    // the QueryService pattern at `QueryService.scala:248`.
    val getRunner = HandlerRunner.of(
      (ctx: dev.restate.sdk.Context, req: GetModelRequest) => {
        if (req.name == model.name)
          GetModelResponse(summary = ModelSummary.fromModel(model))
        else
          throw new TerminalException(
            404,
            s"sm8: model '${req.name}' not found (loaded: ${model.name})"
          )
      },
      jacksonSerdeFactory,
      HandlerRunner.Options.DEFAULT
    )

    val describeRunner = HandlerRunner.of(
      (ctx: dev.restate.sdk.Context, _: DescribeRequest) => {
        DescribeResponse(model = ModelSummary.fromModel(model))
      },
      jacksonSerdeFactory,
      HandlerRunner.Options.DEFAULT
    )

    ServiceDefinition.of(
      "ModelService",
      ServiceType.SERVICE,
      java.util.List.of(
        HandlerDefinition.of(
          "listModels",
          HandlerType.SHARED,
          listRequestSerde,
          listResponseSerde,
          listRunner
        ),
        HandlerDefinition.of(
          "getModel",
          HandlerType.SHARED,
          getRequestSerde,
          getResponseSerde,
          getRunner
        ),
        HandlerDefinition.of(
          "describe",
          HandlerType.SHARED,
          describeRequestSerde,
          describeResponseSerde,
          describeRunner
        )
      )
    )
  }
}
