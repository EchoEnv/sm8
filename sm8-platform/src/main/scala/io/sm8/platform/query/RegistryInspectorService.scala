/*
 * SM8 Platform — RegistryInspectorService (C10-PR-B).
 *
 * Exposes two read-only Restate handlers over the boot-stable
 * plugin + hook registries:
 *   - `listPlugins` — every Plugin the deployment discovered (with
 *     a `registered` flag distinguishing `Engine.use(plugin)`
 *     registration from classpath-only discovery)
 *   - `listHooks`   — every hook registered on the engine's
 *     `HookManager`, with full RFC §8 metadata (stage, priority,
 *     origin, registering plugin)
 *
 * == Why `ServiceType.SERVICE` (and not `VIRTUAL_OBJECT`) ==
 *
 * Same rationale as MetaInspectorService: the registries are
 * boot-stable in-process state owned by the deployment's engine
 * instance — NOT Restate-journaled per-key state. Restate is used
 * purely as the wire protocol + invocation routing. Keying as a
 * VIRTUAL_OBJECT would force callers to enumerate entries up-front
 * and would dedupe repeat calls (stale journaled results) — both
 * wrong for diagnostic reads.
 *
 * == Why `HandlerType.SHARED` ==
 *
 * Both handlers are read-only queries over stable state. SHARED
 * allows concurrent execution; EXCLUSIVE would needlessly queue
 * diagnostic reads against each other.
 *
 * == Wire shape policy ==
 *
 * The handlers project the core types (`PluginMetadata`,
 * `RegisteredHook`, `HookStage`, `HookOrigin`) into flat string
 * DTOs (`PluginEntry`, `HookEntry`) defined in RegistryRequests.scala.
 * The wire uses `HookStage.wireName` (e.g. `"pre:resolve"`) and the
 * `HookOrigin` constructor name (e.g. `"FirstParty"`) — NOT
 * case-object `toString` — so the JSON shape is stable across ADT
 * refactors. The transport never imports plugin-domain types beyond
 * the SDK seams (per RFC §3 layer discipline).
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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import io.sm8.sdk.{Plugin, SetupStatus, HookStage}

/**
 * ADDITIVE in C10-PR-B final-gate (architect F5): named container for
 * the two deployment-supplied registry closures, replacing the
 * anonymous Option-tuple so call sites self-document
 * (`RegistrySources(hooksFn = ..., pluginsFn = ...)`) and future
 * fields can be added without breaking positional destructuring.
 *
 * @param hooksFn   per-call source of the engine's registered hooks
 *                  (typically `() => engine.hooks.listAllHooks()`)
 * @param pluginsFn per-call source of the discovered plugins with
 *                  their setup status
 */
final case class RegistrySources(
    hooksFn:   () => Seq[io.sm8.sdk.RegisteredHook],
    pluginsFn: () => Seq[(Plugin, SetupStatus)]
)

/**
 * Service definition for the `listPlugins` + `listHooks` handlers.
 *
 * @param hooksFn   closure returning the engine's registered hooks
 *                  (typically `() => engine.hooks.listAllHooks()`,
 *                  wired by the deployment module). Evaluated per
 *                  call — the registries are boot-stable so the cost
 *                  is one seq projection.
 * @param pluginsFn closure returning the deployment's discovered
 *                  plugins + their setup status (wired from the
 *                  `discovered` list + the engine's `seen` set at
 *                  boot). Evaluated per call.
 */
object RegistryInspectorService {

  /**
   * Project a discovered plugin + its setup status to the flat wire
   * entry. Pure function extracted from the handler closure so it
   * can be unit-tested without driving the Restate machinery.
   *
   * @param p      the plugin instance
   * @param status its setup status (from the deployment wiring)
   * @return       the flat wire entry
   */
  private[query] def toPluginEntry(p: Plugin, status: SetupStatus): PluginEntry = {
    val md = p.metadata
    PluginEntry(
      name       = p.name,
      coords     = md.coords,
      version    = md.version,
      registered = status match {
        case SetupStatus.Registered(_) => true
        case SetupStatus.NotRegistered(_, _) => false
      }
    )
  }

  /**
   * Project a core `RegisteredHook` to the flat wire entry. Pure
   * function; uses `HookStage.wireName` for stable wire naming.
   */
  private[query] def toHookEntry(h: io.sm8.sdk.RegisteredHook): HookEntry =
    HookEntry(
      name       = h.name,
      stage      = HookStage.wireName(h.stage),
      priority   = h.priority,
      origin     = h.origin.toString,
      pluginName = h.pluginName
    )

  /**
   * Build the Restate `ServiceDefinition` exposing both handlers.
   *
   * @param hooksFn   per-call source of the registered hooks
   * @param pluginsFn per-call source of the discovered plugins with
   *                  status
   * @return          the `ServiceDefinition` (SERVICE + SHARED per
   *                  the rationale above)
   */
  def definition(
      sources: RegistrySources
  ): ServiceDefinition = {
    // Per the convention from QueryService.scala + MetricsService:
    // explicit `DefaultScalaModule` registration — the SDK's
    // `JacksonSerdeFactory.DEFAULT` mapper doesn't reliably auto-load
    // `jackson-module-scala` via SPI.
    val scalaMapper: ObjectMapper =
      new ObjectMapper().registerModule(DefaultScalaModule)
    val jacksonSerdeFactory = new JacksonSerdeFactory(scalaMapper)

    val listPluginsRequestSerde =
      jacksonSerdeFactory.create(classOf[ListPluginsRequest])
    val listPluginsResponseSerde =
      jacksonSerdeFactory.create(classOf[ListPluginsResponse])
    val listHooksRequestSerde =
      jacksonSerdeFactory.create(classOf[ListHooksRequest])
    val listHooksResponseSerde =
      jacksonSerdeFactory.create(classOf[ListHooksResponse])

    val listPluginsRunner: HandlerRunner[ListPluginsRequest, ListPluginsResponse] =
      HandlerRunner.of(
        (_: dev.restate.sdk.Context, _: ListPluginsRequest) => {
          val entries = sources.pluginsFn()
            .map { case (p, s) => toPluginEntry(p, s) }
            .sortBy(_.name)
          ListPluginsResponse(count = entries.size, plugins = entries)
        },
        jacksonSerdeFactory,
        HandlerRunner.Options.DEFAULT
      )

    val listHooksRunner: HandlerRunner[ListHooksRequest, ListHooksResponse] =
      HandlerRunner.of(
        (_: dev.restate.sdk.Context, _: ListHooksRequest) => {
          val entries = sources.hooksFn().map(toHookEntry)
          ListHooksResponse(count = entries.size, hooks = entries)
        },
        jacksonSerdeFactory,
        HandlerRunner.Options.DEFAULT
      )

    ServiceDefinition.of(
      "RegistryInspectorService",
      ServiceType.SERVICE,
      java.util.List.of(
        HandlerDefinition.of(
          "listPlugins",
          HandlerType.SHARED,
          listPluginsRequestSerde,
          listPluginsResponseSerde,
          listPluginsRunner
        ),
        HandlerDefinition.of(
          "listHooks",
          HandlerType.SHARED,
          listHooksRequestSerde,
          listHooksResponseSerde,
          listHooksRunner
        )
      )
    )
  }
}
