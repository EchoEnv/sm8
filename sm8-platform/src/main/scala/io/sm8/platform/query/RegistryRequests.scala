/*
 * SM8 Platform — RegistryRequests (C10-PR-B).
 *
 * Wire DTOs for the `RegistryInspectorService` Restate handlers
 * (`listPlugins`, `listHooks`). The transport knows NOTHING about
 * plugin domain semantics — it serves the core-sourced registries
 * as flat JSON. The plugin owns the value schema; the transport
 * only commits to "round-trip via Jackson with `DefaultScalaModule`".
 *
 * Layer: platform (per RFC §3; transport library, no plugin-domain
 * knowledge). Mirrors MetaRequest.scala's shape.
 */
package io.sm8.platform.query

/**
 * Request payload for the `listPlugins` handler. Empty body —
 * the registry is boot-stable state; there is nothing to filter on
 * in v1 (per map #306 "Not yet specified" — richer filtering is a
 * future PR if needed).
 */
final case class ListPluginsRequest()

/**
 * One plugin entry returned by `listPlugins`.
 *
 * @param name     the plugin's self-declared name (`Plugin.name`,
 *                 e.g. `"semantic-graph"`)
 * @param coords   Maven coordinates `groupId:artifactId`
 * @param version  Maven version (synthetic `"0.0.0"` when the
 *                 plugin does not override `Plugin.metadata`)
 * @param registered `true` when the plugin was registered via
 *                 `Engine.use(plugin)` (i.e. its hooks are live);
 *                 `false` when it was only discovered on the
 *                 classpath
 */
final case class PluginEntry(
    name:       String,
    coords:     String,
    version:    String,
    registered: Boolean
)

/**
 * Response payload for the `listPlugins` handler.
 *
 * @param count   the number of plugins returned (client convenience)
 * @param plugins the entries, sorted by `name` (deterministic wire
 *                order)
 */
final case class ListPluginsResponse(
    count:   Int,
    plugins: Seq[PluginEntry]
)

/**
 * Request payload for the `listHooks` handler. Empty body.
 */
final case class ListHooksRequest()

/**
 * One hook entry returned by `listHooks`.
 *
 * The `stage` field is the wire name (e.g. `"pre:resolve"`) — NOT
 * the case-object toString — so the wire shape is stable across
 * refactors of the `HookStage` ADT. Same reasoning as the plugin
 * namespacing convention in `MetaRequest`.
 *
 * @param name       the hook's self-declared name
 * @param stage      wire name of the pipeline stage (`pre:parse` …
 *                   `post:format`)
 * @param priority   the RFC §8 priority (0-99 core, 100-899
 *                   first-party, 900+ community)
 * @param origin     the declared origin (Core | FirstParty | Community)
 * @param pluginName the name of the Plugin that registered this
 *                   hook (per C10-PR-A thread-local attribution)
 */
final case class HookEntry(
    name:       String,
    stage:      String,
    priority:   Int,
    origin:     String,
    pluginName: String
)

/**
 * Response payload for the `listHooks` handler.
 *
 * @param count the number of hooks returned
 * @param hooks the entries in the core `listAllHooks()` ordering
 *              (pre-hooks then post-hooks, each stage-group in
 *              (priority, seq) order — NOT globally re-sorted by
 *              the transport; the transport preserves the core
 *              ordering verbatim)
 */
final case class ListHooksResponse(
    count: Int,
    hooks: Seq[HookEntry]
)
