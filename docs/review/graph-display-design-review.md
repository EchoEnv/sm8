# Graph-Display Design Review (PR-150 + 151 + 152 at HEAD 16cfdaa)

> Verdict: **REVERT the original `sm8-platform` change (GraphService + GraphRequest + GraphResult + HttpTransport graphFn) and re-apply via Option 1 (plugin-owned schema + generic platform meta-reader).** Per `architecture-spec §3` + `plugins.md` Rules 2/3 + `hooks.md §6`, the **plugin owns the schema**, the **transport library owns only the wire-meta DTO**, and there is **no `graphFn` parameter on `HttpTransport`**. PR-151 and PR-152 are structurally fine if Option 1 is in place — they consume the same generic meta-inspector endpoint.

## §1 — Architecture-spec compliance

The user's pushback was right. Re-reading the RFCs confirms it:

- **`architecture-spec §3 Core Boundary`**: Plugin "knows about business rules (caching, auth, logging, retry) — yes, via hooks it registers". The graph-display IS an instance of "publishing domain state through `context.meta`" — that is a property of the plugin's purpose (semantic-graph validation), not of the transport. The plugin owns the schema; the transport knows wire shapes only.
- **`plugins.md` Rule 2**: "A plugin should have one clear purpose." The graph-display is part of the **purpose** of the semantic-graph plugin.
- **`hooks.md §6` "Types of Hooks, by What They Do"**: five types — Validator / Short-circuit / Enricher / Mutator / Observer. **"Publish graph to context.meta" is an Observer**.
- **`architecture-spec §10` "Extension Points"`**: "**Add cross-cutting behavior (logging, caching, auth check):** write hook functions, register them on the relevant `pre:`/`post:` stage inside a `Plugin.setup()`. No core changes."

**Conclusion §1**: Option 1 conforms to the spec; the original code does not.

## §2 — Current-shape analysis (REJECTED)

Concrete violations, by file:line:

- `sm8-platform/src/main/scala/io/sm8/platform/query/GraphResult.scala:47-52` — `GraphResult(vertices, edges, hasCycle, cycleError, danglingRightNodes)`. These encode the plugin's domain inside the transport library. Violates `plugins.md` Rule 3.
- `sm8-platform/src/main/scala/io/sm8/platform/query/GraphService.scala:54-95` — `GraphService.definition(model, graphFn: Model => GraphResult)`. The `graphFn` parameter type LEAKS the plugin's domain into the transport's signature.
- `sm8-platform/src/main/scala/io/sm8/platform/query/HttpTransport.scala:83-93` — `HttpTransport(model, registry, cache, graphFn: Option[Model => GraphResult] = None)`. The 4th constructor arg only exists to glue a specific plugin into a generic transport.

## §3 — Alternative shape (Option 1: Plugin snapshot + generic platform meta-reader)

1. **Plugin adds a `PostResolve` Observer hook** that calls `SemanticGraphBuilder.build(model)` and writes a typed `GraphSnapshot` into `context.meta` at the namespaced key.
2. **Transport adds a generic `MetaInspectorService`** in `sm8-platform`. The handler invokes the engine once with a synthetic `EngineHookRequest`, lets the pipeline run, and reads `result.meta.get(key)` on completion. Wire DTO is generic — `MetaRequest(key) + MetaResponse(key, present, value)`.
3. **Deployment module loads the plugin via `META-INF/services`** (already done).

**Pros**: transport lib knows ZERO about graph; per-request cost is sub-ms; the same `MetaInspectorService` endpoint serves any future plugin's `context.meta` key.

## §4 — Type locality

Per `karpathy-app-design-mindset`: the transport library's job is "shape JSON in/out". It does NOT know about cycles, dangling edges, vertices/edges cardinality.

- **Schema lives in the plugin**: `GraphSnapshot` (and `GraphNode`) are `io.sm8.plugins.semanticgraph.*` types.
- **Wire DTO for the meta-inspector is generic**: `MetaRequest(key) + MetaResponse(key, present, value)`. The `value` is opaque at the transport layer.

## §5 — Final recommendation

1. **`GraphSnapshot` (and `GraphNode`) live in `plugins/semantic-graph-plugin`**, owned by the plugin.
2. **`MetaInspectorService` (a generic `getMeta(key) -> MetaResponse`) lives in `sm8-platform`**.
3. **The Observer hook (`GraphPostResolveObserver`, `PostResolve`, priority 120) lives in `plugins/semantic-graph-plugin`**.
4. **No `graphFn` parameter exists anywhere in `sm8-platform`.**
5. **`sm8-server` does not name the semantic-graph plugin.** Plugin loading is via `EngineImpl.discoverFromConfig()`.