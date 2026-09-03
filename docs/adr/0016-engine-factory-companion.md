# ADR-0016: `io.sm8.core.EngineFactory` companion + zero-I/O core boundary

## Status

Proposed (C7 wayfinder round, ticket #280). Target: PR-272.

## Context

The C6 audit (map #270) and the AGENTS.md "Common gotchas" entry both flag
that `sm8-platform/src/main/scala/io/sm8/platform/query/QueryService.scala:143`
constructs `EngineImpl` directly inside the platform layer — a layer-leak
that contradicts RFC §3 ("adapters / core / plugins / hooks" decoupling)
and the AGENTS.md "Common gotchas" entry (which lists two outward seams:
`EngineFactory.create` for construction, `PluginDiscovery.discoverFromConfig`
for discovery). The single
call site is the only `new EngineImpl` outside `sm8-core/src/main` today
(verified by `grep -rn 'new EngineImpl' sm8-{server,platform,cli,mcp}/src/main`).

Two prior PRs started the seam-construction arc:
- PR-191 introduced `PluginDiscovery.discoverFromConfig()` so deployment
  modules could trigger plugin discovery without naming `EngineImpl`.
- PR-192 dropped the `cache-plugin` compile-scope dep from `sm8-server/pom.xml`
  to remove the adapter's transitive plugin-impl reach-in.

This ADR is the third step: **add `EngineFactory.create(plugins)` so the
adapter layer can construct a fully-wired `Engine` without naming the
concrete `EngineImpl` class**. `QueryService.scala:143` becomes the
canonical beneficiary. After this lands, `new EngineImpl` outside core
is a layer violation and should be caught by the linter (separate ticket).

## Decision

Add a public factory `object EngineFactory` in `io.sm8.core` alongside
the existing `PluginDiscovery` object.

### Public API (final)

```scala
package io.sm8.core

import io.sm8.sdk.Engine
import io.sm8.sdk.Plugin

/**
 * Factory for constructing fully-wired `Engine` instances.
 *
 * Sole outward seam from the adapter layer for engine construction.
 * Adapters MUST NOT construct `EngineImpl` directly.
 */
object EngineFactory {

  /**
   * Construct an Engine pre-wired with the given plugins.
   *
   * Thread-safe: each call constructs a fresh `EngineImpl` and
   * there is no shared state in the factory itself. Concurrent
   * callers each get their own engine; the `EngineImpl.use`
   * thread-safety (the `seenPlugins` `ConcurrentHashMap.newKeySet`
   * fix from main) applies inside each engine.
   *
   * @param plugins plugins to register on the engine via
   *                `engine.use(plugin)`. Empty Seq is allowed
   *                (matches the unit-test path).
   * @return the wired Engine. The return type is the SDK `Engine`
   *         trait so callers don't bind to the concrete
   *         `EngineImpl` class.
   */
  def create(plugins: Seq[Plugin]): Engine = {
    val engine = new EngineImpl
    plugins.foreach(engine.use)
    engine
  }
}
```

### Rationale

1. **`object` over `class`** — matches the existing `PluginDiscovery`
   style (also an `object`). No testability benefit to a `class`; the
   factory holds no state, has no DI surface, and the engine itself
   is the testable unit.
2. **Return type is `Engine` (SDK trait), not `EngineImpl`** — so the
   adapter layer literally cannot name the concrete class. Type system
   enforces the layer rule.
3. **Takes `Seq[Plugin]` not `List[Plugin]`** — `Seq` is the Scala
   standard collection; callers already pass `Seq` everywhere
   (`QueryService.scala` receives `plugins: Seq[Plugin]` from
   `sm8-server Main.wire`).
4. **No error return** — `EngineImpl.use` already swallows NonFatal
   per the existing `karpathy-app-design §4.2` contract (bad plugins
   warn, never crash). No new error path needed.
5. **No caching / no singleton** — each call constructs a fresh
   `EngineImpl`. Matches `PluginDiscovery.discoverFromConfig()` (also
   per-call, no leak risk across hot-reload).

### Caller-side change (single line)

`QueryService.scala:143`:
```scala
// before
val engine: EngineImpl = new EngineImpl
plugins.foreach(engine.use)

// after
val engine: Engine = EngineFactory.create(plugins)
```

The variable type changes from `EngineImpl` to `Engine` — that's the
load-bearing part of the layer rule. The local `engine.hooks` access
(line 145) still works because `Engine` exposes `hooks: HookManager`.

### Relationship to `PluginDiscovery`

`PluginDiscovery.discoverFromConfig()` stays unchanged. It returns
`List[Plugin]` (a discovery result); `EngineFactory.create(plugins)`
takes `Seq[Plugin]` (a construction seam). The two compose:

```scala
// in sm8-server Main.wire
val plugins = io.sm8.core.PluginDiscovery.discoverFromConfig()
// ... pass plugins down to QueryService via DI ...
val engine = io.sm8.core.EngineFactory.create(plugins)
```

No rename, no merge. They serve different layers: discovery is
"what plugins exist"; factory is "make an Engine from a plugin set".

### Out of scope (deferred)

- The `discover(allowed: Set[String])` non-I/O overload (T4) — that
  refactors `EngineImpl.discoverFromConfig` to take pre-parsed input.
  `EngineFactory.create` doesn't care about the discovery path; it
  just receives the resulting `Seq[Plugin]`.
- The `ModelLoader.parse(InputStream)` signature (T3) — different file,
  different concern.
- Linter rule to forbid `new EngineImpl` outside sm8-core — separate
  ticket once this lands.

## Consequences

Positive:
- Closes the C6 T2.CRIT finding (RULE#1 layer leak at QueryService.scala:143).
- Type system enforces the rule: any adapter that names `EngineImpl`
  fails to compile.
- Symmetric with `PluginDiscovery` (already an outward seam).

Neutral:
- `EngineImpl` stays public (not private[mcp]) because the conformance
  test base extends it. Tightening would require the conformance
  rewrite too — separate ticket.
- `new EngineImpl` is still legal in `sm8-core/src/main` (where
  `EngineFactory.create` itself delegates). The linter rule can
  eventually ban it outside `sm8-core/src/main`, but that needs
  its own design round.

Negative:
- None observed. The change is a 1-line caller refactor + 1 new
  ~15-line factory object.

## Validation

- `mvn test`: all 222 tests pass.
- `bash scripts/smoke-mcp-stdio.sh`: PASS.
- `bash scripts/smoke-e2e.sh`: PASS (live ingress).
- `grep -rn 'new EngineImpl' sm8-{server,platform,cli,mcp}/src/main`:
  returns 0 hits (was 1: QueryService.scala:143).
- `grep -rn 'io.sm8.core.EngineFactory' sm8-server/src/main`:
  returns ≥1 hit (the new caller path).

## References

- C6 audit: wayfinder map #270, ticket #272 (T2).
- AGENTS.md "Common gotchas": `EngineImpl` is plugin-impl-class; sm8-platform
  should not reference plugin implementations directly.
- RFC §3 Core Boundary.
- PR-191 (PluginDiscovery factory rename), PR-192 (cache-plugin dep removal).
- ADR-008-Q (engine discovery + realization flow).
