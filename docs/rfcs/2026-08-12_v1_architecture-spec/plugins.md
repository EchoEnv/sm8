# Plugins

Companion doc to `semantic-layer-engine-architecture.md` (Sections 3, 7, 7a, 10). Read that first for the pipeline and core boundary.

## What a Plugin Is

A plugin is the unit of extension. It's a bundle that, on load, registers one or more adapters and/or one or more hooks with the engine. It is the only thing a contributor publishes and the only thing `engine.use(...)` consumes.

```
Plugin:
  setup(engine) -> void
```

`Engine` never imports a specific plugin by name — it only ever calls the generic `setup(engine)` contract, so any number of plugins can be added or removed without touching core.

## Types of Plugins, by Contents

| Type | Registers | When to use | Example |
|---|---|---|---|
| **Adapter-only** | one or more adapters, no hooks | adding a new data source with no special behavior | `PostgresPlugin`, `RestApiPlugin` |
| **Hook-only** | one or more hooks, no adapters | cross-cutting behavior that applies regardless of data source | `LoggingPlugin`, `CachePlugin` |
| **Composite** | both adapters and hooks | a data source that needs its own supporting logic (e.g. config validation specific to that source) | `PostgresPlugin` also registering a `pre:resolve` hook that checks Postgres-specific config fields |
| **Configuration-only** | neither — just sets values engine-wide (timeouts, feature flags) via `context.meta` defaults on `pre:parse` | tuning behavior without adding new capability | a hook that stamps a default `tenant_id` if none is provided |

In practice, "configuration-only" is a hook-only plugin with a narrow purpose — listed separately here because it's a very common first plugin for a junior contributor to write.

## Types of Plugins, by Origin (governs hook priority range)

| Type | Priority range | Ships in |
|---|---|---|
| **Core / built-in** | 0–99 | `/plugins` (reference implementations, e.g. in-memory adapter + basic logging) |
| **First-party / official** | 100–899 | separate repo or `/plugins/official` |
| **Community** | 900+ | external repos, installed by the consumer |

This maps directly onto the priority convention in `hooks.md` and the architecture doc — origin determines where in hook execution order a plugin's hooks land by default.

## Example

```
plugin CachePlugin implements Plugin:
  setup(engine):
    engine.hooks.register("pre:execute",  cacheReadHook,  priority = 50)
    engine.hooks.register("post:execute", cacheWriteHook, priority = 50)
```

```
plugin PostgresPlugin implements Plugin:
  setup(engine):
    engine.adapters.register(new PostgresAdapter())
    engine.hooks.register("pre:resolve", validatePostgresConfigHook, priority = 100)
```

## Rules

1. **A plugin's `setup()` must be idempotent-safe to call once at startup** — no runtime side effects beyond registration (don't open connections here; that's the adapter's `connect()` job). **Connection establishment belongs in the connector's `MCPEngineProvider` implementation** (its ctor or a dedicated `realize(url)` method), **never in the platform transport library and never in a deployment reflection layer** (added 2026-08-15, per ADR-006 Post-#65 Refinement).
2. **A plugin should have one clear purpose.** If a plugin is registering unrelated adapters and hooks for different features, split it into separate plugins.
3. **Plugins depend on core's contracts, never on each other directly.** If `PluginB` needs data `PluginA` produced, it reads it from `context.meta`, not by importing `PluginA`.
4. **Naming convention:** `<Thing>Plugin` (e.g. `CachePlugin`, `PostgresPlugin`) to distinguish at a glance from `<Thing>Adapter` and `<thing>Hook`.

## Where Plugins Live

`/plugins` for built-in reference plugins, external packages/repos for first-party and community plugins — see architecture doc Section 11 (Repo Structure).
