# sm8-core

The **frozen Core** of the SM8 engine. Spark-free. Implements the SDK
surface and the Core Boundary from the v1 architecture spec in
[`docs/rfcs/2026-08-12_v1_architecture-spec/`](../docs/rfcs/2026-08-12_v1_architecture-spec/).

## Status

The SDK is the frozen public surface. The engine implementation
(`io.sm8.core.EngineImpl`), the 4-stage pipeline, ServiceLoader
discovery (`PluginDiscovery`), and the allowlist filter all ship in
this module alongside the SDK; the SDK trait itself is the stability
promise. See "What's in this module" for what lives in Core versus
what is deliberately left to plugins/connectors.

## What's in this module

### SDK (`io.sm8.sdk`)

The single import path for Plugin authors. Every type a Plugin
author touches lives here:

| Type              | Kind                  |
|-------------------|-----------------------|
| `Plugin`          | trait                 |
| `PreHook`         | trait                 |
| `PostHook`        | trait                 |
| `Transformer`     | trait                 |
| `Context`         | case class            |
| `Engine`          | trait (orchestrator facade) |

These are the SDK stability promise. Any change to the public
surface of any of these types is a breaking change. Version bumps,
MiMa exclusions, and CHANGELOG entries required.

### Marker types in the SDK

These are marker traits; concrete request/result shapes are carried
by the `EngineHookRequest` / `EngineHookResult` / `PipelineError` /
`PipelineSkipped` families in `io.sm8.core` and by the per-connector
realization types. They stay abstract in the SDK so the public
surface does not grow with every engine.

| Type                | Notes                              |
|---------------------|------------------------------------|
| `Request`           | Marker trait — input to the engine |
| `Result`            | Marker trait — output of the engine |

### Supporting types

| Type                | Defined in                  | Notes                              |
|---------------------|-----------------------------|------------------------------------|
| `PipelineStage`     | `Context.scala`             | Sealed: 4 cases                    |
| `HookStage`         | `Hooks.scala`               | Sealed: 8 named attachment points  |

## What's in this module (and what's deliberately not)

These are deliberate omissions per the karpathy "smallest correct
core" rule. The pipeline runner (`Pipeline`), `EngineImpl` (the
`Engine` trait's implementation), `HookManagerImpl` (priority-ordered
dispatch), `TransformerRegistry` (exactly-one-active swap), and
`ServiceLoader` discovery (`PluginDiscovery`) all ship **inside**
`io.sm8.core` in this module — they are not part of the SDK's
public trait surface, but they are on the Core classpath. The Maven
_coordinates_ allowlist filter and MiMa are release-readiness gates
that land with the first v1.0.0 RC.

What Core never gains (per RFC §3 layer discipline):

- A dependency on any specific adapter or plugin implementation
  (`maven-enforcer-plugin` rejects `org.apache.spark:*` here).
- Connector/engine grammar — that lives in `connectors/*`.

## Build

```bash
cd /home/emilio/app/projects/sm8
mvn -pl sm8-core -am compile
mvn -pl sm8-core test
```

Expected:

- `mvn compile` succeeds; zero warnings about unused params.
- `mvn test` runs the full Core suite — the unified contract bases
  (`PluginContractSpec`, `HookContractSpec`), the engine smoke /
  hook-dispatch / transformer-swap / discovery specs, and
  `CoreClasspathSpec` (asserts no Spark on the classpath). All green.

## Zero-Spark invariant

Verified at **two layers** (per [[scala-jar-packaging-mindset]]):

1. **Compile-time:** `maven-enforcer-plugin` rejects
   `org.apache.spark:*` in the dependency tree. Wired in `pom.xml`.
2. **Test-time:** `CoreClasspathSpec` walks the runtime classpath and
   fails if any `org.apache.spark` class is loaded.

If either fails, the Core is broken — Spark classes belong in
`connectors/spark-connector/`, never here.

## Binary compatibility

`mima-maven-plugin` is **not** wired yet — it's a release-readiness gate
that lands when we cut the first v1.0.0 release candidate. The
module is currently 0.1.0-SNAPSHOT and the SDK surface is still
settling. Once MiMa is wired, baseline is `0.1.0` (first public
release), and any public-API change after that blocks `mvn verify`
until either (a) the change is justified via MiMa exclusion, or (b)
the baseline advances to a new release.

## What a Plugin author sees

```scala
import io.sm8.sdk._

class MyPlugin extends Plugin {
  def setup(engine: Engine): Unit = {
    engine.hooks.registerPostHook(/* audit/metrics hook here */)
  }
}
```

Data-source wiring is NOT a Plugin concern — engine adapters are
realized by the `EngineProvider` ServiceLoader seam in the connector
modules. No internal types leak. No
Spark, no Trino, no DuckDB — those are Plugin authors' problem, not
the Core's.

### Hook contracts: priority ranges + error policy

Every hook registered on the engine sorts by `(priority ASC,
registration order)` (RFC §8). The ranges below are reserved by the
hook's origin so independently-authored plugins don't collide:

| Range | Owner |
|---|---|
| 0–99 | Core / built-in behavior |
| 100–899 | First-party / official plugins |
| 900+ | Community / user plugins |

The 3-arg `registerPreHook`/`registerPostHook` overloads require
non-negative priority and tag the hook First-party by default. The
4-arg overload (with `HookOrigin`) enforces the full range by origin
through `HookOrigin.validate`; an out-of-range priority throws
`IllegalArgumentException` at registration.

Error handling (RFC §9): a hook that throws a `NonFatal`
exception surfaces as a typed `EngineError.HookFailed(name,
priority, stage, message)` that fails the current pipeline stage
and propagates to the caller; fatal `Error`s (e.g. OOM) propagate
unchanged. The pipeline fails loudly (typed error visible to caller)
without losing the hook author's intent. Hooks that must survive
non-fatal failures catch their own exceptions and return a normal
`Context`. Observer-style hooks should never throw; mutator
post-hooks that should skip the short-circuit path override
`runsOnStop` to `false`. A hook that sets `context.stop = true`
short-circuits the remaining stages but post-hooks (including
observer-style audits) still fire on the short-circuit path so
observability is preserved.

For a copy-and-modify starting point, see
[`plugins/example-plugin/README.md`](../plugins/example-plugin/README.md).

## What's next

- **Release gate**: wire `mima-maven-plugin` against a `0.1.0`
  baseline when cutting the first v1.0.0 release candidate.
- **Connector conformance**: a shared contract suite that every
  `connectors/*` engine must pass (mirrors the plugin-side
  `HookContractSpec` / `PluginContractSpec` unification).
- **Feature work** continues against the v1 architecture spec in
  `docs/rfcs/2026-08-12_v1_architecture-spec/`.