# sm8-core

The **frozen Core** of the SM8 engine. Spark-free. Implements the SDK
surface (7 types) and the Core Boundary from the karpathy-app-design
pattern as instantiated by the [SM8 plan][plan].

[plan]: /home/emilio/.claude/plans/agile-kindling-beacon.md

## Status

**Step 1 of the 11-step migration.** Ships the SDK only — no Pipeline
runner, no ServiceLoader discovery, no allowlist filter. Those land in
Steps 3–7.

## What's in this module

### SDK (`io.sm8.sdk`)

The single import path for Plugin authors. Every type a Plugin or
Connector author touches lives here:

| Type              | Kind                  | Frozen after |
|-------------------|-----------------------|--------------|
| `Plugin`          | trait                 | Step 1       |
| `Connector`       | trait                 | Step 1       |
| `PreHook`         | trait                 | Step 1       |
| `PostHook`        | trait                 | Step 1       |
| `Transformer`     | trait                 | Step 1       |
| `Context`         | case class            | Step 1       |
| `Engine`          | trait (registry only) | Step 1       |

These are the SDK stability promise (per [[Q2 = A]] and [[Q5 = A]]).
Any change to the public surface of any of these types is a breaking
change. Version bumps, MiMa exclusions, and CHANGELOG entries required.

### Marker types in the SDK

These are abstract in Step 1 and gain their concrete shape in later
steps. They exist now so the SDK compiles and the test skeletons have
something to reference:

| Type                | Concrete shape lands in | Notes                                |
|---------------------|-------------------------|--------------------------------------|
| `Request`           | Step 3                  | Marker trait — full shape is JSON    |
| `Result`            | Step 3                  | Marker trait — full shape is JSON    |
| `ConnectorConfig`   | Step 3                  | Full shape includes connection opts   |
| `SemanticQuery`     | Step 3                  | Lowered from the IR in Step 0        |
| `ResultRows`        | Step 3                  | Portable row shape                   |
| `ConnectorSchema`   | Step 3                  | Portable schema shape                |

### Supporting types

| Type                | Defined in                  | Notes                              |
|---------------------|-----------------------------|------------------------------------|
| `PipelineStage`     | `Context.scala`             | Sealed: 4 cases                    |
| `HookStage`         | `Hooks.scala`               | Sealed: 8 named attachment points  |

## What's NOT in this module

These are deliberate omissions per the karpathy "smallest correct
core" rule and the plan's B-style sequencing:

- **Pipeline runner** (parse → resolve → execute → format) — Step 3
- **`EngineImpl`** (the `Engine` trait's implementation) — Step 3
- **HookManager** (priority-ordered dispatch) — Step 4
- **ConnectorRegistry** (name → Connector lookup) — Step 3
- **TransformerRegistry** (exactly-one-active swap) — Step 6
- **`ServiceLoader` discovery** — Step 7
- **Maven-coords allowlist filter** — Step 7
- **`EngineError` ADT** (typed error type for `connect()` / `query()`) — Step 0 (moves from `semanticdf-core`)
- **Manifest validation** (Jackson + JSON Schema) — Step 0

## Build

```bash
cd /home/emilio/app/projects/sm8
mvn -pl sm8-core -am compile
mvn -pl sm8-core test
```

Expected:

- `mvn compile` succeeds; zero warnings about unused params.
- `mvn test` runs 4 specs:
  - `PluginContractSpec` (3 tests)
  - `ConnectorContractSpec` (3 tests)
  - `HookContractSpec` (5 tests)
  - `CoreClasspathSpec` (1 test, asserts no Spark on classpath)
- All 12 tests green.

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
that lands when we cut the first v1.0.0 release candidate (Step 9 in
the sequencing). Step 1 is 0.1.0-SNAPSHOT; the SDK surface is still
settling. Once MiMa is wired, baseline is `0.1.0` (first public
release), and any public-API change after that blocks `mvn verify`
until either (a) the change is justified via MiMa exclusion, or (b)
the baseline advances to a new release.

## What a Plugin author sees

```scala
import io.sm8.sdk._

class MyConnector extends Connector {
  def name = "my-source"
  def connect(config: ConnectorConfig): Unit = ???
  def query(request: SemanticQuery): ResultRows = ???
  def schema(): ConnectorSchema = ???
}

class MyPlugin extends Plugin {
  def setup(engine: Engine): Unit = {
    engine.use(/* register my connector + hooks here */)
  }
}
```

That's the entire public API surface. No internal types leak. No
Spark, no Trino, no DuckDB — those are Plugin authors' problem, not
the Core's.

## What's next

- **Step 2**: Promote the contract skeletons to abstract base classes
  (`ConnectorContractSpec` enforces the 4 RFC §12 assertions).
- **Step 3**: `Engine` implementation + `InMemoryConnector` built-in
  reference + Pipeline runner.
- **Step 7**: Portal (`ServiceLoader` + allowlist).
- **Step 8**: Repackage `adapters/semanticdf-{spark,trino,…}` as
  `connectors/{spark,trino,…}-connector/`.