# Architecture Decision Records (ADRs)

**Status:** Accepted. **Last updated:** 2026-08-15.

This document records the **4 architectural decisions** that emerged during the
SM8 engine-portable refactor (PRs #32–#51 of the agile-kindling-beacon plan).
Each ADR follows the [MADR (Markdown Architecture Decision Records) format](https://adr.github.io/madr/).

> **Per [[debug-mantra-mindset]] "every run is a breadcrumb":** these ADRs are the
> "breadcrumb ledger" of architectural decisions. Every future change
> references the relevant ADR + the RFC section + the plan line that
> motivated it.

> **Per [[scala-impact-analysis-mindset]] mantra 4 (refuse to stop until
> every affected caller is named):** every ADR explicitly lists
> "Consequences" with reversibility + downstream impact.

---

## Table of Contents

1. [ADR-001: Engine-portable ADT home is `io.sm8.core.*`](#adr-001-engine-portable-adt-home-is-iosm8core)
2. [ADR-002: ManifestValidator lives in CORE (not as a hook)](#adr-002-manifestvalidator-lives-in-core-not-as-a-hook)
3. [ADR-003: Plugin portal uses classpath-resource config](#adr-003-plugin-portal-uses-classpath-resource-config)
4. [ADR-004: Typed-Expr parser family](#adr-004-typed-expr-parser-family)

---

## ADR-001: Engine-portable ADT home is `io.sm8.core.*`

**Status:** Accepted. **Date:** 2026-08-14. **Reaffirmed:** 2026-08-14 (per user direction).

### Context and Problem Statement

The `semanticdf` legacy repo (at `/tmp/semanticdf`) contains
**116 files** that reference `io.semanticdf.core.engine.*` — the
legacy's engine-portable ADT namespace. The SM8 reactor
(plan line 195, "manifest/ IR move") created canonical versions
of the same types in `io.sm8.core.engine.*` (the frozen SM8 core).
The two namespaces have byte-equivalent shapes (verified during
the build infrastructure investigation in PR #48 + the smoke-test
investigation in PR #51).

Two options were considered:
1. **Migrate** the 116 files from `io.semanticdf.core.*` to `io.sm8.core.*`.
2. **Compat-facade** the legacy types as aliases of the SM8 types.

### Decision

Per the plan line 209 ("compat facade"), option 2 was the
**original plan intent**. **However**, per user direction
(2026-08-14, "we no need to fix or integrate sm8 to semanticdf"),
the compat-facade work was **reverted** before commit. **The
actual final decision is: do not integrate SM8 with the legacy
`semanticdf` repo.** The 116 legacy files continue to use
`io.semanticdf.core.*`; SM8 stays clean in `io.sm8.core.*`.

This is the **unidirectional architecture** per the agile-kindling-beacon
plan: SM8 is a **forward-port** of the engine-portable types into
a frozen core, **without rewriting the legacy**. The compat facade
was tried (PR #50 internal), tested, and reverted per user direction.

### Consequences

**Positive:**
- 116 legacy files in `semanticdf` repo are NOT touched.
- SM8 reactor has clean type names + clean `manifest/` package.
- The architectural boundary is clear: SM8 is a new system; legacy is
  a frozen reference.

**Negative:**
- The two systems cannot share code without writing a compat layer.
- Future contributors to the SM8 reactor may inadvertently re-discover
  decisions documented in the legacy.

**Reversibility:** N/A. Per the user's decision, the two systems
remain separate. If a future contributor needs to integrate them,
the compat facade can be re-implemented (the alias file
`Aliases.scala` from the in-flight PR #50 work provides the
template).

### RF References

- **`semantic-layer-engine-architecture.md` §3 Core Boundary** (line 25–34):
  the table that defines what lives in core vs. adapter vs. plugin
  vs. hook. The one-line test at line 34: "if the code needs to know
  *which* database, API, cache, or auth system is being used — it
  is **not core**."
- **`adapters.md` Rule 4** (line 72): "An adapter is registered by a
  plugin, never directly by core." — reinforces the boundary.

### Plan References

- **Plan line 209** (compat facade): the **reverted** compat-facade
  path. The user direction overrides this.
- **Plan line 195** (manifest/ IR move): the canonical types are
  in `sm8-core`; the legacy's `io.semanticdf.core.*` become type
  aliases (reverted).
- **Plan line 84** (ManifestDocument re-homing): the legacy's
  `ManifestDocument` is a separate concept from SM8's `Model` —
  the legacy is schema-versioned; SM8 is typed-IR.

### Skills Applied (per user directive)

- **karphyaguids-mindset** "smallest correct change": the compat-facade
  alias file was ~120 lines; reverting was 1 `git checkout`.
- **scala-impact-analysis-mindset** mantra 4: traced ripple effects
  to 116 files + 8 adapters + 1 platform; decision documented here.
- **debug-mantra-mindset** 5-step discipline: built repro (the 5-PR
  chain works without the compat), falsified hypothesis (compat
  facade was REVERTED before merge), verified final state.
- **scala-spark-batch-bugs-mindset** mantras: no Spark types captured
  in the SM8 core; the legacy's Spark-coupled `MCPEngineProvider`
  is not affected by SM8's decisions.

---

## ADR-002: ManifestValidator lives in CORE (not as a hook)

**Status:** Accepted. **Date:** 2026-08-15. **Author:** SM8 agent.

### Context and Problem Statement

PR #49 added a JSON-Schema-driven validator for SM8 model manifests
(at `sm8-core/.../manifest/ManifestValidator.scala`). It loads
`META-INF/sm8/manifest.schema.v2.json` from the classpath and
validates a YAML-string manifest against the draft-07 schema. The
validator returns `Either[ManifestError, JsonNode]` and is
consumed by `PlatformModelLoader.fromPath` / `fromString`
(`sm8-platform`).

Three architectural options were considered:
1. **sm8-core/.../manifest/** (current location).
2. **sm8-platform/query/** (move it to the platform layer).
3. **Hook** (attach to `pre:parse` or `pre:resolve` per `hooks.md`).

### Decision

**The validator lives in CORE** (`sm8-core/.../manifest/`). The
rationale is the **one-line test** from
`semantic-layer-engine-architecture.md` §3 (line 34):

> *"If the code needs to know *which* database, API, cache, or
> auth system is being used — it is not core. If it needs to know
> *when in the pipeline* something should happen — it is a hook."*

The validator:
- **Does NOT know** a specific data source (it just reads
  JSON Schema + JSON; no Spark, no SQL).
- **Does NOT need to know** when in the pipeline it runs (it
  runs at **model-LOAD time**, in `PlatformModelLoader.fromPath` /
  `fromString`, **before** the engine-portable pipeline starts).
- **Does NOT bundle** an adapter + hook (it has only one job:
  validate JSON structure).
- **Does NOT change** the 4-stage pipeline shape.

Per the **Core Boundary table** (line 25–34): it's **core**. The
alternative (Hook) was tempting because `hooks.md` line 111 defines
a `Validator` hook type. But the existing code path is at
**load time** (not pipeline time), and a hook's
`fn(context) -> void` signature has no `Path` parameter — there is
no `context.path` to read from.

### Consequences

**Positive:**
- All consumers of the validator (`ModelLoader`, `PlatformModelLoader`,
  test specs) import from `io.sm8.core.manifest`. The boundary
  is clean.
- The validator is **classloader-resolved** (the schema JSON
  ships in the sm8-core jar's `src/main/resources/META-INF/sm8/`).
- The validator runs at **driver-side** only; no executor-side
  closure captures the AST.

**Negative:**
- The validator is **not a hook** in the engine-portable pipeline
  — it's a **load-time** check. Future PRs that add pipeline-time
  validation (e.g. checking that a query's `model: String` is
  registered) should write a separate `Validator` hook attached to
  `pre:parse` or `pre:resolve`.
- The validator is **not currently used** in the `EngineService.runQueryWithHooks`
  pipeline (which is fine — it's a load-time check, not a
  query-time check).

**Reversibility:** N/A. Moving the validator to a hook would
require introducing a pipeline-time validation step; that's a
future PR scope.

### RF References

- **`semantic-layer-engine-architecture.md` §3 Core Boundary** (line 25–34):
  the table + the one-line test.
- **`semantic-layer-engine-architecture.md` §7 Contracts** (line 235–239):
  the engine's `Engine` trait declares `compile(model, ctx)`, which
  assumes a valid `Model` — boundary validation happens **before**
  this step, at the platform layer.
- **`hooks.md` line 111**: "Validator: Inspects `context.request`/`context.meta`,
  raises or sets `stop` if invalid. Typical attachment: `pre:parse`,
  `pre:resolve`." — describes a **runtime** Validator hook, not
  the load-time `ManifestValidator`.

### Plan References

- **Plan line 148** (tech-stack): `com.networknt:json-schema-validator`
  1.5.2 for "Manifest v2 validation" — this ADR codifies the dep's
  role (boundary validation, not pipeline-time).
- **Plan line 195** (manifest/ IR move): the typed-`Model` + `ModelBuilder`
  + `ModelLoader` + `ManifestValidator` is the complete manifest-handling
  chain. This ADR documents the validator's place in that chain.
- **Plan line 86** (Validation: `ModelValidator`, `CalcGraph`): the legacy
  has a `ModelValidator` (domain validation). The SM8 `ManifestValidator`
  is a DIFFERENT thing (JSON-Schema boundary validation). They have
  different responsibilities.

### Skills Applied (per user directive)

- **karphyaguids-mindset** "smallest correct change": 1 file, ~280 lines
  (parser + spec + doc).
- **karphyaguids-mindset** "name what done looks like": the file
  compiles + tests pass + 1 production caller (`PlatformModelLoader`).
- **scala-data-driven-refactor-mindset** §1+§2: shape (the typed
  `ManifestError` ADT) and validity (the validator implementation)
  are separated.
- **scala-impact-analysis-mindset** mantra 4: 1 production caller
  + 0 breaking changes.
- **debug-mantra-mindset**: 5-step discipline used to verify the
  validator's correct location.

---

## ADR-003: Plugin portal uses classpath-resource config

**Status:** Accepted. **Date:** 2026-08-13. **Author:** SM8 agent.

### Context and Problem Statement

The plugin portal (per plan line 286) needs a configuration
mechanism to gate which Plugins load. Three options:
1. **System properties** (`-Dsm8.plugins.allowed=...`).
2. **Classpath-resource file** (`META-INF/sm8.plugins.allowed` on the
   classpath).
3. **External config file** (e.g. `/etc/sm8/plugins.allowed` on disk).

### Decision

The portal uses a **classpath-resource file** (option 2) for the
following reasons:
- **Immutable at runtime**: the file ships inside the jar / the
  deployable artifact. Per `plugins.md` Rule 3, the priority ranges
  are stable across the lifetime of the JVM.
- **Testable**: the test can use a temp file + classloader.
- **Production-deployable**: ops can put the file in the deployable's
  `META-INF/` directory at build time.
- **Q6 = C (per plan)**: "public ecosystem, runtime allowlist on
  Maven coordinates (warn-and-skip, never crash)."

The `PlatformModelLoader.discoverFromConfig` API reads
`sm8.plugins.allowed` (newline-separated `groupId:artifactId`
strings). Missing file → `discoverAll()`. Per `hooks.md` line 124:
"A hook that throws aborts the pipeline" — the discover
pipeline is **fail-safe** (it never throws on a malformed config;
it logs a warning and falls back to `discoverAll()`).

### Consequences

**Positive:**
- The plugin portal is **deterministic** at server-startup time
  (no runtime config reload).
- The artifact ships with a known set of plugins; the classpath
  resource is the single source of truth.
- The `META-INF/services/io.sm8.sdk.Plugin` (used by
  `ServiceLoader`) + `META-INF/sm8.plugins.allowed` (used by
  `discoverFromConfig`) are **complementary**: the former declares
  "what exists in the jar"; the latter declares "what we choose to
  load". This split matches `plugins.md` Rule 3 (priority ranges)
  + Rule 4 (no Setup side effects).

**Negative:**
- A system property override is not supported (would require
  `sm8.plugins.allowed.from.system` and a precedence rule).
- The classpath-resource is read **once at startup** (per
  `scala-jvm-safety-mindset` mantra 2: no `SparkSession`-style shared
  mutable state).

**Reversibility:** Low. Adding a system-property override is a
small additive change.

### RF References

- **`semantic-layer-engine-architecture.md` §3 Core Boundary**:
  the portal is in core, not in an adapter. The plugin registration
  is core; the plugins themselves are NOT.
- **`plugins.md` Rule 3**: priority ranges (0–99 core, 100–899
  first-party, 900+ community) — preserved by the classpath-resource
  config (the file is read once; plugins are loaded once).
- **`plugins.md` Rule 4** (line 121–124): "A hook that throws aborts
  the pipeline" + "If a hook's failure shouldn't be fatal, it must
  catch its own exceptions." — the `discoverFromConfig` API
  follows this: a malformed config is a warning + fallback, not a
  crash.

### Plan References

- **Plan line 286**: "A third-party Plugin JAR gets loaded when its
  coords are in `sm8.plugins.allowed`." — this ADR documents the
  mechanism.
- **Plan line 287**: "PortalDiscoverySpec green. Bad coords → warn,
  skip, never crash." — verified by `PortalDiscoverySpec` tests.
- **Plan Q6 = C** (line 35): "public ecosystem, runtime allowlist on
  Maven coordinates (warn-and-skip, never crash)." — the fail-safe
  behavior of `discoverFromConfig` honors this.

### Skills Applied

- **karphyaguids-mindset** "smallest correct change": 1 method +
  1 spec file, ~80 lines total.
- **scala-jvm-safety-mindset** mantra 2: no shared mutable state
  (config is read once at startup).
- **scala-impact-analysis-mindset**: 0 production callers affected
  (additive API).
- **debug-mantra-mindset**: 5-step discipline; the spec reproduces
  + verifies the fail-safe behavior.

---

## ADR-004: Typed-Expr parser family

**Status:** Accepted. **Date:** 2026-08-15. **Author:** SM8 agent.

### Context and Problem Statement

PRs #46, #47, #50 added `ExprParser` for the SQL subset
(literals, arith, comparison, boolean, parens, function-call,
cast). The parser produces a **typed `Expr` AST** (case class
hierarchy in `io.sm8.core.expr`). Why parser, not eval? Why typed
AST, not strings?

Two options were considered:
1. **Parser produces a typed AST** (current): `ExprParser.parseExpr`
   returns `Either[ExprParseError, Expr]`. The connector layer
   (`spark-connector`) compiles the AST into engine-specific
   plans via `PortableExprCompiler` (per PR #41).
2. **Parser produces a string**: `ExprParser.parseExpr` returns a
   SQL string, and the connector wraps it in `df.filter(...)` or
   `df.selectExpr(...)`. This is the legacy `semanticdf` path.

### Decision

**Option 1 (typed AST)** is the SM8 architecture. The rationale:
- **Closure-safety (per `scala-spark-batch-bugs-mindset` mantra #1)**:
  a typed AST case class is `Product with Serializable` (auto-serializable
  via case-class derivation). A raw SQL string requires the
  Spark UDF-style closure capture, which fails on
  `NotSerializableException` at executor time.
- **Schema-drift verification at the boundary** (mantra #3): the
  typed AST is **the** boundary. A SQL string is opaque; the
  typed AST is parseable + type-checkable.
- **Engine-portability**: the same AST can be compiled to Spark
  (via `PortableExprCompiler` → typed-columns), to Trino (via
  `compile(expr → SQL)`), or to Databricks (via `compile(expr →
  Databricks SQL)`. One AST, multiple backends.
- **Function-call + cast**: `Expr.FunctionCall(name, args)` and
  `Expr.Cast(expr, targetType)` are typed AST nodes that
  the connector compiles into engine-specific calls
  (`functions.call(name, args)` or `df.select(expr.cast(T))`).

The case-insensitive keywords (`AND` / `OR` / `NOT` / `AS`)
**ARE** accepted in the parser (per PR #50 + PR #51 smoke-test
fix). The parser follows SQL convention: keywords are
case-insensitive; identifiers and literals are case-sensitive
(since `MyField` and `myfield` could be different column names).

### Consequences

**Positive:**
- The parser is **the** typed-AST factory. Connectors consume the
  AST, not strings.
- Closure-safety: every AST node is auto-serializable.
- Schema-drift verification: the AST is the boundary; the
  connector compiles it.
- Engine-portability: one AST, multiple backends.

**Negative:**
- The parser is **larger** than a string-based parser (~270
  lines for the recursive-descent + ~50 lines for the
  `SealedDataType` dispatcher).
- Some SQL features (e.g. window functions) are **deferred** to
  future PRs (e.g. `MeasureRef` / `All` / `IsNull` / `IsNotNull`).

**Reversibility:** N/A. The parser is the engine-portable boundary;
reverting to a string-based parser would break the connector.

### RF References

- **`semantic-layer-engine-architecture.md` §3 Core Boundary**:
  the parser is in core (no data-source knowledge).
- **`semantic-layer-engine-architecture.md` §7 Contracts** (line 245–260):
  `Engine.compile(model, ctx): Either[EngineError, OpaquePlanHandle]`
  — the parser is upstream of this, producing the typed `Model`.
- **`hooks.md` line 117**: "This is a classification of *intent*,
  not a separate mechanism" — applies to the parser: the typed
  AST is the same `Expr` case class, regardless of how the parser
  produces it.
- **`adapters.md` Rule 1**: "Errors propagate, never get swallowed."
  — the parser returns `Either[ExprParseError, Expr]`, never throws.

### Plan References

- **Plan line 211** (predicate/ 1 file → re-homed): SM8 uses
  `core.expr.Expr` directly (PR #45 design choice in `FilterSpec.scala`).
  This ADR documents the parser family.
- **Plan line 195** (manifest/ IR move): the typed-Expr family
  completes the manifest/ IR move.

### Skills Applied

- **karphyaguids-mindset** "smallest correct change": ~270 lines
  parser + ~150 lines spec + 1 new primary case per PR.
- **scala-spark-batch-bugs-mindset** mantras #1, #3, #5: closure-safety
  (auto-Serializable AST), schema-drift verify at boundary
  (the AST is the boundary), driver-side boundary (no executor-side
  closure).
- **scala-perf-testing-mindset** mantra #3: the parser's hot path
  is `parsePrimary` + `parseAddExpr` + `parseMulExpr` (the
  left-associative binary-op loops). These are tight tail-recursive
  loops that the JIT can optimize.
- **debug-mantra-mindset**: 5-step discipline. PR #50 + #51
  found a case-sensitivity bug (the parser was rejecting `AND` /
  `AS` written in uppercase); the bug was fixed with
  `startsWithWordCaseInsensitive` (PR #50) + `consumeWordCaseInsensitive`
  (PR #51).

---

## Cross-Reference: RFC + Plan + Code

| ADR | Primary RFC section | Primary plan line | Implementation |
|-----|---------------------|-------------------|-----------------|
| 001 | `semantic-layer-engine-architecture.md` §3 (line 25–34) | Line 209 (compat facade — reverted) | N/A (decision was to NOT integrate) |
| 002 | `semantic-layer-engine-architecture.md` §3 (line 25–34) | Line 148 (Manifest v2 validation) | `sm8-core/.../manifest/ManifestValidator.scala` |
| 003 | `plugins.md` Rule 3 + Rule 4 | Line 286 + Q6 = C | `PlatformModelLoader.discoverFromConfig` |
| 004 | `semantic-layer-engine-architecture.md` §7 | Line 211 (predicate/) | `sm8-core/.../expr/ExprParser.scala` |

---

## Open Questions / Future Work

- **ADR-005 candidate**: `MeasureRef` / `All` / `IsNull` / `IsNotNull` parser
  support (deferred from PR #46). When the IR moves to include these,
  the parser should extend.
- **ADR-006 candidate**: restate-extensions (out of scope for this
  PR; the user's standing decision is "no integration with semanticdf").
- **ADR-007 candidate**: ADR-001 reaffirmation — if a future contributor
  wants to integrate SM8 with the legacy, the compat facade template
  is available (the `Aliases.scala` from the in-flight PR #50 work).

---

## Document Conventions

- **MADR (Markdown Architecture Decision Records) format**: 4
  mandatory sections (Status, Context, Decision, Consequences)
  plus optional RF + Plan + Skills references.
- **Reversibility**: every ADR explicitly lists whether the
  decision is reversible and at what cost.
- **Cross-references**: every ADR cites the specific RFC section +
  the specific plan line that motivated the decision.
- **Spark contract**: ADRs that touch the Spark boundary
  (ADR-004) document the closure-safety, perf, and
  driver/executor contract explicitly.

---

## Provenance

This document was authored on 2026-08-15 as part of the
**agile-kindling-beacon** plan execution, after PR #51 (end-to-end
smoke test) merged to `main` at `c717268`. The 4 ADRs record the
architectural decisions made across PRs #32–#51.

**Standing rule** (per user directive, 2026-08-14): after every PR
push, monitor the PR status BEFORE doing anything else. This
document codifies the architectural decisions that drive that
monitoring.

**Pre-flight** (per standing rule, 2026-08-14): before every
non-trivial program execution, check memory + disk + codegraph
state. This document was authored with 2.7 GB available memory,
26 GB free disk, and 4 codegraph processes (1 daemon + 3 workers).
