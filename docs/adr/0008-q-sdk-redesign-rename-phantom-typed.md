# ADR-008-Q: Post-ADR-008-P SDK Redesign (MCP prefix → Engine + phantom-typed SDK + typed URL + EngineLoader)

**Status:** Implemented — was Proposed (v1.0), promoted to Implemented on PR-105 (#105, 8b4c8ac) merge. The 3-PR atomic sequence per ADR-0008-Q §'Implementation summary' shipped: PR-14 (0115d88) rename MCPEngine* → Engine*, PR-15 (e2f4448) typed URL grammar + EngineLoader, PR-16 (8b4c8ac) phantom-typed Dimensions + Measures. Phantom-typed SDK redesign is live on main; SDK source-compatibility preserved per the ADR's §'Skill-mindset coverage' table. Local-only `.omp/WATCHDOG.yml` (gitignored) tunes the advisor roster for this project.. **Date:** 2026-08-19. **Author:** SM8 agent (consolidated from senior reviews on 2026-08-18 + audit follow-up on 2026-08-19 + 2 fresh senior reviews on 2026-08-19).

> **Revision history**
> - **v1 (2026-08-19)**: initial proposal; reviewed by 2 senior subagents (DE: approved with minor changes; Architect: needs revision).
> - **v2 (2026-08-19, this revision)**: addresses 21 review findings (C1–C21 below). Changes: §Decision rewritten to honor RFC `adapters.md` Rule 4 (per-connector validation); `realizeTyped` reframed as subtrait with default delegate; explicit wire-shape decision; canonical `cacheKey`; added Consequences / Alternatives Considered / References sections per ADR convention; EngineLoader placement clarified; per-witness perf math corrected; SPI key + MCPRegistry alias + Streaming forward-looking note added; EngineUrl case shape sketch included.

## Context and Problem Statement

ADR-008-P (Accepted 2026-08-18, implementation complete 2026-08-18) closed all 3 cross-validated P0s + 9 P1s + 6 P2s from the 2026-08-18 senior reviews and shipped the 9-PR atomic sequence (#91-#99) that brought the reactor to **697/697 tests passing** with `MCPEngineProvider.query` wired through `EngineHookDispatcher`. **v0.1.0 tag cut remains GATED** by the standing user directive "dont bump version yet" (2026-08-17).

A **post-ADR-008-P audit** on 2026-08-19 — triggered by the user's question "is the `MCP...` prefix related to MCP or is it a previous MCP server come frame?" — surfaced **3 cross-cutting design debts** that must be resolved **before the v0.1.0 tag**:

| ID | Debt | Source |
|----|------|--------|
| **Q-A** | The `MCP...` prefix in `MCPEngineProvider`, `MCPEngineRegistry`, `MCPQueryRequest`, and the forthcoming `MCPEngineUrl` / `MCPEngineLoader` is **misleading**: it originates from the legacy `semanticdf` "MCP server" framing (per Agile plan line 11), NOT the **Anthropic MCP protocol** (a different 2024 project). A user reading the codebase in 2026+ would reasonably assume the wrong meaning. | User question 2026-08-19; ADR-001 framing of "engine-portable ADT home" |
| **Q-B** | The SDK exposes a **verbose, untyped surface** (`Model.of(List(Dimension(...), Measure(...), CalculatedMeasure(...)))`). The pattern repeats in every example + plugin + test. The verbose surface is API surface tax, not perf cost (measured 1.18 us/call per the one-shot `ModelBuildBench`; 22 allocs/call; sm8-server startup ≈ 0.03 ms one-time; zero per-query overhead). | Production-readiness audit 2026-08-19; `karpathy-guidelines-mindset` §2 "simplicity first" |
| **Q-C** | Engine provider realize is **untyped** (`realize(url: String): Option[MCPEngineProvider]` — silent `None` on failure). Per RFC `adapters.md` Rule 4 ("Per-connector `realize()` validates its own URL grammar"), URL grammar validation is duplicated across 3 connectors + 3 test sites + `sm8-server/Main.scala` + `sm8-cli`. | ADR-006 (Post-#65 Refinement); `scala-error-handling-mindset` §1 "errors are data" |

These 3 debts share **a single root cause**: the SDK exposes types as **stringly-typed, runtime-validated** instead of **typed, compile-time-checked**. Per `karpathy-app-designmindset` §3.1 (Protocols before implementations), the fix is a **type-level redesign** that:

1. **Renames** the misleading `MCP...` prefix to `Engine...` (per ADR-001 framing + user's question)
2. **Adds phantom-typed witnesses** to the SDK for typeclass-safe dimension/measure references (per `karpathy-app-designmindset` §3.1 + upstream `semanticdf.SemanticDimension.of[T]("name")` pattern)
3. **Adds a typed URL grammar** (`EngineUrl`) with a smart constructor returning `Either[EngineError, EngineUrl]` (per `scala-error-handlingmindset` §1)
4. **Centralizes engine discovery + realization** in `EngineLoader.discoverAndRealize(...)` (per `karpathy-app-designmindset` §3.1; eliminates 3-line boilerplate duplicated across examples, tests, sm8-server, sm8-cli)

The 3 fixes are **layered** (PR-14 rename → PR-15 URL grammar + loader → PR-16 phantom-typed SDK); each is additive + backward-compatible for source; the v0.1.0 tag cut is the first release with MiMa enabled (per ADR-008-P §E2), so pre-1.0 API churn is permitted.

### Findings from the post-ADR-008-P senior reviews

1. **`/tmp/reviews/post-adr-008-p-data-engineer-review.md`** — 4 P0 + 7 P1 + 3 P2 (Spark runtime + data correctness + perf)
2. **`/tmp/reviews/post-adr-008-p-architect-review.md`** — 3 P0 + 5 P1 + 4 P2 (RFC §3 layer ownership + SDK surface + MiMa + vocabulary)

### Final-review findings on this ADR (2026-08-19)

1. **`/tmp/reviews/post-adr-008-q-data-engineer-review.md`** — Verdict: Approved with minor changes (medium confidence). 2 P0 + 5 P1 + 5 P2 findings, addressed in §Decision and §Consequences below.
2. **`/tmp/reviews/post-adr-008-q-architect-review.md`** — Verdict: Needs revision. 2 P0 + 5 P1 + 4 P2 findings, addressed in §Decision and §Consequences below.

The 21 findings are consolidated and addressed inline below (see `## Decision` for the per-finding resolution). Per the ADR convention (`docs/adr/README.md`), this ADR now includes `## Consequences`, `## Alternatives Considered`, and `## References` sections (added in response to Architect P1-1).

## Why this is a structural ADR (not a "next steps" doc)

Per ADR-008-O §"Cross-cutting principles" #1 (RFC §3 layer ownership preserved) and #2 (skills-first review per commit), each fix below is **bounded by its layer**:

| Fix | Layer | Layer rationale (per RFC §3) |
|-----|-------|------------------------------|
| Q-P0-A rename | core + platform + connectors + plugins + tests + docs | Cross-cutting but atomic; LSP `rename` is the right tool |
| Q-P0-B + Q-P0-E phantom-typed witnesses | core (the trait + factory); plugins (the witnesses) | Core owns the Protocol; plugins own the implementations (per `karpathy-app-designmindset` §3.1) |
| Q-P0-C typed URL | core (`EngineUrl` sealed trait + smart ctor signature); connectors (override `realizeTyped` for engine-specific grammar validation + typed errors) | **Connector owns the validation** (per RFC `adapters.md` Rule 4 — see C1 below) |
| Q-P0-D `EngineLoader` | sm8-server (the `ServiceLoader` + realize orchestration); sm8-platform (the typed-error mapper, optional) | Server owns deployment discovery (per ADR-006 §Post-#65 Refinement — see C11 below) |

**None of the 4 P0 fixes require an RFC change** — they all stay within RFC §3 layer ownership. The PR sequence is therefore bounded and reviewable per-PR (matching the ADR-008-P sequence).

## Decision

The 4 P0 fixes ship as **3 atomic PRs** (PR-14 + PR-15 + PR-16), each independent, each additive, each with full reactor test verification.

### PR-14: `MCPEngine* → Engine*` rename (cross-cutting, atomic)

**Scope**: 1 atomic commit that renames 5 types across the entire workspace using LSP `rename`:

| Type | From | To | Verified callsite count |
|------|------|----|--------------------------|
| Provider trait | `MCPEngineProvider` | `EngineProvider` | **18 callers** across 4 modules + 6 test files (per CodeGraph 2026-08-19) |
| Registry | `MCPEngineRegistry` | `EngineRegistry` | **12 callers** across 4 modules + 4 test files |
| Request shape | `MCPQueryRequest` | `QueryRequest` | **19 callers** across 3 modules + 6 test files |
| Forthcoming URL | `MCPEngineUrl` (in PR-15) | `EngineUrl` (lands in PR-15) | (deferred to PR-15) |
| Forthcoming loader | `MCPEngineLoader` (in PR-15) | `EngineLoader` (lands in PR-15) | (deferred to PR-15) |

**Additional rename scope (per DE P2-C)**:
- `MCPRegistry` legacy alias (if any — verified at PR-14 implementation time via `grep -rn 'MCPRegistry' sm8-core/`).
- SPI key `META-INF/services/io.sm8.core.engine.MCPEngineProvider` → `META-INF/services/io.sm8.core.engine.EngineProvider` (the SPI key MUST be renamed in lockstep per `karpathy-app-design-mindset` §3.1; otherwise ServiceLoader discovery breaks silently).
- All Spark connector descriptor files (`SparkEngineProviderDescriptor`, `InMemoryEngineProvider`, `TrinoEngineProvider` descriptors) — they reference the SPI key.

**Canonical `cacheKey` method (per DE P0-E)**: PR-14 adds a `cacheKey: String` method to the renamed `QueryRequest` case class that normalizes via `dimensions.sorted.mkString(",")` + `measures.sorted.mkString(",")` + `filters.hashCode` to eliminate the `Seq` subtype-drift risk. **Alternative**: explicitly document deferral with rationale + open a follow-up issue. **The PR-14 implementation MUST choose one path or the other** — silence is anti-pattern per `scala-data-driven-refactor-mindset` §2 ("shape vs validity are separate").

**Files**: ~25-30 files touched, ~340-400 LOC total (mechanical).

**Per `scala-impact-analysis-mindset` §1 (cross-cutting renames MUST be atomic)**: a multi-PR rename would leave the codebase half-renamed (build broken between PRs). The single atomic PR is the only correct option.

**Per `scala-impact-analysis-mindset` §3 (binary compat — per Architect P0-2)**: the v0.1.0 tag is the FIRST release with MiMa enabled (per ADR-008-P §E2); pre-1.0 API churn is permitted. The rename is safe. **However** (per Architect P0-2): adding a NEW METHOD to a published trait (`realizeTyped` in PR-15) requires more care — see PR-15 below for the subtrait + default-delegate solution.

**Skill-mindset applied**:
- `karpathy-guidelines-mindset` §3 surgical changes (one atomic PR, no shims)
- `karpathy-app-designmindset` §3.1 Protocols before implementations (rename reflects the engine-portable ADT home, not a transient legacy prefix)
- `scala-impact-analysis-mindset` 4-step (LSP `references` for every type before rename; `references` for every callsite)
- `debug-mantra` 5-step (reproduce by reading every file that imports `MCPEngine*`; trace the rename; verify with reactor test)
- `scala-bug-hunting-mindset` §3 (every match must be exhaustive — `cacheKey` method eliminates the `toString` drift)

**Acceptance**:
- 697/697 tests pass (zero regression)
- `grep -r 'MCPEngineProvider\|MCPEngineRegistry\|MCPQueryRequest\|MCPEngineUrl\|MCPEngineLoader\|MCPRegistry' --include='*.scala' --include='*.md' .` returns **zero matches** EXCEPT in the ADR Provenance section (where historical mentions are exempt) + in the SPI service file (renamed in lockstep).
- LSP `references` for each of the 5 types returns zero results (the symbol-level smoke test).
- `META-INF/services/io.sm8.core.engine.EngineProvider` exists; `META-INF/services/io.sm8.core.engine.MCPEngineProvider` is deleted.
- The renamed `QueryRequest.cacheKey` method passes a new `QueryRequestCacheKeySpec` test that demonstrates `Seq` subtype drift is now deterministic.
- Atomic commit + push + PR-14 opened; reviewable diff in ~340-400 LOC.

### PR-15: Typed URL grammar + `EngineLoader.discoverAndRealize`

**Scope**: 3 new files + 5 modified files (~330 LOC new).

**Per C1 (Arch P0-1 — RFC `adapters.md` Rule 4 honor)**:

```scala
// sm8-core/src/main/scala/io/sm8/core/engine/EngineUrl.scala (NEW — Core layer)
// EngineUrl is a CONNECTOR-NEUTRAL typed carrier. Per-connector
// validation lives in the connector (NOT in core). The core smart
// constructor does NOT contain per-engine grammar parsers.
sealed trait EngineUrl extends Product with Serializable {
  def raw: String
  def engineName: String
}
object EngineUrl {
  final case class Spark(master: String)                 extends EngineUrl {
    val raw: String = master; val engineName: String = "spark"
  }
  final case class Trino(jdbcUrl: String)                extends EngineUrl {
    val raw: String = jdbcUrl; val engineName: String = "trino"
  }
  final case class InMemory(seed: Option[String] = None) extends EngineUrl {
    val raw: String = "in-memory"; val engineName: String = "in-memory"
  }
  /** Engine-name-only factory: dispatches to the engine-specific
    * parser registered via SPI (see `EngineUrlParser` subtrait).
    * The default registry contains 3 parsers (spark/trino/in-memory);
    * external connectors register their parser via SPI at startup.
    * Returns `Either[EngineError, EngineUrl]` (typed error, per
    * `scala-error-handling-mindset` §1).
    */
  def parse(engineName: String, raw: String): Either[EngineError, EngineUrl]
}
```

```scala
// sm8-core/src/main/scala/io/sm8/core/engine/EngineUrlParser.scala (NEW — Core layer)
// Per-connector URL parser, discovered via SPI. Each connector
// implements its own grammar validation (per RFC adapters.md Rule 4).
trait EngineUrlParser extends Serializable {
  def engineName: String
  def parse(raw: String): Either[EngineError, EngineUrl]
}
object EngineUrlParser {
  /** SPI lookup; default registry is populated at engine-impl class load.
    * External connectors register via META-INF/services/io.sm8.core.engine.EngineUrlParser
    */
  def lookup(engineName: String): Either[EngineError, EngineUrlParser]
}
```

**Per C2 (Arch P0-2 — subtrait + default delegate for binary compat)**: `realizeTyped` lives in a **separate subtrait** `TypedRealizationProvider` discovered by `EngineLoader`, not added directly to `EngineProvider`:

```scala
// sm8-core/src/main/scala/io/sm8/core/engine/TypedRealizationProvider.scala (NEW — Core layer)
import io.sm8.core.rel.AggregateFn

/** Typed realization contract (per `scala-error-handling-mindset` §1).
  *
  * Per RFC `adapters.md` Rule 4: the connector validates its own URL
  * grammar. `realizeTyped(parsedUrl: EngineUrl)` accepts a
  * pre-parsed typed URL and returns `Either[EngineError, EngineProvider]`
  * (typed error, not silent `Option`).
  *
  * Per `scala-impact-analysis-mindset` §3 (binary compat per Architect
  * P0-2): this is a **separate subtrait**, NOT added directly to
  * `EngineProvider`. Existing implementors of `EngineProvider` are
  * NOT broken. External ServiceLoader discoverers opt into typed
  * realization by also implementing `TypedRealizationProvider`.
  */
trait TypedRealizationProvider extends EngineProvider {
  def realizeTyped(parsedUrl: EngineUrl): Either[EngineError, EngineProvider]
}
```

**Per C7 (DE P1-B — single realization path)**: The default `realizeTyped` impl in the subtrait delegates to the existing `realize(url: String)`:

```scala
object TypedRealizationProvider {
  /** Default impl: converts `EngineUrl` → raw string, delegates to
    * `realize(url: String)`, maps `Some(p)` → `Right(p)`, `None` →
    * `Left(EngineError.ConnectionFailed(...))`. Guarantees ONE
    * realization path (no parallel parsers; per DE P1-B).
    *
    * Concrete providers MAY override for engine-specific typed errors
    * (e.g. SparkSession construction failure returns the actual
    * exception message). The default impl is safe for any connector.
    */
  def defaultRealizeTyped(self: EngineProvider, parsedUrl: EngineUrl)
    : Either[EngineError, EngineProvider] =
    self.realize(parsedUrl.raw).toRight(
      EngineError.ConnectionFailed(
        engine = parsedUrl.engineName,
        reason = "realize returned None for parsed url",
        message = s"sm8: ${parsedUrl.engineName} engine rejected URL '${parsedUrl.raw}'"
      )
    )
}
```

**Files (new)**:
- `sm8-core/src/main/scala/io/sm8/core/engine/EngineUrl.scala` (~60 LOC): sealed trait + 3 cases + `parse` factory only (no per-engine parser bodies)
- `sm8-core/src/main/scala/io/sm8/core/engine/EngineUrlParser.scala` (~50 LOC): subtrait + SPI lookup
- `sm8-core/src/main/scala/io/sm8/core/engine/TypedRealizationProvider.scala` (~40 LOC): subtrait + default delegate
- `sm8-core/src/test/scala/io/sm8/core/engine/EngineUrlSpec.scala` (~120 LOC): 10 tests (parse via SPI; missing-parser error; case shapes; `extends Serializable`)
- `sm8-core/src/test/scala/io/sm8/core/engine/TypedRealizationProviderSpec.scala` (~80 LOC): 4 tests (default delegate; connector override; typed-error vs Option-error)
- `sm8-server/src/main/scala/io/sm8/server/EngineLoader.scala` (~50 LOC, **moved from sm8-platform per C11**): `discover(classLoader): List[EngineProvider]` + `discoverAndRealize(classLoader, parsedUrl): List[Either[EngineError, EngineProvider]]`
- `sm8-server/src/test/scala/io/sm8/server/EngineLoaderSpec.scala` (~80 LOC): 3 tests (realize on real classloader; parse-error path; engine-name-mismatch path)

**Files (modified)**:
- `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/SparkEngineProvider.scala` (modified): implement `TypedRealizationProvider` + `EngineUrlParser("spark")` (~25 LOC added)
- `connectors/trino-connector/src/main/scala/io/sm8/connectors/trino/TrinoEngineProvider.scala` (modified): same (~20 LOC added)
- `connectors/in-memory-connector/src/main/scala/io/sm8/connectors/inmemory/InMemoryEngineProvider.scala` (modified): same (~15 LOC added)
- `connectors/spark-connector/src/main/resources/META-INF/services/io.sm8.core.engine.EngineUrlParser` (NEW): SPI registration for the Spark parser
- (similar SPI files for trino + in-memory)
- `connectors/spark-connector/src/main/resources/META-INF/services/io.sm8.core.engine.EngineProvider` (modified): rename SPI key from `MCPEngineProvider` → `EngineProvider`

**Per C11 (Arch P1-4 — `EngineLoader` placement)**: `EngineLoader` lives in **sm8-server** (per ADR-006 §Post-#65 Refinement: sm8-server owns deployment discovery). The sm8-platform transport library stays pure (zero `ServiceLoader` calls; per ADR-006). The platform exposes the **typed-error mapper** (a pure function on `(List[EngineProvider], EngineUrl) → List[Either[EngineError, EngineProvider]]`) that sm8-server uses.

**Per C17 (DE P2-E — split into 2 helpers)**: `EngineLoader.discoverAndRealize` is internally split into `discover(classLoader)` (ServiceLoader call) + `realize(providers, url)` (per-provider `realizeTyped`). The public `discoverAndRealize` is a convenience that calls both. This makes the per-phase split visible + testable.

**Per `scala-error-handling-mindset` §1 (errors are data)**: the smart constructor returns `Either[EngineError, EngineUrl]`, NOT silent `None`. The user gets a specific reason ("Trino URL must start with `jdbc:trino://`, got `jdbc:postgres://...`") instead of guessing.

**Per `scala-spark-batch-bugs-mindset` §1 (closure-safety)**: `EngineUrl`, `EngineUrlParser`, `TypedRealizationProvider` all `extend Serializable`. Safe to capture in any Spark closure.

**Skill-mindset applied**:
- `karpathy-guidelines-mindset` §2 simplest correct change
- `karpathy-app-designmindset` §3.1 (Protocol in core subtrait; per-connector validation in connectors; per-connector parser via SPI)
- `scala-error-handling-mindset` §1 (typed `Either`, not `Option`)
- `scala-data-driven-refactor-mindset` §1 (data is data: `EngineUrl` is connector-neutral)
- `scala-data-driven-refactor-mindset` §3 (N=3 engines → sealed-trait enum + per-engine SPI parser, NOT Map of URL parsers)
- `scala-impact-analysis-mindset` §3 (subtrait for binary compat — per Arch P0-2)
- `scala-bug-hunting-mindset` §3 (single realization path — per DE P1-B)
- `debug-mantra` 5-step (one parse per test case; falsify the parser; cross-reference every error path)

**Acceptance**:
- 697/697 tests pass + 17 new tests (10 EngineUrlSpec + 4 TypedRealizationProviderSpec + 3 EngineLoaderSpec) = **714/714** (baseline + 17; per Arch P2-4 disambiguation)
- `EngineUrlSpec` exhaustively tests parse(spark, "local[*]"), parse(spark, ""), parse(spark, "wrong:"), parse(trino, "jdbc:trino://..."), parse(trino, "jdbc:postgres://..."), parse(in-memory, ""), parse("unknown-engine", "..."), parse(null, ...), SPI lookup success/failure, `extends Serializable` round-trip
- `EngineLoaderSpec` tests `discoverAndRealize` on real classloader (the in-memory connector) + parse-error path + engine-name-mismatch path
- SPI files for `EngineUrlParser` registered in spark + trino + in-memory connectors

### PR-16: Phantom-typed SDK (`TypedDimension[D]` + `TypedMeasure[M]` + closure-safety)

**Scope**: 2 new files + 4 modified files (~370 LOC new).

**Per C3 (DE P0-D — wire-shape decision)**: PR-16 lands the phantom-typed SDK as an **additive parallel surface**. `MCPQueryRequest.dimensions: Seq[String]` (renamed `QueryRequest.dimensions: Seq[String]` in PR-14) **stays unchanged**. The phantom witness `.name: String` extractor is the only bridge to the wire. No `QueryRequest` field-type change → no Restate journal migration → all 19 callers unaffected.

```scala
// examples/hospital-cleaning/.../Main.scala (BEFORE — current)
val queryRequest = MCPQueryRequest(
  model = patientsModel,
  dimensions = Seq("patient_id", "gender", "insurance"),
  measures = Seq("patient_count", "average_age")
)

// examples/hospital-cleaning/.../Main.scala (AFTER — PR-16)
// Per C3: the wire DTO stays unchanged; the typed SDK builds the same Seq[String]
// via phantom-witness .name extractors. Compile-time-checked at the call site.
val queryRequest = QueryRequest(
  model = patientsModel,
  dimensions = Seq(Refs.patientId.name, Refs.gender.name, Refs.insurance.name),
  measures = Seq(Refs.patientCount.name, Refs.averageAge.name)
)
// At the call site: typo in `Refs.patienId` = compile error
// (per scala-bug-hunting-mindset §1 trust compiler)
```

```scala
// sm8-core/src/main/scala/io/sm8/core/model/TypedDimension.scala (NEW — Core layer)
sealed trait TypedDimension[D] extends Serializable {
  def name: String
  def fieldName: String
  def asFieldRef: Expr = Expr.FieldRef(fieldName)
}

object TypedDimension {
  /** The only way to instantiate a `TypedDimension[D]`. The returned
    * witness captures ONLY Strings + the phantom `D` ref — no
    * enclosing scope capture, no Spark types, no resource acquisition.
    *
    * Per C9 + `scala-spark-batch-bugs-mindset` §1: the witness MUST
    * be defined at `object` level (singleton, class-load time) for
    * Spark closure-safety. Method-local definitions capture the
    * enclosing scope (which may include non-Serializable locals)
    * and break Spark closure serialization. This rule is documented
    * at the trait level + tested by `TypedDimensionClosureSafetySpec`.
    *
    * Per `scala-jvm-safety-mindset` §2: the witness holds NO resources.
    * Per `karpathy-app-designmindset` §3.1: the trait in core; the
    * witness in the consumer's `Refs` object (plugin or example).
    */
  def of[D](name: String, fieldName: String = name): TypedDimension[D] =
    new TypedDimension[D] {
      val name: String = name
      val fieldName: String = fieldName
    }
}
```

**Per C9 (DE P1-D — Restate journal-capture is dormant)**: The Restate journal-capture path is dormant today (`RestatedEngineRunner.runJournaled` is a no-op; handler-class wiring is `TODO(PR-C5b-ext-γ'-follow-up)`). The `extends Serializable` constraint on `TypedDimension[D]` and `TypedMeasure[M]` is **forward-looking**: once Restate lands, `EngineRegistry`, `QueryRequest`, and the phantom witnesses must all be `Serializable` for the journal to round-trip. We add the constraint NOW so the future PR doesn't break.

**Per C10 (DE P1-E — perf math correction)**: per-witness object-load cost = ~30-50 ns (1 case-class instance); per-query cost = 0 (JIT inlines static field read); **total one-time cost = N_witnesses × ~40 ns** (e.g., 23 witnesses ≈ 1 us one-time at JVM startup). NOT "< 50 ns total" — that was v1's undercount.

**Per C8 (DE P1-C — closure-safety spec)**: `TypedDimensionClosureSafetySpec` has **3 tests**:
1. **Positive round-trip** (per DE P1-C): object-level witness survives `ObjectOutputStream` round-trip + phantom tag preserved (via `summon[TypedDimension[D]]` post-deserialization).
2. **Spark UDF closure-safe**: object-level witness captured in a Spark UDF closure does NOT throw `NotSerializableException` (proves the safe pattern works in realistic Spark context).
3. **Documented failure mode (separate test)**: method-local witness + non-Serializable enclosing local throws `NotSerializableException`; the test's name + comment point the reader to the fix ("define the witness at `object` level").

**Files (new)**:
- `sm8-core/src/main/scala/io/sm8/core/model/TypedDimension.scala` (~35 LOC)
- `sm8-core/src/main/scala/io/sm8/core/model/TypedMeasure.scala` (~50 LOC, with 6 specialized factories: `Count`/`Sum`/`Avg`/`Min`/`Max`/`CountDistinct`)
- `sm8-core/src/test/scala/io/sm8/core/model/TypedDimensionSpec.scala` (~100 LOC): 6 tests (1-arg overload; 2-arg form; Serializable round-trip; phantom tag preservation; object-level-only rule documented)
- `sm8-core/src/test/scala/io/sm8/core/model/TypedMeasureSpec.scala` (~120 LOC): 8 tests (6 specialized factories + Serializable round-trip + phantom tag preservation)
- `sm8-core/src/test/scala/io/sm8/core/engine/TypedDimensionClosureSafetySpec.scala` (~80 LOC): 3 tests (positive round-trip + Spark UDF safe + documented failure mode)

**Files (modified)**:
- `examples/hospital-cleaning/src/main/scala/com/example/hospital/Main.scala` (~+30/-10 LOC): add `object Refs { ... }` with phantom witnesses; convert `Model.of(List(Dimension(...)))` calls to use `Refs.*.name` for the wire `dimensions`/`measures` Seq per C3
- `plugins/cache-plugin/.../Refs.scala` (NEW, ~20 LOC): the cache plugin's typed measure witnesses (e.g. `cacheHits: TypedMeasure[CacheHits]`)
- `plugins/audit-plugin/.../Refs.scala` (NEW, ~15 LOC): audit plugin's typed witnesses
- `plugins/row-cap-plugin/.../Refs.scala` (NEW, ~15 LOC): row-cap plugin's typed witnesses

**Skill-mindset applied**:
- `karpathy-guidelines-mindset` §1, §2, §3, §4 (surface assumptions + simplicity + surgical + verifiable)
- `karpathy-app-designmindset` §3.1 (Protocols in core; witness in plugins)
- `scala-bug-hunting-mindset` §1 (trust compiler: typo = compile error, not runtime)
- `scala-jvm-safetymindset` §2, §3 (`extends Serializable`; singleton lifecycle)
- `scala-perf-testing-mindset` §3 (allocation is the tax: per-query overhead = 0; per-witness object-load = ~30-50 ns)
- `scala-spark-batch-bugs-mindset` §1 (closure-safety: object-level rule + Serializable trait + 3 closure-safety tests)
- `scala-spark-streaming-bugs-mindset` §2 (checkpoint/recovery: Serializable survives — **forward-looking** per C14)
- `scala-error-handlingmindset` §1 (errors are data: typed errors in `EngineLoader`, not silent `None`)
- `scala-data-driven-refactor-mindset` §1 (data is data: phantom traits are pure type-level data)
- `scala-chaos-testing-mindset` §2 (silence is a symptom: closure-safety test demonstrates the failure mode)

**Acceptance**:
- 714/714 tests pass + 17 new tests (6 TypedDimensionSpec + 8 TypedMeasureSpec + 3 TypedDimensionClosureSafetySpec) = **731/731** (baseline + 34 total new tests across PR-15 + PR-16; per Arch P2-4 disambiguation)
- `TypedDimensionClosureSafetySpec` demonstrates the safe pattern (object-level witness survives `ObjectOutputStream` round-trip + phantom-tag preserved) AND the documented failure mode (method-local witness + non-Serializable local throws `NotSerializableException`)
- The example file (`examples/hospital-cleaning/`) uses the new typed APIs end-to-end; demonstrates the `Refs` pattern; wire `dimensions`/`measures` Seq built via `Refs.*.name`

## Consequences

Per the ADR convention (`docs/adr/README.md`).

### Positive

- **Naming consistency**: `Engine*` prefix matches ADR-001's "engine-portable ADT home is `io.sm8.core.*`" framing. Future readers do not mistake `MCPEngine*` for the Anthropic MCP protocol.
- **Type safety**: phantom-typed witnesses catch typos at compile time. A `summon[TypedDimension[PatientId]]` with no implicit in scope fails compilation (no runtime `NoSuchElementException`).
- **Typed errors**: `realizeTyped` returns `Either[EngineError, ...]` instead of `Option`; users get specific failure reasons.
- **Centralization**: `EngineLoader.discoverAndRealize` eliminates 1 callsite today (sm8-server) + 1 example callsite (hospital-cleaning); future connectors (duckdb, unity-catalog) + sm8-cli will use it on landing.
- **Closure-safety guarantee**: `extends Serializable` at the trait level + documented object-level rule + 3-test spec makes the Spark-closure risk visible (per `scala-spark-batch-bugs-mindset` §1).
- **Wire-shape stability**: phantom-typed SDK is additive parallel; no `QueryRequest` field-type change; no Restate journal migration.

### Negative / tradeoffs

- **Cross-cutting rename risk**: PR-14 touches ~25-30 files atomically; LSP `references` must be re-verified before commit. Any missed callsite breaks the build.
- **Subtrait adds a layer**: `TypedRealizationProvider` is a separate trait discovered by `EngineLoader`. Connectors that want typed errors implement BOTH `EngineProvider` and `TypedRealizationProvider`. Slightly more surface area than adding `realizeTyped` directly to `EngineProvider` (which was rejected per Architect P0-2).
- **Phantom-typed SDK is additive**: users must still write `Seq(Refs.patientId.name, ...)` to build the wire `dimensions` Seq (per C3 — wire shape unchanged). The phantom type carries no runtime benefit at the wire; its benefit is **compile-time** typo safety at the call site.
- **One-time object-load cost**: N_witnesses × ~40 ns per model load (e.g., 23 witnesses ≈ 1 us). Negligible (per `scala-perf-testing-mindset` §3), but not literally "< 50 ns total" as v1 stated.
- **SPI key renaming**: `META-INF/services/io.sm8.core.engine.MCPEngineProvider` must be renamed in lockstep to `META-INF/services/io.sm8.core.engine.EngineProvider`. External connectors (none today) must update their SPI files too.
- **Dormant Restate constraint**: `extends Serializable` on the phantom trait is a forward-looking requirement (per C9). The constraint adds no value today; it prevents future breakage once `RestatedEngineRunner.runJournaled` becomes active.
- **Streaming claim is forward-looking** (per C14): the sm8 reactor has zero streaming code today. The Serializable-witness claim is correct in principle (Spark serializes query plans + closures at every Trigger boundary) but is not testable in the current reactor. PR-16 makes no streaming test.
- **Vocabulary drift in RFC docs** (per Arch P2-3): RFC §3 and companion docs still use "Adapter" terminology. ADR-008-Q does not propose an RFC change; this is explicitly tracked in ADR-008-P §"What's Next" backlog.
- **One-shot ModelBuildBench removed** (per C13): the cited 0.03 ms one-time cost is unfalsifiable from the current tree. The qualitative verdict ("negligible") is correct but not numerically reproducible.

### Migration cost

- **Source code**: ~25-30 files touched in PR-14; LSP `rename` handles the mechanical work.
- **Test files**: ~10-15 test files touched; SPI service files renamed.
- **Plugins**: 3 plugins (cache, audit, row-cap) gain a `Refs` object in PR-16; ~50 LOC added.
- **Examples**: 1 example (hospital-cleaning) demonstrates the typed API; ~30 LOC added.
- **External implementors** (none today): no action required — the rename is internal-only; the subtrait `TypedRealizationProvider` is opt-in.
- **Backwards compatibility**: source-compatible (LSP rename); binary-compatible for in-reactor implementors (default delegate in subtrait); **NOT binary-compatible for separately compiled external implementors** of `EngineProvider` that use the default `realize(url: String)` — they continue to work, but they will not benefit from `realizeTyped` typed errors unless they implement the subtrait (which is opt-in). This is acceptable for v0.1.0 (pre-1.0 churn per ADR-008-P §E2).

### Rollback

Each PR is **independently revertible** (atomic commits on a single branch):
- **Revert PR-14**: revert the rename commit; LSP `rename` was atomic, so a single revert restores the original names. The SPI key revert is a 1-line file rename. Test count returns to 697/697.
- **Revert PR-15**: revert the typed-URL + EngineLoader commits; the existing `realize(url: String): Option[...]` continues to work. The new SPI files (`EngineUrlParser`) are deleted; `META-INF/services/io.sm8.core.engine.EngineUrlParser` is removed from the 3 connectors.
- **Revert PR-16**: revert the phantom-witness commits; the wire `dimensions: Seq[String]` is unchanged (so revert is safe); the typed SDK is removed; `Refs` objects in plugins + example are removed.

### Out of scope (explicit non-goals)

- **No new engine implementations** (duckdb, unity-catalog, hive-metastore) — these remain ADR-008-P §"What's Next" backlog (AR-P1-3+AR-P1-4).
- **No EngineHookTypes de-duplication** (sm8-core + sm8-platform duplicates) — ADR-008-P §"What's Next" backlog (AR-P1-3).
- **No `compileSteps` 2x perf cliff** (DE post-P T2-2) — already addressed in PR-9 (T2-3) per ADR-008-P.
- **No `SparkEngineProvider.close()` throwable swallowing** (DE post-P T2-7) — separate PR.
- **No unpersist log-to-stderr fix** (DE post-P T2-8) — separate PR.
- **No streaming code** — the sm8 reactor is batch-only; the Serializable claim is forward-looking (per C14).
- **No v0.1.0 tag cut** — GATED by the standing user directive "dont bump version yet" (2026-08-17).

## Alternatives Considered

Per the ADR convention (`docs/adr/README.md`).

### Alternative 1: Defer the entire redesign to post-v0.1.0

- **Pros**: zero churn before v0.1.0 tag; preserves MiMa binary compat for any external consumers.
- **Cons**: the v0.1.0 tag ships with the misleading `MCPEngine*` prefix; the verbose untyped SDK; the silent `Option` realize. These are user-facing quality issues that compound post-v0.1.0.
- **Rejected because**: the v0.1.0 tag is the first MiMa release (per ADR-008-P §E2); pre-1.0 API churn is permitted. Now is the right time for additive API churn.

### Alternative 2: Keep the `MCPEngine*` prefix + add a documentation note

- **Pros**: zero code churn.
- **Cons**: the naming is still misleading; future readers (especially post-Anthropic MCP popularity in 2024+) will reasonably assume the wrong meaning. The Scaladoc note is invisible at the call site.
- **Rejected because**: per the user's question 2026-08-19, the naming collision is real; a Scaladoc note is insufficient.

### Alternative 3: Add `realizeTyped` directly to `EngineProvider` (NOT a subtrait)

- **Pros**: simpler trait hierarchy; one realization method per provider.
- **Cons**: breaks binary compat for separately compiled external implementors (per Architect P0-2); future contributors might still pick the legacy `realize(url)` and bypass typed errors.
- **Rejected because**: the subtrait approach preserves binary compat (the default delegate works for any `EngineProvider` impl) AND lets connectors opt into typed errors explicitly.

### Alternative 4: Centralize URL grammar validation in core (NOT in connectors)

- **Pros**: one place to find all grammar rules; easier to maintain.
- **Cons**: violates RFC `adapters.md` Rule 4 ("Per-connector `realize()` validates its own URL grammar"); imports connector knowledge into the supposedly engine-portable core; violates ADR-001's core boundary.
- **Rejected because**: the RFC + ADR-001 boundary is the explicit constraint; the SPI-based per-connector parser honors it.

### Alternative 5: Replace `MCPQueryRequest.dimensions: Seq[String]` with `Seq[TypedDimension[?]]`

- **Pros**: the wire DTO is also typed; phantom type survives serialization.
- **Cons**: **19 callers of `MCPQueryRequest`** are affected; the Restate journal-capture path must serialize the new type; if the phantom witness references a non-Serializable enclosing scope, the journal breaks. Per `scala-impact-analysis-mindset` §2 + DE P0-D.
- **Rejected because**: the additive parallel surface (per C3) is safer; the wire stays `Seq[String]`; the phantom SDK builds the Seq via `Refs.*.name` extractors at the call site.

### Alternative 6: Skip the rename; just add phantom-typed SDK + typed URL

- **Pros**: smaller PR scope; PR-15 + PR-16 only.
- **Cons**: the `MCPEngine*` prefix is still misleading; the phantom witnesses live alongside a name that suggests Anthropic MCP; reader confusion remains.
- **Rejected because**: per the user's question 2026-08-19, the rename is the highest-leverage, lowest-risk first step. Doing it before the phantom SDK means the new types land on the cleaner surface.

### Alternative 7: Use Scala 3 `using`/`given` syntax for phantom witnesses

- **Pros**: more modern Scala syntax; explicit context bounds.
- **Cons**: ADR-007 hard-pins Scala 2.13; using Scala 3 syntax would break the constraint.
- **Rejected because**: ADR-007 is binding; Scala 2.13 syntax is the only option.

### Alternative 8: Typeclass-encoded witnesses (e.g., `trait Dimension[D] { type Repr; def toRepr: Repr }`)

- **Pros**: full typeclass pattern; very expressive.
- **Cons**: more surface area; less ergonomic for the user's "less tedious" requirement; harder to test for closure-safety (the `Repr` type parameter adds complexity).
- **Rejected because**: the upstream `semanticdf.SemanticDimension.of[T]("name")` pattern is a phantom-typed witness (NOT a typeclass); simpler; matches the existing pattern; easier to test.

## Status

**Status: Proposed (2026-08-19, rev2 — addresses 21 review findings).** Implementation pending user approval. 4 P0 fixes + 9 P1 fixes + 6 P2 fixes identified across 2 senior reviews + audit; 3 atomic PRs sequenced (PR-14 + PR-15 + PR-16); each additive + backward-compatible for source; v0.1.0 tag cut remains GATED by the standing user directive "dont bump version yet" (2026-08-17).

### Implementation summary (proposed)

| # | PR | Title | LOC (new) | Files (new) | Files (modified) | Effort |
|---|----|-------|-----------|-------------|------------------|--------|
| 1 | PR-14 | `refactor(engine): rename MCPEngine* → Engine* (+ canonical cacheKey on QueryRequest)` | ~340-400 (mechanical) | 0 | ~25-30 | 30-45 min |
| 2 | PR-15 | `feat(engine): typed URL grammar + TypedRealizationProvider subtrait + EngineLoader` | ~330 | 7 | 5 | 2h |
| 3 | PR-16 | `feat(model): phantom-typed Dimensions + Measures (additive parallel surface)` | ~370 | 5 | 4 | 1.5h |

### Skill-mindset coverage (3 PRs combined)

| Skill | Where applied |
|-------|---------------|
| `karpathy-guidelines-mindset` §1, §2, §3, §4 | All 3 PRs (surface assumptions + simplicity + surgical + verifiable) |
| `karpathy-app-design-mindset` §3.1, §1.3 | All 3 PRs (Protocols in core; impls in platform/connectors/plugins; single import path) |
| `debug-mantra` 5-step | All 3 PRs (reproduce, trace, falsify, cross-reference, verify) |
| `scala-bug-hunting-mindset` §1, §3 | PR-14 (cacheKey normalization eliminates `toString` drift) + PR-16 (typo = compile error; single realization path) |
| `scala-data-driven-refactor-mindset` §1, §2, §3 | PR-15 (data is data: connector-neutral EngineUrl + per-engine SPI parser; N=3 engines → sealed trait, NOT Map) + PR-16 (phantom traits are pure type-level data) |
| `scala-error-handling-mindset` §1, §3 | PR-15 (typed `Either`, not `Option`; default delegate chains to existing realize) + PR-16 (errors are data) |
| `scala-impact-analysis-mindset` 4-step | PR-14 (cross-cutting renames MUST be atomic; SPI key in lockstep) + PR-15 (subtrait for binary compat) + PR-16 (wire-shape additive — no QueryRequest change) |
| `scala-jvm-safety-mindset` §2, §3 | PR-16 (`extends Serializable`; singleton lifecycle at `Refs` level) |
| `scala-perf-testing-mindset` §3 | PR-16 (allocation is the tax: per-query = 0; per-witness object-load = ~30-50 ns; total = N × ~40 ns) |
| `scala-jar-packaging-mindset` §1 | All 3 PRs (no new deps; ships in sm8-core + sm8-server) |
| `scala-chaos-testing-mindset` §2 | PR-16 (silence is a symptom: 3 closure-safety tests, one demonstrates failure mode) |
| `scala-spark-batch-bugs-mindset` §1 | PR-16 (closure-safety: object-level rule + Serializable trait + 3 closure-safety tests) |
| `scala-spark-streaming-bugs-mindset` §2 | PR-16 (checkpoint/recovery: Serializable survives — forward-looking per C14) |
| `scala-spark-streaming-bugs-mindset` §1, §3 | N/A (no streaming code in PRs) |

### Cross-validated findings resolved

#### From `data-engineer-review.md` (post-ADR-008-Q, 2026-08-19)

| Finding | Resolution in rev2 |
|---------|-------------------|
| **P0-D** (wire-shape decision) | C3: explicit decision — phantom-typed SDK is additive parallel; `QueryRequest.dimensions: Seq[String]` stays unchanged |
| **P0-E** (canonical cacheKey) | C4: PR-14 adds `cacheKey` method to `QueryRequest` (sort + hashCode) |
| **P1-A** (EngineLoader deduplication claim) | C6: reframed as "helper added + 1 callsite migrated; next 2 connectors + sm8-cli will use on landing" |
| **P1-B** (parallel parsers trap) | C7: default `realizeTyped` impl delegates to existing `realize(url)` |
| **P1-C** (closure-safety spec) | C8: 3-test spec — positive round-trip + Spark UDF safe + documented failure mode |
| **P1-D** (dormant Restate constraint) | C9: explicit note in PR-16 — constraint is forward-looking |
| **P1-E** (perf math undercount) | C10: corrected to N × ~40 ns one-time; per-query = 0 |
| **P2-A** (ModelBuildBench provenance) | C13: noted as unfalsifiable; future PR may add permanent bench |
| **P2-B** (streaming claim) | C14: marked forward-looking + conditional |
| **P2-C** (PR-14 rename scope incomplete) | C12 + C15: SPI key + `MCPRegistry` legacy alias added to PR-14 scope |
| **P2-D** (EngineUrl case shapes unspecified) | C16: 5-line shape sketch included in §PR-15 |
| **P2-E** (EngineLoader signature split) | C17: internally split into `discover(classLoader)` + `realize(providers, url)` |

#### From `architect-review.md` (post-ADR-008-Q, 2026-08-19)

| Finding | Resolution in rev2 |
|---------|-------------------|
| **P0-1** (RFC Rule 4 contradiction) | C1: per-connector URL parser via `EngineUrlParser` SPI subtrait; core `EngineUrl` is connector-neutral typed carrier |
| **P0-2** (binary compat overstated) | C2: `realizeTyped` in separate subtrait `TypedRealizationProvider`; default impl delegates to existing `realize(url)` |
| **P1-1** (ADR incomplete — Consequences / Alternatives / References) | C5: all 3 sections added (this revision) |
| **P1-2** (phantom API lacks integration) | C3 + example before/after in §PR-16 |
| **P1-3** (Serializable claim overstated) | C8: 3-test spec including positive round-trip + transitive-capture rule documented |
| **P1-4** (EngineLoader platform/server ownership) | C11: `EngineLoader` moved to sm8-server (per ADR-006) |
| **P1-5** (PR-14 scope/count unverifiable) | C12: explicit callsite counts from CodeGraph (18 / 12 / 19) |
| **P2-1** (perf claims unsupported) | C10 + C13: corrected math + provenance noted as unfalsifiable |
| **P2-2** (streaming misapplied) | C14: removed from skill-mindset coverage table; noted as forward-looking |
| **P2-3** (RFC vocabulary incomplete) | C20: explicit backlog entry in §Consequences |
| **P2-4** (test-count ambiguity) | C21: 697/697 baseline + 714/714 projected (PR-15) + 731/731 projected (PR-16) |

## What's Next (post-ADR-008-Q)

After PR-14 + PR-15 + PR-16 land, per the production-readiness audit 2026-08-19:

| PR | Title | Effort |
|----|-------|--------|
| **PR-17** | `examples/pipeline/` (second end-to-end example: streaming + RESTate workflow) | 2h |
| **PR-18** | `examples/customer-analytics/` (third end-to-end example: customer segmentation) | 2h |
| **PR-19** | top-level `README.md` + `docs/getting-started.md` | 1h |
| **PR-20** | observability: SLF4J facade + Micrometer + OpenTelemetry hooks (Phase E extension) | 3h |
| **PR-21** | deployment: Dockerfile + helm chart + CI/CD (Phase E extension) | 3h |
| **PR-22** | security: OAuth + RBAC for sm8-server | 3h |
| **PR-6** (v0.1.0 tag cut) | atomic E2 version bump + tag + MiMa re-enable | GATED by user "dont bump version yet" |

### Backlog from prior ADRs (continues)

From ADR-008-P §"What's Next" (unchanged):
- AR-P1-3: de-dupe `EngineHookTypes` (sm8-core + sm8-platform duplicates)
- AR-P1-3+AR-P1-4: 3 missing connectors (duckdb, unity-catalog, hive-metastore)
- DE-P2-5: semanticdf parity (time-grain, having, inline `t.all(...)`, window functions)
- Phase D: timeout/cancel/SourceSchemaChanged/DecimalOverflow
- Phase E: T2-1 (cacheKey determinism — **partially resolved by C4 in PR-14**), T2-5/T2-6 (SparkSourceResolver + PortableQueryCompiler.resolveSource Exception catches), T2-7/T2-8 (unpersist logging + JSON-RPC typed errors), T2-9/T2-10 (closure-safety — **resolved by C8 in PR-16**)

## References

Per the ADR convention (`docs/adr/README.md`).

### Internal

- **RFC**: `docs/rfcs/2026-08-12_v1_architecture-spec/semantic-layer-engine-architecture.md` §3 Core Boundary; `hooks.md` 5-behavioral-type classification; `plugins.md` Plugin rules; `adapters.md` Rule 4 "per-connector `realize()` validates its own URL grammar"
- **ADR-001**: `docs/adr/0001-0004-engine-portable-architecture.md` — engine-portable ADT home is `io.sm8.core.*`
- **ADR-006**: `docs/adr/0006-step-11-sm8-mcp-server.md` — Post-#65 Refinement (sm8-platform pure transport; sm8-server deployment module)
- **ADR-007**: `docs/adr/0007-v0.1.0-cut-plan.md` — Scala 2.13 hard-pinned; v0.1.0 tag cut plan; MiMa; SemVer
- **ADR-008-P**: `docs/adr/0008-p-post-review-followup.md` — prior ADR; §C1 (typed realize); §E2 (v0.1.0 = first MiMa release)
- **Reviews (input to this ADR)**:
  - `/tmp/reviews/post-adr-008-p-data-engineer-review.md` (4 P0 + 7 P1 + 3 P2)
  - `/tmp/reviews/post-adr-008-p-architect-review.md` (3 P0 + 5 P1 + 4 P2)
  - `/tmp/reviews/adr2-review-consolidated.md`
- **Reviews (this ADR)**:
  - `/tmp/reviews/post-adr-008-q-data-engineer-review.md` (verdict: Approved with minor changes; 2 P0 + 5 P1 + 5 P2)
  - `/tmp/reviews/post-adr-008-q-architect-review.md` (verdict: Needs revision; 2 P0 + 5 P1 + 4 P2)
- **Codebase (verified via codegraph 2026-08-19)**:
  - `MCPEngineProvider`: 18 callers across 4 modules + 6 test files; trait at `sm8-core/src/main/scala/io/sm8/core/engine/MCPEngineProvider.scala:43`
  - `MCPEngineRegistry`: 12 callers across 4 modules + 4 test files; class at `sm8-core/src/main/scala/io/sm8/core/engine/MCPEngineRegistry.scala:34`
  - `MCPQueryRequest`: 19 callers across 3 modules + 6 test files; case class at `sm8-core/src/main/scala/io/sm8/core/engine/MCPEngineProvider.scala:157`
  - `ServiceLoader.load`: 1 callsite (`sm8-server/Main.scala:173`)
  - `descriptor.realize(url)`: 1 example callsite (`examples/hospital-cleaning/.../Main.scala:366`)
  - `EngineHookDispatcher.firePost`: `sm8-platform/.../hooks/EngineHookDispatcher.scala:132` — observer-on-stop pattern already implemented via `runsOnStop` flag (PR-94 follow-up)

### External

- **Upstream semanticdf pattern**: `semanticdf.SemanticDimension.of[T]("name")` — phantom-typed witness pattern (per ADR-008-Q §PR-16)
- **Spark**: `org.apache.spark.sql.SparkSession` — closure serialization contract; Spark `ClosureCleaner` walks closure graph + rejects non-Serializable locals (per `scala-spark-batch-bugs-mindset` §1)
- **Restate**: `dev.restate.sdk.Restate.run` — journal capture requires `Serializable` types (forward-looking per C9)
- **Scala 2.13**: case-class `extends Product with Serializable`; `ObjectOutputStream`/`ObjectInputStream` round-trip semantics; trait evolution + binary compat (per Architect P0-2)

### Skills (all 13 re-read from `~/.claude/skills/<name>/SKILL.md`)

`karpathy-guidelines`, `karpathy-app-design`, `scala-bug-hunting`, `scala-chaos-testing`, `scala-data-driven-refactor`, `scala-error-handling`, `scala-impact-analysis`, `scala-jar-packaging`, `scala-jvm-safety`, `scala-perf-testing`, `scala-spark-batch-bugs`, `scala-spark-streaming-bugs`, `debug-mantra`.

## Provenance

- **Reviews**: 2 senior reviews on 2026-08-19 (DE: approved with minor changes; Architect: needs revision). All 21 findings addressed in this rev2 (C1–C21).
- **User trigger**: "do u summarize or write into new ADR docs yet for these all" (2026-08-19); "yes, do u summarize or write into new ADR docs yet for these all" (2026-08-19); "wait shall we also rename MCP... to other? do u think it related to MCP or it's previousely comeframe MCP server??" (2026-08-19); "do u want to do final review again???" (2026-08-19).
- **Per-skill reference**: all 13 skills re-read from `~/.claude/skills/<name>/SKILL.md`
- **Per-RFC reference**: see §References above
- **Per-ADR reference**: see §References above
- **Perf reference**: one-shot `ModelBuildBench` in `sm8-core/src/test/scala/io/sm8/core/bench/` (created + run + removed 2026-08-19; measured 1.18 us/call for `Model.of`, 22 allocs/call, sm8-server startup ≈ 0.03 ms; verdict NEGLIGIBLE per `scala-perf-testing-mindset` §3 — **provenance unfalsifiable per C13**)
- **Closure-safety reference**: `scala-spark-batch-bugs-mindset` §1 (object-level witness = safe; method-local witness = captures enclosing scope; `extends Serializable` at the trait level is the SDK enforcement)
- **Wire-shape reference**: per DE P0-D + C3 — `MCPQueryRequest.dimensions: Seq[String]` is the Restate-journal wire DTO (19 callers); phantom-typed SDK is additive parallel, NOT a wire replacement
- **Restate reference**: per DE P1-D + C9 — `RestatedEngineRunner.runJournaled` is dormant today; `extends Serializable` constraint is forward-looking
