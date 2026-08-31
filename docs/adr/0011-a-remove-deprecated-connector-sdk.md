# ADR-011-a: Remove the deprecated `Connector` SDK surface — production pivoted to `EngineProvider`; legacy Pipeline seam retired with it

| **Status** | **Proposed (v0.1 draft — pending dual-reviewer validation)** |

## Context

The sm8 codebase carries two parallel abstractions for "the thing that connects
the engine to a data source":

1. **Production abstraction — `EngineProvider`** (`io.sm8.core.engine`).
   Chosen in ADR-001 §P1-3 + refined in ADR-006 Post-#65 + renamed in
   ADR-008-Q (SDK redesign). Discovered via ServiceLoader through
   `TypedRealizationProvider` descriptors; realized per-URL by
   `EngineUrlParser` implementations. Every production request flows
   `EngineService → EngineRegistry.select → provider.query`.
2. **Deprecated abstraction — `Connector`** (`io.sm8.sdk`).
   The RFC `adapters.md` "What an Adapter Is" contract
   (`name` / `connect(config)` / `query(semantic_query)` / `schema()`)
   rendered as an SDK trait. Deprecated in place by ADR-008-P §AR-P1-7,
   which explicitly promised removal "in v1.0.0". The trait's own scaladoc
   states it is "retained ONLY for the `ConnectorContractSpec` abstract
   conformance test".

### The blast radius is larger than the original estimate

The original M3 audit estimate priced this as "~9 files deleted". The
actual surface, mapped by codegraph + grep:

**SDK types (all in `io.sm8.sdk`):**
- `Connector.scala` — `@deprecated trait Connector` + `trait ConnectorConfig` + `trait SemanticQuery`
- `ConnectorRegistry.scala` — `trait ConnectorRegistry`
- `ConnectorSchema.scala` — `final case class ConnectorSchema`
- `ResultRows.scala` — `final case class ResultRows` (only produced by `Connector.query`)

**Core implementation:**
- `ConnectorRegistryImpl.scala` (core) — the only implementation of the registry trait
- `RequestResult.scala` (core) — `ConnectorRequest` / `ConnectorResult` / `ConnectorError` (depend on `ConnectorSchema` / `SemanticQuery` / `ResultRows`)

**Production-code call sites (all surviving types must keep compiling):**
- `Engine.scala` (SDK) — `def connectors: ConnectorRegistry`
- `EngineImpl.scala` (core) — `_connectors` field, `connectors` override, `pipeline` wiring
- `Pipeline.scala` (core) — ctor param + `StageEnv.connectors` + `Execute` stage dispatch (`env.connectors.get(...)`), plus the stub-empty-result fallback that builds a `ConnectorResult`
- `package.scala` (SDK) — `type ConnectorRequest = io.sm8.core.ConnectorRequest` re-export

**Test surface (7 specs + 5 plugin tests, all exercising this seam):**
- `ConnectorContractSpec` + `StubConnectorSpec` (+ `StubConfig` / `StubQuery` fixtures) — pure Connector-contract fixtures, deleted outright
- `EngineSmokeSpec` — routes via `engine.connectors.register(...)` + `ConnectorRequest`
- `HookDispatchSpec` + `TransformerSwapSpec` — test HOOK / TRANSFORMER semantics but ride the Connector seam as a vehicle
- 5 plugin `*StubSpec` suites (audit / broadcast / materialize / row-cap / skew) — assert "hook fires once per `engine.run`" through the same seam

### Why now

ADR-008-P §AR-P1-7 promised removal "in v1.0.0"; the audit-cycle backlog
(2026-08-27 audit, A2 finding) listed it for exactly this purpose. Every
additional month the dual abstraction persists, new code risks wiring
against the dead seam (the `Pipeline` is documented DORMANT in production
but still compiles, still takes `_connectors`, still dispatches
`ConnectorRequest`s — a new contributor cannot tell dormant from live
without reading the scaladoc).

## Decision

**Full removal of the deprecated `Connector` SDK surface, with the three
live-code test specs rewritten to exercise hook / transformer semantics
through a minimal in-test `Request`/`Result` shape instead.**

Specifically:

1. **Delete** (7 production files + 2 contract-fixture test files):
   `Connector.scala`, `ConnectorRegistry.scala`, `ConnectorSchema.scala`,
   `ResultRows.scala` (all `io.sm8.sdk`); `ConnectorRegistryImpl.scala`,
   `RequestResult.scala` (both `io.sm8.core`);
   `ConnectorContractSpec.scala`, `StubConnectorSpec.scala` (test).

2. **Trim the SDK `Engine` trait** — drop `def connectors: ConnectorRegistry`.
   This is the only breaking SDK change. It is justified because:
   - the RFC `engine.adapters.register(.)` pattern is already served by
     `EngineProvider` ServiceLoader discovery (no registration API
     needed), per ADR-008-Q;
   - every in-tree plugin registration flows through hooks /
     transformers, not `engine.connectors`;
   - the trait is `@deprecated`-adjacent surface (its only purpose was
     the retired Connector contract).

3. **Trim `EngineImpl`** — drop `_connectors` + `connectors` override +
   the `_connectors` argument to `Pipeline`.

4. **Trim `Pipeline` / `StageEnv`** — drop the `connectors` param; the
   `Execute` stage's `ConnectorRequest` dispatch arm is removed. The
   stage keeps its typed-failure behavior for unknown request types
   (`ConnectorError("-", UnsupportedCapability)` becomes a
   platform-owned equivalent or is dropped with the type).

5. **Rewrite 3 live-code specs** to preserve hook / transformer coverage
   without the deprecated seam:
   - `EngineSmokeSpec` → keeps the plugin-registration / forgiving-setup /
     typed-error assertions; drops connector-routing assertions (the
     routing contract is already owned by `EngineRegistry` specs in the
     platform tests).
   - `HookDispatchSpec` → fires hooks through a minimal `Request {}`
     + a test `Plugin` whose hook writes `ctx.meta`; asserts the same
     stop-flag / priority / short-circuit semantics as today.
   - `TransformerSwapSpec` → transformer registration + swap +
     format-stage invocation, using a test `Result` subtype instead of
     `ConnectorResult`.

6. **Rewrite 5 plugin stub specs** — keep the
   "setup registers exactly one hook at stage X" assertions verbatim;
   replace the "fires once per engine.run" test with a direct
   hook-dispatch invocation (construct `Context`, call
   `engine.hooks`-registered hook through the same fold the pipeline
   uses, count invocations). The hook-fire contract is preserved; the
   dead Connector vehicle is gone.

### Options considered

- **Option A (chosen): full removal + test rewrite.** Single PR, ends
  the dual abstraction, keeps hook/transformer coverage.
- **Option B (rejected): delete only the `Connector` trait itself, keep
  registry + pipeline + tests.** Rejected: leaves `ConnectorRegistry` /
  `ConnectorSchema` / `ResultRows` / `ConnectorRequest` as
  no-consumer orphans (worse than deprecated — they'd be
  not-even-deprecated dead code), and the "removal" wouldn't remove the
  dormant-Pipeline confusion that motivated the audit finding.
- **Option C (rejected): defer to v1.0.0.** Rejected: ADR-008-P
  §AR-P1-7's "v1.0.0" has no date; deferral keeps paying the
  dormant-vs-live confusion tax every PR that touches the SDK.

## Consequences

**Positive:**
- Single data-source abstraction (`EngineProvider`) — contributors can
  no longer wire against the dormant seam.
- `Engine` SDK trait shrinks by one member (smaller frozen surface).
- ~600 LOC of dead production code + orphan fixtures removed.
- The `Execute` stage's dormant dispatch arm disappears; the pipeline
  keeps only what production actually drives via `HookRunnerOrchestration`.

**Negative / risks:**
- **Breaking SDK change** (`Engine.connectors` gone). Third-party
  plugins calling `engine.connectors.register(...)` will not compile.
  Mitigation: the release notes must flag it; the trait was documented
  deprecated-with-removal-since ADR-008-P, so the contract of
  deprecation has been honored.
- **Test rewrite risk**: the 3 rewritten specs must provably preserve
  hook / transformer coverage. Mitigation: reviewers diff the assertion
  inventory before/after; any dropped assertion is flagged as a review
  blocker.

**Neutral:**
- `Context` / `Request` / `Result` marker traits stay (they carry the
  hook + transformer contracts, which are NOT deprecated).
- `Pipeline` itself stays (the in-tree fallback runner per RFC §5 and
  the canonical reference for `HookRunnerOrchestration`).

## RFC alignment

- `adapters.md` "What an Adapter Is": the `name/connect/query/schema`
  contract this PR deletes is the prose contract of that RFC section.
  The RFC's binding rule set (per-connector `realize()` grammar
  validation, typed errors, ServiceLoader registration) survives
  unchanged in the `EngineProvider` family — which is what the RFC's
  Rule 3/4 now actually describe. Post-merge, a one-line doc note in
  `adapters.md` ("the reference implementation of this contract is
  `EngineProvider` + `TypedRealizationProvider`") keeps the RFC honest.
- `semantic-layer-engine-architecture.md` §3 core boundary: core keeps
  zero data-source knowledge — removal *strengthens* this (the
  `ConnectorRegistry` was a core-held registry of source connectors).
- `plugins.md` / `hooks.md`: plugin registration surface
  (`Plugin.setup(engine)` → hooks / transformers) is untouched; the 8
  hook points and `Context` semantics are untouched.

## Skill alignment

- **scala-impact-analysis**: full blast radius enumerated (11 prod
  files, 7 test specs) before any edit; every survivor's call sites
  traced via codegraph + grep.
- **scala-data-driven-refactor**: `Request` / `Result` stay pure data;
  the deletion removes behavior stubs, adds none.
- **scala-error-handling**: the typed-failure contract
  (`EngineUnavailable` / `UnsupportedCapability` in the Result
  envelope) is preserved for the surviving pipeline path; no new
  catch-alls introduced.
- **scala-jvm-safety / closure-safety (ADR-008-ah)**: nothing deleted
  here was on a Spark serialization path (`Connector.query` never ran
  inside a closure; the production path was always `EngineProvider`).
- **karpathy-guidelines**: this IS the karpathy move — delete the dead
  extension portal, keep the one live one.
- **scala2-scaladoc**: rewritten specs carry no new PR/ADR refs;
  `@deprecated` annotation and its promise-text are deleted with the
  trait.

## Verification plan

- `mvn test` full reactor green post-rewrite (baseline 1296 tests;
  expected delta: contract-fixture suites deleted, rewritten suites
  preserve assertion counts).
- Reviewers diff assertion inventories (list of `should` clauses) for
  the 3 rewritten specs + 5 plugin specs before/after.
- Grep gates: zero references to `io.sm8.sdk.Connector`,
  `ConnectorRegistry`, `ConnectorSchema`, `ResultRows`,
  `SemanticQuery`, `ConnectorConfig` anywhere in the repo
  (main + test).

## Status

**Proposed (v0.1)** — pending dual-reviewer validation of:
1. the Option A/B/C choice (is full removal right, or is Option B's
   narrower cut preferred for v0.x?);
2. the test-rewrite plan (do the rewritten specs preserve coverage?);
3. whether the `Engine.connectors` SDK break should ride in this PR or
   be staged (trait deleted but `Engine.connectors` kept one more
   release).
