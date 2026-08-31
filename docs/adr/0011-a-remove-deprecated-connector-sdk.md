# ADR-011-a: Remove the deprecated `Connector` SDK surface — production pivoted to `EngineProvider`; legacy Pipeline seam retired with it

| **Status** | **Accepted (v0.2) — dual-reviewer scope validation passed (arch: APPROVE-SCOPE, data-eng: APPROVE-SCOPE); implementation pending** |

## Revision history

| Version | Change |
|---|---|
| v0.1 | Initial draft — Option A/B/C laid out, blast-radius inventory, test-rewrite sketch. |
| v0.2 | Post dual-review: (1) Pipeline.run stub-empty fallback gets explicit replacement types (`PipelineError` / `PipelineSkipped`); (2) EngineSmokeSpec:98 typed-failure-envelope assertion preserved via `PipelineError` (closes P1-A3-E4 regression risk); (3) HookDispatchSpec short-circuit sentinel redefined; (4) 10 doc-only edit sites added to blast radius (2 compile-breaking dead imports); (5) RFC doc updates ride the same PR (architecture-spec §3/§11 + adapters.md note); (6) conformance-contract migration note (RFC §12 → EngineProvider suites); (7) grep gates extended + assertion-count baseline recorded; (8) `sdk/package.scala` ConnectorRequest re-export deleted (arch finding adopted over data-eng's "survives" note — the alias target is itself deleted); (9) house style: revision table + references added. |

## References

- ADR-008-P §AR-P1-7 — deprecation decision + "removed in v1.0.0" promise
- ADR-008-Q — SDK redesign (`EngineProvider` pivot); establishes pre-1.0 API-churn policy
- ADR-001 §P1-3, ADR-006 Post-#65 — original `EngineProvider` selection + typed `realize(url)` refinement
- ADR-010-a — `HookRunnerOrchestration` (the live production pipeline driver this ADR's `Pipeline` trims must stay consistent with)
- RFC `docs/rfcs/2026-08-12_v1_architecture-spec/` — `adapters.md` ("What an Adapter Is"), architecture spec §3 (core boundary), §11 (repo structure), §12 (conformance)
- `docs/project_status/2026-08-15-de-code-review.md:44` — sm8-core test-jar publishes `ConnectorContractSpec` for third-party extension

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
actual surface, mapped by codegraph + grep and validated by both
reviewers:

**SDK types (all in `io.sm8.sdk`):**
- `Connector.scala` — `@deprecated trait Connector` + `trait ConnectorConfig` + `trait SemanticQuery`
- `ConnectorRegistry.scala` — `trait ConnectorRegistry`
- `ConnectorSchema.scala` — `final case class ConnectorSchema`
- `ResultRows.scala` — `final case class ResultRows` (only produced by `Connector.query`)

**Core implementation:**
- `ConnectorRegistryImpl.scala` (core) — the only implementation of the registry trait
- `RequestResult.scala` (core) — `ConnectorRequest` / `ConnectorResult` / `ConnectorError` — **replaced by two minimal types (see Decision)**

**Production-code call sites:**
- `Engine.scala` (SDK) — `def connectors: ConnectorRegistry` (removed)
- `EngineImpl.scala` (core) — `_connectors` field, `connectors` override, `pipeline` wiring (trimmed)
- `Pipeline.scala` (core) — ctor param + `StageEnv.connectors` + `Execute` stage dispatch + stub-empty fallback (trimmed + replaced, see Decision)
- `package.scala` (SDK) — `type ConnectorRequest = io.sm8.core.ConnectorRequest` re-export (deleted; alias target is itself deleted)

**Compile-breaking dead imports (found in review):**
- `Model.scala:21` — `import io.sm8.sdk.SemanticQuery` (unused in file)
- `ModelSpec.scala:11` — same dead import

**Test surface (7 specs + 5 plugin tests):**
- `ConnectorContractSpec` + `StubConnectorSpec` (+ `StubConfig` / `StubQuery` fixtures) — pure Connector-contract fixtures, deleted outright
- `EngineSmokeSpec` — routes via `engine.connectors.register(...)` + `ConnectorRequest` (rewritten; 7 assertions baseline)
- `HookDispatchSpec` — tests HOOK semantics via the Connector seam (rewritten; 5 assertions baseline)
- `TransformerSwapSpec` — tests TRANSFORMER semantics via the seam (rewritten; 5 assertions baseline)
- 5 plugin `*StubSpec` suites (audit / broadcast / materialize / row-cap / skew) — "hook fires once per run" (rewritten; 2+2+2+4+6 assertions baseline)

**Doc-only stale sites (must ride the same PR):**
- `Plugin.scala` scaladoc (registers-Connectors text), `Engine.scala` scaladoc (holds-ConnectorRegistry text)
- `SealedDataType.scala:14` (Connector in the no-SDK-changes enumeration)
- `EngineHookDispatcher.scala:212` (historical AR-P1-7 breadcrumb → cite this ADR)
- `HookManagerImpl.scala:17-19, 57-58` ("same pattern as ConnectorRegistryImpl" caveat → stand alone)
- `sm8-core/README.md` (SDK type table + marker table + MyConnector example) and top-level `README.md` (FlightsConnector example)
- `plugins/audit-plugin/.../AuditStub.scala:18` (stale TrinoConnector breadcrumb)

**RFC doc updates (same PR):**
- `semantic-layer-engine-architecture.md` §3 core table + §11 repo structure: adapter contract realized by `EngineProvider` + `TypedRealizationProvider` (ServiceLoader), not a core-held `AdapterRegistry`
- `adapters.md`: one-line note that the reference implementation of the adapter contract is the `EngineProvider` family

### Why now

ADR-008-P §AR-P1-7 promised removal "in v1.0.0"; the audit-cycle backlog
(2026-08-27 audit, A2 finding) listed it for exactly this purpose. Every
additional month the dual abstraction persists, new code risks wiring
against the dead seam (the `Pipeline` is documented DORMANT in production
but still compiles, still takes `_connectors`, still dispatches
`ConnectorRequest`s — a new contributor cannot tell dormant from live
without reading the scaladoc). ADR-008-P establishes that pre-1.0 API
churn is permitted; we are still pre-v0.1.0.

## Decision

**Full removal of the deprecated `Connector` SDK surface (Option A), with
the fallback `Pipeline` slimmed to its hook/transformer semantics and two
minimal replacement result types preserving the P1-A3-E4 typed-failure
contract.**

Specifically:

1. **Delete** (6 production files + 2 contract-fixture test files):
   `Connector.scala`, `ConnectorRegistry.scala`, `ConnectorSchema.scala`,
   `ResultRows.scala` (all `io.sm8.sdk`); `ConnectorRegistryImpl.scala`,
   `RequestResult.scala` (both `io.sm8.core`);
   `ConnectorContractSpec.scala`, `StubConnectorSpec.scala` (test).
   Delete the `package.scala` `ConnectorRequest` re-export and the two
   dead `SemanticQuery` imports.

2. **Trim the SDK `Engine` trait** — drop `def connectors: ConnectorRegistry`.
   Justification: the RFC `engine.adapters.register(.)` pattern is served
   by `EngineProvider` ServiceLoader discovery (no registration API
   needed); every in-tree plugin registers hooks / transformers only;
   the deprecation-then-removal contract (ADR-008-P §AR-P1-7) is honored,
   and pre-1.0 API churn is explicitly permitted. Reviewers confirmed
   staging is incoherent (`Engine.connectors: ConnectorRegistry` cannot
   compile once the type is gone).

3. **Trim `EngineImpl`** — drop `_connectors` + `connectors` override +
   the `_connectors` argument to `Pipeline`.

4. **Slim `Pipeline` / `StageEnv`** — drop the `connectors` param. The
   `Execute` stage's `ConnectorRequest` dispatch arm is removed. Two
   minimal core-local `Result` subtypes replace the deleted envelope
   (new file `PipelineResult.scala`, `io.sm8.core`):
   - `final case class PipelineError(engine: String, error: EngineError) extends Result`
     — the `Execute` stage's unknown-request arm produces
     `PipelineError("-", EngineError.UnsupportedCapability(engine = "pipeline", capability = "RequestType", ...))`.
     This preserves the EngineSmokeSpec typed-failure-envelope assertion
     (P1-A3-E4: unknown request types must never pass through as silent
     success).
   - `final case class PipelineSkipped(stage: String) extends Result`
     — the `run()` no-result fallback (a hook set `stop = true` before
     `Execute`). This is the NEW short-circuit sentinel that
     HookDispatchSpec asserts on (replacing the old
     `ConnectorResult("", empty, empty)` observation). It is an explicit
     marker, not a silent empty success — the reason field names the
     stage where the pipeline halted.
   Note: with the Connector arm gone, `Execute` always produces
   `PipelineError` in the fallback pipeline (there is no engine dispatch
   left to perform); the stage remains because the 8 hook points
   (pre/post × 4 stages) are load-bearing for the plugin test suites
   and mirror the production `HookRunnerOrchestration` shape (ADR-010-a).

5. **Rewrite 3 live-code specs** to preserve hook / transformer coverage
   without the deprecated seam:
   - `EngineSmokeSpec` → keeps the plugin-registration, forgiving-setup,
     and typed-error-envelope assertions (the latter via `PipelineError`);
     drops connector-routing + duplicate-name assertions (routing is
     owned by `EngineRegistry` specs; the registry API is deleted).
   - `HookDispatchSpec` → priority order / registration-order tiebreak /
     stage-isolation assertions survive verbatim (pure `HookManagerImpl`);
     fail-fast + short-circuit tests switch to `new Request {}` vehicles;
     the short-circuit assertion becomes `PipelineSkipped`-shaped.
   - `TransformerSwapSpec` → auto-activate / swap / setActive-unknown
     assertions survive verbatim (pure `TransformerRegistry`); the two
     pipeline-integration tests ride a `Request {}` vehicle.

6. **Rewrite 5 plugin stub specs** — keep the
   "setup registers exactly one hook at stage X" assertions verbatim;
   replace the "fires once per engine.run" test with the same
   `engine.run(new Request {})` vehicle (hooks fire regardless of request
   type; the assertion is about dispatch semantics, not routing).

7. **Conformance migration note (data-eng finding)** — the RFC §12
   4-assertion Connector conformance contract dies with
   `ConnectorContractSpec`. Future data-source conformance is validated
   by the per-connector `EngineProvider` test suites under `connectors/`
   (typed `realize` grammar tests, `EngineRegistry` routing tests). The
   sm8-core test-jar no longer publishes a Connector contract for
   third-party extension — release notes must flag this.

8. **RFC doc updates ride this PR** (see the doc-sites list above), so
   the repo and its architecture docs never disagree.

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
- Core's data-source knowledge shrinks (RULE#1 strengthened): the
  core-held `ConnectorRegistryImpl` was exactly the "knows WHICH
  database" knowledge RFC §3 forbids in core.

**Negative / risks:**
- **Breaking SDK change** (`Engine.connectors` gone). Third-party
  plugins calling `engine.connectors.register(...)` will not compile.
  Mitigation: release notes flag it; the deprecation contract has been
  honored since ADR-008-P.
- **sm8-core test-jar surface change**: third-party modules can no
  longer extend `ConnectorContractSpec`.
- **Test rewrite risk**: mitigated by the assertion-count baseline
  below; reviewers diff the assertion inventory before/after and any
  dropped assertion is a review blocker.

**Neutral:**
- `Context` / `Request` / `Result` marker traits stay (they carry the
  hook + transformer contracts, which are NOT deprecated).
- `Pipeline` stays (in-tree fallback per RFC §5, canonical reference
  for `HookRunnerOrchestration`).

## Skill alignment

- **scala-impact-analysis**: full blast radius enumerated (11 prod
  files, 7 test specs, 10 doc sites, 2 RFC docs) before any edit; every
  survivor's call sites traced via codegraph + grep; assertion-count
  baseline captured pre-rewrite.
- **scala-data-driven-refactor**: `Request` / `Result` stay pure data;
  `PipelineError` / `PipelineSkipped` are pure data; the deletion
  removes behavior stubs, adds none.
- **scala-error-handling**: the P1-A3-E4 typed-failure contract is
  preserved (`PipelineError` carries `EngineError` in the Result
  envelope); the short-circuit path gets an explicit marker
  (`PipelineSkipped`), not a silent empty success.
- **scala-jvm-safety / closure-safety (ADR-008-ah)**: reviewers verified
  zero Spark-serialization impact — `SparkEngineProvider` implements
  `EngineProvider`, never `Connector`; the deleted types never appear in
  a closure.
- **karpathy-guidelines**: delete the dead extension portal, keep the
  live one.
- **scala2-scaladoc**: rewritten specs and trimmed docs carry no new
  PR/ADR refs; the `@deprecated` promise-text is deleted with the trait
  (historical cites in surviving comments re-point to this ADR).

## Verification plan

- `mvn test` full reactor green post-rewrite.
- **Assertion-count baseline (captured pre-rewrite)**:
  EngineSmokeSpec 7 · HookDispatchSpec 5 · TransformerSwapSpec 5 ·
  plugin specs 2+2+2+4+6 = 16. Post-rewrite inventory must account for
  every baseline assertion as KEPT / REWRITTEN / DROPPED-WITH-REASON;
  any unaccounted drop is a review blocker.
- Grep gates (all must return zero hits in main + test):
  `grep -rn 'io\.sm8\.sdk\.(Connector|ConnectorRegistry|ConnectorSchema|ResultRows|SemanticQuery|ConnectorConfig)' --include='*.scala'`
  and `grep -rn 'ConnectorRegistryImpl\|ConnectorRequest\|ConnectorResult\|ConnectorError\|engine\.connectors' --include='*.scala'`.
- Release notes entry: SDK break (`Engine.connectors`), test-jar
  conformance-surface change, `PipelineResult` replacement types.
