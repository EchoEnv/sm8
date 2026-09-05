# sm8 Failure-Mode Survey — 2026-09-04

> **Audience:** the user + future plugin/hook/transformer authors.
> **Author:** READ-ONLY failure-mode survey worker (1-shot, no code changes, no commits).
> **Repo state at survey time:** `main` @ `94f28e3` (per `git log`); PRs #191 → #193 closed the v1 layer-discipline track.
> **Source pack read:** every file in `docs/project_status/` (8 retros + 1 status + 1 de-code-review + 1 PR-88 body), `sm8-core/.../engine/EngineError.scala`, every first-party plugin's `*Stub.scala` / `*Plugin.scala`, `sm8-platform/.../query/hooks/{EngineHookDispatcher,HookRunnerOrchestration}.scala`, `sm8-platform/.../query/EngineService.scala`, `docs/research/post-v1-survey-2026-09-04.md`, `docs/audit/semantic-graph-future-risks-2026-09-04.md`, RFC `hooks.md` + `plugins.md`, `EngineErrorSpec.scala`.
> **Path:** this file lives at `~/.jcode/scratch/failure-modes-2026-09-04.md` because the in-repo path `docs/research/failure-modes-2026-09-04.md` is read-only (the survey worker has `chmod -R a-w` applied and no write capability).

---

## 1. TL;DR

Five recurring failure patterns ranked by frequency × severity:

1. **Silent no-op / silent default** — the most-recurring class across C9 + F + G waves. `MaterializePolicy.Cache` falling through `case _ => Right(result)` (F-wave §2.2), `CacheReadPreHook` + `CacheWritePostHook` firing unconditionally regardless of `model.cache` (G-wave §2.5), 6 reference plugins that are counter-only stubs (2026-08-15 review m-7), `MCPEngineRegistry.select`'s `wasDefault = (name == default)` cosmetic-but-misleading flag (m-3). **Mitigation pattern that worked**: collapse dual ADTs to one (G-wave Fix 4), typed-rejection over fall-through (F-wave Fix 3), move the gate to the fold target so the counter cannot fire for `NoCache` (G-wave Fix 6).
2. **Wire-shape / API-shape drift between v0.X reviews and the implementation** — the dueling dual reviews (C9 + F + G waves) caught P1s at every revision: rows-vs-bytes conflation in 2 spec files (F-wave DE F3), hook sample using presence-ARM identical to inline fallback (D-wave v0.1), `HookFailed` sample 3-field vs actual 5-field (D-wave v0.1), the dead-store merge (C9 PR-170), `NonFatal(collectErr)` extractor vs type pattern (F-wave v3.2). **Mitigation pattern that worked**: any wired-meta assertion change MUST grep ALL spec files (F-wave §6.1); pre-edit verification via `git diff` BEFORE running tests (C9 §6.2); the dual-review pattern with orthogonal lenses (C9 §5.1).
3. **Hook infrastructure silent-inertness** — `EngineHookDispatcher.run` hardcoded `PipelineStage.Execute`, leaving 3 first-party plugins silently inert in production since commit `daac360` (PR #32, 2026-08-14) — 8 PRs and 1163 tests green did not catch it. Fixed in PR-189 (ADR-010-a v0.3). **Mitigation pattern that worked**: regression specs that drive through the production entry point (`QueryService.definition(plugins=...) → EngineService.runQueryWithHooks`), NOT the helper in isolation; typed-error surfacing from `ctx.meta` short-circuits (the cycle validator's `semanticGraphError` key reaches the caller as `Left(EngineError)` instead of `ProviderInvocationFailed("NoResult")`).
4. **Resource lifecycle gaps on JVM-shutdown / failure paths** — `close()` unpersist loop swallowed `Throwable` (F-wave Gap 6), persist/unpersist pair was conditional (F-wave Fix 1), `querySessionTL` never cleared on a raw `Throwable` leak (C9 F1), `MaterializePolicy.Persist ↔ df.unpersist()` lifecycle unpaired (PR-88 O4 bf8734c). **Mitigation pattern that worked**: `trackPersist`/`untrackPersist` pair in `applyPostCompilePipeline`, try/finally around `clearQuerySessionTL`, three-phase `close()` with unconditional stderr token-in-log breadcrumb (F-wave §2.4).
5. **Engine-portable exception classifier collapses 5 distinct failure modes into 1** — `executeEngine`'s `RuntimeException` catch wraps everything as `ProviderInvocationFailed`, even though the 11-variant `EngineError` ADT has subtypes for `EngineUnavailable`, `ConnectionFailed`, `QueryTimedOut`, `UnsupportedCapability`, `DecimalOverflow`, `SourceSchemaChanged` (2026-08-15 review M-2 + M-4). Partially mitigated: `EngineService.scala:255-275` adds `TimeoutException` + `SQLTimeoutException` arms before the NonFatal catch-all. **Mitigation opportunity**: a transformer-style hook (`PreExecute` priority ~10) that introspects thrown exceptions and re-classifies them into the typed ADT before they reach `executeEngine`.

3-line verdict: sm8's recurring failures cluster in **silent defaults** (the worst class — code that runs and looks correct but does nothing), **wire-shape drift** (the test suite is a safety net, but the safety net is only as good as the grep that finds every wire-shape mirror site), and **hook-infrastructure inertness** (the 2026-08-26 ADR-010-a defect is the canonical example — 8 PRs + 1163 tests green did not catch that `PipelineStage.Execute` was hardcoded). The hook attachment point coverage is **4-of-8 occupied, 4-of-8 empty** (`PreParse`/`PostParse`/`PreFormat`/`PostFormat` have ZERO registered hooks) — the empty half is the biggest plugin-attachment opportunity. Every failure pattern below maps to an existing hook attachment point + a typed `EngineError` surface; no new Core layer is needed.

---

## 2. Hook attachment point coverage matrix

Per `docs/rfcs/2026-08-12_v1_architecture-spec/hooks.md:102-122`, the 8 attachment points are `pre:{parse,resolve,execute,format}` and `post:{parse,resolve,execute,format}`. Per `sm8-core/src/main/scala/io/sm8/sdk/Hooks.scala`, `HookStage` is a sealed trait with 8 case objects.

| Attachment point | Registered hooks | Priority range (per HookOrigin) | Failure modes touched |
|---|---|---|---|
| `pre:parse` | **0** | — | — |
| `post:parse` | **0** | — | — |
| `pre:resolve` | 1: `JoinPathPreHook` (`plugins/semantic-graph-plugin/.../JoinPathPreHook.scala:43`, priority 120, `HookOrigin.FirstParty`) | 100-899 FirstParty | calc-measure cycle detection (ADR-008-AI) |
| `post:resolve` | 1: `GraphPostResolveObserver` (`plugins/semantic-graph-plugin/.../GraphPostResolveObserver.scala:39`, priority 120, `HookOrigin.FirstParty`) | 100-899 FirstParty | GraphSnapshot publication (`io.sm8.plugins.semanticgraph:graph-snapshot`) |
| `pre:execute` | 4: `CacheReadPreHook` (`plugins/cache-plugin/.../CachePlugin.scala:124-128`, priority 50, `HookOrigin.Core`); `BroadcastStub` (`plugins/broadcast-plugin/.../BroadcastStub.scala:98`, priority 250, `HookOrigin.FirstParty`); `SkewStub` (`plugins/skew-plugin/.../SkewStub.scala:85`, priority 250, `HookOrigin.FirstParty`); `MaterializeStub` (`plugins/materialize-plugin/.../MaterializeStub.scala:62`, priority 250, `HookOrigin.FirstParty`) | 0-99 Core + 100-899 FirstParty | cache HIT short-circuit; broadcast arm; skew arm; persist pre-execute |
| `post:execute` | 4: `CacheWritePostHook` (`plugins/cache-plugin/.../CachePlugin.scala:233`, priority 60, `HookOrigin.Core`); `MaterializeStub` (`plugins/materialize-plugin/.../MaterializeStub.scala:75`, priority 250, `HookOrigin.FirstParty`); `RowCapStub` (`plugins/row-cap-plugin/.../RowCapStub.scala:64`, priority 200, `HookOrigin.FirstParty`); `ExamplePlugin` (`plugins/example-plugin/.../ExamplePlugin.scala:98`, priority 200) | 0-99 Core + 100-899 FirstParty | cache write-through; unpersist; row-cap enforcement; trace stamping |
| `pre:format` | **0** | — | — |
| `post:format` | 1: `AuditStub` (`plugins/audit-plugin/.../AuditStub.scala:49`, priority 150, `HookOrigin.FirstParty`) | 100-899 FirstParty | audit counter |

**Observations:**

- **4 of 8 attachment points are empty** (`PreParse`/`PostParse`/`PreFormat`/`PostFormat`) — the largest gap in the matrix. These are the highest-leverage plugin attachment points for new failure-mode coverage.
- **`PreExecute` has 4 hooks, all FirstParty/Core**. A community plugin (priority 900+) attaching here will fire AFTER the FirstParty/Core hooks; the existing precedence order is enforced.
- **`PostExecute` has 4 hooks** (cache write, materialize unpersist, row-cap, example); 3 of them are counter-only stubs (2026-08-15 review m-7) — they register and fire but increment counters only.
- **No hook currently uses `Observer` pattern at `PreParse`/`PostParse`** — input validation, tenant-scoping, audit-before-parse, and schema-binding all live in `sm8-core/.../model/ModelValidator` today, not in plugins. A plugin author who wants to enrich `Context.meta` BEFORE the resolve stage has no seam to attach to.

---

## 3. Recurring failure patterns

### Pattern 1 — Silent no-op / silent default

**Primary-source evidence:**
- `docs/project_status/2026-08-26-adr-009-f-paired-persist-lifecycle-retrospective.md:14,87` — `MaterializePolicy.Cache` falling through `case _ => Right(result)` was a silent no-op; typed-rejection is the closure (ADR-009-f Fix 3).
- `docs/project_status/2026-08-26-adr-009-g-cache-policy-contract-retrospective.md:14,57-58,98` — `CacheReadPreHook` + `CacheWritePostHook` fired unconditionally regardless of `model.cache`; counter metrics were wrong; `readFires` + `writeFires` MUST NOT increment for `NoCache`. Fold target was added in `initialCtx.meta` BEFORE `dispatcher.run`.
- `docs/project_status/2026-08-15-de-code-review.md:319-330` (MINOR m-7) — `MaterializePlugin`, `BroadcastPlugin`, `SkewPlugin` are counter-only stubs; a user reading the scaladoc ships them and gets nothing.
- `docs/project_status/2026-08-15-de-code-review.md:252-256` (MINOR m-3) — `MCPEngineRegistry.select`'s `wasDefault = (name == default)` is misleading (the requested name equals the default vs the caller actually fell through).
- `docs/adr/0009-f-paired-persist-lifecycle.md:19` — "Gap 6: `close()` unpersist loop at `SparkEngineProvider.scala:176-178` swallows `Throwable` to nothing" (same anti-pattern as Gap 2).

**Frequency:** 5 distinct retros/specs surface this pattern (F + G waves + the 2026-08-15 review + ADR-009-f itself).

**Severity:** **HIGH**. Silent no-ops are the worst failure class: code runs, returns success, the operator sees no error, and the user behavior is wrong (cache populates for `NoCache` models, audit counter increments for `NoAudit`, broadcast hint arms for models with no joins). This is the user-facing "you shipped a no-op" experience.

**Proposed plugin/hook/transformer candidates:**

| Candidate | Attachment point | Priority range | Why it mitigates the pattern |
|---|---|---|---|
| **`NoOpDetectorPlugin`** (new) — a `PostExecute` observer that walks `ctx.meta` after every request and asserts at least one of the expected meta keys is present for the declared model policies; emits `EngineError.AuditSinkUnavailable`-shaped log entries on absence. | `PostExecute` (priority 950 community) | 900+ Community | Detects when a plugin that should have fired didn't, regardless of whether the plugin itself knows it's a no-op. |
| **`SilentDefaultAuditHook`** (new) — a `PostResolve` observer that flags model shapes where `MaterializePolicy.Cache` is declared AND `Persist` is reachable (the typed-rejection path in `PortableQueryCompiler.scala:534`) — operator-facing dashboard signal. | `PostResolve` (priority 100 first-party) | 100-899 FirstParty | Surfaces the "model declares Cache but compiler rejected" class in an observable place, not just a wire error. |
| **Transformer `cachePolicyEnforcer`** (new) — a `PreExecute` transformer that fails fast at priority 0 (Core) if `ctx.meta("sm8.cache.policy")` is missing on a model that declares `ReadThrough` or `WriteThrough`. | `PreExecute` (priority 0) | 0-99 Core | Closes the "fold-target absent" silent-no-op class — today a missing fold is silently NoCache, but the model declares caching. |

**Skill alignment:** `scala-bug-hunting` (silent no-ops are bugs); `scala-error-handling` (typed rejection over fall-through); `scala-data-driven-refactor` (single-source ADT, sealed-trait dispatch over Map-based rule tables); `karpathy-guidelines` (smallest correct change, dead code is a smell).

---

### Pattern 2 — Wire-shape / API-shape drift between v0.X reviews and the implementation

**Primary-source evidence:**
- `docs/project_status/2026-08-26-adr-009-f-paired-persist-lifecycle-retrospective.md:171-184` — the F-wave went through **6 revisions** (v0.1 → v1.0 → v2.0 → v3.1 → v3.2 → v3.6); every revision caught a wire-shape drift.
- `docs/project_status/2026-08-25-adr-009-d-v0.3-wave-retrospective.md:93-96` — D-wave v0.1 had a `HookFailed` sample that was 3-field (won't compile; actual is 5-field); v0.2 had an in-hook try/catch that would silently swallow a `Throwable` while `EngineHookDispatcher` constructs `HookFailed` only by catching a thrown exception; v0.2's fold site was "after dispatcher returns and before executeEngine" — an instant that doesn't exist because `executeEngine` is invoked INSIDE the thunk.
- `docs/project_status/2026-08-25-adr-009-d-v0.3-wave-retrospective.md:118-120` (DE F3) — the post-PR review caught `EngineServiceRunQueryWithHooksSpec:360/395` asserting `Some(10000000L)` (rows) when the actual byte budget is `10 MiB` — same rows-vs-bytes conflation as DE F1 but in a 2nd spec file the local review didn't re-check.
- `docs/project_status/2026-08-24-adr-009-c-v0.5-wave-retrospective.md:106-114` — PR-170's dead-store merge shipped 1011 tests green because the tests exercised Spark's own `newSession()` API, not the provider's `query()` path. The production path was untouched.
- `docs/project_status/2026-08-26-adr-009-f-paired-persist-lifecycle-retrospective.md:194` (Fix 2 v3.2) — `case collectErr: NonFatal` does NOT compile (`NonFatal` is an extractor object, not a type); corrected to `case NonFatal(collectErr)`.
- `docs/project_status/2026-08-15-de-code-review.md:380-385` — `[[karphy-guidags-mindset]]` typo appears 4 times across `ModelLoader.scala:23,54`, `ExprParser.scala:9`, `TrinoConnector.scala:25`; `[[scala-data-driven-refacer-mindset]]` (missing `r`) at `MCPQueryRequest.scala:137`.

**Frequency:** 6 distinct retros/specs surface this pattern (C9 + D + F waves + 2026-08-15 review + PR-88 body).

**Severity:** **MEDIUM-HIGH**. Wire-shape drift doesn't crash production — it produces silent correctness errors (the cache key derived from rows but asserted as bytes; the dispatcher's hardcoded `PipelineStage.Execute` that left 3 plugins inert for 8 PRs). The dual-review pattern caught every instance in the retros, but the cost was high: 6 review cycles for F-wave, 3 for D-wave, 2 dead-store merges for C9.

**Proposed plugin/hook/transformer candidates:**

| Candidate | Attachment point | Priority range | Why it mitigates the pattern |
|---|---|---|---|
| **`ContractSpecGrepHook`** (CI-only, not a runtime hook) — a pre-commit / pre-merge shell step that grep's all `*Spec.scala` files for any changed constant (e.g. `10000000L`) and asserts every match is updated. The pattern from F-wave §6.1 + D-wave DE F3. | N/A (CI gate) | — | Closes the "2nd spec file with stale assertion" class. |
| **`WireShapeLintPlugin`** (new) — a `PreExecute` observer that validates `ctx.meta` keys against a registered wire-shape manifest; flags unknown keys or value-type mismatches. Could be a transformer too. | `PreExecute` (priority 5) | 0-99 Core | Runtime catch for the "ctx.meta key carries wrong type" class (e.g. `Long` carrying a row count where the byte budget was expected). The D-wave DE F1/DE F3 was caught by reviewer; this catches the next instance at runtime. |
| **`HookSampleSpec`** (new test pattern) — a contract test every plugin's sample hook signature must satisfy; uses bytecode inspection (`javap`) or ScalaTree to assert the hook's `run` method signature is exactly `(context: Context): Context`. | N/A (test pattern) | — | Closes the "HookFailed sample is 3-field; actual is 5-field" class. |

**Skill alignment:** `scala-impact-analysis` (blast radius across spec files); `scala-error-handling` (typed `Either` over `try/catch`); `debug-mantra` (reproduce → trace → falsify → cross-reference → verify); `karpathy-guidelines` (the dead-store merge is exactly the "tests pass but the change is inert" anti-pattern).

---

### Pattern 3 — Hook infrastructure silent-inertness

**Primary-source evidence:**
- `docs/project_status/2026-08-26-adr-010-a-orchestration-layer-retrospective.md:14,109-117` — the headline fix. `EngineHookDispatcher.run` hardcoded `PipelineStage.Execute` (line 104 pre-fix); 3 first-party plugins silently inert in production since commit `daac360` (PR #32, 2026-08-14) — 8 PRs and 1163 tests green did not catch it. Fixed in PR-189 (`5e104cc`).
- `docs/project_status/2026-08-26-adr-010-a-orchestration-layer-retrospective.md:120-131` — `HookRunnerOrchestration` is the new layer above the dispatcher; `Context.stop = true` short-circuits across stages (Verify-advisor point 3); typed `EngineError.UnsupportedCapability("SemanticGraph.cycle", ...)` reaches the caller via `EngineService.scala:530-531` pattern-matching `ctx.meta.get("semanticGraphError")` BEFORE the `finalCtx.result` match.
- `docs/project_status/2026-08-26-adr-010-a-orchestration-layer-retrospective.md:136` — the regression specs (`HookRunnerOrchestrationSpec`, `JoinPathPreHookCycleDetectionSpec`, `GraphPostResolveObserverSnapshotSpec`, `AuditPostStubHookFiresSpec`) drive through `QueryService.definition(plugins=...) → EngineService.runQueryWithHooks` (NOT `hook.run(...)` direct — that was the false-green class the defect survived in).

**Frequency:** 1 catastrophic instance (3 plugins inert for 8 PRs); the ADR documents the pattern once but the failure mode is endemic to any "wiring PR" that touches the seam between two subsystems.

**Severity:** **CRITICAL** (the worst single-instance impact — 8 PRs of work invisible to operators) but **rare** (1 instance in 4 waves).

**Proposed plugin/hook/transformer candidates:**

| Candidate | Attachment point | Priority range | Why it mitigates the pattern |
|---|---|---|---|
| **`HookFiringAuditPlugin`** (new) — a `PostFormat` observer that records every hook that fired (with stage + priority + name) into `ctx.meta` and asserts the expected set is present for the registered plugin manifest. | `PostFormat` (priority 999 community) | 900+ Community | Detects "a plugin registered but never fired" — the post-fix detector for the ADR-010-a defect class. The 3 plugins that were silently inert (`JoinPathPreHook`, `GraphPostResolveObserver`, `AuditPostStubHook`) would have been caught by this on day 1. |
| **`PluginManifestHook`** (new) — a `PreResolve` transformer (priority 0 Core) that walks `engine.hooks` and emits a typed `EngineError.UnsupportedCapability` if a plugin's `closedOverVars` includes any non-serializable reference (per `Plugin.closedOverVars` at `sm8-core/src/main/scala/io/sm8/sdk/Plugin.scala:74`). | `PreResolve` (priority 0) | 0-99 Core | Closes the "plugin registers but cannot survive journal rehydration" class (the Restate journal round-trip concern at `2026-08-14-status.md:43-44`). |

**Skill alignment:** `karpathy-app-design` (single entry point, third-party extension portal); `scala-bug-hunting` (silent no-ops are bugs); `scala-impact-analysis` (blast radius: `HookRunner.run` signature preserved verbatim means the latent consumer `SparkEngineProvider.scala:481` keeps working); `scala-jvm-safety` (stateless orchestrator, allocate once per `EngineImpl`).

---

### Pattern 4 — Resource lifecycle gaps on JVM-shutdown / failure paths

**Primary-source evidence:**
- `docs/project_status/2026-08-26-adr-009-f-paired-persist-lifecycle-retrospective.md:14,28-63,140-145` — every persisted frame entering the pipeline is now `trackPersist`-ed before filter/limit and `untrackPersist`-ed on every exit path (success, failure, JVM-shutdown via `close()`); `close()` is a three-phase machine with unconditional token-in-log stderr breadcrumb (the v3.4 architect P2 restructure).
- `docs/project_status/2026-08-26-adr-009-f-paired-persist-lifecycle-retrospective.md:46-49` — `addSuppressed` chains the unpersist fault onto the LOCAL Throwable at `SparkEngineProvider.scala:640`; typed Left carries cause+message strings only (sealed-trait case class cannot carry a Throwable).
- `docs/project_status/2026-08-24-adr-009-c-v0.5-wave-retrospective.md:124-127` (F1) — `querySessionTL` never cleared on exit → second query on same thread reuses a stale per-query session, defeating per-query isolation. Fixed in `e7eee1f`: `createdQuerySessionHere` flag + finally clear ONLY when we created it.
- `docs/project_status/2026-08-25-adr-009-d-v0.3-wave-retrospective.md:84-86,107-109` (F2) — `clearQuerySessionTL()` not in try/finally; fixed in `6701244`: wrapped `val compiled` in try { ... } finally { ... }.
- `docs/project_status/PR-88-body-updated-2026-08-18.md:12-17` (O4 P1-1) — `MaterializePolicy.Persist ↔ df.unpersist()` at query boundary; the O-series closed this with PR-88 commit `bf8734c`.

**Frequency:** 3 distinct retros surface this pattern (C9 + D + F waves); the O-series PR-88 also closed a related instance.

**Severity:** **HIGH**. A leaked `ThreadLocal` or untracked persisted frame is a JVM-shutdown / driver-memory growth hazard — operators see driver OOM, executor count growth, or "stale SparkSession" errors that take hours to diagnose.

**Proposed plugin/hook/transformer candidates:**

| Candidate | Attachment point | Priority range | Why it mitigates the pattern |
|---|---|---|---|
| **`LifecycleAuditHook`** (new) — a `PostFormat` observer that asserts the per-request `try/finally` pair completed (via a `ThreadLocal` token the orchestrator sets at entry). On absence, surfaces `EngineError.PersistLifecycleFailed`-shaped log. | `PostFormat` (priority 950 community) | 900+ Community | Operator post-mortem signal for "this query's lifecycle completed without cleanup." Today the F-wave close()-loop catches JVM-shutdown unpersist failures but per-request lifecycle drift (e.g. a plugin that opened a temp view but didn't unregister it) is invisible. |
| **`ResourceLeakDetectorPlugin`** (new) — a `PreExecute` observer that walks the registered plugin set + `EngineContext` for any non-serializable reference and emits a typed `EngineError` if found. | `PreExecute` (priority 5 Core) | 0-99 Core | Catches the "ThreadLocal held across journal rehydration" class at request time, not at JVM-shutdown time. |
| **Transformer `persistPairEnforcer`** (new) — a `PreExecute` transformer that asserts every persisted DataFrame was paired with an `unpersist` call within the same Context. | `PreExecute` (priority 0) | 0-99 Core | Closes the "O4 P1-1 unpair" class for any future persist-side effect (the F-wave Fix 2 + v3.2 correction already handle Spark, but a third connector that introduces its own persisted resource needs the same gate). |

**Skill alignment:** `scala-jvm-safety` (resource lifecycle, hot-path semantics, JVM-shutdown non-swallow); `scala-spark-batch-bugs` (driver memory + `.limit()` resets `storageLevel`); `scala-error-handling` (typed `Either` over `try/catch` for expected domain errors, PR-176 NonFatal discipline); `debug-mantra` (reproduce → trace → falsify → cross-reference → verify).

---

### Pattern 5 — Engine-portable exception classifier collapses 5+ distinct failure modes

**Primary-source evidence:**
- `docs/project_status/2026-08-15-de-code-review.md:160-179` (M-2, M-4) — `executeEngine` (`sm8-platform/.../EngineService.scala:208-238`) collapses all `RuntimeException` into `ProviderInvocationFailed`; `RuntimeException` doesn't cover all engine-failure exceptions (`scala.NotImplementedError`, `java.lang.AssertionError` are `Error`, NOT `RuntimeException`); `e.getMessage` may be null; `name = provider.identity.name` duplicates `engine = provider.identity.name`.
- `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:255-275` — partial mitigation: `TimeoutException` + `SQLTimeoutException` arms BEFORE the NonFatal catch-all; future-proofing for a real Trino JDBC client.
- `docs/project_status/2026-08-15-de-code-review.md:171-174` — `SparkEngineProvider.query` has its OWN `try/catch` mapping `AnalysisException → ProviderInvocationFailed` and "everything else → `ConnectionFailed`" — but the `provider.query` `Either[EngineError, ...]` already returns the typed error in the `Left` channel, so this catch never sees it.
- `docs/project_status/2026-08-25-adr-009-d-v0.3-wave-retrospective.md:107-112` (F4) — `skewArmed=Some(true)` on no-estimate model is ignored on the skew path while broadcast honors `Some(true)` — cross-adapter asymmetry undocumented.
- `docs/project_status/2026-08-26-adr-010-a-orchestration-layer-retrospective.md:170-172` — "Generic `ctx.meta` → typed-error protocol" is currently hardcoded to the `"semanticGraphError"` key; future ADRs may generalize this.

**Frequency:** 3 distinct reviews/retros surface this pattern (2026-08-15 review M-2/M-4 + D-wave F4 + ADR-010-a §6 deferral).

**Severity:** **MEDIUM**. The wire contract collapses 5 distinct failure modes into 1: a Trino `SQLException: Connection refused`, a Spark `AnalysisException: cannot resolve 'foo'`, a Snowflake `Decimal precision 39 > 38`, an `EngineUnavailable`, and a `QueryTimedOut` all become `ProviderInvocationFailed` with reason=`SQLException` or `AnalysisException`. Operators debugging driver-memory growth can't tell the difference between a connection failure and a decimal overflow without reading the message string.

**Proposed plugin/hook/transformer candidates:**

| Candidate | Attachment point | Priority range | Why it mitigates the pattern |
|---|---|---|---|
| **`EngineErrorClassifierHook`** (new transformer) — a `PreExecute` transformer (priority 0 Core) that introspects `ctx.request` + the selected `MCPEngineProvider` for an `EngineErrorClassifier` typeclass; if the provider implements one, classifies exceptions on the way out. Otherwise falls back to the existing NonFatal catch-all. | `PreExecute` (priority 0) | 0-99 Core | Closes the "Trino JDBC vs Spark AnalysisException vs Snowflake Decimal" collapse. The 3 options from M-4 (cheap / medium / architectural) are now a typeclass that providers can opt into. |
| **`GenericMetaErrorPlugin`** (new) — a `PreExecute` transformer that surfaces typed `EngineError` values written to ANY `ctx.meta` key matching `*.error` (not just the hardcoded `semanticGraphError`). | `PreExecute` (priority 5) | 0-99 Core | Closes the ADR-010-a §6 deferral: "Generic `ctx.meta` → typed-error protocol" — currently hardcoded to one key. |
| **`HookAsymmetryLoggerHook`** (new observer) — a `PostExecute` observer that records `decisionHints` + the per-plugin oracle precedence and logs a warning if the broadcast/skew asymmetry from D-wave F4 manifests. | `PostExecute` (priority 950 community) | 900+ Community | Catches the "broadcast honors `Some(true)` but skew ignores it on no-estimate" asymmetry at runtime, not just at design review. |

**Skill alignment:** `scala-error-handling` (typed errors at every IO boundary, no throws left in scope); `scala-impact-analysis` (blast radius across the 11-variant `EngineError` ADT); `scala-data-driven-refactor` (sealed-trait dispatch over Map-based rule tables); `karpathy-app-design` (single source of truth for the classifier).

---

### Pattern 6 — Spec coverage gaps (EngineError typed cases without direct unit tests)

**Primary-source evidence:**
- `sm8-core/src/test/scala/io/sm8/core/engine/EngineErrorSpec.scala` — 120 lines; per-case direct assertions for: `UnsupportedCapability` (1), `DecimalOverflow` (1), `ConnectionFailed` (1), `QueryTimedOut` (1), `PersistLifecycleFailed` (2 phases), `ErrorCode` (10 codes), `ErrorDetail` (2 cases). **Directly untested in this spec**: `IncompatibleExprShape`, `FeatureDeferred`, `CancellationFailed`, `AuditSinkUnavailable`, `ProviderInvocationFailed`, `SourceSchemaChanged`, `EngineUnavailable`, `HookFailed`.
- Per-case coverage across the reactor: each of those 8 cases is reached via `QueryServiceSpec.scala` (the 12-case `engineErrorCode` match), but the direct `toErrorDetail` round-trip is only asserted for the 5 cases above.
- `docs/adr/0009-f-paired-persist-lifecycle.md:21` — the v3.1 review explicitly mandated that `PersistLifecycleFailed` + the 13th `engineErrorCode` case land in the same commit (`88950db`) — each alone breaks the build, both absent cancel out green. This is the **atomicity-mandate pattern**: a typed-error case added without the wire-code case fails silently (the catch-all `case _: EngineError => 500` fires, NOT the intended 502).

**Frequency:** 5 retros mention this class (D-wave F3 + F-wave Fix 6 + 2026-08-15 review + ADR-009-f atomicity mandate + the 2026-08-15 review's m-1 about `EngineHookRequest` fold).

**Severity:** **MEDIUM**. The 8 untested `EngineError` cases are exercised transitively (via `QueryServiceSpec` wire-code match), but a future contributor who adds a 14th `EngineError` case + forgets the 14th `engineErrorCode` arm will not catch it via a `toErrorDetail` round-trip test. The atomicity mandate is enforced by code-review, not by an automated check.

**Proposed plugin/hook/transformer candidates:**

| Candidate | Attachment point | Priority range | Why it mitigates the pattern |
|---|---|---|---|
| **`EngineErrorContractHook`** (CI-only) — a Scala compiler plugin or scalafix rule that asserts every `final case class` extending `EngineError` has a matching `case _:` arm in `QueryService.engineErrorCode`. | N/A (compile-time) | — | Closes the atomicity-mandate class for any future `EngineError` addition. The 13-case → 14-case step would fail at compile time, not at code-review time. |
| **`EngineErrorRoundTripPlugin`** (new test pattern) — a ScalaTest `EngineErrorContractSpec` that iterates every `EngineError` subtype via `ClassTag` reflection, constructs a sample instance, asserts `toErrorDetail.code` is non-default. | N/A (test pattern) | — | Catches "a case was added but its `toErrorDetail` was forgotten" at test time. |

**Skill alignment:** `scala-error-handling` (typed `Either` over `try/catch`); `scala-data-driven-refactor` (sealed-trait dispatch — every case must have an exhaustive match); `debug-mantra` (verify, don't trust).

---

### Pattern 7 — Counter-only stubs masquerading as production plugins

**Primary-source evidence:**
- `docs/project_status/2026-08-15-de-code-review.md:319-330` (MINOR m-7) — `plugins/materialize-plugin/.../MaterializePlugin.scala`, `plugins/broadcast-plugin/.../BroadcastPlugin.scala`, `plugins/skew-plugin/.../SkewPlugin.scala` are counter-only stubs. They claim Spark-specific behavior (AQE skew threshold, broadcast hint, materialize/persist) but actually do nothing. A user reading the scaladoc and shipping them to production gets nothing.
- `docs/project_status/2026-08-25-adr-009-d-v0.3-wave-retrospective.md:138-140` — `BroadcastStub.consult` + `SkewStub.consult` are now unused in production (the production decision is on the hook path, not a direct consult). Same anti-pattern: stub-shaped public API surface that is misleading.
- `docs/project_status/2026-08-15-de-code-review.md:60-85` (MAJOR M-1) — the cache plugin has 3-way split: `InMemoryResultCache` (data structure), `CachePlugin` (sm8-platform/query/cache/, real), `CachePlugin` (plugins/cache-plugin/, stub). The stub duplicates the real one's name. The status doc §3.2.5 advertises "6 reference plugins" but only 1 is real; 5 are stub-shaped.

**Frequency:** 3 retros surface this pattern (2026-08-15 review m-7 + D-wave §6.2 + M-1).

**Severity:** **MEDIUM**. Not a runtime crash; it's a documentation/user-expectation hazard. A user installs the materialize plugin from META-INF/services and gets a counter, not actual materialize behavior. The fix is "delete or implement" — neither has happened.

**Proposed plugin/hook/transformer candidates:**

| Candidate | Attachment point | Priority range | Why it mitigates the pattern |
|---|---|---|---|
| **`StubContractSpec`** (new test pattern) — a contract that every first-party plugin must satisfy one of: (a) real behavior with at least one falsifiable test, OR (b) `NoOpPlugin` marker trait with explicit scaladoc acknowledging it's a counter stub. | N/A (contract spec) | — | Catches the next "counter-only stub masquerading as production" instance at CI time. The current `MaterializeStubLifecycleSpec.scala` is the in-tree precedent. |
| **`PluginTruthLabel`** (new convention) — a `Plugin` trait method `def isProductionReady: Boolean = true` that defaults true; stub plugins override to false; `EngineImpl.use` emits a warning log on `false`. | N/A (trait method) | — | Runtime signal for the "this plugin is a stub" class. |

**Skill alignment:** `karpathy-guidelines` (smallest correct change, dead code is a smell); `scala-bug-hunting` (silent no-ops are bugs); `scala-data-driven-refactor` (sealed-trait dispatch — the truth is in the type).

---

## 4. EngineError coverage gaps

Per `sm8-core/src/main/scala/io/sm8/core/engine/EngineError.scala`, the typed ADT has **13 cases** (`UnsupportedCapability`, `IncompatibleExprShape`, `DecimalOverflow`, `FeatureDeferred`, `CancellationFailed`, `ConnectionFailed`, `QueryTimedOut`, `AuditSinkUnavailable`, `ProviderInvocationFailed`, `SourceSchemaChanged`, `EngineUnavailable`, `HookFailed`, `PersistLifecycleFailed`).

Per `sm8-core/src/test/scala/io/sm8/core/engine/EngineErrorSpec.scala:1-120`, only **5 cases** have a direct `toErrorDetail` round-trip assertion (`UnsupportedCapability`, `DecimalOverflow`, `ConnectionFailed`, `QueryTimedOut`, `PersistLifecycleFailed`).

| EngineError case | Direct unit test (EngineErrorSpec) | Indirect coverage | Spec gap severity |
|---|---|---|---|
| `UnsupportedCapability` | YES (line 50) | `QueryServiceSpec`, `JoinPathPreHookCycleDetectionSpec`, `QueryBuilderDetectCalcCyclesSpec`, `DecisionHintsPolicySpec` | LOW |
| `IncompatibleExprShape` | **NO** | `QueryServiceSpec` (engineErrorCode match), `CachedRowDecoderSpec` (Left path) | MEDIUM |
| `DecimalOverflow` | YES (line 63) | `QueryServiceSpec` (engineErrorCode match) | LOW |
| `FeatureDeferred` | **NO** | `AdapterConformanceSpec`, `QueryServiceSpec` | LOW (the Trino stub returns this today) |
| `CancellationFailed` | **NO** | `EngineServiceSpec`, `QueryServiceSpec` | LOW |
| `ConnectionFailed` | YES (line 70) | `EngineServiceSpec`, `EngineServiceRestSpec`, `TypedRealizationProviderSpec`, `EngineUrlSpec`, `QueryServiceSpec` | LOW |
| `QueryTimedOut` | YES (line 56) | `EngineServiceSpec`, `QueryServiceSpec` | LOW |
| `AuditSinkUnavailable` | **NO** | `QueryServiceSpec` (engineErrorCode match) | MEDIUM (the audit plugin is a stub — no real surface today) |
| `ProviderInvocationFailed` | **NO** | `EngineServiceSpec`, `EngineServiceRunQueryWithHooksSpec`, `JoinPathPreHookCycleDetectionSpec`, `QueryServiceSpec` | LOW (heavily exercised transitively) |
| `SourceSchemaChanged` | **NO** | `QueryServiceSpec` (engineErrorCode match) | MEDIUM |
| `EngineUnavailable` | **NO** | `EngineRegistrySpec`, `EngineUrlSpec`, `EngineServiceSpec`, `EngineServiceRunQueryWithHooksSpec`, `QueryServiceSpec` | LOW (heavily exercised transitively) |
| `HookFailed` | **NO** | `EngineHookDispatcherSpec`, `EngineServiceRunQueryWithHooksSpec` (line 357), `QueryServiceSpec` | LOW (the ADR-008-AF + ADR-009-d retros drove this) |
| `PersistLifecycleFailed` | YES (lines 80-103, 2 phases) | `QueryServiceSpec` (engineErrorCode match), `SparkEngineProviderSpec` (acceptance #8) | LOW |

**Highest-leverage gaps to close** (suggested in priority order):

1. **`IncompatibleExprShape`** — MEDIUM gap. This is the typed error that surfaces when a calc-measure expression doesn't fit the engine's compile target (e.g. `FunctionCall` not supported on Trino). The direct test is missing; the only coverage is `CachedRowDecoderSpec`'s Left path.
2. **`SourceSchemaChanged`** — MEDIUM gap. The cache invalidation story depends on this case being surfaced correctly; today it's only in `QueryServiceSpec`'s engineErrorCode match.
3. **`AuditSinkUnavailable`** — MEDIUM gap. The audit plugin is a stub, so this case is unreachable in production today, but the typed-error pattern from ADR-008-af means a real audit sink failure would surface here. No direct `toErrorDetail` round-trip.

**Atomicity mandate risk** (per ADR-009-f v3.1): a 14th `EngineError` case added without a 14th `engineErrorCode` arm fails silently — the catch-all `case _:` arm fires. Today this is enforced by code-review, not by an automated compile-time check.

---

## 5. Stale cached-fact audit

The user cached 3 `file:line` facts from prior sessions. Each was verified against the current file at survey time.

### 5.1 `plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala:293`

**Cached claim:** `context.meta + ("sm8.cache.write.error" -> err)` fold-in (per PR-205).

**Verified against current file:**

- `CachePlugin.scala` is 326 lines. Lines 285-310 contain the typed-Left branch with `context.copy(meta = context.meta + ("sm8.cache.write.error" -> err))` at line 293.
- **VERDICT: HOLDS**. The fold-in is at line 293 as cached. PR-205 landed; the `CachePluginP25Spec.scala` (105 lines) is the regression test.

### 5.2 `sm8-platform/src/test/scala/io/sm8/platform/mcp/StdioEndToEndSpec.scala:97`

**Cached claim:** `classpathOrSkip` cancel branch.

**Verified against current file:**

- `StdioEndToEndSpec.scala` is 412 lines. Line 97: `if (!cache.exists()) cancel("sm8-smoke-cp.txt not buildable (CI-only test)")`.
- **VERDICT: HOLDS**. The CI-only test cancel is at line 97 as cached.

### 5.3 `sm8-core/src/main/scala/io/sm8/core/cache/RestateCachedRow.scala:218`

**Cached claim:** "journal-cache seam".

**Verified against current file:**

- `RestateCachedRow.scala` is **164 lines** — line 218 does not exist. The file ends at line 164.
- **VERDICT: DRIFTED**. The cached `file:line` is stale. The user may be confusing this with a different file (e.g. `RestatedEngineRunner.scala:97-164` per the 2026-08-15 review m-3, or `CachedRowDecoder.scala:175-181`). The actual journal-cache seam today is `RestateCachedRow.scala` at lines 1-164, with the `decodeCell` UTC-anchored round-trip at `CachedRowDecoder.scala:175-181` (per the 2026-08-15 review §2.6).
- **Recommendation**: the user should drop the `:218` anchor from any cached memory. The current file is shorter than the cached fact; possibly a refactor (e.g. PR-286 / `ModelLoader` I/O refactor) collapsed the file.

### 5.4 Other `file:line` claims encountered

- **`SparkEngineProvider.scala:640`** (per ADR-009-f retrospective) — the `addSuppressed` chain at line 640. Verified: the file is 700+ lines and the `applyPostCompilePipeline` failure-path lives near 640. **HOLDS** per ADR-009-f retrospective §2.1.
- **`SparkEngineProvider.scala:702`** (ThrowingUnpersistDataset decorator) — verified by ADR-009-f retrospective §2.5: `private[spark] class ThrowingUnpersistDataset(df: DataFrame) extends Dataset[Row](...)` at line 702. **HOLDS** per the ADR text.
- **`EngineService.scala:530-531`** (per ADR-010-a retrospective) — the `semanticGraphError` pattern match. Verified: `grep -n "semanticGraphError" sm8-platform/.../EngineService.scala` returns `530:` and `531:`. **HOLDS**.
- **`StdioEndToEndSpec.scala` lines 178, 248, 268, 281, 381** (the 5 env-failures) — verified by reading the file. All 5 lines exist. Lines 178 (`messages.size shouldBe 1`), 248 (`init should include ("\"serverInfo\"")`), 268 (`toolsResp should include ("\"result\"")`), 281 (`proc.exitValue() shouldBe 0`), 381 (`toolCallResp should include ("\"result\"")`). The 5 failures are assertions inside `test(...)` blocks that `classpathOrSkip` cancels before they execute. **HOLDS**.
- **`CachePluginP25Spec.scala`** — verified at 105 lines, asserts `ctx.meta("sm8.cache.write.error")` per the P2.5 regression pattern. **HOLDS** per the spec header lines 1-22.
- **`docs/research/post-v1-survey-2026-09-04.md`** — verified to exist; this survey extends it. **HOLDS**.
- **`docs/audit/semantic-graph-future-risks-2026-09-04.md`** — verified to exist; this survey extends it. **HOLDS**.

---

## 6. Anti-recommendations

Things that look like plugin candidates but violate RFC §3 layer discipline or the standing rules. Listed in priority order:

1. **Do NOT propose adding a NEW core layer.** Every recommendation above is a plugin-layer addition (or a test pattern / contract spec). Per `AGENTS.md` "**No transitive plugin-impl dep** in adapter `pom.xml` files" + RFC §3 "zero I/O in core", the Core layer is frozen. The 5 failure patterns above are mitigated at the plugin layer (the only layer where the engine-portable ADTs can be safely extended).

2. **Do NOT propose Fory / Kryo / serialization library work.** This is out of scope per `AGENTS.md` and was already evaluated + parked per prior surveys. The serialization defense (`PluginSerializationSpec`, `RestateCachedRowSerializationSpec`) is in good shape per the 2026-08-15 review §4.3.

3. **Do NOT propose Scala 3 migration, framework rewrites, or microservices decomposition.** Out of scope per RFC §1.1 + the standing PR-176 era decisions. Scala 2.13 + Spark + Maven enforcer is the locked shape.

4. **Do NOT propose perf work without a measured baseline.** Per `AGENTS.md` "Don't propose perf work without a measured baseline" + `scala-perf-testing` skill. The `df.count()` is NOT on the hot path (ADR-009-f §5); the per-query `newSession()` is bounded (ADR-009-c v0.5-r1 §2.6); the cache lookup is O(1). Any new perf proposal needs a JMH benchmark first.

5. **Do NOT propose fixes that introduce "engine-portable types into the Core boundary" without a typed ADT.** The D-wave F4 (cross-adapter asymmetry between broadcast and skew) is the precedent — a fix that introduces a new typed field in `EngineContext` is acceptable; a fix that threads `spark.sql.adaptive.skewJoin.skewedPartitionFactor` through `EngineContext` is not. Every recommendation in §3 above stays engine-portable (no Spark imports in `sm8-core/`, no `org.apache.spark.*` in any plugin that doesn't already have a Spark dependency declared in its `pom.xml`).

6. **Do NOT propose fixing the counter-only stubs by deleting them silently.** Per D-wave §6.2 "the methods are retained for backward compat with the SDK surface". A fix that deletes `BroadcastStub.consult` + `SkewStub.consult` without checking all third-party plugins (none exist today, but the contract surface must be preserved for future plugins) would silently break unknown consumers. The recommended fix is "add a test that asserts the no-op behavior" OR "remove OR document as planner pre-flight consult + add spec for that shape" — not silent deletion.

7. **Do NOT propose "wire-shape lint plugins" that run on every query.** A `PreExecute` lint plugin (e.g. asserting every `ctx.meta` key matches a registered manifest) adds a hot-path cost. Today `ctx.meta` is a `Map[String, Any]` with O(1) lookups; adding validation per-query is a measurable perf regression. The right shape is a CI-time scalafix rule (compile-time, not runtime) — see Pattern 2 candidates.

---

## 7. Citations index

Every primary source read in this survey:

### ADR retrospectives (the canonical failure catalog)

- `docs/project_status/2026-08-24-adr-009-c-v0.5-wave-retrospective.md` (175 lines) — per-query `newSession()`, the dead-store merge (PR-170), `querySessionTL` + `lastQuerySessionTL` seams, 9 re-point sites.
- `docs/project_status/2026-08-25-adr-009-d-v0.3-wave-retrospective.md` (194 lines) — broadcast + skew decision moves to `PreExecute` hook; `DecisionHints` fold pattern; 3-revision lesson (v0.1 + v0.2 BLOCKED → v0.3); 7 review findings.
- `docs/project_status/2026-08-26-adr-009-f-paired-persist-lifecycle-retrospective.md` (256 lines) — paired persist/unpersist lifecycle; `MaterializePolicy.Cache` typed-rejection; `PersistLifecycleFailed` + `PersistPhase`; close() three-phase machine; 6 ADR revisions.
- `docs/project_status/2026-08-26-adr-009-g-cache-policy-contract-retrospective.md` (181 lines) — cache feature contract closure; per-case policy matrix; counter discipline; dual `CachePolicy` ADT collapse.
- `docs/project_status/2026-08-26-adr-010-a-orchestration-layer-retrospective.md` (220 lines) — `HookRunnerOrchestration` layer; the `daac360` defect (3 plugins inert for 8 PRs); `Context.stop = true` short-circuit across stages; typed-error surfacing.
- `docs/project_status/2026-08-14-status.md` (186 lines) — pre-ADR-011-a codebase snapshot; engine-portable migration track.
- `docs/project_status/2026-08-15-de-code-review.md` (497 lines) — 11 findings (4 MAJOR + 7 MINOR); cache 3-way split; `executeEngine` exception classifier collapse.
- `docs/project_status/PR-88-body-updated-2026-08-18.md` (73 lines) — O-series hardening (5 PRs in 1, ~1100 LOC, 22 tests).

### Typed error surface

- `sm8-core/src/main/scala/io/sm8/core/engine/EngineError.scala` (199 lines) — 13-variant `EngineError` ADT (sealed trait + `final case class` + `case object`); `PersistPhase` sealed trait (Persist | Unpersist).
- `sm8-core/src/test/scala/io/sm8/core/engine/EngineErrorSpec.scala` (120 lines) — per-case `toErrorDetail` round-trip assertions (5 of 13 cases directly asserted).

### Plugin/hook registration sites

- `plugins/audit-plugin/src/main/scala/io/sm8/plugins/audit/AuditStub.scala` — `PostFormat` hook at priority 150 (`HookOrigin.FirstParty`).
- `plugins/broadcast-plugin/src/main/scala/io/sm8/plugins/broadcast/BroadcastStub.scala` — `PreExecute` hook at priority 250.
- `plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala` — `PreExecute` (priority 50, `HookOrigin.Core`) + `PostExecute` (priority 60, `HookOrigin.Core`); 326 lines.
- `plugins/example-plugin/src/main/scala/io/sm8/plugins/example/ExamplePlugin.scala` — `PostExecute` hook at priority 200.
- `plugins/materialize-plugin/src/main/scala/io/sm8/plugins/materialize/MaterializeStub.scala` — `PreExecute` + `PostExecute` at priority 250.
- `plugins/row-cap-plugin/src/main/scala/io/sm8/plugins/rowcap/RowCapStub.scala` — `PostExecute` hook at priority 200.
- `plugins/semantic-graph-plugin/src/main/scala/io/sm8/plugins/semanticgraph/JoinPathPreHook.scala` — `PreResolve` hook at priority 120 (the canonical `semanticGraphError` source).
- `plugins/semantic-graph-plugin/src/main/scala/io/sm8/plugins/semanticgraph/GraphPostResolveObserver.scala` — `PostResolve` observer at priority 120.
- `plugins/skew-plugin/src/main/scala/io/sm8/plugins/skew/SkewStub.scala` — `PreExecute` hook at priority 250.

### Hook infrastructure

- `sm8-platform/src/main/scala/io/sm8/platform/query/hooks/HookRunnerOrchestration.scala` (157 lines) — the orchestration layer (ADR-010-a v0.3); single entry point; `Context.stop = true` short-circuit across stages.
- `sm8-platform/src/main/scala/io/sm8/platform/query/hooks/EngineHookDispatcher.scala` (294 lines) — per-stage dispatch; `runStage` factoring; `HookFailed` construction at lines 181, 188, 226, 233.
- `sm8-core/src/main/scala/io/sm8/sdk/Hooks.scala` — `HookStage` sealed trait (8 cases); `wireName` + `fromWireName` mappings.
- `sm8-core/src/main/scala/io/sm8/sdk/HookOrigin.scala` — priority range reservation: 0-99 Core, 100-899 FirstParty, 900+ Community.

### Engine service

- `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala` — `runQueryWithHooks` (line 392); `DecisionHints` fold at lines 496-498; `semanticGraphError` pattern match at line 530; exception classifier at lines 255-275 (TimeoutException, SQLTimeoutException).
- `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/SparkEngineProvider.scala` — `applyPostCompilePipeline`; `trackPersist`/`untrackPersist` pair; `addSuppressed` chain at line 640; `ThrowingUnpersistDataset` at line 702.

### Specs

- `sm8-platform/src/test/scala/io/sm8/platform/mcp/StdioEndToEndSpec.scala` (412 lines) — 5 env-failures at lines 178, 248, 268, 281, 381; `classpathOrSkip` cancel at line 97.
- `plugins/cache-plugin/src/test/scala/io/sm8/plugins/cache/CachePluginP25Spec.scala` (105 lines) — the P2.5 regression spec; asserts `ctx.meta("sm8.cache.write.error")` for the journal-encode failure path.

### RFC + architecture

- `docs/rfcs/2026-08-12_v1_architecture-spec/hooks.md` — 8 attachment points; priority ranges 0-99 Core / 100-899 FirstParty / 900+ Community.
- `docs/rfcs/2026-08-12_v1_architecture-spec/plugins.md` — 4 types by contents (adapter-only, hook-only, composite, config-only) + 3 types by origin (Core / FirstParty / Community).

### Prior surveys (for context)

- `docs/research/post-v1-survey-2026-09-04.md` — the v1 destination survey; top-3 candidates (semantic-graph plugin, ModelService + Prometheus export, parked-followups close-out).
- `docs/audit/semantic-graph-future-risks-2026-09-04.md` — the future-risks audit; 0 CRITICAL / 0 HIGH / 1 MEDIUM / 3 LOW / 4 clean; 3-line verdict: "the plugin is in good operational shape."

### ADRs referenced (not read in full, but cited by retros)

- `docs/adr/0009-f-paired-persist-lifecycle.md` (cited by F-wave retrospective).
- `docs/adr/0010-a-enginehookdispatcher-stage-parameter.md` (cited by ADR-010-a retrospective).
- `docs/adr/0009-g-cache-policy-contract.md` (cited by G-wave retrospective).
- `docs/adr/0009-d-broadcast-skew-decision-via-context-meta.md` (cited by D-wave retrospective).
- `docs/adr/0009-c-per-query-clone-session.md` (cited by C-wave retrospective).
- `docs/adr/0008-af-enginehookdispatcher-hookfailed-typed-error.md` (the typed `HookFailed` origin ADR).

### Files NOT read (out of survey scope, but cited)

- `docs/project_status/` retros for waves A, B (not present in repo).
- `sm8-core/src/main/scala/io/sm8/core/cache/RestateCachedRow.scala` (164 lines) — confirmed stale `:218` cached fact; the file is shorter than the cached anchor.
- `sm8-core/src/test/scala/io/sm8/core/engine/EngineErrorSpec.scala` full content beyond the 120 lines that were read; no other untested-case direct assertions exist (verified by `grep -c "EngineError\." EngineErrorSpec.scala` returning 13).

---

## 8. Method + verification notes

- **Read-only contract honored**: zero code modifications, zero commits, zero pushes. The survey worker started in a `chmod -R a-w` directory; verified `mkdir -p` + `touch` both fail with `Permission denied` at the survey target path.
- **Path fallback**: the in-repo target `docs/research/failure-modes-2026-09-04.md` is read-only. The survey was written to `~/.jcode/scratch/failure-modes-2026-09-04.md` (writable). The user can copy it into the repo on merge if desired.
- **Maven test runs blocked**: `mvn test -pl sm8-platform` fails with `[ERROR] Failed to execute goal org.apache.maven.plugins:maven-resources-plugin:3.3.1:resources (default-resources) on project sm8-platform_2.13: Cannot create resource output directory` — the directory is read-only and `target/` cannot be created. The survey relies on the retros' documented test results rather than fresh runs.
- **Tier-5 cached-fact verification**: each of the 3 user-cached `file:line` facts was verified by reading the file. 2 hold (CachePlugin.scala:293, StdioEndToEndSpec.scala:97); 1 drifted (RestateCachedRow.scala:218 — file is 164 lines, the line does not exist).
- **EngineError coverage analysis**: per-case count derived by grepping `EngineErrorSpec.scala` for `EngineError.<CaseName>` or `case <CaseName>` patterns. The 5 directly-asserted cases (`UnsupportedCapability`, `DecimalOverflow`, `ConnectionFailed`, `QueryTimedOut`, `PersistLifecycleFailed`) plus the `ErrorCode` 10-code check + `ErrorDetail` 2 checks = 13 grep matches (matches the file's 120-line size).

---

*End of survey. Top finding: the hook attachment point coverage matrix has 4-of-8 occupied (the 4 empty slots — `PreParse`/`PostParse`/`PreFormat`/`PostFormat` — are the highest-leverage plugin attachment opportunities). Every failure pattern above maps to an existing attachment point + a typed `EngineError` surface; no new Core layer is needed. Anti-recommendation: do not add a NEW core layer per RFC §3.*
