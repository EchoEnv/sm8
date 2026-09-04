# Post-v1 Destination Survey — 2026-09-04

> **Audience:** the user, who will read this and pick a destination.
> **Author:** READ-ONLY survey worker (1-shot, no code changes, no commits).
> **Repo state at survey time:** `main` @ `50a2cef` (C10 PR-C2, plugin/hook inspection complete).
> **Source pack read:** v1 RFC (5 files), v1.1 semantic-graph RFC, ADR index + every non-Implemented ADR head, project retros (2026-08-24 to 2026-08-26), AGENTS.md.

---

## 1. TL;DR

After v1, the highest-leverage next destinations are concentrated in three places:

- **The semantic-graph plugin** (`docs/rfcs/2026-08-23_feat_semantic-graph/semantic-graph_proposal.md`) — the largest known unbuilt proposal, fully designed, blocked only by the cycle-validation work `QueryBuilder.detectCalcCycles` already does. Ships as a plugin module, no Core change.
- **The Observability / introspection surface gap** — the `MetricsService` + `RegistryInspectorService` were just shipped (PRs #254 / #312-#314), but **model discovery** (ADR-012-a, `ModelService` Proposed since 2026-09-01) and **Prometheus export** (ADR-012-b-export, Proposed since 2026-09-01) are still parked. Both are small, layered additions on the same seam and would complete the operator's view.
- **Parked correctness + E2E cleanups** — `StdioEndToEndSpec` 5 environment failures (CI-only spec that `cancel`s when `mvn dependency:build-classpath` is unavailable; per `sm8-platform/.../StdioEndToEndSpec.scala:97`) and the `CachePlugin` P2.5 `sm8.cache.write.error` ctx.meta fold (the regression spec exists; whether the fold has been wired into the post-hook itself needs verification). Cheap, contained, unblock release-readiness.

**Top-3 candidates, in order of leverage:**

1. **Semantic-graph plugin** (`semantic-graph-plugin_2.13`) — RFC §3 layer-clean, fills a known gap the v1 graph-proposal already names, lifts the cycle-check + join-path resolution + impact-analysis + cardinality hints for broadcast/skew plugins into a shared structure.
2. **ModelService + Prometheus export close-out** — ship ADR-012-a + ADR-012-b-export to close the v1 observability/introspection surface (3rd wire service + a dedicated `/metrics` endpoint on `--metrics-port`).
3. **Parked-follow-ups close-out** — triage `StdioEndToEndSpec` 5 env failures (decide: CI-only vs ship a slim non-`mvn` variant) and verify `CachePlugin` P2.5 ctx.meta fold is wired into `CacheWritePostHook` (`plugins/cache-plugin/.../CachePlugin.scala:226-230`).

---

## 2. Current state — what shipped, what's parked, what status docs say

### 2.1 What shipped (per `git log --oneline -20` at `50a2cef`)

| Wave | PR(s) | Subject |
|---|---|---|
| **C10 (2026-09-04)** | #311 → #314 | plugin + hook registries exposed: `listAllHooks` + `discoverAll` in core; `RegistryInspectorService` (3 handlers) on the Restate ingress; shared Engine between `QueryService` + `RegistryInspectorService` |
| **C9 (2026-08-30)** | #305 | semantic-graph polish (nodeKey helper + drop AsSynchronizedGraph — applies review fixes from the v1.1 RFC) |
| **C8** | #296 | review fixes (dup import + stale doc) |
| Pre-C | #289, #288, #287 | scalafmt format-on-touch pre-commit hook; skill-citation linter; stale comment updates |
| Pre-C | #286, #285, #278 | `ModelLoader` I/O refactor (PR-273 target); `EngineFactory` companion; `McpStdioRoute` partial-line EOF `ParseError` |
| v1 RFC track (locked) | #268 → #265 → #263 → #261 → #259 | Restate SDK 2.1.1→2.9.4; MCP stdio (ADR-013) + Streamable HTTP (ADR-014) + in-process stdio (ADR-015); MCP server module |

The full v1 RFC track is closed: the C10 work delivered the final layer — surfacing the plugin + hook registries to the wire (operators can now query what plugins + hooks a deployment has loaded via `RegistryInspectorService/listPlugins` and `/listHooks`).

### 2.2 What's parked (per the survey brief + retros)

| Item | Where | Why parked |
|---|---|---|
| **`StdioEndToEndSpec` 5 environment failures** | `sm8-platform/src/test/scala/io/sm8/platform/mcp/StdioEndToEndSpec.scala:97` | Spec `cancel`s with `"sm8-smoke-cp.txt not buildable (CI-only test)"` when `mvn dependency:build-classpath` fails. Lives in dev containers that lack Maven or network. Not a code bug — an environment-shape gap. |
| **`CachePlugin` P2.5 `discard-copy` bug** | `plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala:226-230` (the `CacheWritePostHook.run` post-hook) | The regression spec `plugins/cache-plugin/src/test/scala/io/sm8/plugins/cache/CachePluginP25Spec.scala` already exists and asserts `ctx.meta.contains("sm8.cache.write.error")` for the journal-encode failure path. Whether the fold-in has actually been wired into `CachePlugin.scala` needs verification — the spec header at line 1 calls it a "regression test for the ctx.meta fold pattern" but the test could pass against a not-yet-folded implementation if the assertion was added speculatively. **Honest caveat:** this needs an empirical read of `CachePlugin.scala:226-230` before recommending as a "fix"; could be a "verify + ship" rather than a "design + implement" workstream. |

### 2.3 ADR status landscape (per `docs/adr/README.md` + per-ADR headers)

The ADR README at `docs/adr/README.md` is **partially stale**: ADR-014 and ADR-015 read as "Proposed" but shipped (PR-261, PR-263, PR-264, PR-278, PR-285 per `git log`). Per-ADR headers are the authoritative source.

| ADR | Title | Status (per-ADR) | Notes |
|---|---|---|---|
| **0012-a** | ModelService — Restate-handler surface for loaded model(s) | **Proposed** (2026-09-01) | The 3rd wire service that would let Restate UI list the loaded model. Read-only. `ModelSummary` DTO lives in `sm8-core/.../model/`. |
| **0012-b** | MetricsService — Restate-handler surface for invocation metrics | Accepted (wire surface only) | PR-254 implementation |
| **0012-b-followup** | Real counter instrumentation for `MetricsService` | Accepted (impl. PR-256) | `QueryMetrics` singleton, 6 `AtomicLong` counters |
| **0012-b-export** | Prometheus metrics export | **Proposed** (2026-09-01) | Separate Vert.x `HttpServer` on `--metrics-port 9090`. REVISION 3 pivot from "same port" to "dedicated port" (Prometheus sidecar pattern). 9 metrics, 8 unit tests + smoke assertion. |
| **0012-c** | ConfigService — Restate-handler surface for runtime config | Accepted (hold / negative decision) | Revisit-gate-1 already cleared (PR-253). |
| **0008-q** | SDK redesign: `MCP... → Engine...` rename + phantom-typed SDK + typed URL + `EngineLoader` | **Proposed** (v2 2026-08-19) | The structural ADR for the phantom-typed SDK. **Significant** — 21 review findings resolved, 3 atomic PR sequence, but kept `Proposed` (not `Accepted`) per the v2 revision history. |
| **0008-r** | Aggregation / groupBy / having / limit / parts + window functions (3-PR atomic sequence: PR-17/18/19, PR-M4) | **Proposed** (v1 2026-08-19) | Closes ADR-008-L GAPs 5/6/7/8 + adds the window-function family per ADR-008-P §DE-P2-5. |
| **0016** | `EngineFactory` companion + zero-I/O core boundary | Proposed (C7 wayfinder round, ticket #280) | **Status needs verification — PR-272/#285 may have shipped it.** The git log shows `1bcc395 feat(sm8-core): PR-272 EngineFactory companion + QueryService refactor`; ADR body claims "Proposed, target PR-272". |
| **0017** | `EngineImpl.discoverFromConfig(stream)` non-`()` overload + I/O cleanup | Proposed (C7 wayfinder round, ticket #283) | **Status needs verification — PR-274/#286 may have shipped it.** Git log shows `03d1a59 fix(sm8-core): PR-273 ModelLoader I/O refactor` (different ADR sibling); PR-274 may or may not have shipped. |
| 0013 / 0014 / 0015 | MCP server + Streamable HTTP + in-process stdio | **All shipped** (PR-259 + PR-261 + PR-264) despite the headers still reading "Proposed" — the README + ADR index is stale here. |

### 2.4 What the retros say

- **ADR-010-a retrospective** (`docs/project_status/2026-08-26-adr-010-a-orchestration-layer-retrospective.md`) confirms the 4-stage orchestration is end-to-end correct, the 3 first-party plugins (`JoinPathPreHook` cycle validator + `GraphPostResolveObserver` + `AuditPostStubHook`) now fire in production, and typed errors from short-circuited pre-hooks surface as `Left(EngineError.UnsupportedCapability(...))`. **Critical implication**: the cycle-detection hook is wired and the semantic-graph v1.1 RFC can build on a working `PreResolve` hook infrastructure.
- **ADR-009-d retrospective** (`docs/project_status/2026-08-25-adr-009-d-v0.3-wave-retrospective.md`) confirms `BroadcastStub` + `SkewStub` `PreExecute` hooks consult + write decisions into `Context.meta`, the spark connector consumes via `EngineContext.decisionHints`. **Critical implication**: the broadcast/skew plugins have oracle slots (`sm8.broadcast.arm`, `sm8.broadcast.thresholdBytes`, `sm8.skew.arm`) ready to consume cardinality hints — exactly what the semantic-graph RFC §2 row 4 calls out as a planned integration point.
- **ADR-009-c retrospective** (`docs/project_status/2026-08-24-adr-009-c-v0.5-wave-retrospective.md`) confirms per-query `newSession()` (so `JoinHints.skewFactor` binds per query), no follow-ups.

---

## 3. Candidate destinations

### 3.1 — Semantic-graph plugin (the largest known unbuilt proposal)

**Title.** `semantic-graph-plugin_2.13`: build `SemanticGraphBuilder` + `JoinPathPreHook` per the v1.1 RFC.

**Why it might be the next thing.** Three converging arguments:

1. **The proposal already exists, fully designed, with v1.1 review fixes applied.** `docs/rfcs/2026-08-23_feat_semantic-graph/semantic-graph_proposal.md` is a 330-line design that names the API (JGraphT-backed `SemanticGraph` + `SemanticGraphBuilder.buildAcross(models)` + `JoinPathPreHook` at priority 120, `HookStage.PreResolve`), the layer map (new plugin module only — zero Core changes), the dependency (`org.jgrapht:jgrapht-core:1.5.2`, pure JVM, no Spark transitive dep), the v1.1 corrections (cache dropped because JGraphT's `DefaultDirectedWeightedGraph` is not thread-safe — `javadoc` "No concurrent modifications…"; typed-error fold pattern; dangling-right-nodes surfacing; `enforce-no-spark` block; root-pom `<module>` line). This is the largest *known unbuilt proposal* in the repo.
2. **The supporting infrastructure is now live.** ADR-010-a shipped the orchestration layer so `PreResolve` hooks fire in production (retrospective §1, TL;DR). ADR-009-d shipped the broadcast/skew oracle slot pattern so the cardinality hint output of the graph can actually be consumed. C10 shipped `RegistryInspectorService` so the plugin is discoverable through the wire.
3. **It fills a known gap the v1 graph already calls out.** RFC §2 row 1 (calc-measure cycle detection) currently lives in `sm8-core/.../query/QueryBuilder.scala:255` as a hand-rolled iterative DFS that handles exactly one problem. The semantic-graph plugin would generalize this into a reusable adjacency structure any other plugin can query (RFC §2 row 4), feed the broadcast/skew plugins (RFC §2 row 4), and expose join-path resolution (RFC §2 row 2) and impact analysis (RFC §2 row 3) as new capabilities — none of which exist in any form today.

**Prerequisite work / blockers.** Per the RFC §3 + §5 + ADR-010-a retrospective:

- The RFC §3 implementation note explicitly states the plugin `setup()` must be idempotent-safe and **doesn't open connections** in `setup()`. The semantic-graph plugin has zero I/O (pure data → graph derivation) so the prerequisite is trivial.
- v1.1 §5.1 already dropped the cache (BLOCKER 3a). No perf concern at realistic model sizes (≤100 calc-measures × 5 refs; build cost < 1 ms — RFC §5.1).
- v1.1 §5.2 already aligned the cycle error with the `EngineError.UnsupportedCapability` typed-error pattern that ADR-010-a §2.3 wired through `EngineService.runQueryWithHooks`. So the typed-error plumbing already exists.
- **Not a blocker but worth noting**: ADR-008-L's open GAPs (the PR-M3/PR-M4 territory for cross-model right-key validation) still need a real schema resolver before the graph can validate join edges against actual schemas. The RFC §4 is explicit about this — the graph represents the edge, validation comes later. So the plugin ships BEFORE that work.

**Estimated effort.** **1 wayfinder map (3-5 sessions).** The RFC §3 sketch is detailed enough that the implementation is mostly mechanical: one module (~10 files), one new dependency (`jgrapht-core:1.5.2`), one META-INF/services registration, one `<module>` line in root pom, plus a perf-correctness spec that feeds `examples/hospital-cleaning` model and asserts the graph matches `detectCalcCycles` acceptance/rejection (RFC §6).

**Skill alignment.** `karpathy-guidelines`, `karpathy-app-design` (frozen core + plugin extension portal — exactly the pattern), `scala-impact-analysis` (the graph consumes `Calculator.measureNamesOf` + `Calculator.fieldNamesOf` — the canonical walkers), `scala-error-handling` (typed `EngineError.UnsupportedCapability` per v1.1 §5.2), `scala-spark-batch-bugs` (the cardinality hints feeding broadcast/skew), `scala-bug-hunting` (RFC §5.1 algorithm chose `AsSynchronizedGraph` then dropped it — needs verification it stays dropped), `scala2-scaladoc`.

**Honest risk — what could make this wrong.**

- **JGraphT dependency drift.** `jgrapht-core:1.5.2` is a real, current version but `1.5+` introduced `AsSynchronizedGraph`. v1.1 §5.1 dropped it but did not add `AsSynchronizedGraph` back conditionally on profile. If profiling later shows the build is hot, the upgrade path is `org.jgrapht.graph.concurrent.AsSynchronizedGraph` — but **the v1.1 RFC does not document this as a follow-up** (just a one-line note). A future maintainer who hits the perf cliff will need to discover the path. **Mitigation**: add a comment in `SemanticGraph.scala` referencing the v1.1 §5.1 footnote so the upgrade path is one search away.
- **Cross-model right-key validation still needs PR-M3 + PR-M4 work.** The semantic-graph plugin will surface dangling right-nodes (v1.1 §5.3) but cannot validate them against real schemas until `ModelValidator`'s PR-M3 + PR-M4 work lands. This is a known gap the RFC is honest about — but a future user might treat the graph's silence on the right node as validation.
- **Cycle detection is now duplicated.** The RFC §4 explicitly says `QueryBuilder.detectCalcCycles` stays as the Core's own guarantee and the plugin's `hasCycle` check is a *pre-flight* duplicate. Two algorithms walking the same data is acceptable (RFC §4 makes the case) but it doubles the test surface. A future refactor could unify on the graph, but that's not this PR.

---

### 3.2 — Observability / introspection surface close-out

**Title.** Ship ADR-012-a (`ModelService`) + ADR-012-b-export (Prometheus metrics on dedicated port) as one bounded wave.

**Why it might be the next thing.** Three converging arguments:

1. **Both ADRs are Proposed since 2026-09-01, fully designed, with review-fix revisions applied.** ADR-012-a has 3 handlers (`listModels` + `getModel` + `describe`), a `ModelSummary` DTO in `sm8-core/.../model/`, and a documented "Out-of-scope follow-up" list (ADR-012-d for `ModelRegistry` data structure, hot-add/remove, multi-model serving). ADR-012-b-export has 9 Prometheus metrics, a separate Vert.x `HttpServer` on `--metrics-port 9090`, an 8-test unit spec, and a smoke assertion. Both ADRs underwent dual-review (architect + data-eng).
2. **The existing wiring is identical-shaped.** ADR-012-b-followup already shipped `QueryMetrics` (sm8-platform singleton, 6 `AtomicLong` counters) and `MetricsService.snapshot` reads them through the Restate bidi-stream. ADR-012-b-export just reads the same singleton and exposes them as `/metrics` text. ADR-012-a piggybacks on the existing `HttpTransport.endpoint` composition exactly like `QueryService` + `MetaInspectorService` already do.
3. **Operators are currently blind to two things.** The Restate UI's Services page lists `QueryService` + `MetaInspectorService` + `RegistryInspectorService` (per C10) but cannot answer "which model is this deployment serving?" — operators shell into the host to read the `--model <yaml>` arg. And the 6 live counters PR-256 shipped are only accessible via Restate ingress — Prometheus / Grafana / Datadog / standard dashboards can't scrape them. The "observability surface gap" is concrete and named.

**Prerequisite work / blockers.** Per ADR-012-a §Layer discipline + ADR-012-b-export §Layer discipline + ADR-010-a retrospective §2.3:

- ADR-012-a needs `ModelSummary` in `sm8-core/.../model/ModelSummary.scala` + `ModelService` in `sm8-platform/.../query/ModelService.scala` + one new line in `HttpTransport.endpoint` (`.bind(ModelService.definition(model))`). The wire DTOs already follow `QueryService.scala:248` (`dev.restate.sdk.common.TerminalException`) + `MetaInspectorService.scala` template.
- ADR-012-b-export needs `MetricsHttpRoute` in `sm8-platform/.../query/MetricsHttpRoute.scala` (~50 LOC per ADR §Implementation sketch) + 8 unit tests + `sm8-server/Main.scala` `--metrics-port <n>` flag (default 9090). The Prometheus format is fixed (`text/plain; version=0.0.4`), counters are read fresh per scrape (~70 ns per ADR §Implementation sketch).
- **Layer discipline**: per RFC §3, both ADRs land entirely in `sm8-core` (the DTO) + `sm8-platform` (the wire / route) — zero changes to `sm8-server` beyond CLI flags. No plugin or connector changes.

**Estimated effort.** **1 wayfinder map (2-3 sessions)** — both ADRs are small enough they could ship as one bounded wave (split into PR-1: ModelService + ModelSummary; PR-2: MetricsHttpRoute + CLI flag + smoke). 1-shot reviewers possible (each PR is ≤ ~150 LOC per its ADR).

**Skill alignment.** `karpathy-guidelines`, `karpathy-app-design` (additive wire surface, layer-pure), `scala-impact-analysis` (HttpTransport.endpoint composition order, MetricsHttpRoute vs RestateHttpServer port split), `scala-error-handling` (the `TerminalException(404)` pattern from ADR-012-a §Alternatives Considered), `building-restate-services` (skill name explicitly cited in ADR-014 §Decision — applies to both), `scala-jvm-safety` (the Prometheus `/metrics` route is OUTSIDE Restate's journal pipeline so `Instant.now()` is correct per `building-restate-services` rule), `scala2-scaladoc`.

**Honest risk — what could make this wrong.**

- **REVISION 3 of ADR-012-b-export pivoted twice already.** The same-port approach (Rev 1) and the sub-router approach (Rev 2) both failed r2 dual-review. Rev 3 (separate port) was the third try. The skeleton is correct now (the javap-verified Vert.x 4.5.11 APIs are documented at ADR §Why this works (REVISION 3 verified APIs)) but the history shows this was hard-won. **A future proposal to merge `/metrics` onto the Restate port will relitigate Rev 1's failure mode** — document the rejection rationale in the ADR §Open questions section so future contributors don't re-walk this path.
- **`ModelService.getModel` returns no YAML.** Per ADR-012-a §Alternatives Considered: "`PlatformModelLoader` does NOT expose a `toYaml(model): String` method… Dropped for now; can revisit in ADR-012-d." A user who wants round-trip YAML editing will hit this gap. **Mitigation**: ADR-012-a's Out-of-scope follow-ups list is explicit — `registerModel(yaml)`/`deleteModel(name)`/`ModelRegistry` are deferred.
- **Multi-model serving is not on the menu.** ADR-012-a's `listModels` returns the single `Model.of(...)`-loaded model. ADR-012-a §Out-of-scope explicitly defers `setDefault(name)` + multi-model serving to ADR-012-d. A user with multiple YAMLs to serve will hit the gap.

---

### 3.3 — Parked follow-ups close-out (StdioEndToEndSpec + CachePlugin P2.5)

**Title.** Close the two parked follow-ups so the v0.1.0 / v1.0 release-readiness gate is clean.

**Why it might be the next thing.**

1. **Both items are explicitly named in the survey brief as parked.** They're not a re-derivation — they're on the agenda by directive.
2. **They're cheap.** The spec-level work for both is mechanical (env-gating the Stdio spec; verifying the P2.5 fold in `CachePlugin.scala:226-230`). Per AGENTS.md "Common gotchas", neither touches layer discipline.
3. **They unblock release-readiness.** The C10 wave + MCP ship-side means the user-visible surface is stable. The remaining gaps are: (a) the E2E spec doesn't run in dev containers without Maven + network — so it can't be a "always-green" guarantee, and (b) the cache-plugin fold pattern (PR-191-PR-193 era's "ADRs ship in lockstep with regression specs" pattern per the cache-plugin P2.5 spec header) needs to verify the spec body matches the implementation.

**Prerequisite work / blockers.**

- `StdioEndToEndSpec`: per the spec at `sm8-platform/src/test/scala/io/sm8/platform/mcp/StdioEndToEndSpec.scala:97`, the test `cancel`s with `"sm8-smoke-cp.txt not buildable (CI-only test)"` when `mvn dependency:build-classpath` fails. The 4 stdio tests + 1 garbage-input test all share `classpathOrSkip()`. The real fix is to pre-stage `$JCODE_SCRATCH_DIR/sm8-smoke-cp.txt` from a CI job OR rewrite the spec to skip cleanly in non-CI environments without relying on Maven. The simplest path: pre-stage the classpath file via the existing `scripts/smoke-e2e.sh` build step (which already does `mvn dependency:build-classpath`).
- `CachePlugin` P2.5: per `plugins/cache-plugin/src/test/scala/io/sm8/plugins/cache/CachePluginP25Spec.scala:11-19`, the spec asserts `ctx.meta.contains("sm8.cache.write.error")` after a journal-encode failure. The spec header calls it a "regression test" — **but the implementation file `plugins/cache-plugin/.../CachePlugin.scala:226-230` has not been verified by this survey to actually fold the typed `Left` into `ctx.meta`**. The spec could pass against an unimplemented implementation if the assertion was added speculatively. **Action**: read `CachePlugin.scala:226-230` and either (a) ship a one-PR fix if the fold is missing, or (b) close the workstream with "spec is the contract, implementation matches" if the fold is present.

**Estimated effort.** **1-2 1-shot sessions.** No wayfinder map needed.

**Skill alignment.** `karpathy-guidelines` (smallest correct change), `scala-impact-analysis` (P2.5 fold touches `ctx.meta` channel which is shared with other hooks — ADR-009-d establishes `sm8.cache.write.error` as a new key, ADR-010-a establishes the orchestration layer that surfaces it), `debug-mantra` (the spec body asserts the meta key — the falsifiable criterion is whether the implementation actually writes the key).

**Honest risk — what could make this wrong.**

- **"Spec exists" ≠ "Implementation matches."** The P2.5 spec could be aspirational. A 1-shot close-out that doesn't read the implementation file could close the workstream with a green spec against an unwired post-hook. **Mitigation**: the close-out session must read `CachePlugin.scala:226-230` line-by-line and verify the `ctx.meta + ("sm8.cache.write.error" -> typedLeft)` write exists before declaring done.
- **`StdioEndToEndSpec` cancellation is by design.** The spec line 97 cancel message is intentional: it's an environment-shape gap, not a code bug. A close-out that "fixes" the cancel by removing it (e.g. making it `ignore`) would silently regress the smoke signal. **Mitigation**: the close-out must preserve the cancel OR replace it with a real CI-only conditional (e.g. `cancel` only when both Maven and the cached classpath file are unavailable).

---

### 3.4 — Triage the Proposed-but-not-implemented ADR backlog

**Title.** Audit which Proposed ADRs should ship vs be put on hold. The backlog is small but real.

**Why it might be the next thing.**

1. **There are 4-5 Proposed ADRs in the repo** (per the per-ADR header survey at §2.3): ADR-012-a + ADR-012-b-export (both covered by 3.2), plus possibly ADR-0008-q + ADR-0008-r + ADR-0016 + ADR-0017. The README index is stale on ADR-014/015 (they shipped) so the reader can't trust the README as the source of truth.
2. **Each has a known cost in review cycles.** Per the ADR-009-d retrospective, "the cost of 6 review cycles is real; the alternative — shipping a non-falsifiable design — would have been worse." Each Proposed ADR is a 1-3 dual-review cycle commitment if it ships.
3. **Some are stale-by-design.** ADR-0008-q (phantom-typed SDK redesign) was v2-revised 2026-08-19 and may have been partially implemented via the typed-realize + EngineFactory + PluginDiscovery wave (PR-191 / PR-272 / PR-286). Need to verify which of the 3 atomic PRs (PR-14 rename / PR-15 URL grammar / PR-16 phantom witnesses) shipped vs which are still pending. **Honest gap**: this survey couldn't verify all 4 ADR statuses against the git log without more focused reads.

**Prerequisite work / blockers.** For each Proposed ADR:

- Read the per-ADR header + the per-ADR "References" section + `git log --all -- docs/adr/NNNN-*.md` to see whether the referenced PR numbers shipped.
- Update `docs/adr/README.md` to fix the stale entries (ADR-013/014/015 should read `Implemented`, ADR-016/0017 may need updates).
- For each remaining Proposed ADR: write a 1-line decision ("ship next" / "defer" / "supersede") with the rationale anchored to the existing retros / RFCs.

**Estimated effort.** **1 wayfinder map (2-3 sessions)**: first session = read every Proposed ADR head + verify git status; second session = draft the "ship / defer / supersede" decisions; third session = update README + ADR headers.

**Skill alignment.** `karpathy-guidelines` (no fabrication — every decision must cite a primary source), `scala-impact-analysis` (each ADR references PR numbers; verify each), `scala-data-driven-refactor` (the README is data; the staleness is the bug), `writing-for-agents` (the README is auto-loaded into the agent context per the global AGENTS.md — so a stale README actively misleads future agents).

**Honest risk — what could make this wrong.**

- **Verification cost dominates.** Each ADR references 3-15 PRs across the wave. Verifying all 4-5 ADRs against `git log` is a focused read task but not trivial. **Mitigation**: a 1-shot worker with a focused brief (`docs/adr/README.md` + 5 ADR files + `git log --all -- docs/adr/`) can do this in one session.
- **The user may want to ship, not triage.** This is a meta-candidate. If the user's intent is "what's next, ship it" then this triage is a blocking activity, not a destination. **Mitigation**: position this as a "prerequisite" rather than a "destination" — it unblocks confident picking from the other candidates.

---

### 3.5 — v1.0 release-readiness audit

**Title.** Audit which Proposed ADRs should ship before v1.0 vs which should be put on hold; surface any DoD gaps vs RFC §13.

**Why it might be the next thing.**

1. **ADR-007 (v0.1.0 cut plan) explicitly enumerates a DoD checklist against RFC §13.** Per `docs/adr/0007-v0.1.0-cut-plan.md`, the v0.1.0 freeze is "structurally in place" but has TWO halves of RFC §13 DoD: "reference adapter + reference plugin conformance." Adapters are covered (`EngineProvider` suites per `cross-engine-conformance-matrix.md` §2). **Plugins are not structurally enforced** — none of the 6 reference plugins extend `HookContractSpec` or `PluginContractSpec` (ADR-007 §"However"). The conformance is verified by inspection, not by inheritance.
2. **The MCP wire surface has grown.** C10 + ADR-013/014/015 + ADR-012 series added 3+ new Restate services + the stdio transport + the Prometheus export. Each is "additive + backward-compat" per its ADR but the cumulative surface needs an integration smoke.
3. **The user's standing directive "dont bump version yet" (per ADR-008-Q §v0.1.0 tag cut remains GATED)** implies a tag-cut review is the eventual next step. A release-readiness audit before that review pre-empts the P0/P1 findings.

**Prerequisite work / blockers.**

- RFC §13 DoD item 2: "At least one reference adapter and one reference plugin exist and pass conformance tests." Per `cross-engine-conformance-matrix.md` §5 + §7, the adapter floor is the 5 shared mechanical checks + 9 abstract members per connector. **A parallel floor for plugins does not exist.** ADR-007 PR-D proposed closing this but never shipped.
- The 6 reference plugins (`audit`, `broadcast`, `cache`, `materialize`, `row-cap`, `skew`) ship standalone specs but not the unified contract. A audit would identify which ones already match the contract spec by inspection vs which need to extend it.
- The MCP wire surface has grown enough that the existing `scripts/smoke-e2e.sh` may not cover all the new handlers. An integration smoke audit would identify coverage gaps.

**Estimated effort.** **1 wayfinder map (3-5 sessions).** The audit is bounded by the explicit RFC §13 checklist + the plugin-impl conformance gap + the MCP wire surface growth.

**Skill alignment.** `karpathy-guidelines` (the audit is a checklist, not a design), `karpathy-app-design` (RFC §13 DoD is the gold standard), `scala-impact-analysis` (each plugin's spec inventory vs the contract base), `scala-data-driven-refactor` (the contract spec is the data; the 6 plugins either match or don't), `building-restate-services` (the integration smoke audit), `scala2-scaladoc` (each gap needs a Scaladoc-driven resolution).

**Honest risk — what could make this wrong.**

- **The audit is a meta-activity.** If the user's intent is "ship something," a release-readiness audit is overhead. **Mitigation**: position as "prerequisite for the v1.0 tag cut" — a release-readiness audit *before* the tag is cheaper than a release-readiness audit *during* the tag-cut review.
- **The plugin conformance gap may have been partially closed by the C8-C10 work.** C10 added `RegistryInspectorService` which surfaces the plugin set — that's operator-visible but not conformance-test visible. A focused read of `sm8-core/src/test/scala/io/sm8/sdk/contract/` would clarify whether the gap is open or closed.

---

### 3.6 — Post-v1 architectural direction (NOT a rewrite)

**Title.** Identify one extension portal or layer that completes the v1 surface without violating RFC §3 / RFC §2 non-goals.

**Why it might be the next thing.**

1. **RFC §2 non-goals explicitly defer 5 things**: "Query optimization / planning", "Multi-source join engine", "Caching, retries, connection pooling", "Auth / row-level security", "A metric definition DSL or query language." Per RFC §2: "These are legitimate future needs but belong in **plugins**, not core." So the post-v1 direction is plugin-layer extensions, not core rewrites.
2. **RFC §3 core boundary is load-bearing.** The plugin extension portal pattern (the 8 attachment points × 5 hook behavioral types × 4 plugin origin types per the RFCs) is the design. A "post-v1 architectural direction" candidate would name one new extension portal that the v1 surface doesn't yet expose.
3. **The semantic-graph RFC §2 table is a starting point.** The 5 rows (calc-measure cycles / join-path resolution / impact analysis / feeding broadcast-skew / cross-model discovery) are the candidate extension portals the v1.1 proposal identifies. Of those, calc-measure cycles + feeding broadcast-skew are partially in scope (candidate 3.1) — the OTHER THREE (join-path resolution, impact analysis, cross-model discovery) are untouched.

**Prerequisite work / blockers.**

- The semantic-graph plugin (candidate 3.1) is a prerequisite for all 3 — the graph structure is the data the other 3 extensions would query. Without it, join-path / impact / cross-model discovery would each have to build their own graph from scratch.
- A "new layer" would violate RFC §3 unless the layer is a NEW plugin (RFC §3 row "ships inside `/plugins`"). The candidate must be framed as a NEW PLUGIN, not a NEW CORE LAYER.

**Estimated effort.** **1 wayfinder map (3-5 sessions)** if shipped as a follow-on to 3.1; **1 session (planning only)** if scoped to "name the extension portal + draft the ADR" without the implementation.

**Skill alignment.** `karpathy-app-design` (the entire post-v1 framing is the frozen-core + plugin-portal pattern), `karpathy-guidelines` (smallest correct change — name ONE portal, not three), `scala-impact-analysis` (the new plugin must not collide with existing extensions), `scala-data-driven-refactor` (each extension portal is data the existing plugins could consume), `writing-for-agents` (the resulting ADR is the seed for future contributors).

**Honest risk — what could make this wrong.**

- **Scope creep.** "Post-v1 architectural direction" is open-ended — easy to sprawl into 5 candidate extensions. **Mitigation**: the candidate is bounded to ONE extension portal (pick the highest-leverage one of {join-path resolution, impact analysis, cross-model discovery}) with a 1-paragraph rationale.
- **The user may not want architectural-direction work right now.** If the user wants to ship the parked items + the semantic-graph plugin + the observability close-out first, this candidate is a later-cycle activity. **Mitigation**: position as "after 3.1 + 3.2 + 3.3 ship" rather than "before everything else."
- **RFC §1.1 non-goals are explicit.** Any direction that drifts toward "build a query optimizer in core" or "build an auth system in core" violates RFC §1.1. The candidate must enforce the plugin boundary.

---

## 4. Cross-cutting observations

1. **4 of 6 candidates share an observability / introspection thread.** 3.2 (ModelService + Prometheus), 3.4 (the ADR triage), 3.5 (release-readiness audit), and 3.6 (post-v1 direction naming the new portals) all touch "what does an operator see about a deployed sm8". The C10 wave's `RegistryInspectorService` is the seed — 3.2 completes it for models + metrics, and 3.4/3.5/3.6 each need a slice of operator-visible introspection to track their own progress.
2. **The semantic-graph plugin (3.1) is a prerequisite for 3.6.** RFC §2 of the v1.1 graph proposal names 5 candidate extensions; the graph structure unblocks 4 of them. Shipping 3.1 first unblocks the entire "post-v1 direction" conversation.
3. **3.3 (parked follow-ups) is the cheapest possible destination.** It's bounded, contains no design decisions, and unblocks the v0.1.0 tag-cut conversation. If the user wants a "fast win," this is it.
4. **ADR README staleness is a real risk to future contributors.** Per the global AGENTS.md "AGENTS.md is session bootstrap input" — the auto-loaded `docs/adr/README.md` actively misleads agents about which ADRs are Proposed vs Implemented (the MCP ADRs are all shipped but listed as Proposed). 3.4 (the triage candidate) should include a README fix.
5. **The 6 candidates are not equally bounded.** 3.1 / 3.2 / 3.6 are each 1 wayfinder map (3-5 sessions). 3.3 is 1-2 1-shot sessions. 3.4 / 3.5 are 1 wayfinder map each but the work is mostly audit + decision, not code. The user should know the rough effort split before picking.
6. **The "what's NOT in the survey" gap is real.** 3.4 + 3.5 both surface gaps this survey couldn't close without more focused reads (specifically: which ADR-016 / ADR-017 PRs shipped, whether the C8-C10 work closed the plugin-conformance gap, whether `CachePlugin.scala:226-230` actually folds the typed Left into ctx.meta). The user should know these gaps before picking — they're each cheap to close with a focused 1-shot.

---

## 5. What I CANNOT recommend without more info

- **Whether `CachePlugin.scala:226-230` already folds the typed Left into `ctx.meta`.** The P2.5 spec asserts the meta key is set; the implementation file wasn't read in this survey. The 3.3 candidate is "verify + ship (small) fix" or "verify + close (trivial)" — I cannot distinguish from outside the file.
- **The current status of ADR-0016 (`EngineFactory` companion) and ADR-0017 (`EngineImpl.discoverFromConfig(stream)` overload).** The ADR bodies claim "Proposed, target PR-272 / PR-274" but `git log` shows PR-272 (`feat(sm8-core): PR-272 EngineFactory companion`) shipped and PR-273 (a sibling, ModelLoader I/O refactor) shipped. PR-274 may or may not have shipped (not visible in the first 20 `git log` entries). The triage candidate (3.4) needs this verification to be accurate.
- **Whether the plugin-conformance gap (RFC §13 DoD item 2) is open or closed.** ADR-007 said open as of 2026-08-15. The C8-C10 work added `RegistryInspectorService` which surfaces the plugin set but not the conformance test base. A focused read of `sm8-core/src/test/scala/io/sm8/sdk/contract/` would close this.
- **Whether the MCP wire surface smoke coverage is complete.** `scripts/smoke-e2e.sh` was updated per C5/PR-265. But the stdio path is now covered by `scripts/smoke-mcp-stdio.sh`. The HTTP path is covered by `scripts/smoke-mcp-http.sh`. Whether these cover all 3 services × all handlers is unclear from this survey.
- **The user's preference for "ship something" vs "audit the backlog."** The candidates split roughly 50/50 between "ship" (3.1, 3.2, 3.3, 3.6) and "audit" (3.4, 3.5). The user should know before picking.
- **The full Proposed ADR list.** Per §2.3, 4-5 are visible from the README + per-ADR headers. The full list requires reading every ADR file's `**Status:**` line which this survey did only for the most-recent 15. Anything older (0008-*) may also have Proposed entries.

---

## 6. Citations index

### RFCs (5 primary files)

- `docs/rfcs/2026-08-12_v1_architecture-spec/semantic-layer-engine-architecture.md` — v1 architecture spec, §1 Goal, §2 Non-Goals, §3 Core Boundary, §5 Pipeline, §7 Contracts, §11 Repo Structure, §11a Deployment Module, §12 Adapter Conformance, §13 Definition of Done.
- `docs/rfcs/2026-08-12_v1_architecture-spec/plugins.md` — Plugin types (adapter-only, hook-only, composite, configuration-only); origin types (core, first-party, community); Rules 1-4; priority ranges.
- `docs/rfcs/2026-08-12_v1_architecture-spec/hooks.md` — 8 attachment points (pre/post × 4 stages); 5 behavioral types (validator, short-circuit/cache, enricher, mutator, observer); Rules 1-4.
- `docs/rfcs/2026-08-12_v1_architecture-spec/adapters.md` — 5 source categories (database, API, file, streaming, in-memory); capability dimensions; Rule 4 (per-connector URL grammar).
- `docs/rfcs/2026-08-12_v1_architecture-spec/cross-engine-conformance-matrix.md` — 5 shared mechanical checks; 9 abstract members; 4 reference engines (in-memory, trino, spark, duckdb); template for adding a 5th.
- `docs/rfcs/2026-08-23_feat_semantic-graph/semantic-graph_proposal.md` — v1.1 proposal with 2 BLOCKER + 2 required edit fixes from the 2026-08-23 review; §3 module shape; §4 known gaps; §5 v1.1 fixes; §6 suggested next step.

### ADRs (head + key body sections read)

- `docs/adr/README.md` — index, **partially stale** on ADR-013/014/015 (all shipped per git log; README still reads "Proposed").
- `docs/adr/0007-v0.1.0-cut-plan.md` — v0.1.0 freeze state, RFC §13 DoD, plugin-conformance gap (PR-D proposed, not shipped).
- `docs/adr/0008-q-sdk-redesign-rename-phantom-typed.md` (lines 1-100) — Status: Proposed (v2 2026-08-19); 21 review findings resolved; 3 atomic PR sequence.
- `docs/adr/0008-r-aggregation-groupby-having-limit-parts-window.md` (lines 1-100) — Status: Proposed (2026-08-19); closes ADR-008-L GAPs 5/6/7/8; adds window functions.
- `docs/adr/0009-e-driver-materialization-bounds.md:1-10` — Status: Accepted.
- `docs/adr/0010-a-enginehookdispatcher-stage-parameter.md:1-10` — Status: Implemented (PR-189).
- `docs/adr/0012-a-modelservice-restate-handler.md` (full, 224 lines) — Status: Proposed; 3 handlers (listModels, getModel, describe); ModelSummary DTO; Out-of-scope: ModelRegistry, hot-add/remove, multi-model serving.
- `docs/adr/0012-b-metricsservice-restate-handler.md:1-10` — Status: Accepted (wire surface only).
- `docs/adr/0012-b-followup-real-counter-instrumentation.md:1-10` — Status: Accepted (impl. PR-256).
- `docs/adr/0012-b-export-prometheus-metrics.md` (full, 254 lines) — Status: Proposed; REVISION 3 pivot to dedicated port (`--metrics-port 9090`); 9 metrics; 8 unit tests + smoke.
- `docs/adr/0012-c-configservice-restate-handler.md:1-10` — Status: Accepted (hold, negative decision); revisit-gate-1 cleared (PR-253).
- `docs/adr/0013-mcp-server.md:1-3` — Status: Proposed (header); shipped as PR-259 per git log.
- `docs/adr/0014-mcp-http-transport.md` (full) — Status: Proposed (header); shipped as PR-261 / PR-263 per git log; review-fix revisions applied.
- `docs/adr/0015-mcp-inprocess-stdio.md` (full) — Status: Proposed (header); shipped as PR-264 / PR-278 per git log; stdout-redirect refactor + in-process `--mcp-transport stdio`.
- `docs/adr/0016-engine-factory-companion.md` (full) — Status: Proposed (header); PR-272 / `1bcc395` may have shipped per git log.
- `docs/adr/0017-engine-impl-discover-iostream.md` (full) — Status: Proposed; PR-274 status unclear from this survey.

### Project status + retros

- `docs/project_status/2026-08-24-adr-009-c-v0.5-wave-retrospective.md` (lines 1-80) — per-query `newSession()`, skew seed helper, no follow-ups.
- `docs/project_status/2026-08-25-adr-009-d-v0.3-wave-retrospective.md` (lines 1-100) — broadcast + skew decision via `Context.meta` + `DecisionHints`; 3 revision lesson; oracle-first pattern.
- `docs/project_status/2026-08-26-adr-010-a-orchestration-layer-retrospective.md` (lines 1-120) — `HookRunnerOrchestration` drives all 4 stages; typed-error surfacing via `ctx.meta`; `Context.stop = true` honored across stages.
- `docs/release-notes/pr213-connector-sdk-removal.md` — release notes for PR-213 (Connector SDK removal).

### Repo conventions

- `AGENTS.md` (120 lines) — layer discipline (RULE#1), dual-review (RULE#5), RFC/ADR conventions (RULE#2), test conventions, output format, user preferences, jcode setup, common gotchas, recent shipped PRs.

### Code references (parked follow-ups + cross-cutting)

- `sm8-platform/src/test/scala/io/sm8/platform/mcp/StdioEndToEndSpec.scala:97` — `cancel("sm8-smoke-cp.txt not buildable (CI-only test)")` (the 5 environment failures).
- `plugins/cache-plugin/src/test/scala/io/sm8/plugins/cache/CachePluginP25Spec.scala:1-19` — P2.5 regression spec header naming `CacheWritePostHook.run` (CachePlugin.scala:226-230) as the dual-review site.
- `plugins/cache-plugin/src/test/scala/io/sm8/plugins/cache/CachePluginP25Spec.scala:90-104` — the assertion: `out.meta.contains("sm8.cache.write.error") shouldBe true`.
- `plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala:226-230` — **NOT VERIFIED by this survey** — needs read before recommending 3.3 as a "fix" vs "verify+close".
- `sm8-core/src/test/scala/io/sm8/sdk/contract/` — directory that ADR-007 flagged as missing the `PluginContractSpec` extension for the 6 reference plugins; not verified open/closed by this survey.

### Git context

- `git log --oneline -20` at `50a2cef` (the survey-time HEAD) — confirms C10 PR-C2 landed as the most recent merge; the MCP wave is fully shipped; v1 RFC track is closed.
- `git log --oneline --all -- docs/release-notes` — only PR-213 has a release-notes file; no `v0.1.0` or `v1.0` release notes exist yet (consistent with the standing "dont bump version yet" directive from ADR-008-Q).
