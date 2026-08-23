# Final Pre-Merge Review: PR-157 + #158 + #159 follow-up chain (HEAD 66139d5)

**Reviewer**: `followup-3pr-review` scout | **Verdict**: **APPROVE** | **Zero blockers**

## Summary

All 3 deferred advisory findings from the 3rd-pass cumulative-session-review are addressed. 2 new test files (PR-157, PR-158) plus 1 docstring fix (PR-159). Zero production-code changes. 981-test reactor passes. Zero orphan processes. Chain is final-merge-ready.

---

## Section 1 — PR-157: `SparkEngineProviderCloseLifecycleSpec` — PASS

**Test file**: `connectors/spark-connector/src/test/scala/io/sm8/connectors/spark/SparkEngineProviderCloseLifecycleSpec.scala` (103 lines, 2 tests)

**Evidence**:
- Test 1 (`close(): unpersists tracked DataFrames, clears the map, and is idempotent`) correctly asserts `df1.storageLevel == StorageLevel.NONE` and `df2.storageLevel == StorageLevel.NONE` after `provider.close()` — this is the visible side effect of the close()-loop calling `df.unpersist()` on each tracked DataFrame.
- Test 2 (`close(): with no tracked DataFrames is a no-op (does not throw)`) correctly exercises the empty-map branch.
- `trackPersist` visibility verified: `private[spark]` at `SparkEngineProvider.scala:120` — accessible from any class in package `io.sm8.connectors.spark`, including this test which shares the same package.
- `@transient persistedFrames` lifecycle verified: PR-148 L2 documentation note in test header (lines 9-12) correctly explains the `@transient` annotation is irrelevant for `close()` because `close()` runs in the live JVM, not via deserialization.
- `SparkSession.stop()` called twice is safe per Spark docs — second stop is a no-op.

**Notes**: The test correctly observes the unpersist side effect via `df.storageLevel` transition (MEMORY_AND_DISK → NONE), which is the canonical Spark way to verify cache state. The double-close idempotency assertion (line 75 + line 79) confirms both that `close()` doesn't throw on an already-stopped session AND that the persistedFrames map clearing handles re-entry.

---

## Section 2 — PR-158: `MetaInspectorServiceE2ESpec` — PASS

**Test file**: `sm8-platform/src/test/scala/io/sm8/platform/query/MetaInspectorServiceE2ESpec.scala` (273 lines, 2 tests)

**Evidence**:
- Test 1 (`end-to-end: MetaInspectorService.getMeta returns the snapshot's wire projection`) correctly wires a real `MetaInspectorService.definition` (not a mock), invokes the `getMeta` handler via `HandlerRunner`, and verifies the wire JSON round-trip.
- Jackson null handling verified: test uses `(cycleError == null || cycleError == None) shouldBe true` (line 226) which correctly accepts both Jackson's null serialization for missing JSON keys AND Scala's `None` — defensive and correct.
- `StubEngineProvider` correctly implements `EngineProvider` (line 79-87): identity, available=true, query=???, explain=???, close=(). It is NOT a null cast — it is a proper in-test stub overriding all 5 methods.
- **No plugin module dependency**: test imports only `sm8-core` (Model, EngineRegistry, EngineProvider) and `sm8-platform` (MetaInspectorService, MetaRequest, MetaResponse). It does NOT import from `plugins/semantic-graph-plugin`. The `StubGraphSnapshot` case classes mirror GraphSnapshot's shape in-test without depending on the plugin module — **architecture-spec §3 boundary preserved**.

**Notes**: The `HandlerContext` stub is hand-rolled with all 20+ methods throwing `UnsupportedOperationException` for unused methods — matches the `QueryServiceSpec` pattern. The test mirrors the production wire shape via `toMetaValue` (in-test projection of the snapshot to `Map[String, Any]`). The absent-key test (Test 2) correctly asserts `resp.present=false` and `resp.value=None`.

---

## Section 3 — PR-159: `SemanticGraphPlugin` docstring fix — PASS

**File**: `plugins/semantic-graph-plugin/src/main/scala/io/sm8/plugins/semanticgraph/SemanticGraphPlugin.scala` (modified, +12/-5 lines)

**Evidence**:
- Class-level Scaladoc now correctly states: *"Idempotency note: this `setup` method itself does NOT dedupe — calling it twice would register each hook twice. The idempotency boundary is `Engine.use(plugin)` (in the deployment module), which registers a `Plugin` instance exactly once per JVM."* (lines 28-34 of new file)
- Method-level Scaladoc now correctly states: *"Idempotency is enforced at the caller (see the class-level Scaladoc); this method must be called exactly once."* (lines 43-45)
- Docstring is consistent with the actual code — `setup()` does NOT deduplicate (it calls `registerPreHook` and `registerPostHook` unconditionally); `Engine.use()` guarantees one-shot registration per the SDK trait (`sm8-core/src/main/scala/io/sm8/sdk/Engine.scala:38-43` explicitly states *"the same Plugin instance is not registered twice"*).
- **Zero production code changes** — this is docstring-only.

**Notes**: The fix correctly shifts the idempotency claim from `setup()` (incorrect — setup does not dedupe) to `Engine.use()` (correct — use() enforces one-shot registration). This aligns the docstring with the actual SDK contract.

---

## Section 4 — Skill alignment re-audit (1-line verdict per skill, all 9)

| Skill | Verdict |
|---|---|
| `karpathy-guidelines-mindset` | **PASS** — surgical 3-file diff, no over-engineering, each PR <500 LOC, verifiable success criteria |
| `karpathy-app-design-mindset` | **PASS** — plugins remain hook-only (no adapters), extension authors still go through `Engine.use(plugin)`, no core boundary violations |
| `scala-error-handling-mindset` | **PASS** — PR-157 `close()` swallows per-frame unpersist errors with try/catch (typed `Throwable`), PR-158 uses `Either` for handler return, PR-159 docstring-only (no logic change) |
| `scala-jvm-safety-mindset` | **PASS** — PR-157 `close()` finally block ensures `spark.stop()` runs even on assertion failure; PR-158 `HandlerContext` stub uses `CompletableFuture.completedFuture` for void returns; no resource leaks |
| `scala-impact-analysis-mindset` | **PASS** — blast radius verified via codegraph_explore: PR-157 affects 1 file in test scope only, PR-158 affects 1 file in test scope only, PR-159 is docstring-only with zero callsite changes |
| `scala-spark-batch-bugs-mindset` | **PASS** — PR-157 test exercises the actual `SparkSession.stop()` lifecycle and `@transient persistedFrames` field; the close()-time unpersist is verified via `storageLevel` transition |
| `scala-perf-testing-mindset` | **PASS** — PR-157 uses `local[*]` master (minimum overhead), PR-158 uses minimal `StubEngineProvider` (no real engine calls), no perf claims made |
| `scala-bug-hunting-mindset` | **PASS** — PR-158 Jackson null handling explicitly tested (`cycleError == null || == None`), PR-157 idempotency explicitly tested (double-close), PR-159 docstring now matches SDK contract |
| `scala2-scaladoc-mindset` | **PASS** — PR-159 Scaladoc is precise and accurate: names the idempotency boundary, cites `Engine.use`, uses class-level Scaladoc for the global rule and method-level for the per-call constraint |

---

## Section 5 — Final-merge-readiness checklist

| # | Item | Status | Notes |
|---|---|---|---|
| 1 | All 3 deferred advisory findings addressed | **PASS** | data-eng WARN-1 (SparkEngineProvider close lifecycle) → PR-157; data-eng WARN-2 (MetaInspectorService wire round-trip) → PR-158; architect LOW (SemanticGraphPlugin idempotency docstring) → PR-159 |
| 2 | No new BLOCKER/HIGH/MEDIUM findings vs prior 6 reviews | **PASS** | Zero new findings; all 3 PRs are test additions or docstring fixes with no production-code callsite changes |
| 3 | Zero production-code callsites changed | **PASS** | PR-157 = new test file; PR-158 = new test file; PR-159 = docstring-only (no code changes) |
| 4 | Zero Spark types captured | **PASS** | PR-157 uses SparkSession/DataFrame in test scope (expected); PR-158 test does NOT capture Spark types (uses `Map[String, Any]`); PR-159 has no code |
| 5 | Scaladoc noise scan | **PASS** | grep across changed files for TODO/FIXME/XXX → 0 hits in PR-157, PR-158, PR-159 files. Pre-existing TODOs in `docs/` and `RestatedEngineRunner.scala` are out of scope (not introduced by this chain) |
| 6 | Memory + disk under 90% | **PASS** | memory 74%, disk 65% |
| 7 | Zero orphan codegraph/metals/bloop processes | **PASS** | verified via environment |
| 8 | All 3 PRs independently revertable | **PASS** | PR-157 = 103 lines new test; PR-158 = 273 lines new test; PR-159 = +12/-5 docstring-only. Each <500 LOC, self-contained, no shared types touched |
| 9 | No `TODO`/`FIXME`/`XXX` markers in new code | **PASS** | grep confirmed 0 hits in all 3 changed files |
| 10 | v0.1.0 tag readiness | **GATED** | per the user's 2026-08-20 "don't bump version yet" directive. The 981-test reactor pass, zero orphan processes, zero noise, and all 3 advisory findings closed make this chain **ready for the tag the moment the user gives the signal** |

---

## Final verdict

**APPROVE** — the 3-PR follow-up chain (PR-157 + #158 + #159) is final-merge-ready. All 3 deferred advisory findings are addressed with surgical, self-contained changes. Zero production-code callsite changes. Zero new findings. The chain is v0.1.0-ready pending the user's signal.

**Zero blockers.**
