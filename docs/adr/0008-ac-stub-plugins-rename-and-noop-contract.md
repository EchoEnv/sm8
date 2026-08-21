# ADR-008-AC: Stub plugins — rename + verify no-op contract

| Field | Value |
| **Status** | **v1.0 — under senior dual review** (DataEng 0.92; Architect result not captured; Option B adopted per senior dual review) |
| **Date** | 2026-08-22 |
| **Module** | `plugins/materialize-plugin`, `plugins/row-cap-plugin`, `plugins/broadcast-plugin`, `plugins/skew-plugin`, `plugins/audit-plugin` |
| **Closes** | Senior dual review (Wave 1 + Wave 2 planning) — 4 stub plugins are counter-only; AuditPlugin has the same shape |
| **Author** | Wave 2 PR-140 |
| **Skill alignment** | `scala-data-driven-refactor-mindset`, `karpathy-app-design-mindset`, `karpathy-guidelines-mindset`, `karpathy-impact-analysis-mindset`, `debug-mantra-mindset`, `scala2-scaladoc-mindset` |

## Decision-at-a-glance

Rename the 5 stub plugins (`*Plugin` → `*Stub`) and add a `*StubNoOpContractSpec` per plugin asserting the no-op contract (the hook runs without throwing AND the returned `Context` equals the input `Context`). Document that the plugins are scaffolding awaiting the typed `Result` API + Spark-specific paths (which live in `connectors/spark-connector`).

## Revision history

| Version | Date | Change |
|---|---|---|
| v1.0 | 2026-08-22 | Initial draft — Option B (rename + no-op contract) per DataEng review |

---

## Context

### Senior dual review finding (2026-08-21)

> `MaterializePlugin` / `RowCapPlugin` / `BroadcastPlugin` / `SkewPlugin` are counter-only stubs — they register a hook that increments a counter and returns `context` unchanged. They don't do the work their names imply. `AuditPlugin` is structurally identical.

### Codegraph evidence (2026-08-22)

- `MaterializePlugin.scala:64-67` (MaterializePreHook.run) + `MaterializePlugin.scala:77-80` (MaterializePostHook.run) → both `counter.incrementAndGet(); context`
- `RowCapPlugin.scala:70-74` (RowCapPostHook.run) → `counter.incrementAndGet(); context` (comment: "Real implementation: ctx.result would be capped to config.maxRows when the typed Result shape ships")
- `BroadcastPlugin.scala:50-53` (BroadcastPreHook.run) → `counter.incrementAndGet(); context` (comment: "Real implementation will set the broadcast threshold on the SparkConf before query execution")
- `SkewPlugin.scala:37-40` (SkewPreHook.run) → `counter.incrementAndGet(); context` (comment: "real implementation will set the AQE skew threshold via spark.sql.adaptive.skewJoin.skewedPartitionFactor")
- `AuditPlugin.scala:51-56` (AuditPostHook.run) → `counter.incrementAndGet(); context` (same shape — AuditPlugin is NOT a real reference impl despite the Scaladoc framing)

### Why Option A (real implementation) is infeasible

Per the senior dual review (DataEng verdict 0.15):

1. **Spark bannedDependencies**: 4 plugin `pom.xml`s have `<bannedDependencies>org.apache.spark:*</bannedDependencies>` (MaterializePlugin.scala:64-67 comment: "The Spark-specific StorageLevel capture lives in the spark-connector per the Module Map"). Implementing real behavior requires lifting the ban, which violates ADR-008-M6 Module Map.
2. **Spark-connector already handles Materialize**: `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/PortableQueryCompiler.scala:507-514` already handles `MaterializePolicy.Persist` with typed `UnsupportedCapability` error. `SparkEngineProvider.scala:435-437` already handles persist + collect + unpersist per ADR-008-P §A3. Implementing the same logic in plugins would duplicate the spark-connector paths.
3. **Racy SparkConf mutations**: `BroadcastPlugin` and `SkewPlugin` would need to call `SparkSession.conf.set(...)` inside per-request hooks. This mutates the global session config under concurrent queries — a known correctness anti-pattern. The correct place for this is query-plan construction in the spark-connector, not hook-time mutation.
4. **Result API is frozen**: `Context.result: Option[Result]` where `Result` is a marker trait (sm8-core/src/main/scala/io/sm8/sdk/Context.scala:16: "Frozen after Step 1. Any change to field set is a breaking SDK change"). `RowCapPlugin` cannot call `ctx.result.take(maxRows)` because `Result` has no `take` method.
5. **SDK `Plugin` trait is frozen**: `Plugin.scala:15: "Frozen after Step 1. The setup(engine: Engine) method signature is the SDK contract. Any change is a breaking SDK change."` — adding new abstract methods would break every third-party plugin author.

### Why Option C (delete) is infeasible

- The 4 plugins + AuditPlugin are the **only examples** of extending `HookContractSpec` + `PluginContractSpec` from `sm8-sdk/test-jar`. Deleting them loses the only ADR-007 contract-test fixtures.
- The parent reactor test command (ADR-008-Y:316) includes the 4 plugins. Deleting them breaks the test command.
- The plugins are the third-party-plugin-author template. Deleting them removes the canonical example.

---

## Considered options

### Option A: Implement real behavior

**Pros:**
- The plugins would actually do what their names imply.
- Users would see observable persistence / capping / broadcast / skew behavior.

**Cons:**
- **400+ LOC** across 4 plugins.
- **SDK break**: `Result` needs `Seq[Expr]` extension + `take` method.
- **Spark boundary violation**: 4 plugin `pom.xml`s would need `<bannedDependencies>` lifted.
- **Correctness anti-pattern**: per-request `SparkConf.set(...)` mutation is racy under concurrent queries.
- **Duplicates spark-connector**: `PortableQueryCompiler.scala:507-514` and `SparkEngineProvider.scala:435-437` already handle the same logic in the right place.
- **Out of PR-140 scope**: 1 PR cannot ship all 4 implementations + the SDK break + the spark-connector dedup + the test matrix.

**Decision: REJECT** (per DataEng verdict 0.15).

### Option B: Rename + document + verify no-op contract (CHOSEN)

1. **Rename**: `MaterializePlugin` → `MaterializeStub`, `RowCapPlugin` → `RowCapStub`, `BroadcastPlugin` → `BroadcastStub`, `SkewPlugin` → `SkewStub`, `AuditPlugin` → `AuditStub`. Update META-INF/services/io.sm8.sdk.Plugin entries accordingly.
2. **Update Scaladoc**: each `*Stub` class has a 3-line Scaladoc explaining (a) what the real behavior would be, (b) where the real implementation lives (spark-connector for Spark-specific paths), (c) the gate condition for the real implementation.
3. **Add `*StubNoOpContractSpec` per plugin**: 5 new tests, 1 per plugin, asserting that `run(inputContext) shouldBe inputContext` AND the counter is incremented. This locks in the no-op contract mechanically so future contributors can't accidentally add real behavior without a test signal.
4. **Update `parent reactor test command`** (ADR-008-Y:316): no change to the command itself; only the artifact names change from `*Plugin` to `*Stub`.

**Pros:**
- ~80 LOC across 5 plugins.
- 0 SDK breaks.
- 0 Spark boundary violations.
- Preserves the contract-test fixtures.
- Honest naming: a `*Stub` is honest about doing nothing.
- Mechanical single-reviewer pass.
- The `*StubNoOpContractSpec` locks in the no-op contract mechanically.

**Cons:**
- The plugins still don't do anything (just acknowledged now via the `*Stub` naming).
- The 5 META-INF/services entries need renaming (mechanical, 5 files).
- The parent reactor test artifact names change (the test command itself is stable; just the artifact names in the compiled output).

**Decision: ADOPT** (per DataEng verdict 0.92).

### Option C: Delete

**Pros:**
- Cleans the reactor.
- Removes misleading plugins.

**Cons:**
- Loses the only ADR-007 contract-test fixtures (HookContractSpec + PluginContractSpec examples).
- Breaks the parent reactor test command (ADR-008-Y:316).
- Removes the third-party-plugin-author template.
- Future third-party plugin authors have no example to copy from.

**Decision: REJECT** (per DataEng verdict 0.30).

---

## Decision outcome

**Adopt Option B**.

### Implementation plan

1. **Rename 5 plugins** (10 files: 5 source + 5 META-INF/services):
   - `MaterializePlugin` → `MaterializeStub` (`MaterializePreHook` → `MaterializePreStubHook`, `MaterializePostHook` → `MaterializePostStubHook`)
   - `RowCapPlugin` → `RowCapStub` (`RowCapConfig` stays; `RowCapPostHook` → `RowCapPostStubHook`)
   - `BroadcastPlugin` → `BroadcastStub` (`BroadcastPreHook` → `BroadcastPreStubHook`)
   - `SkewPlugin` → `SkewStub` (`SkewPreHook` → `SkewPreStubHook`)
   - `AuditPlugin` → `AuditStub` (`AuditPostHook` → `AuditPostStubHook`)
2. **Update each `*Stub` Scaladoc** with the 3-line "real behavior lives at..." note.
3. **Add `*StubNoOpContractSpec`** per plugin (5 tests, 1 per plugin).
4. **Update META-INF/services entries** (5 files).
5. **Update existing `*Spec.scala` files** (5 files) — change class name + test descriptions to reference `*Stub`.

### Files touched

| File | Change | LOC |
|---|---|---|
| 5 `*Plugin.scala` files | Rename to `*Stub` + update Scaladoc | +15, -10 = +5 net |
| 5 `META-INF/services/io.sm8.sdk.Plugin` files | Update class names | 5 lines |
| 5 existing `*Spec.scala` files | Update class references + test names | +10, -5 = +5 net |
| 5 new `*StubNoOpContractSpec.scala` files | Add no-op contract test | +100, -0 = +100 net |
| `docs/adr/0008-ac-stub-plugins-rename-and-noop-contract.md` | This ADR | NEW |
| **Total** | | **+115, -15 = +100 net** |

### Tests to add

1. `MaterializeStubNoOpContractSpec`: `run(inputContext) shouldBe inputContext` + counter incremented
2. `RowCapStubNoOpContractSpec`: same
3. `BroadcastStubNoOpContractSpec`: same
4. `SkewStubNoOpContractSpec`: same
5. `AuditStubNoOpContractSpec`: same

### Binary compatibility

- **Source-CHANGED**: every reference to `MaterializePlugin`, `RowCapPlugin`, `BroadcastPlugin`, `SkewPlugin`, `AuditPlugin` in downstream code must be updated to the `*Stub` name. Verified: 0 production consumers in `sm8-platform/src/main` or `sm8-core/src/main`; only the spec files instantiate the plugins.
- **Wire-CHANGED**: the META-INF/services entries now name `*Stub` classes. Any third-party plugin code that explicitly loaded the old names would break. The classes themselves are not on any wire — only the SPI registration name changes.
- **SDK-compatible**: the `Plugin` trait signature is unchanged. Third-party plugin authors continue to implement `Plugin` + `setup(engine: Engine)`.

### Spec alignment

- The parent reactor test command (ADR-008-Y:316) compiles + tests the 4 plugins + AuditPlugin. After the rename, the same command tests the 5 `*Stub` artifacts.
- The `PluginContractSpec` + `HookContractSpec` base classes are unchanged. The existing 5 `*ContractSpec.scala` files (which test shape) are updated to reference `*Stub` classes.

---

## Skill alignment

### `scala-data-driven-refactor-mindset`

- **Apply:** the `*Stub` naming is honest data (each stub is a placeholder) — the alternative (counter-only with a non-stub name) is dishonest data.
- **Apply §1 "data is data, behavior lives elsewhere":** the real behavior lives in `connectors/spark-connector` (Spark-specific) and in the SDK (typed `Result` API). The stubs are scaffolding, not behavior.
- **Apply §3 "escalate to Map only when the rule set must change without a deploy":** N/A — the rule set is fixed at compile time.

### `karpathy-app-design-mindset`

- **Apply "frozen core + extension portal":** the SDK `Plugin` trait is the frozen core; the `*Stub` plugins are extension portal examples. The stubs are honest about being placeholders.
- **Apply:** third-party plugin authors copy the `*Stub` pattern + replace the counter with real behavior. The rename makes the copy pattern explicit.

### `karpathy-guidelines-mindset`

- **Apply "smallest correct change":** Option B is the smallest change that closes the senior dual review's HIGH finding.
- **Apply "verifiable success":** the 5 `*StubNoOpContractSpec` tests verify the no-op contract mechanically.

### `karpathy-impact-analysis-mindset`

- **Apply:** 0 production consumers of the old class names. The rename is local to the plugins module + META-INF/services + spec files.
- **Apply:** the wire-compat impact is limited to SPI registration names; the SDK surface (Plugin trait) is unchanged.

### `debug-mantra-mindset`

- **Apply SS1 (reproduce):** the 5 `*StubNoOpContractSpec` tests reproduce the "no-op" invariant.
- **Apply SS2 (trace):** each test asserts `run(inputContext) shouldBe inputContext` AND the counter is incremented.
- **Apply SS5 (verify):** the existing `*ContractSpec` tests still pass after the rename (no behavior change).

### `scala2-scaladoc-mindset`

- **Apply §1:** each `*Stub` Scaladoc describes the current state + the future-real-implementation location. No `[[wikilinks]]`, no PR/Phase/ADR/process references in the new code.
- **Apply §3:** TODOs are attributed (`// FUTURE: see ADR-008-AC for the real-implementation path`).

---

## Acceptance criteria

1. The 5 plugins are renamed to `*Stub` (MaterializeStub, RowCapStub, BroadcastStub, SkewStub, AuditStub).
2. The 5 META-INF/services entries are updated to the new class names.
3. The 5 `*StubNoOpContractSpec.scala` files exist and assert `run(inputContext) shouldBe inputContext` + counter incremented.
4. The 5 existing `*ContractSpec.scala` files are updated to reference `*Stub` classes.
5. The 5 existing `*Spec.scala` files are updated to reference `*Stub` classes.
6. The 4 `*Plugin.scala` source files have updated Scaladoc with the 3-line "real behavior lives at..." note.
7. The parent reactor test command passes (`mvn -pl ... test`).
8. The 908 existing tests pass (zero regression).
9. The 5 new `*StubNoOpContractSpec` tests pass (5 new tests).
10. No Spark bannedDependencies are lifted.

## Verification plan

```bash
mvn -B -ntp -pl plugins/materialize-plugin,plugins/row-cap-plugin,plugins/broadcast-plugin,plugins/skew-plugin,plugins/audit-plugin,sm8-core,connectors/spark-connector,connectors/in-memory-connector,connectors/trino-connector,plugins/cache-plugin,sm8-cli,sm8-server,sm8-platform test 2>&1 | grep -E 'Tests: succeeded|BUILD' | tail -15

# Beyond test count:
# 1. javap -c -p on the 5 *Stub classes: checkcast count stable (no new wire types)
# 2. scaladoc noise scan: 0 new noise in my added lines
# 3. Memory + disk under 90% throughout
```

## Risks

| Risk | Mitigation |
|---|---|
| Downstream consumers reference the old `*Plugin` class names | 0 production consumers verified by codegraph; the spec files are updated in the same PR |
| META-INF/services entries name classes that don't exist after the rename | The entries are updated in the same PR (5 files, 5 lines) |
| The `*StubNoOpContractSpec` tests don't catch all future regressions | The tests assert BOTH identity (`run(inputContext) shouldBe inputContext`) AND state change (`counter.get shouldBe 1 after run`) — both must hold |
| Third-party plugin authors copy the `*Stub` pattern and ship stubs in production | The `*Stub` naming is honest; the Scaladoc explicitly states "scaffolding"; the `*StubNoOpContractSpec` is a template for the no-op contract |

## Open questions

1. Should the rename also extend to `AuditPlugin`? My recommendation: **YES** — for symmetry (same shape; same honesty requirement).
2. Should the 5 `*StubNoOpContractSpec` files live in each plugin's test/ folder, or in a shared `sm8-sdk` test-jar? My recommendation: **each plugin's test/ folder** — closer to the implementation, easier to evolve together.
3. Should we deprecate the SDK `Plugin` trait's `name` field (it's redundant with the class name)? Out of scope for PR-140.
4. Should we add a 6th plugin (e.g., `CacheEvictStub`) for symmetry? Out of scope — the existing `CachePlugin` is a real implementation, not a stub.
