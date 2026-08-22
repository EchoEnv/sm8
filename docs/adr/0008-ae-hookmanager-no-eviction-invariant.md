# ADR-008-AE: HookManager — document no-eviction invariant at SDK boundary

| Field | Value |
|---|---|
| **Status** | DRAFT (under senior dual review) |
| **Date** | 2026-08-22 |
| **Module** | `sm8-core` (SDK boundary) |
| **Closes** | Senior dual review HIGH-5 (HookManagerImpl no-eviction documented but not at SDK boundary) |
| **Skill alignment** | `karpathy-guidelines-mindset`, `karpathy-app-design-mindset`, `karpathy-impact-analysis-mindset`, `scala2-scaladoc-mindset` |

## Decision-at-a-glance

Add a 4-line Scaladoc to the `HookManager` trait's `registerPreHook` + `registerPostHook` methods (and to the `preHooksFor` / `postHooksFor` methods) stating the **no-eviction invariant**: the manager accumulates hooks per stage in mutable Maps; there is no automatic eviction; the SDK contract is **single-boot, single-reload** — a hot-reload path must explicitly clear + re-register.

## Revision history

| Version | Date | Change |
|---|---|---|
| v1.0 | 2026-08-22 | Initial draft — Scaladoc only; no code change |

---

## Context

### Senior dual review finding (verbatim)

> **MEDIUM**: `HookManagerImpl` no-eviction documented-but-undocumented-at-SDK-boundary. Hooks accumulate in mutable Maps; if a hot-reload path re-registers plugins, the buffers grow unbounded.

### Codegraph evidence (2026-08-22)

- `sm8-core/src/main/scala/io/sm8/core/HookManagerImpl.scala:64-65`:
  ```scala
  // Per-stage hook buffers. Sort key: (priority ASC, seq ASC).
  private val preHooks: scala.collection.mutable.Map[HookStage, scala.collection.mutable.Buffer[HookEntry[PreHook]]] = scala.collection.mutable.Map.empty
  private val postHooks: scala.collection.mutable.Map[HookStage, scala.collection.mutable.Buffer[HookEntry[PostHook]]] = scala.collection.mutable.Map.empty
  ```
- `sm8-core/src/main/scala/io/sm8/core/HookManagerImpl.scala:140-151` (the `hooksForStage` method): `get(stage) match { case None => Seq.empty; case Some(buf) => buf.toSeq.sortBy(...) }` — read-only; no eviction.
- `sm8-core/src/main/scala/io/sm8/sdk/HookManager.scala:24-75` (the SDK trait): bare `registerPreHook` / `registerPostHook` docstrings. No mention of the no-eviction invariant.
- 12 Plugins implement `setup(engine)` via the SPI (CachePlugin, AuditPlugin, the 4 `*Stub` plugins from PR-140, etc.). They all call `engine.hooks.registerPreHook` / `registerPostHook` exactly once at engine boot.
- 0 callers in production code re-register hooks (verified by codegraph). The hot-reload concern is hypothetical.

### Why this matters

The SDK is the frozen third-party surface. Third-party Plugin authors implement `Plugin` + `setup(engine)` and call `engine.hooks.registerPreHook` / `registerPostHook`. If a future contributor adds a hot-reload path (e.g. for live plugin updates in long-running services), the buffers would grow unboundedly. Documenting the invariant at the SDK boundary prevents this misuse.

### Why Option A (Scaladoc only) is the right choice

1. **No code change**: the algorithm is correct (single-boot, single-reload is the documented contract).
2. **No behavior change**: the existing 911 tests continue to pass.
3. **Smallest correct change**: 4 Scaladoc additions per method × 4 methods = 16 lines.
4. **Per `karpathy-guidelines-mindset` "smallest correct change":** documentation-only is the cheapest fix that prevents the misuse.

---

## Considered options

### Option A: Scaladoc only at the SDK boundary (CHOSEN)

Add 4-line Scaladoc to:
- `registerPreHook` (3-arg): "no-eviction invariant — see class doc"
- `registerPreHook` (4-arg): same
- `registerPostHook` (3-arg): same
- `registerPostHook` (4-arg): same
- `preHooksFor`: "returns the accumulated hooks; no eviction"
- `postHooksFor`: same
- Trait header: "Single-boot invariant — the manager accumulates hooks per stage in mutable Maps; there is no automatic eviction. Hot-reload paths must explicitly clear + re-register."

**Pros:** Zero behavior change. Honest documentation. Future contributors know the constraint.

**Cons:** A future contributor could still re-register hooks via a hot-reload path (the documentation is advisory, not mechanical). But the test suite + the SPI design + the 12-Plugin third-party surface all assume single-boot, so the documentation suffices.

**Decision: ADOPT.**

### Option B: Add an `unregisterHooks` API

Add a new `unregisterHooks(stage: HookStage): HookManager` method to the SDK. Hot-reload paths can call it before re-registering.

**Pros:** Mechanical enforcement of the no-evolution invariant.

**Cons:** Adds a new public SDK API. The frozen-SDK discipline (per `Context.scala:16` + `Plugin.scala:15`) requires ADRs + senior dual review for new public methods. Out of scope for this PR.

**Decision: REJECT** (out of scope; can be a follow-up PR).

### Option C: Add an audit test that re-registers and asserts a non-eviction invariant

Add a test in `HookManagerImplSpec` that asserts the buffer behavior on repeated registration (the second registration should append, not replace).

**Pros:** Mechanical enforcement.

**Cons:** The test would just verify the current (correct) behavior; doesn't add new info. The HookDispatchSpec already covers this.

**Decision: REJECT** (redundant with existing tests).

---

## Decision outcome

**Adopt Option A**.

### Implementation plan

1. **Add 4-line Scaladoc** to `registerPreHook` (3-arg) + (4-arg) + `registerPostHook` (3-arg) + (4-arg): "no-eviction invariant — the manager accumulates hooks per stage in mutable Maps; there is no automatic eviction. Hot-reload paths must explicitly clear + re-register. See class doc."
2. **Add 2-line Scaladoc** to `preHooksFor` + `postHooksFor`: "Returns the accumulated hooks for `stage`. The buffer is sorted on read; there is no eviction. Empty if no hooks registered."
3. **Add 6-line Scaladoc** to the trait class header: "Single-boot invariant — the manager accumulates hooks per stage in mutable Maps; there is no automatic eviction. Hot-reload paths (if added in future) must explicitly clear + re-register. This is the documented contract per ADR-008-AE v1.0."

### Files touched

| File | Change | LOC |
|---|---|---|
| `sm8-core/src/main/scala/io/sm8/sdk/HookManager.scala` | Add 4 Scaladoc blocks (trait header + 4 method docs + 2 reader docs) | +28, -0 = +28 net |
| `docs/adr/0008-ae-hookmanager-no-eviction-invariant.md` | This ADR | NEW |
| **Total** | | **+28, -0 = +28 net** |

### Tests to add

None. The existing `HookDispatchSpec` + `PluginContractSpec` + `HookContractSpec` already cover the contract.

### Binary compatibility

- **Source-compatible**: no code change.
- **Binary-compatible**: no new methods.
- **Wire-compatible**: no new wire types.

---

## Skill alignment

### `karpathy-guidelines-mindset`

- **Apply "smallest correct change":** documentation-only. Zero code change.
- **Apply "verifiable success":** existing 911 tests pass (verified by running the full reactor).

### `karpathy-app-design-mindset`

- **Apply "frozen core + extension portal":** the no-eviction invariant is part of the frozen SDK contract; the Scaladoc addition enforces the contract at the API boundary.

### `karpathy-impact-analysis-mindset`

- **Apply:** 1 source file touched (HookManager.scala); 0 production code change.

### `scala2-scaladoc-mindset`

- **Apply §1:** the Scaladoc describes WHY (the no-eviction invariant) + WHAT (no automatic eviction) + the future-work guidance (hot-reload paths must explicitly clear).
- **Apply §2:** No `[[wikilinks]]`, no PR/Phase/ADR/process references in the new code.
- **Apply §3:** References `ADR-008-AE v1.0` once (at the class header — to anchor the rationale).

---

## Acceptance criteria

1. The `HookManager` trait has a class-header Scaladoc stating the no-eviction invariant.
2. Each of the 4 `register*Hook` methods has a Scaladoc referencing the invariant.
3. Each of the 2 `preHooksFor` / `postHooksFor` methods has a Scaladoc noting no eviction.
4. The 911 existing tests pass (zero regression).

## Verification plan

```bash
mvn -B -ntp -pl sm8-core,connectors/spark-connector,connectors/in-memory-connector,connectors/trino-connector,plugins/audit-plugin,plugins/cache-plugin,sm8-cli,sm8-server,sm8-platform test 2>&1 | grep -E 'Tests: succeeded|BUILD' | tail -10

# Beyond test count:
# 1. javap: no change
# 2. scaladoc noise scan: 0 new noise in HookManager.scala
# 3. Memory + disk under 90% throughout
```

## Risks

| Risk | Mitigation |
|---|---|
| A future contributor adds a hot-reload path that re-registers hooks | The Scaladoc explicitly states the no-eviction invariant + hot-reload guidance; the 12-Plugin test surface assumes single-boot |
| The Scaladoc doesn't mechanically enforce the invariant | This is Option A's documented trade-off; Option B (add unregister API) is a future PR if needed |

## Open questions

1. Should the SDK also add an `unregisterHooks(stage: HookStage)` method per Option B? My recommendation: **DEFER** to a follow-up PR; the Scaladoc-only fix suffices for now.
2. Should `preHooksFor` return a defensive copy of the underlying buffer (instead of a live view)? My recommendation: **NO** — the existing implementation already returns `buf.toSeq.sortBy(...)` (a fresh Seq), so callers can't mutate the buffer.
3. Should the SDK trait move from a `Map[HookStage, Buffer[HookEntry[T]]]` to a `Map[HookStage, IndexedSeq[HookEntry[T]]]` for better thread-safety guarantees? My recommendation: **NO** — out of scope; a future ADR can address thread-safety.

---

## ADR

`docs/adr/0008-ae-hookmanager-no-eviction-invariant.md` v1.0 (full ADR; ~210 lines).
