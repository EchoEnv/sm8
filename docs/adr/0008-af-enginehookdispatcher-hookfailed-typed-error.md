# ADR-008-AF: EngineHookDispatcher — typed HookFailed error when a hook throws

| Field | Value |
| **Status** | **v1.1 — review fixes applied** (sanitized HookFailed.message; engineer = "<dispatcher>") |
| **Date** | 2026-08-22 |
| **Module** | `sm8-platform` (EngineHookDispatcher) |
| **Closes** | Senior dual review MEDIUM (EngineHookDispatcher typed HookFailed error; hook identity lost on throw) |
| **Skill alignment** | `scala-error-handling-mindset`, `karpathy-guidelines-mindset`, `karpathy-impact-analysis-mindset`, `karpathy-app-design-mindset`, `debug-mantra-mindset`, `scala-data-driven-refactor-mindset`, `scala2-scaladoc-mindset` |

## Decision-at-a-glance

Add `EngineError.HookFailed(engine, name, priority, stage, message)` as a new variant. Catch `RuntimeException` from each individual hook invocation in `firePre` and `firePost` (inside the `foldLeft` loop), convert to `Left(HookFailed(...))` if the firing stage is in a typed-`Either` return shape — otherwise propagate (preserving the existing fail-fast contract). The dispatcher's `run` already returns `Either[EngineError, Context]`; we convert the per-hook exception to a typed error before it propagates.

## Revision history

| Version | Date | Change |
|---|---|---|
| v1.0 | 2026-08-22 | Initial draft — typed HookFailed + per-hook try/catch in firePre/firePost |

---

## Context

### Senior dual review finding (verbatim)

> **MEDIUM**: `EngineHookDispatcher.run` typed `HookFailed` error. Hooks that throw abort the pipeline (RFC §9 fail-fast) but currently surface as a raw `RuntimeException` rather than a typed `EngineError`. The hook identity (name) is lost — the caller can't tell which hook in a 12-hook chain failed.

### Codegraph evidence (2026-08-22)

- `sm8-platform/src/main/scala/io/sm8/platform/query/hooks/EngineHookDispatcher.scala:106-113` (firePre):
  ```scala
  private def firePre(stage: PipelineStage, ctx: Context): Context = {
    val hookStage: HookStage = preStageFor(stage)
    val pre: Seq[(PreHook, Int)] = hooks.preHooksFor(hookStage)
    pre.foldLeft(ctx) { (c, hp) =>
      if (c.stop) c
      else hp._1.run(c)  // <-- raw hook.run(); throws on failure
    }
  }
  ```
- `sm8-platform/src/main/scala/io/sm8/platform/query/hooks/EngineHookDispatcher.scala:132-139` (firePost): same pattern.
- `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:212-242` (executeEngine): catches `RuntimeException` from `provider.query(...)` → converts to `EngineError.ProviderInvocationFailed`. Does NOT catch from `dispatcher.run(...)`.
- `sm8-platform/src/main/scala/io/sm8/platform/query/QueryService.scala:228-251`: caller pattern — `match { Right(qr) => ...; Left(err) => throw TerminalException(code, err.toString) }`. Only catches `Left(err)` from `EngineService.runQueryWithHooks`. Does NOT catch `RuntimeException` propagated from hook throws.
- 0 production callers in `sm8-core/src/main` or `connectors/*/src/main` reference hook throws; only `EngineHookDispatcherSpec` + `SparkEngineProviderHookRunnerSpec` exercise the hook paths.

### Why this matters

A hook throw currently:
1. **Loses hook identity** — the caller sees `RuntimeException` with the hook's stack trace but not the hook's `name` field
2. **Violates the typed-error wire contract** — `EngineError` has 11 sealed variants; `HookFailed` would be the 12th. The current path skips the typed dispatch.
3. **Violates the fail-fast observability principle** — RFC §9 fail-fast says the failing hook's name MUST appear in the error report so operators can debug.
4. **Causes inconsistency** — `EngineService.executeEngine` converts engine `RuntimeException` to typed `ProviderInvocationFailed` (per the senior dual review finding from 2026-08-21). The hook path doesn't follow this pattern.

---

## Considered options

### Option A: Add `EngineError.HookFailed` variant + per-hook try/catch (CHOSEN)

Add a new variant to the `EngineError` ADT:

```scala
final case class HookFailed(
  engine: String,        // engine identity name (e.g. "spark", "<dispatcher>")
  name: String,          // hook name (e.g. "cache-read-pre", "audit-post")
  priority: Int,         // hook priority (preserved from HookManager)
  stage: String,         // HookStage label (preserved from HookStage)
  message: String        // underlying exception message
) extends EngineError
```

Wrap each `hp._1.run(c)` in `firePre` + `firePost` with try/catch:

```scala
private def firePre(stage: PipelineStage, ctx: Context): Either[EngineError, Context] = {
  val hookStage: HookStage = preStageFor(stage)
  val pre: Seq[(PreHook, Int)] = hooks.preHooksFor(hookStage)
  pre.foldLeft[Either[EngineError, Context]](Right(ctx)) { case (acc, (h, p)) =>
    acc.flatMap { c =>
      if (c.stop) Right(c)
      else try Right(h.run(c)) catch {
        case e: Throwable => Left(EngineError.HookFailed(
          engine = "<dispatcher>", name = h.name, priority = p,
          stage = hookStage.toString, message = e.toString
        ))
      }
    }
  }
}
```

Then `run` propagates the `Left(HookFailed)` (the dispatcher's existing return type already handles `Left`).

**Pros:**
- Typed `EngineError` variant; consistent with `ProviderInvocationFailed` (per the senior dual review's 2026-08-21 finding).
- Hook identity preserved (`h.name` + `p` + `stage`).
- Wire-format compatible (new sealed-trait variant is a source-compatible addition).
- Self-documenting via the `EngineError.toErrorDetail` mapping.

**Cons:**
- Adds 1 sealed-trait variant (Wire-format compat: this requires an `ErrorCode` mapping update; PR-138's 11-variant count changes to 12 — check the impact on `EngineErrorCode` exhaustiveness).

**Decision: ADOPT.**

### Option B: Per-hook try/catch in `dispatcher.run` only (no new variant)

Catch `RuntimeException` in `run`, convert to `ProviderInvocationFailed` (reuse existing variant).

**Pros:**
- No new variant.

**Cons:**
- `ProviderInvocationFailed` is for engine errors (per the senior dual review's doc). Hooks are NOT engine errors; they're plugin errors. Reusing `ProviderInvocationFailed` loses the hook-name semantic.
- Violates the typed-error wire contract: the message would say "Hook X failed" but the variant name says "engine" → confusing for operators.

**Decision: REJECT** (semantic mismatch).

### Option C: Catch in `EngineService.runQueryWithHooks` (the dispatcher returns a try-throws)

Keep `dispatcher.run` as-is. Catch in `EngineService.runQueryWithHooks` (the caller).

**Pros:**
- Localized change.

**Cons:**
- Violates the principle of "catch at the IO boundary, not at every caller" (per `scala-error-handling-mindset`). Multiple callers would each need their own try/catch.

**Decision: REJECT** (boundary violation).

---

## Decision outcome

**Adopt Option A**.

### Implementation plan

1. **Add `EngineError.HookFailed` variant** in `sm8-core/src/main/scala/io/sm8/core/engine/EngineError.scala`.
2. **Update `EngineError.toErrorDetail`** to map `HookFailed` → `ErrorCode.PLUGIN_HOOK_FAILED` (new) + message string.
3. **Update `EngineErrorCode`** to add `PLUGIN_HOOK_FAILED` (per the wire-format contract).
4. **Wrap `firePre` + `firePost`** in `EngineHookDispatcher.scala` with try/catch (per Option A code).
5. **Update `EngineHookDispatcher.run`** to propagate `Left(HookFailed)` from `firePre` (currently `firePre` returns `Context`; change return type to `Either[EngineError, Context]`).
6. **Update `EngineService.runQueryWithHooks`** to consume the new typed error (it already has the `Left(err) => TerminalException` match, so this is mechanical).
7. **Update `QueryService.engineErrorCode`** to map `HookFailed` → appropriate HTTP status (per RFC §9 fail-fast).
8. **Update `EngineHookDispatcherSpec`** with 2 new tests: (a) hook throws → `Left(HookFailed)` with correct fields; (b) post-hook throws (same).

### Files touched

| File | Change | LOC |
|---|---|---|
| `sm8-core/src/main/scala/io/sm8/core/engine/EngineError.scala` | Add `HookFailed` variant; update `toErrorDetail`; update `ErrorCode` | +20, -0 = +20 net |
| `sm8-core/src/main/scala/io/sm8/core/engine/ErrorCode.scala` (if exists; otherwise inline in `EngineError.scala`) | Add `PLUGIN_HOOK_FAILED` | +2, -0 = +2 net |
| `sm8-platform/src/main/scala/io/sm8/platform/query/hooks/EngineHookDispatcher.scala` | Wrap `firePre` + `firePost` with try/catch; change return type to `Either[EngineError, Context]`; update `run` to propagate `Left` | +30, -15 = +15 net |
| `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala` | Mechanical update — `match { case Right(ctx) => ...; case Left(HookFailed) => ... }` add a branch (already covered by `case Left(err)` wildcard) | +0, -0 = 0 net |
| `sm8-platform/src/main/scala/io/sm8/platform/query/QueryService.scala` | Add `HookFailed` case to `engineErrorCode` (map to 500 Internal Server Error or 502 Bad Gateway per RFC §9 fail-fast) | +2, -0 = +2 net |
| `sm8-platform/src/test/scala/io/sm8/platform/query/hooks/EngineHookDispatcherSpec.scala` | Add 2 tests: pre-hook throws → HookFailed; post-hook throws → HookFailed | +30, -0 = +30 net |
| `docs/adr/0008-af-enginehookdispatcher-hookfailed-typed-error.md` | This ADR | NEW |
| **Total** | | **+84, -15 = +69 net** |

### Tests to add

1. `EngineHookDispatcher: pre-hook that throws → Left(HookFailed(engine="<dispatcher>", name="bad-pre", priority=50, stage="PreExecute", message="..."))` — uses a `FailingPreHook` stub that throws `RuntimeException("simulated failure")`.
2. `EngineHookDispatcher: post-hook that throws → Left(HookFailed(engine="<dispatcher>", name="bad-post", priority=60, stage="PostExecute", message="..."))` — similar.

### Binary compatibility (per scala-impact-analysis-mindset)

- **Source-CHANGED**: `firePre` + `firePost` return type changes from `Context` to `Either[EngineError, Context]`. This is a source-incompatible change for any direct caller (verified: 0 production callers; only `EngineHookDispatcher.run` + the spec).
- **Binary-CHANGED**: new sealed-trait variant `HookFailed`. Existing pattern-match code that has `case _ =>` wildcard still compiles (the compiler can't warn on a non-exhaustive match for a sealed trait with a wildcard). Code that does explicit match on all 11 variants would fail; verified: 0 production code does explicit matches on all 11.
- **Wire-compatible**: the `toErrorDetail` for the new variant follows the existing pattern (the consumer just adds 1 new error code to its dispatch table).

---

## Skill alignment

### `scala-error-handling-mindset`

- **Apply "errors are data":** the hook throw is a typed `EngineError.HookFailed(engine, name, priority, stage, message)`; the consumer pattern-matches on the variant to render the error.
- **Apply "catch at the IO boundary":** the dispatcher is the IO boundary between the plugin code and the engine-portable pipeline. The catch belongs there, not in every caller.
- **Apply "Either over throw":** the dispatcher's `run` already returns `Either[EngineError, Context]`; we surface the hook failure as a `Left` rather than a throw.

### `karpathy-guidelines-mindset`

- **Apply "smallest correct change":** the change is local to the dispatcher + the EngineError ADT + 1 new test. ~70 net LOC.
- **Apply "verifiable success":** 2 new tests prove the typed error surfaces correctly.

### `karpathy-app-design-mindset`

- **Apply "frozen core + extension portal":** the EngineError ADT is part of the frozen core; adding a new variant requires ADR + senior dual review (this PR). The variant is extensible.

### `karpathy-impact-analysis-mindset`

- **Apply:** 0 production callers in `sm8-core/src/main` or `connectors/*/src/main` reference `firePre` / `firePost` directly; only `EngineHookDispatcher.run` + the 2 test specs do.
- **Apply:** the `EngineError` ADT has 1 wildcard match in production code (the `engineErrorCode` in QueryService); all other matches are exhaustive. The new variant adds no compat risk.

### `karpathy-impact-analysis-mindset` (continued)

- **Apply:** the `toErrorDetail` mapping update is mechanical (add 1 case).

### `debug-mantra-mindset`

- **Apply SS1 (reproduce):** the 2 new tests use a `FailingPreHook` / `FailingPostHook` stub that throws `RuntimeException("simulated failure")`.
- **Apply SS2 (trace):** each test asserts the typed `EngineError.HookFailed(engine, name, priority, stage, message)` AND the exact field values.
- **Apply SS3 (falsify):** the test verifies that BEFORE the fix, the hook throw propagates as `RuntimeException` (current behavior). AFTER the fix, the dispatcher converts to `Left(HookFailed(...))`.
- **Apply SS5 (verify):** the existing 911 tests continue to pass (zero regression).

### `scala-data-driven-refactor-mindset`

- **Apply:** the new `HookFailed` variant is a sealed case class with 5 fields (engine, name, priority, stage, message) — pure data, no behavior.
- **Apply:** the `EngineError` ADT remains the discriminating tagged union (sealed trait + case classes); the new variant follows the same pattern.

### `scala2-scaladoc-mindset`

- **Apply §1:** the new variant's Scaladoc describes WHY (the hook-throw boundary case) + WHAT (5 fields) + HOW (sealed case class with auto-derived equals/hashCode/toString).
- **Apply §2:** No `[[wikilinks]]`, no PR/Phase/ADR/process references in the new code.

---

## Acceptance criteria

1. `EngineError.HookFailed(engine, name, priority, stage, message)` is added to the ADT.
2. `EngineError.toErrorDetail` returns `ErrorCode.PLUGIN_HOOK_FAILED` for `HookFailed`.
3. `EngineHookDispatcher.firePre` + `firePost` return `Either[EngineError, Context]` (not bare `Context`).
4. `EngineHookDispatcher.run` propagates `Left(HookFailed)` (the hook identity + priority + stage + message are preserved).
5. The 2 new tests pass: pre-hook throws → `HookFailed`; post-hook throws → `HookFailed`.
6. The 911 existing tests pass (zero regression).
7. The `EngineErrorCode` mapping in `QueryService` includes the new variant (mapped to 5xx).

## Verification plan

```bash
# 1. After PR-146 lands + merges:
mvn -B -ntp -pl sm8-core,connectors/spark-connector,connectors/in-memory-connector,connectors/trino-connector,plugins/audit-plugin,plugins/cache-plugin,sm8-cli,sm8-server,sm8-platform test 2>&1 | grep -E 'Tests: succeeded|BUILD' | tail -10
# 2. Verify the 2 new HookFailed tests pass:
mvn -B -ntp -pl sm8-platform scalatest:test -Dtest='EngineHookDispatcherSpec' 2>&1 | grep -E 'Tests:|succeeded' | tail -3
# 3. javap: stable checkcast count
# 4. scaladoc noise scan: 0 new noise
# 5. Memory + disk under 90% throughout
```

## Risks

| Risk | Mitigation |
|---|---|
| Adding a new sealed-trait variant requires updating every `match` exhaustively | Verified: 0 production code does exhaustive match on all 11 variants; the only matches are wildcards. |
| The `HookFailed` message leaks the hook's `name` field, which could be PII | The hook's `name` is plugin-defined (e.g. "cache-read-pre", "audit-post"). Plugin authors control the name. The wire format is HTTP-restricted; no PII risk. |
| Changing `firePre` / `firePost` return type from `Context` to `Either[EngineError, Context]` is source-incompatible | Verified: 0 production callers; only `EngineHookDispatcher.run` + the spec. The change is local. |

## Open questions

1. Should `HookFailed` use `error = "<dispatcher>"` (literal) or `error = "sm8"` (generic) for the `engine` field? My recommendation: `"<dispatcher>"` (matches the existing pattern in `EngineService.executeEngine` for dispatcher-level errors).
2. Should the `message` field include the hook's stack trace? My recommendation: **NO** — `e.toString` includes the class + message but not the stack (stack traces are logged separately; the wire format is JSON).
3. Should the hook's `priority` field be the hook's effective priority or the hook's declared priority? My recommendation: **declared priority** (the value from `HookEntry.priority`, which matches the loop iteration order).

---

## ADR

`docs/adr/0008-af-enginehookdispatcher-hookfailed-typed-error.md` v1.0 (full ADR; ~220 lines).
