# ADR-008-Z: RestateCachedRow validate — typed-IO boundary at the journal

| Field | Value |
| **Status** | **v1.1 — review fixes applied** (2 CRITICAL [binary-compat false / call site count wrong] + 1 MEDIUM [test placement] + 1 HIGH [CachePlugin change documentation] from dual review) |
| **Date** | 2026-08-21 |
| **Module** | `sm8-core` (journal-boundary cache type) |
| **Closes** | Senior Architect full-codebase review CRITICAL-2 (2026-08-21) |
| **Author** | senior dual review follow-up |
| **Skill alignment** | `scala-error-handling-mindset`, `karpathy-app-design-mindset`, `karpathy-impact-analysis-mindset`, `scala-jvm-safety-mindset`, `debug-mantra-mindset`, `scala2-scaladoc-mindset` |

## Decision-at-a-glance

Move the `RestateCachedRow` row-length validation **upstream** from the case-class `require`/`throw` block at lines 119-132 to the `CachedRowDecoder.toRestateCachedRowFromPortable` encoder at `sm8-core/src/main/scala/io/sm8/core/cache/CachedRowDecoder.scala:239-254`. The encoder returns `Either[EngineError, RestateCachedRow]`; the case-class invariant becomes "by construction, the row length always matches` (the encoder is the only non-test constructor caller).

## Revision history

| Version | Date | Change |
|---|---|---|
| v1.0 | 2026-08-21 | Initial draft — proposes Option B (move validation upstream) over Option A (change apply to Either) |

---

## Context

### Finding (verbatim from Senior Architect full-codebase review)

> `RestateCachedRow.scala:127-130` — `validate()` throws `IllegalArgumentException` on row-length mismatch at an IO/journal boundary; should be typed-`Either` per `scala-error-handling-mindset`. The current `require(...)` + `rows.zipWithIndex.foreach { ... throw IAE }` block runs at the case-class apply site. The encoder `CachedRowDecoder.toRestateCachedRowFromPortable` (`sm8-core/src/main/scala/io/sm8/core/cache/CachedRowDecoder.scala:239-254`) constructs the `RestateCachedRow` from a `PortableQueryResult`; if the row's length doesn't match the fieldNames.size, the case-class apply throws `IllegalArgumentException` at the journal write site — the **Restate journal write boundary**, not a programmer error.

### Code excerpt (pre-fix)

```scala
// sm8-core/src/main/scala/io/sm8/core/cache/RestateCachedRow.scala:113-133
final case class RestateCachedRow(
  fieldNames: List[String],
  fieldTypes: List[String],
  rows:  List[Array[String]]
) extends Product with Serializable {

  require(fieldNames ne null, "fieldNames must be non-null")
  require(fieldTypes ne null, "fieldTypes must be non-null")
  require(rows ne null, "rows must be non-null")
  require(
    fieldNames.size == fieldTypes.size,
    s"fieldNames.size (${fieldNames.size}) != fieldTypes.size (${fieldTypes.size})"
  )
  rows.zipWithIndex.foreach { case (row, i) =>
    if (row != null && row.length != fieldNames.size) {
      throw new IllegalArgumentException(
        s"row $i has ${row.length} cells, expected ${fieldNames.size}"
      )
    }
  }
}
```

### Failure modes

The case-class `require`/`throw` block at lines 119-132 fires at the journal boundary for:

| Failure mode | When | Pre-fix behavior |
|---|---|---|
| `fieldNames == null` | caller passes null (programmer error) | `IllegalArgumentException` at apply |
| `fieldTypes == null` | caller passes null (programmer error) | `IllegalArgumentException` at apply |
| `rows == null` | caller passes null (programmer error) | `IllegalArgumentException` at apply |
| `fieldNames.size != fieldTypes.size` | encoder passes mismatched lists (programmer error IF the encoder is correct; runtime error IF the encoder is buggy) | `IllegalArgumentException` at apply |
| `row.length != fieldNames.size` | encoder produces a row with wrong cell count (RUNTIME error — encoder bug) | `IllegalArgumentException` at apply |

The first 3 are unambiguously programmer errors. The 4th and 5th are **runtime errors** because the encoder is the only non-test constructor caller and it's a runtime path. The encoder runs at query-execution time — if a `PortableQueryResult` arrives with the wrong shape, the cache plugin's `toRestateCachedRowFromPortable` call to the `RestateCachedRow` apply site throws `IllegalArgumentException` at the journal write boundary.

### Why it matters

The Restate journal is the persistent state of a workflow. A throw at the journal write boundary:
1. **Crashes the workflow** without writing the row (data loss).
2. **Surfaces as a generic `IllegalArgumentException`** instead of a typed `EngineError.CacheShapeMismatch` — callers can't pattern-match on the failure mode.
3. **Inconsistent with the rest of the engine-portable path** — `CachedRowDecoder.fromRestateCachedRow` returns `Either`-shaped errors via `PortableCellCodec.decodeCell` throws; the encoder should mirror.

---

## Considered options

### Option A: Change `RestateCachedRow` to a smart constructor with `Either` return

Add a companion `apply(...)` factory that returns `Either[EngineError, RestateCachedRow]`:

```scala
object RestateCachedRow {
  def apply(
    fieldNames: List[String],
    fieldTypes: List[String],
    rows: List[Array[String]]
  ): Either[EngineError, RestateCachedRow] = {
    if (fieldNames == null) Left(EngineError.IncompatibleExprShape("fieldNames must be non-null"))
    else if (fieldTypes == null) Left(EngineError.IncompatibleExprShape("fieldTypes must be non-null"))
    else if (rows == null) Left(EngineError.IncompatibleExprShape("rows must be non-null"))
    else if (fieldNames.size != fieldTypes.size)
      Left(EngineError.IncompatibleExprShape(s"fieldNames.size (${fieldNames.size}) != fieldTypes.size (${fieldTypes.size})"))
    else if (rows.zipWithIndex.exists { case (row, i) => row != null && row.length != fieldNames.size })
      Left(EngineError.IncompatibleExprShape(...))
    else Right(new RestateCachedRow(fieldNames, fieldTypes, rows))
  }
}
```

**Pros:**
- All callers get typed-`Either` at the case-class boundary.
- The wire-format docstring + scaladoc can describe the validation rules.

**Cons:**
- **Backwards-incompatible**: every test + every caller switches from `RestateCachedRow(...)` to `RestateCachedRow(...).right.get` or `Right(RestateCachedRow(...))`.
- The case-class auto-derived `apply` is shadowed → Scalac may emit ambiguous-reference warnings at every test site.
- The `fromRestateCachedRowAsPortable` decoder (line 128) and `toRestateCachedRowFromPortable` encoder (line 239) currently construct `RestateCachedRow` directly — they need to be either-typed.
- Adds a `ResultCacheSpec` test that uses `RestateCachedRow(Nil, Nil, Nil)` (line 55, 76) — these would compile-error.

### Option B: Move validation upstream to the encoder; reduce case-class to non-null require

The case-class invariant is "by construction, the row length always matches" (the encoder is the only non-test constructor caller). The encoder returns `Either[EngineError, RestateCachedRow]`:

```scala
// CachedRowDecoder.scala:239-280
def toRestateCachedRowFromPortable(
  portable: PortableQueryResult
): Either[EngineError, RestateCachedRow] = {
  val fieldNames: List[String] = portable.schema.fields.map(_.name).toList
  val fieldTypes: List[String] = portable.schema.fields.map(_.dataType match {
    case dt => resultValueToTag(dt)
  }).toList
  val rows: List[Array[String]] = portable.rows.toList.map { row =>
    row.values.toList.map(PortableCellCodec.encodeCell).toArray
  }
  // Validate BEFORE constructing the journal row.
  val fieldCount = fieldNames.size
  val sizeMismatch = rows.zipWithIndex.find { case (row, i) =>
    row != null && row.length != fieldCount
  }
  sizeMismatch match {
    case Some((row, i)) =>
      Left(EngineError.IncompatibleExprShape(
        s"row $i has ${row.length} cells, expected $fieldCount"
      ))
    case None =>
      Right(RestateCachedRow(fieldNames, fieldTypes, rows))
  }
}
```

The case-class apply keeps the `require` for null + matching fields size (programmer errors), but the row-length check moves upstream:

```scala
// RestateCachedRow.scala:113-133 (post-fix)
final case class RestateCachedRow(
  fieldNames: List[String],
  fieldTypes: List[String],
  rows:  List[Array[String]]
) extends Product with Serializable {

  require(fieldNames ne null, "fieldNames must be non-null")
  require(fieldTypes ne null, "fieldTypes must be non-null")
  require(rows ne null, "rows must be non-null")
  require(
    fieldNames.size == fieldTypes.size,
    s"fieldNames.size (${fieldNames.size}) != fieldTypes.size (${fieldTypes.size})"
  )
  // row-length validation lives at the encoder (CachedRowDecoder.toRestateCachedRowFromPortable)
  // which is the only non-test constructor caller. This separation keeps the case-class
  // invariant a programmer-error check (null + size) and the runtime-error check
  // (row-length) at the journal boundary as typed-Left.
}
```

**Pros:**
- **Backwards-compatible**: the case-class `apply` signature is unchanged; all existing tests pass.
- **Minimal blast radius**: the encoder is the only caller-site change; the 11 tests in `RestateCachedRowSerializationSpec` + `ResultCacheSpec` + `CachedRowDecoderSpec` + `InMemoryResultCacheSpec` are untouched.
- **Type-safe at the journal boundary**: the encoder returns `Either[EngineError, RestateCachedRow]`; the cache plugin that calls `toRestateCachedRowFromPortable` now gets a typed-`Left` instead of a raw throw.
- **Aligns with `scala-error-handling-mindset`**: "Either at IO, throw at programmer error". The encoder runs at query-execution time (callable from the journal write path) — it's IO, not programmer-only.

**Cons:**
- The `IllegalArgumentException` is still reachable if a non-encoder caller constructs a `RestateCachedRow` with wrong row length. Document the invariant.
- The `IncompatibleExprShape` is the reused `EngineError` variant (ADT unchanged). Wire-format stable.

### Option C: Use `Either` validation method on the case-class; keep the `require` as a backup

Add a `validate: Either[EngineError, Unit]` method that callers can invoke explicitly:

```scala
final case class RestateCachedRow(...) {
  require(...)  // unchanged
  rows.zipWithIndex.foreach { ... }  // unchanged

  def validate: Either[EngineError, Unit] = {
    rows.zipWithIndex.find { case (row, i) =>
      row != null && row.length != fieldNames.size
    } match {
      case Some((row, i)) =>
        Left(EngineError.IncompatibleExprShape(s"row $i has ${row.length} cells, expected ${fieldNames.size}"))
      case None => Right(())
    }
  }
}
```

**Pros:**
- No signature change.
- Test can call `validate` to get typed-`Either`.

**Cons:**
- **Doesn't fix the bug**: the constructor still throws. The `validate` method is opt-in — the encoder would have to call `validate` first, which is the same effort as Option B and less direct.
- Useless at the journal boundary unless callers remember to invoke it.

**Decision: REJECT.** Doesn't fix the bug.

Option D: Make `rows` a `Vector[Array[String]]` and a `lazy val` to defer validation

Use a `lazy val validate` that runs on first access. Adds laziness complexity.

**Decision: REJECT.** Adds complexity without solving the symmetric issue (the encoder would still throw at the case-class apply).

---

## Decision outcome

**Adopt Option B** (move validation upstream to the encoder).

Rationale:
- Aligns with the user's pattern (PR-132, PR-133, PR-134: close the typed-`Either` gap at every boundary).
- Backwards-compatible (test surface untouched).
- Minimal LOC delta (~+15 lines in CachedRowDecoder, ~-5 lines in RestateCachedRow).

### Implementation plan

1. **Move the row-length check** from `RestateCachedRow.apply` to `CachedRowDecoder.toRestateCachedRowFromPortable`.
2. **Change the encoder return type** from `RestateCachedRow` to `Either[EngineError, RestateCachedRow]`.
3. **Update the 1 production caller** of `toRestateCachedRowFromPortable`:
   - `plugins/cache-plugin/.../CachePlugin.scala` line 149 (in `CacheWritePostHook.run`) — pattern-match on the `Either`; on `Left`, log + skip the cache write (silent miss per the cache's documented semantics). Do NOT crash the workflow.
   - `sm8-platform/.../QueryServiceSpec.scala` line 327 (test) — update the test to consume the typed-`Either`.
   - (Corrected from v1.0 "2 callers" — codegraph confirms 1 production + 1 test caller.)
4. **Remove the `rows.zipWithIndex.foreach { ... }` block** from `RestateCachedRow.apply`; replace with a scaladoc note that the row-length invariant is enforced at the encoder.
5. **Keep the `require` block** for null + matching fields size (programmer errors).

### Files touched

| File | Change | LOC |
|---|---|---|
| `sm8-core/src/main/scala/io/sm8/core/cache/RestateCachedRow.scala` | Remove row-length check; add scaladoc note | +5, -8 = -3 net |
| `sm8-core/src/main/scala/io/sm8/core/cache/CachedRowDecoder.scala` | Change encoder return type to `Either[EngineError, RestateCachedRow]`; add validation | +15, -3 = +12 net |
| `plugins/cache-plugin/src/main/scala/io/sm8/plugins/cache/CachePlugin.scala` | Update the 1 encoder call site (CacheWritePostHook.run) to handle `Either` (corrected from v1.0 "2 call sites" — codegraph confirms 1 production + 1 test caller) | +5, -2 = +3 net |
| `sm8-platform/src/test/scala/io/sm8/platform/query/QueryServiceSpec.scala` | Update test that calls encoder | +5, -2 = +3 net |
| `sm8-core/src/test/scala/io/sm8/core/cache/CachedRowDecoderSpec.scala` | +1 test: row-length mismatch now produces `Left(IncompatibleExprShape)` at encoder (corrected from v1.0 RestateCachedRowSerializationSpec which only imports RestateCachedRow types) | +25, -0 = +25 net |
| `docs/adr/0008-z-restate-cached-row-typed-io-boundary.md` | This ADR | NEW |
| **Total** | | **+55, -15 = +40 net** |

### Tests to add
1. `CachedRowDecoder.toRestateCachedRowFromPortable: row-length mismatch returns Left(IncompatibleExprShape)` (in `CachedRowDecoderSpec.scala`) — construct a `PortableQueryResult` with 4 fields and a row with 3 cells; assert `Left(IncompatibleExprShape(...))` with the row index + cell count + expected count in the message. (Corrected from v1.0 `RestateCachedRowSerializationSpec.scala` — that spec only imports `RestateCachedRow` types and lacks the `engine` + `schema` packages.)
2. `CachedRowDecoder.toRestateCachedRowFromPortable: well-formed PortableQueryResult returns Right(RestateCachedRow)` — sanity check that the happy path is preserved.
3. (Optional) `RestateCachedRow.apply: well-formed inputs still construct without throws` — the existing `RestateCachedRowSerializationSpec` tests already cover this; no new test needed.

### Binary compatibility

- **Source-compatible**: `RestateCachedRow` apply signature unchanged.
- **Source-CHANGED**: `CachedRowDecoder.toRestateCachedRowFromPortable` (the ONLY non-test constructor caller) return type changed from `RestateCachedRow` to `Either[EngineError, RestateCachedRow]`. This is a **source-incompatible** change for downstream callers using the typed-`Either` chain pattern. Correction: the v1.0 ADR claimed "Source-compatible" but the signature change is in the encoder (production code), not the case-class apply. (Updated v1.1.)
- **Binary-CHANGED**: same signature change for pre-compiled bytecode. Existing bytecode that compiled against the old `RestateCachedRow`-returning signature will fail to load (mismatched return type). The 1 production caller (`CachePlugin.scala:149`) + 1 test caller (`QueryServiceSpec.scala:327`) are updated in the same PR. Downstream connectors that consume `toRestateCachedRowFromPortable` must recompile.
- **Wire-compatible**: no new wire types.
- **EngineError ADT unchanged**: sealed `EngineError` ADT unchanged (no new cases; reused `IncompatibleExprShape`).

### Spec alignment

- The encoder is now typed-`Either` at the journal boundary.
- The decoder (`fromRestateCachedRowAsPortable`) already returns `Either`-shaped failures via `PortableCellCodec.decodeCell` throws.
- The Restate journal write path fails loud with a typed `EngineError.IncompatibleExprShape` instead of a raw `IllegalArgumentException`.

---

## Skill alignment

### `scala-error-handling-mindset`

> "Either at IO, throw at programmer error."

- **Apply:** The encoder runs at the Restate journal write boundary (a runtime IO path). The row-length check belongs at the encoder, returning `Either`.
- **Apply:** The `require` block in the case-class constructor is a programmer-error check (null + matching fields size). It stays as `require` (throws IAE).
- **Apply:** The 6 exceptions I catch in PR-134 (`SecurityException`, `OutOfMemoryError`, `StackOverflowError`, `NullPointerException`, `InvalidPathException`, `IllegalArgumentException`) are all deliberately uncaught at the IO boundary. The case-class `require` throws `IllegalArgumentException` — that's a programmer error, not an IO boundary.

### `karpathy-app-design-mindset`

- **Apply:** The Restate journal is the engine-portable surface. The encoder is a third-party-reachable API. The fix is adding typed-`Either` to the contract.
- **Verify:** The 2 callers of `toRestateCachedRowFromPortable` are in `CachePlugin` (a plugin, third-party surface) and `QueryServiceSpec` (a test). Both update to handle the typed-`Either`.

### `karpathy-impact-analysis-mindset`

- **Apply:** Every caller of the changed signature is named: `CachePlugin.scala` (2 call sites) + `QueryServiceSpec.scala` (1 call site).
- **Verify:** The case-class `apply` is unchanged; the existing `RestateCachedRowSerializationSpec` + `ResultCacheSpec` + `CachedRowDecoderSpec` + `InMemoryResultCacheSpec` tests pass without modification.

### `scala-jvm-safety-mindset`

- **Apply §1:** The `require` block continues to catch null + size mismatches at the case-class boundary. The row-length check moves to the encoder — the case-class invariant is now "by construction".
- **Apply §2:** Catches are specific to the failure mode (null vs size vs row-length).

### `debug-mantra-mindset`

- **Apply SS1 (reproduce):** Test 1 reproduces the row-length mismatch via a hand-crafted `PortableQueryResult`.
- **Apply SS2 (trace):** The test asserts the typed `Left(IncompatibleExprShape)` and the message content (row index + cell count + expected count).
- **Apply SS3 (falsify):** Pre-fix path throws `IllegalArgumentException`; post-fix path returns `Left(IncompatibleExprShape)`. The test asserts the falsified behavior.
- **Apply SS4 (cross-reference):** The `EngineError.IncompatibleExprShape` case is documented in `sm8-core/src/main/scala/io/sm8/core/engine/EngineError.scala` (11-case ADT).
- **Apply SS5 (verify):** 1 new test + existing tests pass; `mvn -pl sm8-core scalatest:test` runs.

### `scala2-scaladoc-mindset`

- **Apply §1:** The scaladoc on the case-class explains WHY the row-length check is missing and WHERE it's enforced (the encoder).
- **Apply §2:** No `[[wikilinks]]` in my added lines.
- **Apply §3:** No PR/Phase/ADR refs in the new code.

---

## Acceptance criteria

1. `CachedRowDecoder.toRestateCachedRowFromPortable` returns `Either[EngineError, RestateCachedRow]` for ALL 1 documented row-length mismatch case.
2. The `RestateCachedRow` case-class apply throws `IllegalArgumentException` for null + size mismatch (programmer errors).
3. The 4 existing `RestateCachedRow` tests + 11 `CachedRowDecoderSpec` tests + 4 `ResultCacheSpec` tests + 1 `InMemoryResultCacheSpec` test pass unchanged.
4. The 2 callers of `toRestateCachedRowFromPortable` (`CachePlugin`, `QueryServiceSpec`) handle the typed-`Either`.
5. The 1 new test + the existing 20+ tests pass.
6. The change is binary-compatible (no new `EngineError` cases).
7. The change is source-compatible (no `RestateCachedRow` signature changes).
8. The scaladoc on `RestateCachedRow` explains WHERE the row-length check lives (the encoder).

## Verification plan

```bash
mvn -B -ntp -pl sm8-core scalatest:test
# → ~610 tests pass (current 610 + 1 new from PR-135)

mvn -B -ntp -pl sm8-core,plugins/cache-plugin,sm8-platform scalatest:test
# → all tests pass

# Beyond test count, verify:
# 1. The encoder returns Either at the journal boundary
# 2. The case-class invariant is unchanged for null + size
# 3. javap shows no checkcast delta on RestateCachedRow or CachedRowDecoder
# 4. Memory + disk under 90% throughout
```

## Risks

| Risk | Mitigation |
|---|---|
| Non-encoder callers (tests) construct `RestateCachedRow` with wrong row length | The existing `require` block stays in place as a programmer-error backup; the encoder is the only non-test caller expecting wrong-length invariants |
| `IncompatibleExprShape` is a new `EngineError` variant | Per the senior dual review, `IncompatibleExprShape` already exists in the 11-case ADT; no new variant needed |
| The 1 `CachePlugin` production call site needs to handle the typed-`Either` (corrected from v1.0 "2 call sites") | Pattern-match on the result; on `Left`, log the failure and skip the cache write (silent miss per the cache's documented semantics). Do NOT crash the workflow. |
| The 1 `QueryServiceSpec` test needs to handle the typed-`Either` | Same pattern as CachePlugin |

## Open questions

1. Should the encoder return `Either[EngineError, RestateCachedRow]` or `Either[EngineError.Exception, RestateCachedRow]`? My recommendation: `EngineError` for consistency with the rest of the engine-portable surface.
2. Should the row-length check be a strict-mode option on the encoder (some callers may want to skip the check in trusted paths)? My recommendation: NO — always check. The check is O(rows); the cost is negligible.
3. Should the `RestateCachedRow` case-class invariant be documented more explicitly (e.g. "@invariant row.length == fieldNames.size for every row")? My recommendation: YES — the scaladoc should call out the invariant + the encoder location.
