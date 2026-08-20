# ADR-008-Y: PlatformModelLoader.fromPath — typed-IO boundary

| Field | Value |
| **Status** | **v1.1 — review fixes applied** (1 CRITICAL + 2 HIGH + 1 NIT from dual review) |
| **Date** | 2026-08-21 |
| **Module** | `sm8-platform` (platform layer) |
| **Closes** | Senior dual review finding ARCH-C1 (CRITICAL) |
| **Author** | senior dual review follow-up |
| **Skill alignment** | `scala-error-handling-mindset`, `karpathy-app-design-mindset`, `scala-jvm-safety-mindset`, `debug-mantra-mindset`, `scala2-scaladoc-mindset` |

## Decision-at-a-glance

Wrap every `IOException` path inside `PlatformModelLoader.fromPath` in `try/catch` and surface as `Left(PlatformModelError.ParseFailure(...))` so the IO boundary satisfies the typed-Either contract already established by `fromString`, `validateAndLoad`, and the rest of the platform adapters.

## Revision history

| Version | Date | Change |
|---|---|---|
| v1.0 | 2026-08-21 | Initial draft — proposed by Senior Architect full-codebase review (CRITICAL-1) |

---

## Context

### Finding (verbatim from Senior Architect full-codebase review)

> `PlatformModelLoader.fromPath` (`sm8-platform/src/main/scala/io/sm8/platform/query/PlatformModelLoader.scala:155-158`) has a `try/finally` that closes the stream only if `Source.fromInputStream` returns normally — if the YAML body is not UTF-8 decodable the `try` body throws BEFORE `mkString` returns and the `finally` runs, so close() is fine — but the `validateAndLoad` path that follows never sees the malformed stream; the raw `IOException` (or `MalformedInputException` / `CharacterCodingException`) propagates uncaught, violating the typed-`Either` contract established by `fromString`, `validateAndLoad`, and the rest of the platform adapters.

### Code excerpt (pre-fix)

```scala
// lines 146-162
def fromPath(path: Path): Either[PlatformModelError, Model] = {
  if (!java.nio.file.Files.exists(path))
    Left(PlatformModelError.InvalidYaml(CoreManifestError.InvalidYaml(s"file not found: $path")))
  else {
    val rawYaml: String = {
      val stream = java.nio.file.Files.newInputStream(path)
      try scala.io.Source.fromInputStream(stream, "UTF-8").mkString
      finally stream.close()
    }
    validateAndLoad(rawYaml)
  }
}
```

### Failure modes

The IO boundary at line 157 (`Source.fromInputStream(stream, "UTF-8").mkString`) can throw:

 | `java.nio.charset.MalformedInputException` | bytes are not valid UTF-8 (e.g. binary file renamed to `.yml`) | raw throw, propagates uncaught |
 | `java.nio.charset.UnmappableCharacterException` | bytes are UTF-8 but contain unmappable characters under the chosen decoder | raw throw, propagates uncaught |
 | `java.nio.file.NoSuchFileException` | race condition: file deleted between `Files.exists` and `Files.newInputStream` | raw throw, propagates uncaught |
 | `java.nio.file.AccessDeniedException` | permission revoked between `Files.exists` and `Files.newInputStream` | raw throw, propagates uncaught |

### Deliberately uncaught exceptions

These exceptions are NOT caught at the IO boundary — they propagate as programmer errors or JVM faults per `scala-jvm-safety-mindset` §1.

| Exception | When | Why not caught |
|---|---|---|
| `SecurityException` | a `SecurityManager` rejects file access | programmer / deployment configuration error; not an IO boundary failure |
| `OutOfMemoryError` | JVM heap exhausted during file read | JVM fault; catching OOM hides the failure mode |
| `StackOverflowError` | JVM stack exhausted during file read | JVM fault; catching SOF hides the failure mode |
| `NullPointerException` | `path == null` (caller passed null) | programmer error; the precondition is `path != null` |
| `InvalidPathException` | `path` is not a valid filesystem path (e.g. contains NUL bytes) | programmer error; the caller is responsible for path validity |
| `IllegalArgumentException` | the stream constructor rejects an invalid `OpenOption` | programmer error; we always pass default options |

All five IO-mode exceptions are caught. The six uncaught exceptions propagate to the caller. The boundary is asymmetric on purpose: catch IO failures, surface programmer errors as throws.

All five paths violate the typed-`Either` contract. The 8 existing tests in `PlatformModelLoaderSpec` cover schema + parse + semantic errors but NOT the IO boundary.

### Why it matters

The platform layer's adapter contract is "Either at IO, throw at programmer error". A `PlatformModelLoader.fromPath` call from `sm8-server.Main` (the only caller per the codegraph blast radius) that surfaces an `IOException` instead of a typed `Left` would:

1. **Crash the server** at startup if the model file is unreadable (currently `sm8-server.Main.parseArgs` does not catch it).
2. **Mask the real cause** — `MalformedInputException` is more specific than `IOException`; users debugging need to know whether the file is empty, binary, or just unparseable YAML.
3. **Inconsistent with `fromString`** — the in-memory YAML path already returns `Left(PlatformModelError.ParseFailure(...))` for malformed YAML; the file path should mirror that contract.
4. **Breaks uniform error handling** — callers that pattern-match on `PlatformModelError` to render UI messages now have to also wrap in `try/catch` for the file path case.

---

## Considered options

### Option A: Wrap entire `fromPath` body in `try/catch (IOException) { ... }`

```scala
def fromPath(path: Path): Either[PlatformModelError, Model] = {
  try {
    if (!java.nio.file.Files.exists(path))
      Left(PlatformModelError.InvalidYaml(CoreManifestError.InvalidYaml(s"file not found: $path")))
    else {
      val rawYaml: String = {
        val stream = java.nio.file.Files.newInputStream(path)
        try scala.io.Source.fromInputStream(stream, "UTF-8").mkString
        finally stream.close()
      }
      validateAndLoad(rawYaml)
    }
  } catch {
    case e: java.nio.charset.MalformedInputException =>
      Left(PlatformModelError.ParseFailure(CoreManifestError.ParseFailure(s"file is not valid UTF-8: ${e.getMessage}")))
    case e: java.nio.charset.UnmappableCharacterException =>
      Left(PlatformModelError.ParseFailure(CoreManifestError.ParseFailure(s"file contains unmappable characters: ${e.getMessage}")))
    case e: java.nio.file.NoSuchFileException =>
      Left(PlatformModelError.InvalidYaml(CoreManifestError.InvalidYaml(s"file not found: ${e.getMessage}")))
    case e: java.nio.file.AccessDeniedException =>
      Left(PlatformModelError.InvalidYaml(CoreManifestError.InvalidYaml(s"access denied: ${e.getMessage}")))
    case e: java.io.IOException =>
      Left(PlatformModelError.ParseFailure(CoreManifestError.ParseFailure(s"IO error: ${e.getMessage}")))
  }
}
```

**Pros:**
- Covers all 5 failure modes with the most specific exception to the most generic.
- Preserves resource cleanup (the inner `try/finally` already closes the stream).
- Matches the existing `fromString`/`validateAndLoad` Either contract.

**Cons:**
- 5-case pattern match adds ~15 lines to the function body.
- The `case e: IOException` catch is broad but justified by the IO boundary (analogous to `MinimalRelOpLowerer.lowerScan`'s narrow Spark exception pattern).

### Option B: Helper `readFile(path: Path): Either[PlatformModelError, String]`

Extract the file read into a private helper that returns `Either` directly:

```scala
private def readFile(path: Path): Either[PlatformModelError, String] = {
  if (!java.nio.file.Files.exists(path))
    Left(PlatformModelError.InvalidYaml(CoreManifestError.InvalidYaml(s"file not found: $path")))
  else {
    try {
      val stream = java.nio.file.Files.newInputStream(path)
      try scala.io.Source.fromInputStream(stream, "UTF-8").mkString
      finally stream.close()
    } catch {
      case e: java.nio.charset.MalformedInputException =>
        Left(PlatformModelError.ParseFailure(CoreManifestError.ParseFailure(s"file is not valid UTF-8: ${e.getMessage}")))
      case e: java.nio.charset.UnmappableCharacterException =>
        Left(PlatformModelError.ParseFailure(CoreManifestError.ParseFailure(s"file contains unmappable characters: ${e.getMessage}")))
      case e: java.nio.file.NoSuchFileException =>
        Left(PlatformModelError.InvalidYaml(CoreManifestError.InvalidYaml(s"file not found: ${e.getMessage}")))
      case e: java.nio.file.AccessDeniedException =>
        Left(PlatformModelError.InvalidYaml(CoreManifestError.InvalidYaml(s"access denied: ${e.getMessage}")))
      case e: java.io.IOException =>
        Left(PlatformModelError.ParseFailure(CoreManifestError.ParseFailure(s"IO error: ${e.getMessage}")))
    }
  }
}

def fromPath(path: Path): Either[PlatformModelError, Model] =
  readFile(path).flatMap(validateAndLoad)
```

**Pros:**
- Cleaner separation: `fromPath` becomes a 1-line `flatMap`.
- `readFile` is independently testable.
- Resource cleanup is uncontested.

**Cons:**
- Adds a private helper (cosmetic).
- Slightly larger diff (~20 lines vs ~15).

### Option C: Use `Either.catchOnly` or `scala.util.Try` + `.toEither`

```scala
import scala.util.Try
import scala.util.control.NonFatal

def fromPath(path: Path): Either[PlatformModelError, Model] = {
  if (!java.nio.file.Files.exists(path))
    Left(PlatformModelError.InvalidYaml(CoreManifestError.InvalidYaml(s"file not found: $path")))
  else {
    val rawYaml: Either[PlatformModelError, String] = Try {
      val stream = java.nio.file.Files.newInputStream(path)
      try scala.io.Source.fromInputStream(stream, "UTF-8").mkString
      finally stream.close()
    }.toEither.left.map {
      case e: java.nio.charset.MalformedInputException =>
        PlatformModelError.ParseFailure(CoreManifestError.ParseFailure(s"file is not valid UTF-8: ${e.getMessage}"))
      case e: java.nio.charset.UnmappableCharacterException =>
        PlatformModelError.ParseFailure(CoreManifestError.ParseFailure(s"file contains unmappable characters: ${e.getMessage}"))
      case e: java.nio.file.NoSuchFileException =>
        PlatformModelError.InvalidYaml(CoreManifestError.InvalidYaml(s"file not found: ${e.getMessage}"))
      case e: java.nio.file.AccessDeniedException =>
        PlatformModelError.InvalidYaml(CoreManifestError.InvalidYaml(s"access denied: ${e.getMessage}"))
      case e: java.io.IOException =>
        PlatformModelError.ParseFailure(CoreManifestError.ParseFailure(s"IO error: ${e.getMessage}"))
    }
    rawYaml.flatMap(validateAndLoad)
  }
}
```

**Pros:**
- Pattern matching + `left.map` is elegant.

**Cons:**
- `Try` is overkill for a single IO boundary.
- Adds `scala.util.Try` + `NonFatal` imports (more bytes than the 5-case catch).
- `NonFatal` would swallow `OutOfMemoryError` and `StackOverflowError` — anti-pattern per `scala-jvm-safety-mindset` §1.

**Decision: REJECT.** Use explicit `try/catch` (Option A or B).

---

## Decision outcome

**Adopt Option B** (helper `readFile` + `fromPath` becomes 1-line `flatMap`).

Rationale:
- Matches the user's pipeline preference (smallest correct change, surgical, no opportunistic refactors).
- The helper is independently testable (single specific test for `readFile` would be a follow-up PR).
- `fromPath` becomes literally `readFile(path).flatMap(validateAndLoad)` — reads identically to `fromString` (which is `validateAndLoad(yaml)`).
- The 5-case catch is the same in both options; the helper just moves it.

### Implementation plan

1. Extract `private def readFile(path: Path): Either[PlatformModelError, String]` with the 5-case `try/catch` block.
2. Rewrite `fromPath` as `readFile(path).flatMap(validateAndLoad)`.
3. Pre-allocate the resource `try/finally` inside the helper (unchanged).
4. **No new `PlatformModelError` variant** — reuse `InvalidYaml` (file-not-found, access-denied) and `ParseFailure` (UTF-8, IO error).

### Files touched

| File | Change | LOC |
|---|---|---|
| `sm8-platform/src/main/scala/io/sm8/platform/query/PlatformModelLoader.scala` | Add `readFile` helper; rewrite `fromPath` to use it | +20, -5 = +15 net |
| `sm8-platform/src/test/scala/io/sm8/platform/query/PlatformModelLoaderSpec.scala` | +3 regression tests (directory-read + permission + empty-file) | +50, -0 = +50 net |
| `docs/adr/0008-y-platformmodelloader-typed-io-boundary.md` | This ADR | NEW |
| **Total** | | **+85, -5 = +80 net** |

### Tests to add (3)

The 5-test plan in v1.0 was not implementable against the public `fromPath(path: Path)` API. Post-review, the achievable test set is 3 tests:

1. `PlatformModelLoader.fromPath: read-PathIsADirectory returns Left(ParseFailure)` — `Files.newInputStream(directory)` throws `IOException` (per JDK NIO contract). The catch-all `case e: IOException` arm converts this to `Left(ParseFailure("IO error: Is a directory"))`. This is the cheapest path to exercise the `IOException` arm without injecting a mock stream.
2. `PlatformModelLoader.fromPath: permission denied returns Left(InvalidYaml)` — `Files.setPosixFilePermissions(path, java.util.Set.of())` then `fromPath`. Uses `assume(Files.getFileAttributeView(path, classOf[PosixFileAttributeView]) != null, "POSIX only")` to skip on Windows. Add a comment noting the test may run as root in CI containers (where `0o000` is bypassed); if so, the test passes for the wrong reason (file-not-found via root bypass); mark the test as `cancel` in that case.
3. `PlatformModelLoader.fromPath: empty file returns Left(SchemaValidation)` — `Files.write(path, Array.emptyByteArray)` then `fromPath`. The empty body flows to `validateAndLoad` → `ManifestValidator.validate("")` → schema validation rejects empty body. Asserts `Left(SchemaValidation(_))`. This is NOT a new variant — it covers the empty-file case using the existing `SchemaValidation` arm.

Tests 4 (race condition) and 5 (stream closure) from v1.0 are DROPPED:
- Test 4 (race condition) was not actually a race — `Files.delete` before `fromPath` simply hits the existing missing-file branch. Exercising a real race between `Files.exists` and `Files.newInputStream` requires either an injected lambda (refactor) or a custom `FileSystem` wrapper (heavy machinery). The 3-test budget is better spent elsewhere.
- Test 5 (stream closure) is not observable from the public `fromPath(Path)` API. The inner `try { ... } finally stream.close()` is already idiomatic Scala; static reasoning + the `try/finally` is sufficient.

### Binary compatibility

- **Source-compatible:** `fromPath` signature unchanged.
- **Binary-compatible:** sealed `PlatformModelError` ADT unchanged (no new cases; reuse `InvalidYaml` and `ParseFailure`).
- **Wire-compatible:** no new wire types.

### Spec alignment

- `fromPath` now returns `Either[PlatformModelError, Model]` 100% of the time (no hidden throws).
- Resource safety preserved (the inner `try/finally` still closes the stream).
- Matches the `fromString` / `validateAndLoad` contract.

---

## Skill alignment

### `scala-error-handling-mindset`

> "Either at IO, throw at programmer error."

- **Apply:** The IO boundary is the file read (`Source.fromInputStream(...).mkString`). All 5 documented exceptions are IO-derived — they MUST be caught and surfaced as `Left`.
- **Apply:** `Files.exists` + `Files.newInputStream` is the path; the catch happens AFTER the stream is acquired (resource cleanup is the inner `try/finally`).
- **Apply:** `OutOfMemoryError` / `StackOverflowError` are NOT caught — they are programmer errors / JVM faults, not IO.

### `karpathy-app-design-mindset`

- **Apply:** The platform adapter's third-party-extension-portal contract is "produce typed `Either`, never throw". A throw breaks the contract.
- **Verify:** The blast radius is 1 caller (`sm8-server.Main`). The fix is local; the contract is re-established.

### `scala-jvm-safety-mindset`

- **Apply §1:** Resource leak prevention — the `try/finally` already closes the stream.
- **Apply §2:** Catch ON the specific exception type, not on `Throwable` or `Exception`. The 5-case match orders specific → generic.
- **Apply §3:** No `OutOfMemoryError` / `StackOverflowError` swallowed.

### `debug-mantra-mindset`

- **Apply SS1 (reproduce):** Each test reproduces one reachable failure mode (PathIsADirectory exercises the `IOException` arm; permission denied exercises the `AccessDeniedException` arm; empty file exercises the `SchemaValidation` arm).
- **Apply SS2 (trace):** The test asserts the typed `Left` wrapping with the corresponding `PlatformModelError` case AND the message content.
- **Apply SS3 (falsify):** The pre-fix path throws `IOException`; the post-fix path returns `Left(...)`. The test asserts the falsified behavior (Left, not throw).
- **Apply SS4 (cross-reference):** The reachable failure modes are documented in `java.nio.file.Files` Javadoc (PathIsADirectory) + POSIX NIO docs (permission denied) + the existing `SchemaValidation` test (empty file).
- **Apply SS5 (verify):** 3 new tests + 8 existing tests; `mvn -pl sm8-platform test` runs all 11.

### `scala2-scaladoc-mindset`

- **Apply §1:** Strip process noise. The new helper's scaladoc explains WHY the 5 cases are chosen ("binary file renamed to .yml", "race condition", "permission denied", "disk error", "stream read failure"), not WHAT they catch.
- **Apply §2:** No `[[wikilinks]]` in the new code.
- **Apply §3:** No PR/Phase/ADR refs in the new code.

---

## Acceptance criteria

1. `PlatformModelLoader.fromPath` returns `Either[PlatformModelError, Model]` for ALL 3 documented IO failure modes that are reachable via the public API (PathIsADirectory, permission denied, empty file).
2. The 5 documented IO failures (UTF-8 malformed, unmappable characters, race condition, file not found, permission denied) are caught at the `try/catch` block; the `IOException` catch-all covers any IO failure not in the 4 specific catches.
3. The 6 uncaught exceptions (`SecurityException`, `OutOfMemoryError`, `StackOverflowError`, `NullPointerException`, `InvalidPathException`, `IllegalArgumentException`) propagate uncaught per the typed-IO contract.
4. The stream is closed in the failure path (no resource leak; the inner `try/finally` is preserved).
5. The 8 existing tests still pass.
6. The 3 new tests pass.
7. The change is binary-compatible (no new `PlatformModelError` cases).
8. The change is source-compatible (no signature changes).
9. The scaladoc on the new `readFile` helper explains WHY the 5 cases are chosen (justified by the IO boundary per `scala-error-handling-mindset`).

## Verification plan

```bash
mvn -B -ntp -pl sm8-platform test -Dtest='PlatformModelLoaderSpec'
# → 11 tests pass (8 existing + 3 new)

mvn -B -ntp -pl sm8-core,sm8-platform,sm8-cli,sm8-server,connectors/spark-connector,connectors/in-memory-connector,connectors/trino-connector,plugins/audit-plugin,plugins/broadcast-plugin,plugins/cache-plugin,plugins/materialize-plugin,plugins/row-cap-plugin,plugins/skew-plugin test
# → 814 tests pass (current 811 + 3 new from PR-134)

# Beyond test count, verify:
# 1. The stream is closed in the failure path (resource safety; static reasoning)
# 2. javap shows no checkcast delta on PlatformModelLoader or PlatformModelError
# 3. Memory + disk under 90% throughout the test run
```



## Risks

| Risk | Mitigation |
|---|---|
| Adding a `try/catch` around `Files.exists` could mask a programmer error (e.g. `path == null`) | The `try` block starts AFTER the existence check (which can throw `NullPointerException` if `path` is null — that's a programmer error, no catch) |
| `catch (e: IOException)` is broad but justified by the IO boundary | 5-case pattern orders specific → generic; the broad `IOException` is the catch-all |
| The new `readFile` helper is `private` and not directly testable | The 3 new tests exercise it via `fromPath`; a direct test would be a follow-up PR (out of scope) |
| Test for "permission denied" is fragile on POSIX | Use `Files.setPosixFilePermissions(path, java.util.Set.of())` + `assume(...)` to skip on Windows; cancel-on-root-bypass in CI |
| Test for "PathIsADirectory" requires CI to allow `Files.newInputStream(directory)` to throw `IOException` | The JDK NIO contract guarantees this throw on every OS; the test relies on the contract, not on CI behavior |
| Test for "empty file" overlaps with the existing `SchemaValidation` test | Asserts a different scenario (zero-byte file vs. missing name/version); the test is additive, not redundant |

## Open questions

1. Should the empty-file case be a separate `PlatformModelError` variant (e.g. `EmptyFile`) or `ParseFailure("file is empty")`? My recommendation: `ParseFailure` for consistency with other parse errors.
2. Should `fromPath` call `readFile` (Option B) or keep the body inline (Option A)? My recommendation: Option B for readability and testability.
3. Should the catch include `SecurityException` (when the security manager refuses file access)? My recommendation: NO — `SecurityException` is a programmer / JVM-config error, not an IO error.
