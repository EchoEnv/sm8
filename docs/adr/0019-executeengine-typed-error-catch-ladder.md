# ADR-0019: `executeEngine` typed-error catch-ladder — preserve the `EngineError` ADT surface at the IO boundary

## Status

Proposed. **Date:** 2026-09-05. **Author:** SM8 agent (per wayfinder map Ticket #1, `docs/wayfinder/2026-09-05-control-plane-robustness.md`).

## Context and Problem Statement

`sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:222-292` declares the IO boundary where every engine adapter's `provider.query(model, mcpRequest, ctx)` exception (if any) is caught and converted to a typed `EngineError`. The current catch ladder handles exactly four exception types:

- `java.util.concurrent.TimeoutException` → `EngineError.QueryTimedOut`
- `java.sql.SQLTimeoutException` → `EngineError.QueryTimedOut` (Future-proofing only; no production caller today)
- `InterruptedException` → `EngineError.CancellationFailed` (re-sets thread interrupt flag first)
- `NonFatal(e)` (catch-all) → `EngineError.ProviderInvocationFailed(engine, name = provider.identity.name, reason = e.getClass.getSimpleName, message = Option(e.getMessage).getOrElse(e.getClass.getSimpleName))`

The `NonFatal` catch-all collapses **10+ distinct failure modes** into the same `ProviderInvocationFailed` variant with `reason = "<class simple name>"`:

| Real exception class | Currently surfaced as | Should be (per the typed `EngineError` ADT) |
|---|---|---|
| `org.apache.spark.sql.AnalysisException` (e.g. "cannot resolve 'foo'") | `ProviderInvocationFailed(reason="AnalysisException", ...)` — string-only | `EngineError.UnsupportedCapability(engine, capability=<message>, message)` (501 wire code) |
| `org.apache.spark.sql.catalyst.analysis.NoSuchTableException` (Spark only) | `ProviderInvocationFailed` | `EngineError.EngineUnavailable(engine, available=<list from registry>, wasDefault=false, message)` (503 wire code) |
| `org.apache.spark.sql.execution.QueryExecutionException` whose `getMessage` contains "DECIMAL" or "Decimal" | `ProviderInvocationFailed` | `EngineError.DecimalOverflow(engine, value, precision, scale, message)` (422 wire code) |
| `java.io.IOException` (network blip during cache write, JDBC socket drop) | `ProviderInvocationFailed(reason="IOException", ...)` | `EngineError.ConnectionFailed(engine, reason, message)` (502 wire code) — matches the seam `realizeTyped` already uses at connection time (SparkEngineProviderDescriptor.scala:121) |
| `java.lang.NullPointerException` originating inside a plugin (e.g. `Sm8ToolHandlers.scala` accessing an unconfigured dependency) | `ProviderInvocationFailed` | `EngineError.HookFailed(engine, name="<plugin-name>", priority=N, stage="<stage>", message)` (500 wire code) — but NPE origin is generally unknowable from the catch site, so the catch ladder can only emit `HookFailed` with `name="(unknown)"`; a future ticket can introduce a plugin-name-resolution helper. For now: leave NPE under `ProviderInvocationFailed` with `reason="NullPointerException"` and document this as a known-limitation. |
| `java.lang.AssertionError` (a fatal `Error`, not `NonFatal`) | currently propagates through `NonFatal` would also catch, but `NonFatal` is `scala.util.control.NonFatal` which by definition excludes `Error`) | **propagates unchanged** — explicitly desired behavior; a fatal JVM error must propagate to the caller (the Restate retry path will retry per its policy; OOM/StackOverflow abort the request) |

Per the failure-mode survey (`docs/research/failure-modes-2026-09-04.md` §3 Pattern #5 "Engine-portable exception classifier collapses 5+ distinct failure modes" + Pattern #2 "wire-shape / API-shape drift"), this is the same fragility class already flagged there. This ADR closes the catch-side of that pattern; **Ticket #2 in the wayfinder map (generalize typed-error surfacing on the hook short-circuit side, per ADR-0010-a §6 deferral)** closes the meta-key-surfacing side of the same pattern.

## Decision

Extend the `executeEngine` catch ladder in `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:230-291` with three new classification arms **before** the `NonFatal` catch-all (most-specific-first Scala pattern match). Each new arm constructs a typed `EngineError` variant using the data the exception class provides.

**REVISED design (per RFC §3 layer discipline):** sm8-platform is the engine-portable control plane; it MUST NOT reference `org.apache.spark.*` types directly. The Spark-specific exception classes (`NoSuchTableException`, `AnalysisException`, `QueryExecutionException`) are classified via **throwable class-name string matching + exception-message parsing** — NOT via Scala-typed class references. A private helper `engineClass(t: Throwable): String = t.getClass.getName` keeps sm8-platform free of Spark imports while still routing Spark exceptions into typed `EngineError` variants on the wire contract. The catch ladder order matters: more specific classes must appear BEFORE their parent classes (e.g. `NoSuchTableException extends AnalysisException` so it must be caught before `AnalysisException`; `SQLTimeoutException extends SQLException` so it must be caught before `IOException` would matter — currently `SQLTimeoutException` is already caught before `NonFatal`, ordering preserved).

```scala
try {
  provider.query(model, mcpReq, ctx)
} catch {
  case e: java.util.concurrent.TimeoutException =>
    Left(EngineError.QueryTimedOut(...))     // existing — preserved
  case e: java.sql.SQLTimeoutException =>
    Left(EngineError.QueryTimedOut(...))     // existing — preserved (Future-proofing arm)
  case e: org.apache.spark.sql.catalyst.analysis.NoSuchTableException =>
    // Spark-specific: child of AnalysisException so must come BEFORE its parent.
    val available: List[String] = registry.availableProviders.toList
    Left(EngineError.EngineUnavailable(
      engine     = provider.identity.name,
      available  = available,
      wasDefault  = /* set from request.engine == null/blank */,
      message    = s"sm8: ${provider.identity.name}: NoSuchTableException: ${e.getMessage}"
    ))
  case e: org.apache.spark.sql.AnalysisException =>
    // Catches "cannot resolve column X" / "table not found by name" /
    // any other analysis-phase failure. Surfaced as UnsupportedCapability
    // (501) — the wire code that most accurately classifies an
    // analysis-phase failure from the operator's perspective.
    Left(EngineError.UnsupportedCapability(
      engine     = provider.identity.name,
      capability = s"analysis: ${e.getClass.getSimpleName}",
      message    = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
    ))
  case e: org.apache.spark.sql.execution.QueryExecutionException
      if Option(e.getMessage).exists(m => m.contains("Decimal") || m.contains("DECIMAL")) =>
    // Pattern-guard on the message: only surface as DecimalOverflow
    // when the message hints at a decimal-precision overflow. Other
    // QueryExecutionExceptions stay under the NonFatal catch-all
    // (treated as "execution failed unexpectedly").
    // Parse precision/scale from the message if possible; otherwise
    // default to 0/0 with the original message preserved.
    Left(EngineError.DecimalOverflow(
      engine    = provider.identity.name,
      value     = "<unparsed>",
      precision = 0,
      scale     = 0,
      message   = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
    ))
  case e: java.io.IOException =>
    // Move the IOException arm BEFORE NonFatal so it gets the typed
    // ConnectionFailed classification instead of collapsing to
    // ProviderInvocationFailed. Mirrors SparkEngineProviderDescriptor.
    // realizeTyped's classification at the connection seam.
    Left(EngineError.ConnectionFailed(
      engine  = provider.identity.name,
      reason  = e.getClass.getSimpleName,
      message = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
    ))
  case e: InterruptedException =>
    Thread.currentThread().interrupt()
    Left(EngineError.CancellationFailed(...))  // existing — preserved
  case NonFatal(e) =>
    // Catch-all: anything not classified above lands here. This is
    // INTENTIONAL — adds a typed-error path is a future ticket
    // (tracked in the wayfinder map as a follow-up).
    Left(EngineError.ProviderInvocationFailed(...))
  // NOTE: No `case e: AssertionError =>` — `NonFatal` excludes
  // `Error` (and AssertionError extends Error), so AssertionError
  // propagates through to the caller unchanged. Same for OutOfMemoryError,
  // StackOverflowError, etc. — fatal JVM errors must propagate per
  // RFC §13 observability.
}
```

### Why this ADR exists (per karpathy-guidelines-mindset §1)

Multiple reviewers and the failure-mode survey have flagged this exact fragility (Pattern #5 of the failure-mode survey is literally titled "Engine-portable exception classifier collapses 5+ distinct failure modes"). The catch-ladder extension is a **bounded, primary-source-backed** change with no SDK impact (sm8-core unchanged) and no new exception type (existing `EngineError` variants are populated, not extended).

### Why NOT a new `EngineError` variant

We could add `EngineError.SparkAnalysisFailed(engine, kind, message)` as a new variant — but that mixes engine-specific knowledge into the engine-portable ADT (sm8-core), violating RFC §3 layer discipline. The existing 13 variants are sufficient: `UnsupportedCapability` covers "the engine couldn't compile this query", `EngineUnavailable` covers "the engine doesn't know about this resource", `ConnectionFailed` covers "the engine's I/O seam broke", `DecimalOverflow` covers "the data didn't fit", `ProviderInvocationFailed` remains the catch-all for "engine execution failed unexpectedly".

### Why the pattern guard on `QueryExecutionException`

Spark's `QueryExecutionException` is the catch-all for *any* runtime execution failure (decimal overflow, division-by-zero, array index out of bounds, etc.). Surfacing ALL of them as `DecimalOverflow` would be a different misclassification. The `if Option(e.getMessage).exists(_.contains("Decimal"))` pattern guard scopes the `DecimalOverflow` classification to the actually-decimal case; other `QueryExecutionException` instances fall through to the `NonFatal` catch-all with `reason = "QueryExecutionException"`. The pattern guard is the minimum-fidelity heuristic — false negatives (decimal in a message that doesn't say "Decimal") still surface under `ProviderInvocationFailed`; false positives (a "Decimal" word in a non-decimal error) are rare in Spark and produce the typed error rather than a string-classified one.

## Layer placement

`sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala` only. `sm8-core` (the frozen library) unchanged. No new dependency. No transitive impact on connectors (the Spark connector still throws the same exceptions; only the platform's classification changes).

## Consequences

- Operators monitoring the wire can now distinguish Spark `AnalysisException` from Spark `NoSuchTableException` from Spark `QueryExecutionException` (decimal) from `IOException` from "engine execution failed unexpectedly" by reading the wire status code (501/503/422/502/502) and the `errorDetail.capability` field — **without parsing the error message string**.
- The "engine execution failed unexpectedly" catch-all (`ProviderInvocationFailed`) remains the safety net for exception classes that aren't yet classified. Future exception classes (Spark's evolving API, new engine adapters) will continue to surface under `ProviderInvocationFailed` with their class simple-name as `reason` until a future ticket adds arms for them.
- The `NonFatal` discipline (PR-176) is preserved: fatal `Error` (OutOfMemoryError, StackOverflowError, AssertionError, ThreadDeath) is **not** caught — propagates to the Restate retry path or JVM shutdown per RFC §13 observability.
- Existing `EngineServiceSpec` tests (especially the `IOException → ProviderInvocationFailed` test at line 356) **will need updating** — after this change, `IOException` surfaces as `ConnectionFailed`, not `ProviderInvocationFailed`. The existing test's expected type changes; its intent (verify a typed error reaches the caller) is preserved.

## Alternatives Considered

- **Map every Spark exception to a new dedicated `EngineError` variant.** Rejected: mixes engine-specific knowledge into sm8-core, violates RFC §3. The existing 13-variant ADT covers the operational surface needed.
- **Reflective exception-classification table** (Map[Class[_], EngineError factory]). Rejected: not idiomatic Scala 2.13; hides intent behind a Map; brittle if Spark renames exception classes between minor versions. The pattern-match ladder is the Scala idiom per `scala-data-driven-refactor-mindset` "sealed-trait dispatch".
- **Wrap every exception in `Either.try` upstream in each connector.** Rejected: pushes the classification responsibility into every connector (3 today, more later), duplicating logic across the spark/duckdb/trino/in-memory providers and creating per-connector drift risk. The platform-level catch is the single source of truth.
- **Do nothing and file a follow-up.** Rejected: Pattern #5 in the failure-mode survey has been a known gap since 2026-09-04; the wayfinder map Tier 1 row 1 explicitly schedules this as the highest-leverage control-plane improvement.

## References

- `docs/wayfinder/2026-09-05-control-plane-robustness.md` — Ticket #1 source
- `docs/research/failure-modes-2026-09-04.md` Pattern #5 — the survey that flagged this class
- `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:222-292` — the catch ladder this ADR modifies
- `sm8-core/src/main/scala/io/sm8/core/engine/EngineError.scala:39-198` — the typed ADT (13 variants)
- `sm8-platform/src/test/scala/io/sm8/platform/query/EngineServiceSpec.scala:319-547` — existing test fixtures to update
- `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/SparkEngineProviderDescriptor.scala:106-133` — mirror classification at the connection seam
- `connectors/in-memory-connector/src/main/scala/io/sm8/connectors/inmemory/InMemoryEngineProviderDescriptor.scala:56-64` — typed-error connection-seam pattern
- `connectors/trino-connector/src/main/scala/io/sm8/connectors/trino/TrinoEngineProvider.scala:87-95` — typed-error pattern in a stub
- `sm8-platform/src/main/scala/io/sm8/platform/query/QueryService.scala:301-324` — wire-code mapping (the 13 variants → status codes)

Local-only `.omp/WATCHDOG.yml` (gitignored) tunes the advisor roster for this project.
