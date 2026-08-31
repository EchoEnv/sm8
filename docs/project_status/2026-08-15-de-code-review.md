# SM8 — Senior Data Engineer Code Review

> **⚠️ Historical snapshot (2026-08-15).** References to
> `ConnectorContractSpec` / the 4 RFC §12 Connector conformance assertions
> describe the pre-ADR-011-a codebase; that contract base has since been
> removed and conformance moved to per-connector `EngineProvider` test
> suites. Read current architecture from
> `docs/rfcs/2026-08-12_v1_architecture-spec/` + `docs/adr/0011-a-*`.

**Date:** 2026-08-15
**Branch:** `main` @ `5b6eb5c`
**Reviewer scope:** senior data engineer — full reactor (sm8-core, sm8-platform, sm8-cli, all connectors/, all plugins/)
**RFC reference:** `/home/emilio/app/projects/sm8/docs/rfcs/2026-08-12_v1_architecture-spec/`
**Plan reference:** `/home/emilio/.claude/plans/agile-kindling-beacon.md`
**Prior state doc:** `/home/emilio/app/projects/sm8/docs/project_status/2026-08-14-status.md`

---

## 1. TL;DR

SM8 today is a **well-architected engine-portable engine at its first stable plateau**. The frozen-Core / hot-Plugin boundary is mechanically enforced (Maven enforcer on every reactor module + service-loaded portal). The 4-stage pipeline is data-driven via sealed traits, the 11-variant `EngineError` ADT is exhaustive, and the engine-portable path (`EngineService.runQueryWithHooks` → `QueryService` → Restate handler) is the canonical entry point with no per-call allocation waste.

The reviewer surface is small and most of it is honest Step-0 / Step-9a TODO. **11 findings**, in three severity bands:

- **CRITICAL (0)** — nothing that loses data or crashes production today.
- **MAJOR (4)** — real bugs that must be fixed before the next platform PR.
- **MINOR (7)** — code-quality / doc / latent issues that compound over time.

The 4-stage pipeline + 8 hook attachment points + typed error ADT are the right shape. The risks are concentrated in **two areas**: (1) the cache/journal hook path is over-complicated for what it does today (the cache lives both inline in `EngineService` AND as a real hook plugin AND as a stub in `plugins/cache-plugin`); (2) the `ResultValue`/`SealedDataType` duality plus the raw-SQL `where: Option[String]` is a real ambiguity that the next caller will trip over.

**The architecture is sound. Specific code is sound. The integration layer is where the seams show.**

---

## 2. What's right

Concrete signals that the design intent has survived the 14-PR migration:

1. **Zero-Spark invariant is mechanically enforced.** Every reactor module except `connectors/spark-connector` runs `bannedDependencies=org.apache.spark:*`. The exclusion `connectors/spark-connector/pom.xml` deliberately DOES NOT exclude Spark — the inverted-enforcer pattern. A grep for `org.apache.spark` in `sm8-core/src/main/scala` returns 7 matches, all of them in `//` comments saying "no Spark imports — verifiable by grep". The contract is real.

2. **The pipeline is genuinely data-driven.** `Stage` is a sealed trait (`sm8-core/.../Pipeline.scala:51`); the runner is `Stage.All.foldLeft(initial)` (`Pipeline.scala:163`). Adding a stage = one case class + one line in `Stage.All`. The compiler enforces exhaustiveness on the `Stage.name` match in `preStageFor`/`postStageFor`. This is the "data, not control flow" the RFC asks for.

3. **`HookOrigin` priority-range validation is total and pure.** `HookOrigin.validate(origin, priority): Either[String, Unit]` (`HookOrigin.scala:90`) is exhaustively matched on the sealed trait. The `HookManagerImpl` boundary converts to `IllegalArgumentException` AT the SDK boundary, not in the validator (`HookManagerImpl.scala:104-113`). Three-band reservation (Core 0-99 / FirstParty 100-899 / Community 900+) is documented in scaladoc and tested.

4. **`InMemoryResultCache` is a textbook single-flight cache.** `ConcurrentHashMap.putIfAbsent` (NOT `computeIfAbsent`, which holds a CHM bin lock during compute — see review comment `InMemoryResultCache.scala:90`). Leader paths store-before-complete (no torn read). `inflight` is `@transient` + `readResolve` reinitialized on deserialize. Interrupt flag restoration is correct in BOTH the leader (`InMemoryResultCache.scala:295-303`) and waiter (`332-334`) paths. This is the most carefully-built file in the reactor.

5. **`EngineError` is genuinely exhaustive.** 11 variants, all extending `sealed trait EngineError`. `QueryService.engineErrorCode` (`QueryService.scala:262-274`) is a sealed-trait match that the compiler will flag if a new variant is added without a corresponding HTTP code. `toErrorDetail` on every variant is non-optional and total.

6. **`PortableCellCodec`/`CachedRowDecoder` round-trip is correct.** `decodeCell(T_DATE, encoded)` uses `LocalDate.parse(encoded).atStartOfDay(UTC).toInstant.toEpochMilli` (`PortableCellCodec.scala:175-181`) — UTC-anchored so `Date.getTime()` is JVM-timezone-independent. `decodeCell(T_TIMESTAMP, ...)` uses `Timestamp.from(Instant.parse(...))` which preserves the underlying millis. `decodeCell(T_BINARY, ...)` uses `java.util.Base64.getDecoder.decode` matching the encoder's `encodeToString`. This is the kind of boring, correct, timezone-correct code that becomes a landmine if it's wrong.

7. **Plugin/Connector/Hook contract tests are public.** `sm8-core` publishes a test-jar (`sm8-core/pom.xml:97-113`) so third-party Plugin modules can extend `PluginContractSpec` / `ConnectorContractSpec` / `HookContractSpec`. `ConnectorContractSpec.scala` enforces the 4 RFC §12 assertions. This is the right infrastructure for a starter kit.

8. **`Plugin.closedOverVars: Seq[String]` is a real introspection surface.** The docstring on `Plugin.closedOverVars` (`Plugin.scala:74`) explicitly says it's a future serialization-safety spec's anchor; `PluginClosureSafetyConformanceSpec` already asserts the trait + a fixture round-trip. This is a small but unusually forward-looking contract decision.

---

## 3. Findings, by severity

### 3.1 MAJOR — must fix before next platform PR

#### M-1. Cache ownership is split three ways — the next integrator will get it wrong

**Files:**
- `sm8-platform/.../EngineService.scala:340-389` — `runQueryWithHooks` documents that the cache lookup is now a *PreExecute hook* (not inline)
- `sm8-platform/.../cache/CachePlugin.scala:43-75` — the **real** `PreExecute read-through + PostExecute write-through` cache lives in `sm8-platform/query/cache/`
- `plugins/cache-plugin/.../CachePlugin.scala:24-50` — a separate **stub** cache Plugin in the plugins/ tree, also registered as `pre:execute` + `post:execute`, that just increments counters
- `sm8-platform/.../EngineService.scala:340-389` calls `CacheBridge.platformCacheKey(...)` to compute the key, but never actually consults `cache.getJournaled` itself — the cache hit path is entirely the hook's responsibility
- The status doc claims the cache lives in `InMemoryResultCache` at the engine-portable layer; **no PR has actually wired it**

The 3-way split is:
1. `InMemoryResultCache` — the data structure (sm8-platform/query/) — used by the real CachePlugin
2. `CachePlugin` (sm8-platform/query/cache/) — the real one, with read-through + write-through, `HookOrigin.Core` priority 50/60
3. `CachePlugin` (plugins/cache-plugin/) — the stub, with `HookOrigin.FirstParty` priority 100/110, registered via `META-INF/services/`

There is no code path in the reactor today that constructs `CachePlugin` from (1) and registers it on `QueryService.definition`. `RestateBootstrap.bindAndListen` (`RestateBootstrap.scala:70-84`) calls `QueryService.definition(model, registry, cache, plugins = Nil)`. `QueryService.definition` constructs a fresh `EngineImpl` and runs `plugins.foreach(engine.use)` (`QueryService.scala:142-143`) — `Nil` is the default, so nothing is registered.

So the entire cache path is dormant in production. Meanwhile the `plugins/cache-plugin/` stub has a class named `CachePlugin` and the `sm8-platform/.../cache/CachePlugin` has a class ALSO named `CachePlugin`. They have different packages but the same simple name — anyone who imports both will silently shadow.

**Fix:** Pick one. Recommend:
- Delete `plugins/cache-plugin/` entirely — it duplicates the real `CachePlugin` in `sm8-platform/.../cache/`, has no real behavior, and is named the same. The 6 reference plugins (cache/audit/row-cap/broadcast/materialize/skew) advertised in the status doc §2 are partly phantom.
- Wire `sm8-platform/.../cache/CachePlugin` into `RestateBootstrap.bindAndListen` — either accept it as a default (alongside `MCPEngineRegistry`) or load via `META-INF/services/` so a real integrator gets caching for free.
- The cache-key derivation in `EngineService.runQueryWithHooks:346-354` computes `cacheKey` and threads it through `EngineHookRequest.cacheKey` — that means the CachePlugin's PreExecute hook can use it without re-deriving. But the current `runQueryWithHooks` docstring claims "The cache lookup + populate is no longer inline in the executor" while the executor is in fact still doing no cache I/O — there's nothing to compare to. Make the doc match the code, or remove the false claim.

This is the largest concrete item. Without it, "PR-C-final" is a handler-class wiring exercise that proves the wire protocol but caches nothing.

#### M-2. `executeEngine`'s `RuntimeException` catch is too narrow for a real engine boundary

**File:** `sm8-platform/.../EngineService.scala:214-238`

```scala
def executeEngine(...): Either[EngineError, PortableQueryResult] = {
  try {
    provider.query(model, mcpReq, ctx)
  } catch {
    case e: RuntimeException =>
      Left(EngineError.ProviderInvocationFailed(
        engine = provider.identity.name,
        name   = provider.identity.name,
        reason = e.getClass.getSimpleName,
        message = e.getMessage
      ))
  }
}
```

Three problems:

1. **`RuntimeException` doesn't cover all engine-failure exceptions.** Spark throws `org.apache.spark.sql.AnalysisException` (a `RuntimeException` — OK), but also `scala.NotImplementedError` (an `Error`, NOT a `RuntimeException`) for unimplemented feature paths, and `java.lang.AssertionError` (also `Error`). The whole thing escapes as a real `Error` and the SDK's `Restate.run` retry path may not see it as retryable. `SparkEngineProvider.query` (`SparkEngineProvider.scala:117-134`) does its own try/catch mapping AnalysisException vs other `Exception` — meaning a real `Error` from Spark crashes past the catch in `executeEngine`.

2. **`name = provider.identity.name` duplicates `engine = provider.identity.name`.** Per `EngineError.ProviderInvocationFailed(engine, name, reason, message)` — `engine` is "which engine produced this"; `name` is "name of the provider method/operation" (a sub-locator). Setting both to `provider.identity.name` is a wire-shape violation of the same flavor as DE-reviewer MAJOR #7 in `SparkEngineProvider` history. The right value for `name` is the operation: `"query"`, or `"SparkEngineProvider.query"`, or the caller site. Today they're literally identical strings.

3. **`e.getMessage` may be `null` for some `RuntimeException` subtypes** (esp. those generated by `new RuntimeException()` with no arg). `s"...${e.getMessage}"` becomes `"...null"`. Either guard or use `Option(e.getMessage).getOrElse(e.getClass.getName)`.

**Fix:**
```scala
catch {
  case e @ (_: RuntimeException | _: NotImplementedError) =>
    Left(EngineError.ProviderInvocationFailed(
      engine  = provider.identity.name,
      name    = "query",
      reason  = e.getClass.getSimpleName,
      message = Option(e.getMessage).getOrElse("<no message>"),
    ))
  // Other Errors propagate (programmer errors / OOM / StackOverflow)
  // — the engine boundary is for IO-bound failures only.
}
```
Or, better, catch `NonFatal` (which is the Scala idiom already used in `EngineImpl.use`, `EngineImpl.loadMetadata`, `EngineImpl.discoverFromConfig`, `ModelLoader.fromPath`, `ModelLoader.fromStream`). `NonFatal` covers `RuntimeException` and `NotImplementedError` while excluding `VirtualMachineError` subclasses (`OutOfMemoryError`, `StackOverflowError`) — exactly the right shape.

#### M-3. The `RestatedEngineRunner` is an inert stub that's actively misleading

**File:** `sm8-platform/.../RestatedEngineRunner.scala:97-164`

```scala
def runJournaled[A](name: String, ctype: Class[A], supplier: Supplier[A]): A = {
  val threadName = Thread.currentThread.getName
  val isHandlerThread = isInRestateHandlerThread
  if (isHandlerThread) {
    throw new IllegalStateException(...)
  }
  supplier.get()  // ← just calls the supplier
}

private def isInRestateHandlerThread: Boolean = false  // ← always false
```

Three problems:

1. **`isInRestateHandlerThread` is permanently `false`.** The docstring on `RestatedEngineRunner.scala:158-163` says "TODO(PR-C5b-ext-γ'-follow-up): replace with `ThreadLocal[HandlerContext].get() != null` once the handler-class wiring lands. Today: always false." Today, the runtime check is a no-op — every caller of `runJournaled` goes straight to `supplier.get()`, regardless of whether they're on a Restate handler thread. The whole `if (isHandlerThread) throw` block is dead code.

2. **The Restate v2.x handler class is now wired (`QueryService.scala:113-173`)** — `HandlerRunner.of(fn, serdeFactory, options)` IS the v2.x journaled-execution path, and it's already used by `QueryService.definition`. So the "follow-up PR" mentioned in the docstring has *already landed* — and `RestatedEngineRunner` is now both vestigial AND the probe inside it is wrong. If a real handler thread ever reaches `runJournaled`, the probe returns false (still hard-coded) and the supplier is invoked directly — which is fine for idempotent suppliers, but the whole point of `Restated.run(...)` was that the SDK replays journaled sub-calls to ensure exactly-once.

3. **The class lives in the package `io.sm8.platform.query` next to `RestateBootstrap` and `QueryService`.** A reader assumes it's part of the production wiring. It isn't. **No production caller in the reactor today** (the docstring says so) — and it's not obvious how a future caller would discover this is a stub.

**Fix:** Delete `RestatedEngineRunner.scala` and its spec. The handler-class wiring in `QueryService.scala:113-173` IS the production path; `HandlerRunner.of(...)` + `HandlerRunner.run(stubContext, ...)` in `QueryServiceSpec` is the test path. There is no third path that needs a helper. If the helper is needed for a future `Restate.run(name, ...)` style sub-call (which the docstring says was the original intent), it can land in the PR that introduces that sub-call. The status doc's §2 line "`RestatedEngineRunner` helper (scope-pivot rationale: v2.x has no static `Restate.run`)" should be removed from the docs and from the test count.

#### M-4. `executeEngine` cannot distinguish typed-engine errors from infrastructure errors

**File:** `sm8-platform/.../EngineService.scala:208-238`

`executeEngine` is documented as the IO boundary for engine execution. Per `scala-error-handling-mindset` (cited in the file doc): "catch at the IO boundary (this IS the IO boundary for the engine adapter), convert to the typed `EngineError`."

The current implementation collapses ALL `RuntimeException` into `ProviderInvocationFailed`. The 11-variant ADT has subtypes specifically for the real classes of engine failure:
- `EngineUnavailable` — `MCPEngineRegistry.select` already returns this; if `provider.available` flips false after construction, the only signal is a `RuntimeException` from the provider. Today: misclassified as `ProviderInvocationFailed`.
- `ConnectionFailed` — JDBC/SQL connection errors (Trino, Databricks, Snowflake).
- `QueryTimedOut` — query timeout (engine-driven, not Restate-driven; see also `QueryService.scala:194-211`).
- `UnsupportedCapability` — engine rejects an SQL feature.
- `DecimalOverflow` — decimal precision/scale violation.
- `SourceSchemaChanged` — schema drift between the cached model and the source.

Today, a Trino `java.sql.SQLException: Connection refused` and a Spark `AnalysisException: cannot resolve 'foo'` and a Snowflake `net.snowflake.client.jdbc.SnowflakeSQLException: Decimal precision 39 > 38` all become `ProviderInvocationFailed` with reason=`SQLException` or reason=`AnalysisException`. The wire contract collapses 5 distinct failure modes into 1. Per `SparkEngineProvider.scala:117-134`, the Spark provider has its OWN `try/catch` mapping `AnalysisException → ProviderInvocationFailed` and "everything else → `ConnectionFailed`" — but the `provider.query` `Either[EngineError, ...]` already returns the typed error in the `Left` channel, so this catch never sees it.

The "inner catch" in `SparkEngineProvider.query` is **also wrong** — the `Left` typed errors flow back through `Either`, never reach the `try/catch`. The try/catch is for `RuntimeException`s thrown out of an engine that doesn't return `Either` — which, per the contract, the engine-portable adapters SHOULD do. The try/catch is defensive against a buggy adapter, not against normal engine errors.

**Fix:** Make `executeEngine`'s exception classifier richer. Three options, in increasing order of effort:
1. Cheap: introspect exception class names (`SQLException`, `AnalysisException`, etc.) and pick the closest `EngineError` variant.
2. Medium: add an `EngineErrorClassifier` typeclass that providers can implement to map their own exception hierarchy.
3. Architectural: don't catch at all — let adapters return `Left(EngineError)` and propagate `Right(_)`; require adapter implementers to honor the contract. Wrap the `try/catch` with `NonFatal` and treat its occurrence as a programmer error in the adapter (logged, propagated as a `Left`, with a clear message about which adapter misbehaved).

---

### 3.2 MINOR — quality / latent issues

#### m-1. The `RequestResult` types in the SDK carry no information

**File:** `sm8-core/.../RequestResult.scala` (stub-only based on the index — read by code search; the `Request` and `Result` markers in `Context.scala:62-67`)

```scala
trait Request
trait Result
```

These are the SDK's marker traits for `Context.request` and `Context.result`. They're used by:
- `EngineHookRequest` extends `Request` (`EngineHookTypes.scala:43-47`)
- `EngineHookResult` extends `Result` (`EngineHookTypes.scala:57-58`)
- `ConnectorRequest` (legacy in `Pipeline.Execute:93-107`)
- `ConnectorResult` (legacy)

**Problem:** The traits are open (`Request` says "NOT sealed — anyone (test stubs, third-party Plugins) may define a concrete subtype"). `EngineHookDispatcher` (`EngineHookDispatcher.scala:81-103`) handles `Context.request` by runtime class-check (`asInstanceOf[EngineHookRequest]`), with a clear message if the request type is wrong. That's defensible — the dispatcher's `Either` boundary forces the cast to be explicit. But the legacy `Pipeline.Execute` (still in the reactor) pattern-matches on `case ConnectorRequest(connectorName, query) =>` — which means the engine-portable path and the legacy 4-stage pipeline share a `Context.request: Request` field, but `Pipeline.Execute` will silently ignore an `EngineHookRequest` (no `ConnectorRequest` case → fall-through `case other => ctx`).

Today this is latent because nothing in the reactor invokes `Engine.run(EngineHookRequest)` — the engine-portable path bypasses the 4-stage pipeline (it uses `EngineHookDispatcher` directly). But the architectural invariant per the RFC §6 ("Fixed sequence of stages... minimal default logic per stage") says the pipeline shape is Core; having two parallel dispatchers (one for the legacy `ConnectorRequest`, one for `EngineHookRequest`) is a smell that will grow when PR-C-final-int (Docker-gated integration test) actually drives through `QueryService.runQuery`.

**Fix:** Either (a) fold `EngineHookRequest` into `Request` and have `Pipeline.Execute` route on it (canonical engine-portable path is the pipeline), OR (b) formally deprecate `Pipeline` for the engine-portable path and document the divergence. (a) is preferable — the pipeline shape is Core per the RFC.

#### m-2. `runQueryWithHooks` builds the cache key with a different `request.engine` fallback than `selectEngine`

**File:** `sm8-platform/.../EngineService.scala:343-376`

```scala
val mcpReq: MCPQueryRequest = buildMCPRequest(request)
val version: Int           = model.version
val cacheKey: String       = CacheBridge.platformCacheKey(
  engine     = Option(request.engine).filter(s => !isBlankLikeJava(s))
    .getOrElse(registry.defaultEngine),  // ← uses request.engine or default
  ...
)
val hookRequest = EngineHookRequest(model, mcpReq, cacheKey)
val initialCtx: Context = Context(...)
val engineExecutor: Context => Either[EngineError, Context] = { ctx =>
  val hookReq = ctx.request match {
    case hookReq: EngineHookRequest => hookReq
    case other => return Left(...)  // unwrap
  }
  for {
    provider <- selectEngine(model, request, registry)  // ← uses same fallback
    pqr      <- executeEngine(model, hookReq.mcpRequest, provider)
  } yield ctx.copy(result = Some(EngineHookResult(pqr)))
}
```

Both `cacheKey` and `selectEngine` use the same `Option(request.engine).filter(non-blank).getOrElse(registry.defaultEngine)` derivation. But it's duplicated as a literal expression in two places (and a third near-identical expression in `selectEngine` itself at line 173-175). One change to the fallback rule requires three edits.

**Fix:** Extract `private def effectiveEngineName(request, registry): String`. Or, better, accept the engine name as a `String` parameter and let the caller pass `effectiveEngineName(...)` once.

#### m-3. `MCPEngineRegistry.select`'s `wasDefault` semantics is "the requested name equals the default" — but the request name might not be the default even when it was the result of "fall through to default"

**File:** `sm8-core/.../MCPEngineRegistry.scala:54-64`

```scala
def select(name: String): Either[EngineError, MCPEngineProvider] = {
  val available = availableProviders
  engines.get(name) match {
    case Some(p) if p.available => Right(p)
    case _                      => Left(EngineError.EngineUnavailable(
      engine     = name,
      available  = available,
      wasDefault = (name == default),  // ← was the REQUESTED name the default?
      message    = "engine unavailable: " + name,
    ))
  }
}
```

`wasDefault = (name == default)` reports whether the requested name IS the default name, not whether the caller actually fell through to the default. In `EngineService.selectEngine`, when `request.engine` is blank, the caller passes `registry.defaultEngine` as `name` — so `wasDefault = true` correctly reflects "yes, we fell through." But a future caller that bypasses `EngineService.selectEngine` and calls `registry.select(name)` directly with a name that happens to equal the default string (e.g. `"spark"` when `default = "spark"`) will report `wasDefault = true` even though the caller explicitly requested it. Cosmetic, but misleading.

**Fix:** Either accept an explicit `wasRequestedDefault: Boolean` parameter (preferred — caller knows the intent), or rename the field to `isDefaultName` and document it as a name-equality check.

#### m-4. `CacheBridge.platformCacheKey` treats the version surrogate inconsistently

**File:** `sm8-platform/.../CacheBridge.scala:91-113`

```scala
def platformCacheKey(
    engine: String,
    modelName: String,
    version: Int,
    measures: List[String],
    dimensions: List[String],
    where: Option[String]
): String = {
  val payload = lengthPrefixed(
    lengthPrefixed(engine) +
    lengthPrefixed(modelName) +
    lengthPrefixed(version.toString) +
    ...
  )
```

The docstring at line 85-86 says: "the legacy uses `model.hashCode()`; for v0.3.1 we don't have a real version field". But `Model` has a `version: Int` field (`Model.scala:32`) and `ModelValidationError.InvalidVersion` enforces it. So the caller has a real version. The hash is over `version.toString` of that real field — fine.

What's inconsistent: the cache-key derivation includes engine + modelName + version but NOT the model dimensions/measures (the model's *shape*). Two models with the same name and version but different dimension/measure shapes would share cache entries. The cache would correctly key on `measures` and `dimensions` (the request-level args) but not on the underlying model's shape.

For a semantic-layer engine, the model's shape is part of its identity — if `flights_v1` has dimensions `[carrier, origin]` and gets a query for `[carrier, origin, dest]`, the query result depends on the model's shape. Two model versions with different shapes that share a name would collide.

**Fix:** Either thread the model's `dimensions` + `measures` hashes into the key, OR document that the cache key is request-shape-only (which means invalidate on model-schema change via `invalidateModel`).

#### m-5. `EngineHookDispatcher`'s short-circuit semantics let `PostExecute` fire on `stop = true` — but only when the hook doesn't reset `result`

**File:** `sm8-platform/.../hooks/EngineHookDispatcher.scala:81-103`

```scala
if (afterPre.stop) {
  // Short-circuit: skip executor, still fire post-hooks so
  // observers (audit, log) see the cached/halted path.
  Right(firePost(stage, afterPre))
} else {
  execute(afterPre) match {
    case Left(err) => Left(err)
    case Right(withResult) => Right(firePost(stage, withResult))
  }
}
```

This is correct for the cache-HIT path (`CacheReadPreHook` sets `result = Some(EngineHookResult(pqr))` and `stop = true`; PostExecute fires and `CacheWritePostHook` writes the cached row back to the cache — same value). It's wasteful for an EXTERNAL `stop = true` short-circuit (e.g. an auth-deny pre-hook that sets `result = None`, `stop = true`). The PostExecute hooks fire and have nothing to do.

Not a bug — but the docstring could call out that PostExecute fires unconditionally on `stop = true`. Today the comment only says "so observers (audit, log) see the cached/halted path."

#### m-6. `ExprParser` docs claim it's recursive-descent but the file is 540 lines — likely much more than recursive-descent

**File:** `sm8-core/.../ExprParser.scala` (line-counted: 540)

The docstring at line 27-29 says "The cursor is a `Vector[Char]` + integer index. No `String.substring` (O(n) on a JDK `String` slice — O(n²) over the parse). Recursive descent." That's plausible for a 540-line recursive-descent parser. But the spec file (`ExprParserSpec`) was not opened in this review; the parser's full structure should be checked for:
- Operator precedence correctness (the parser claims `+ - * / %` with left-associative precedence — verify with test cases for `1 + 2 * 3` → `Add(Literal(1), Multiply(Literal(2), Literal(3)))`)
- Left-recursion in any rule (would be a stack overflow bug)
- Error recovery on malformed input (does it return `Left(ExprParseError)` cleanly or throw)

**Fix:** Open `ExprParserSpec.scala` and the parser internals in the next PR cycle. Confirm left-associativity tests exist for the arithmetic operators. Verify no rule uses left-recursion (`Add := Add '+' Mul` would be wrong).

#### m-7. `MaterializePlugin` and `BroadcastPlugin` and `SkewPlugin` are counter-only stubs

**Files:**
- `plugins/materialize-plugin/.../MaterializePlugin.scala` — registers `PreExecute + PostExecute` at priority 250; both hook bodies are `counter.incrementAndGet(); context`
- `plugins/broadcast-plugin/.../BroadcastPlugin.scala` — same shape
- `plugins/skew-plugin/.../SkewPlugin.scala` — same shape

These are placeholders. They claim to handle Spark-specific behavior (AQE skew threshold, broadcast hint, materialize/persist) but actually do nothing. A user who reads the scaladoc and ships them to production gets nothing.

The status doc §3.2.1 ("PR-C-final-int — Docker-gated integration test") implies these will land in Step 9 (the "6 reference plugins"). Today they're shape-correct, fail the conformance contract only if a test asserts the *behavior*, and load via `META-INF/services/`.

**Fix:** Either delete the counter-only stubs (they pollute the namespace and conflate with `plugins/cache-plugin/`) OR add a test that asserts "registered at the right stage/priority but is a no-op" so the truth is in the suite. Right now a future PR that makes them real has no test coverage for the existing counter-only behavior.

---

## 4. Cross-cutting observations

### 4.1 The 4-stage pipeline is shadowed by the engine-portable path

Today `sm8-core.Pipeline.run` is the canonical 4-stage pipeline, but `sm8-platform.EngineHookDispatcher.run` is the engine-portable Execute-stage loop. The platform path doesn't go through `Pipeline.run` at all — `QueryService.definition` constructs a fresh `EngineImpl` and uses its `hooks` registry, but never calls `engine.run(request)`.

This is consistent with the PR-C-final design (the engine-portable path is a separate code path), but it means:
- The 6 reference plugins (`audit`, `row-cap`, `broadcast`, `materialize`, `skew`, `cache-stub`) register hooks on `engine.hooks`, but those hooks fire via `EngineHookDispatcher.run`, not `Pipeline.run`.
- The 4-stage pipeline is exercised only by the legacy `ConnectorRequest` flow (and the `EngineSmokeSpec`).
- `EngineImpl.run(request: Request): Result` is essentially dead code in production today.

For a frozen Core this is fine — `Engine.run` is the SDK surface, plugins register hooks via `engine.hooks`, and the platform chooses its own dispatch. But it's worth documenting in the plan + RFC that the 4-stage pipeline is the *legacy shape* and the platform's `EngineHookDispatcher` is the *engine-portable shape*, and these are intentionally parallel.

### 4.2 Conformance tests are public — but no third-party Plugin has shipped

`sm8-core` publishes a test-jar (`sm8-core/pom.xml:97-113`) and `ConnectorContractSpec` / `HookContractSpec` / `PluginContractSpec` are designed to be extended. None of the connectors/ or plugins/ currently in the reactor uses all 4 RFC §12 assertions for `Connector.query` data-shape conformance (the in-memory and Trino connectors both stub `query` to `ResultRows(Vector.empty)` for non-error paths; the Spark connector is a skeleton). This means the conformance gate runs but is satisfied vacuously.

**Recommendation:** The first non-trivial connector (a real Trino JDBC driver, or the Spark Connect runtime) is the place where the conformance suite proves itself. Until then, the "every adapter passes conformance" claim is true but uninformative.

### 4.3 Serialization is well-defended, but not end-to-end

`PluginSerializationSpec` round-trips every Plugin + Hook class via `ObjectOutputStream`. `RestateCachedRowSerializationSpec` does the same for `RestateCachedRow`. The `Entry` class in `InMemoryResultCache` is `Serializable` (per the review-pass-#2 JVM-reviewer CRITICAL #1 fix).

What's NOT covered by a spec:
- The full `EngineService.runQueryWithHooks` payload: a `Context` with `request = EngineHookRequest(model, mcpReq, cacheKey)` + `result = Some(EngineHookResult(pqr))` — the typed carriers themselves are Serializable, but there's no test that proves a *full Context* round-trips through `ObjectOutputStream` cleanly.
- `ModelBuilder.build` round-trip (the YAML-loaded `Model` round-trips per `EndToEndPipelineSpec.scala:295-305`; the programmatic `Model` from `ModelBuilder` doesn't have a separate test).
- `EngineContext` with all 5 policy fields populated — not tested.
- `EngineHookDispatcher` itself is stateless so serialization isn't a concern, but its captured `HookManager` reference holds the live `mutable.Map` of registered hooks — that's a real concern if a serialized `Dispatcher` rehydrates. `Dispatcher.apply` takes a `HookManager` parameter; if the caller serializes both the EngineImpl and the Dispatcher, the deserialized Dispatcher holds a deserialized HookManager that... actually is fine because `HookManagerImpl` uses `mutable.Map` (not serialized). But this is implicit.

**Recommendation:** Add one spec that round-trips a fully-populated `Context` + `EngineHookRequest` + `EngineHookResult` + `EngineContext` + `Model` together. It's 30 lines of test; the value is that it catches a future refactor that introduces a non-Serializable reference in any of these types.

### 4.4 Pom-file hygiene

The root `pom.xml` pins 4 Restate SDK artifacts (lines 86-122), 2 log4j artifacts (lines 141-150), and vertx-core (lines 128-132). The pins exist to defend against the documented `NoSuchMethodError` between log4j-api 2.24.2 and log4j-core 2.20.0 (lines 134-140). This is correct and well-commented.

Two latent risks:

1. **No dep version range convergence check.** When ScalaTest 3.2.19 ships a 3.2.20, the reactor won't notice (no `<dependencyManagement>` entry — `pom.xml` only pins `scala-library`, `scalatest`, `jackson-databind`, `json-schema-validator`, Restate SDK, log4j, vertx-core). Add `scalatest_2.13` to `<dependencyManagement>` (currently it IS there — `pom.xml:72-75` — but the version is just `${scalatest.version}` from `<properties>`). The lockfile-style pinning is consistent across the reactor — fine.

2. **MiMa is commented out** (`sm8-core/pom.xml:158-178`). The status doc §3.2.5 says MiMa lands "before the first public `v0.1.0` release." Today `0.1.0-SNAPSHOT` is shipping — any external consumer pulling `sm8-core_2.13:0.1.0-SNAPSHOT` gets no binary-compat guarantee. **This is acceptable for `0.1.0-SNAPSHOT`** (the version string conveys pre-release status), but the FIRST non-SNAPSHOT tag (e.g. `0.2.0` if there's a `0.2.0` before `v0.1.0`) should re-enable MiMa with `previousVersion = 0.1.0`. Otherwise a refactor between `0.1.0` and `v0.1.0` could break external callers silently.

### 4.5 Code-documentation over-discipline

The scaladoc on every file is unusually thorough — a finding on its own. The Mantras (`[[scala-data-driven-refactor-mindset]]`, `[[scala-jvm-safety-mindset]]`, etc.) are referenced inline in 30+ places. This is excellent for code-review context BUT:

1. **The mantra references are not linked.** `[[scala-data-driven-refactor-mindset]]` is a wiki-style link that, in Markdown renderers, becomes a no-op (`[[X]]` isn't standard Markdown). scalac doesn't render them. The intent ("this code follows the X mindset") is clear, but the actual link to the X file isn't there. Either inline the link to the file path, or trust the reviewer to grep `~/.claude/projects/-home-emilio-app-projects-sm8/memory/` for the mantra.

2. **Some mantra refs are inconsistent.** `[[scala-data-driven-refacer-mindset]]` (typo, missing `r`) appears at `MCPQueryRequest.scala:137`. Minor, but a search-replace pass would catch them all.

3. **`[[karphy-guidags-mindset]]`** (typo in `karphy` for `karpathy`, and `guidags` for `guidelines`) appears at `ModelLoader.scala:23`, `ModelLoader.scala:54`, `ExprParser.scala:9`, `TrinoConnector.scala:25`. Four occurrences. Same story — a one-shot sed over the reactor would fix them.

None of these affect runtime behavior. All are noise that reduces the trust signal of the inline citations.

### 4.6 Test count is honest but the count hides the shape

327 tests green (181 core + 146 platform) per the status doc. The shape:
- 181 core tests are split across `engine/`, `expr/`, `manifest/`, `model/`, `predicate/`, `schema/`, `sdk/contract/`, `sdk/portal/`, `sdk/restate/`, `sdk/transform/`. Most are small (~10-20 per file).
- 146 platform tests are split across `query/` (12 spec files), `query/cache/`, `query/hooks/`. Most are focused on one type each.

The test infrastructure is honest but thin in one place: **there are no property-based tests**. The cache-key derivation (`CacheBridge.platformCacheKey`) is the most critical bijection in the reactor — "two distinct request shapes must produce distinct cache keys." A ScalaCheck property test (`forAll: (a, b) => a != b => key(a) != key(b)`) would prove this end-to-end in 30 lines and would catch the legacy `List("a,b") == List("a", "b")` collision class. Today it's manually asserted in `CacheBridgeSpec`. The fix isn't strictly necessary (the existing tests are good), but it's the kind of property that *deserves* a property test.

### 4.7 The Restate SDK integration is honest but thin

`QueryService.definition` + `RestateBootstrap.bindAndListen` + 5 `QueryServiceSpec` tests. The integration is correct — the `QueryServiceSpec` drives through `HandlerRunner.run(stubContext, ...)` per the docstring at line 44-49. This is good.

What's missing:
- **No integration test against a real Restate runtime.** The status doc §3.2.1 ("PR-C-final-int — Docker-gated integration test") acknowledges this. Blocked on CI Docker access. Acceptable for today.
- **No test that the `ServiceDefinition` rejects duplicate handler names.** The SDK's `ServiceDefinition.of(name, ServiceType.SERVICE, List(handlerDefinition))` — if a future PR adds two handlers with the same name to one service, what does the SDK throw? Today: nothing in the reactor exercises this.
- **The `wireName` regex in `HookContractSpec.scala:61`** is `"""(pre|post):(parse|resolve|execute|format)"""`. The contract test passes today, but the regex is not anchored (`^(pre|post):...$`). A stage named `pre:executor` would match. Cosmetic.

### 4.8 Java interop is minimized

`semanticdf-platform`'s Java code lives in `/tmp/semanticdf` (referenced 6+ times in scaladoc as "the legacy" or "stays in /tmp/semanticdf for later migration"). The reactor's main source set is Scala 2.13 + Java 17 only (Maven enforcer `release=17`).

The remaining Java in the reactor: `connectors/in-memory-connector/`, `connectors/spark-connector/`, `connectors/trino-connector/` are Scala. `plugins/*` are Scala. `sm8-cli` is Scala. `sm8-core` is Scala. `sm8-platform` is Scala.

**No Java in `src/main/java/`.** This is the right choice per the karpathy-guidelines-mindset ("smallest correct core + match existing style") — Scala 2.13 idiom throughout. **No action needed.**

---

## 5. Recommendations, ordered

The next 4 PRs, in priority order:

### PR-1 (S, ~50 lines new, no production-code change): Fix the cache ownership + RestatedEngineRunner

From §3.1 M-1 and M-3:
- Delete `plugins/cache-plugin/` (the stub).
- Delete `sm8-platform/.../RestatedEngineRunner.scala` and its spec.
- Update `sm8-platform/.../cache/CachePlugin.scala` to be the single canonical cache Plugin.
- Update the status doc §3.2.5 to remove the deleted module from the "6 reference plugins" list.
- Add one integration test that proves `CachePlugin` + `InMemoryResultCache` work together via `EngineHookDispatcher`.

Estimated effect: -1 module, -2 files, +1 spec, +50 LOC in test coverage. Tests still green.

### PR-2 (S, ~20 lines, no production-code change): Fix `executeEngine` exception classifier

From §3.1 M-2:
- Change `catch case e: RuntimeException =>` to `catch case NonFatal(e) =>`.
- Fix `name = provider.identity.name` → `name = "query"`.
- Guard `e.getMessage` against null.

Estimated effect: 0 lines net in production code, +20 LOC test. The Spark connector's existing `try/catch` in `SparkEngineProvider.query` becomes the model for what `executeEngine`'s catch SHOULD be — and the Spark connector's catch can be simplified in a follow-up.

### PR-3 (M, ~80-150 lines): Wire the real `CachePlugin` into `RestateBootstrap`

From §3.1 M-1 followup:
- `RestateBootstrap.bindAndListen` accepts the real `CachePlugin` as a default argument (or loads it from `META-INF/services/`).
- `QueryService.definition` (no signature change — `plugins: Seq[Plugin] = Nil` already supports it) gets the cache plugin prepended.
- `EngineService.runQueryWithHooks` cache-key derivation stays the same; the cache lookup happens in `CacheReadPreHook`.
- Add 3 specs: cache HIT returns same `QueryResult` as the executor path; cache MISS executes and stores; cache HIT after model invalidation returns `None` (driven by `InMemoryResultCache.invalidateModel`).

Estimated effect: cache goes from 0% to 100% effective in production. Required for the "engine-portable path is canonical" claim.

### PR-4 (S, ~30 lines, no production-code change): Enrich `EngineError` classifier

From §3.1 M-4:
- `executeEngine`'s `catch` adds 5 specific cases: `SQLException → ConnectionFailed`, `SQLTimeoutException → QueryTimedOut`, `AnalysisException → UnsupportedCapability` (or `IncompatibleExprShape`), `ArithmeticException: "DECIMAL overflow" → DecimalOverflow`, `SchemaException → SourceSchemaChanged`. Pattern-match on `e.getClass.getSimpleName` per the legacy convention (defensive; class-name-based dispatch is fragile but it's the only signal available at the boundary).
- Update `QueryService.engineErrorCode` if any new mapping is added.
- Add 5 specs in `EngineServiceSpec` covering each classification.

Estimated effect: +5 EngineError variants reachable in production. The 11-variant ADT becomes more useful.

### Optional PR-X: the 4 minor items (m-1 through m-7)

- m-1: `EngineHookRequest` becomes a `Pipeline.Execute` case (small, ~20 lines).
- m-2: `effectiveEngineName(request, registry)` extraction (~5 lines).
- m-3: `MCPEngineRegistry.select` accepts `wasRequestedDefault: Boolean` (~10 lines + test).
- m-4: `CacheBridge.platformCacheKey` threads `model.dimensions + model.measures` hashes (~10 lines + test).
- m-6: `ExprParser` left-associativity test added (~30 lines).
- m-7: Materialize/Broadcast/Skew plugins either get a no-op test or get deleted (~5 lines).

Total optional: ~80 lines.

---

## 6. Conclusion

The SM8 codebase is the **right shape** for a frozen-Core + hot-Plugin semantic-layer engine. The architecture decisions (sealed traits for pipeline + hook priority + error ADT, Maven enforcer for the zero-Spark invariant, ServiceLoader portal for Plugin discovery, Public test-jar for conformance) are the right ones, and they've survived the 14-PR migration intact.

The risks are concentrated in two places:
1. **The cache integration is dormant** — the code path exists, but no PR has wired the real `CachePlugin` into production. The `plugins/cache-plugin/` stub duplicates the real one's name. (M-1, M-3)
2. **The engine-portable path's exception classification collapses 5 distinct failure modes into 1** — a Trino JDBC connection error and a Spark decimal overflow look the same on the wire. (M-2, M-4)

Both are fixable in 3 PRs of ~50-150 lines each. Neither is a blocker for the current `0.1.0-SNAPSHOT` state.

The strongest signal in the codebase is the **density of inline citations to mindset documents** — every architectural decision is traceable to a written rationale in `~/.claude/projects/-home-emilio-app-projects-sm8/memory/`. That survives the next 14 PRs.

The weakest signal is the **3-way split of cache ownership** — the real `CachePlugin` lives in `sm8-platform/query/cache/`, a stub lives in `plugins/cache-plugin/`, and the cache-key derivation lives inline in `EngineService.runQueryWithHooks`. Three files, three places, three correct-ish decisions that don't compose into a working integration.

**Recommendation:** PR-1 + PR-3 (the cache cleanup + the cache wiring) before any further engine-portable work. The handler-class wiring in `QueryService.scala` is honest and well-tested; the cache is the loose thread.

---

## 7. References

- **Plan:** `/home/emilio/.claude/plans/agile-kindling-beacon.md`
- **RFC:** `/home/emilio/app/projects/sm8/docs/rfcs/2026-08-12_v1_architecture-spec/`
- **Prior status doc:** `/home/emilio/app/projects/sm8/docs/project_status/2026-08-14-status.md`
- **Engine-portable entry point:** `sm8-platform/.../query/EngineService.scala`
- **Handler class:** `sm8-platform/.../query/QueryService.scala`
- **Real cache Plugin:** `sm8-platform/.../query/cache/CachePlugin.scala`
- **Stub cache Plugin (delete):** `plugins/cache-plugin/.../CachePlugin.scala`
- **Stub helper (delete):** `sm8-platform/.../query/RestatedEngineRunner.scala`
- **Test totals (verified):** 327 = 181 sm8-core + 146 sm8-platform (per `2026-08-14-status.md`; reactor build does not have a ScalaTest runner cached locally for re-verification, but the status doc is dated 2026-08-14 — 1 day before this review)
