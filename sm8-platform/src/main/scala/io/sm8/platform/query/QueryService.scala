/*
 * SM8 Platform — Restate `QueryService` handler class.
 *
 * Per [[karpathy-guidelines-mindset]] (smallest correct core +
 * match existing style): the handler is hand-built — Restate SDK
 * v2.x dropped the v1.x annotation-driven discovery (`@Handler`
 * is `RetentionPolicy.SOURCE`, `@Service` is `@Deprecated(forRemoval=true)`),
 * and the `sdk-api-gen` Java annotation processor cannot run on
 * Scala sources. Per [[scala-impact-analysis-mindset]] "name every
 * caller": the only caller is `RestateHttpServer.listen(endpoint,
 * port)` in `RestateBootstrap` (production) + the unit-test
 * `QueryServiceSpec` (which invokes the `HandlerRunner` directly
 * via `HandlerRunner.run(stubContext, ...)` — no Docker needed for
 * unit tests).
 *
 * ==Why a separate object==
 *
 * Three concerns the `QueryService` separates per
 * [[scala-data-driven-refactor-mindset]]:
 *   1. The ServiceDefinition (wire-stable shape — registered on
 *      Restate's HTTP server, exposed to clients as `/QueryService/runQuery`).
 *   2. The handler body (the engine-portable entry point — calls
 *      `EngineService.runQuery` with a cache + registry).
 *   3. The serde binding (per-handler JSON `Serde[QueryRequest]`
 *      + `Serde[QueryResult]` via jackson-module-scala).
 *
 * ==Handler-thread context==
 *
 * Per [[scala-jvm-safety-mindset]] "null is a liar": the SDK does
 * NOT expose a `RestateContext.current()` static (verified by JAR
 * inspection of sdk-common + sdk-core at v2.1.1). The handler body
 * operates on the `HandlerContext` argument the SDK passes in —
 * we do NOT rely on `HandlerRunner.HANDLER_CONTEXT_THREAD_LOCAL`
 * (which can break under `Options.withExecutor`). The handler
 * body is synchronous inside the `HandlerRunner.of(fn, ...)` call;
 * the SDK wraps it in async + journal semantics.
 *
 * ==Serializable hygiene==
 *
 * Per [[scala-spark-batch-bugs-mindset]] "closures": Restate's
 * journal rehydration requires the handler's captured `Model`,
 * `MCPEngineRegistry`, and `ResultCache` to be `Serializable`.
 * All are already verified Serializable by
 * `EngineServiceSpec.scala:547` ("runQuery: serializable-safe").
 */
package io.sm8.platform.query

import dev.restate.sdk.HandlerRunner
import dev.restate.sdk.endpoint.definition.{
  HandlerDefinition,
  HandlerType,
  HandlerContext => RestateHandlerContext,
  ServiceDefinition,
  ServiceType
}
import dev.restate.serde.jackson.JacksonSerdeFactory

import io.sm8.core.engine.MCPEngineRegistry
import io.sm8.core.model.Model

/**
 * Hand-built Restate v2.x service definition for the engine-portable
 * `runQuery` entry point.
 *
 * Per [[karpathy-guidelines-mindset]]: a singleton `object` (not a
 * class) since the ServiceDefinition is stateless; the
 * per-handler `Model` + `MCPEngineRegistry` + `ResultCache` are
 * captured by the handler-body lambda (which must be Serializable
 * for journal rehydration).
 */
object QueryService {

  /**
   * Build the hand-rolled `ServiceDefinition` for `QueryService`.
   *
   * Per [[karpathy-guidelines-mindset]] "smallest correct core":
   * no builder pattern, no factory method per use-site — the
   * serde factory is constructed once per `definition()` call,
   * the request/response `Serde`s are per-handler, and the whole
   * `ServiceDefinition` is returned so the caller can register it
   * on a Restate HTTP server.
   *
   * Per [[scala-data-driven-refactor-mindset]] "sealed-trait dispatch":
   * the JDK `ServiceType.SERVICE` enum pick + `HandlerType.SHARED`
   * dispatch deterministically picks the wire shape (parallels
   * the legacy v1.x `@Service` annotation).
   *
   * @param model    the engine-portable model (used for engine.compile +
   *                 cache-key derivation). Must be Serializable.
   * @param registry the engine-portable registry. Must be Serializable.
   * @param cache    the result cache. Must be Serializable.
   * @return         a `ServiceDefinition` registered as
   *                 "QueryService" with one SHARED handler
   *                 named "runQuery".
   */
  def definition(
      model: Model,
      registry: MCPEngineRegistry,
      cache: ResultCache
  ): ServiceDefinition = {
    // Per review pass #2 (DE-reviewer #3): the SDK's
    // `JacksonSerdeFactory.DEFAULT` mapper doesn't reliably auto-load
    // `jackson-module-scala` via SPI. Construct the ObjectMapper
    // explicitly with `DefaultScalaModule` so Scala case classes
    // (QueryRequest, QueryResult, RestateCachedRow) serialize correctly.
    val scalaMapper: com.fasterxml.jackson.databind.ObjectMapper =
      new com.fasterxml.jackson.databind.ObjectMapper()
        .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    val jacksonSerdeFactory = new JacksonSerdeFactory(scalaMapper)
    // Per-handler serde (NOT per-Endpoint — the legacy v1.x had
    // one global serde per service; v2.x is per-handler). Each
    // `Serde[T]` encodes/decodes one type via Jackson.
    val requestSerde = jacksonSerdeFactory.create(classOf[QueryRequest])
    val resultSerde  = jacksonSerdeFactory.create(classOf[QueryResult])

    // Per [[karpathy-guidelines-mindset]] "match existing style":
    // the handler body is a Scala function `(CTX, REQ) => RES`.
    // The SDK wraps the synchronous function in async + journal
    // semantics at handler invocation time.
    val handlerRunner: HandlerRunner[QueryRequest, QueryResult] =
      HandlerRunner.of(
        (ctx: dev.restate.sdk.Context, req: QueryRequest) =>
          runQuery(req, model, registry, cache),
        jacksonSerdeFactory,
        HandlerRunner.Options.DEFAULT
      )

    // Hand-build the HandlerDefinition (no annotation scanner —
    // `@Handler` is `RetentionPolicy.SOURCE`).
    val handlerDefinition: HandlerDefinition[QueryRequest, QueryResult] =
      HandlerDefinition.of(
        "runQuery",
        HandlerType.SHARED,
        requestSerde,
        resultSerde,
        handlerRunner
      )

    // Hand-build the ServiceDefinition (no @Service annotation
    // discovery — the annotation is `@Deprecated(forRemoval=true)`
    // in v2.x).
    ServiceDefinition.of(
      "QueryService",
      ServiceType.SERVICE,
      java.util.List.of(handlerDefinition)
    )
  }

  /**
   * The handler body. Synchronous. The Restate SDK wraps this
   * function inside its async + journal pipeline at handler
   * invocation time (via `HandlerRunner.of(fn, serdeFactory,
   * options)`).
   *
   * Per [[scala-error-handling-mindset]]: the legacy Java code
   * threw `RuntimeException` on engine errors; we use the SDK's
   * `TerminalException` (mapped from `EngineError` via
   * `engineErrorCode`) so the journal + retry path preserves the
   * typed ErrorCode. Future PR may convert to a typed-Error
   * return once the @Handler signature supports it (currently the
   * SDK requires `RES` to be JSON-serializable).
   *
   * Per [[scala-jvm-safety-mindset]] "null is a liar": we don't
   * capture the `HandlerContext` — the handler body is a pure
   * function of `(request, model, registry, cache)`. The cache
   * HIT path uses the global `InMemoryResultCache`; the engine
   * MISS path delegates to `EngineService.runQuery`.
   *
   * ==Timeout / retry policy (explicit non-decision)==
   *
   * Per review pass #2 (DE-reviewer #6 + #10): the handler body
   * does NOT use `ctx.timer(...)` or wrap the engine call in any
   * Restate journaled timeout. This is a deliberate non-decision:
   * the synchronous engine execution IS the journaled work.
   * Restate's retry policy applies to `TerminalException` throws
   * (so transient engine failures retry); no inner timeout is
   * added because the engine-portable path already has its own
   * execution-time deadline (the engine adapter's `EngineContext`
   * carries the deadline). Adding a second deadline layer would
   * create a race between the engine's timeout and the journal's
   * retry, which is exactly what the SDK's journaled-retry
   * design is meant to AVOID.
   *
   * Per [[scala-error-handling-mindset]] "errors are data":
   * the engine's `EngineError.QueryTimedOut` already maps to HTTP
   * 504 (see `engineErrorCode` below). The wire contract is
   * honest: the journal re-runs the engine call on retry; the
   * engine decides when to give up.
   */
  private def runQuery(
      request: QueryRequest,
      model: Model,
      registry: MCPEngineRegistry,
      cache: ResultCache
  ): QueryResult = {
    EngineService.runQuery(request, model, registry, cache) match {
      case Right(qr)  => qr
      case Left(err) =>
        // Per review pass #2 (DE-reviewer #1): use Restate's
        // `TerminalException` to preserve the typed ErrorCode in
        // the journal + retry path. `RuntimeException(err.toString)`
        // would lose the 11-subtype EngineError structure.
        // Per [[scala-error-handling-mindset]] "errors are data":
        // a typed-error wire contract is the goal; the SDK v2.x
        // currently requires JSON-serializable `RES`, so we land
        // here as a TerminalException throw (logged + retryable
        // per Restate's default retry policy). Tech debt: add
        // `ErrorEnvelope` as the `RES` type when SDK supports it.
        val code = engineErrorCode(err)
        throw new dev.restate.sdk.common.TerminalException(code, err.toString)
    }
  }

  /**
   * Map the engine-portable `EngineError` to a Restate
   * `TerminalException` HTTP status code. Per RFC §9 fail-fast,
   * we use server-side errors (5xx) for non-recoverable engine
   * failures and client-side errors (4xx) only for inputs that
   * the engine itself rejected.
   *
   * Per [[scala-data-driven-refactor-mindset]] "sealed-trait dispatch":
   * the match is exhaustive over the 11 EngineError variants —
   * the compiler enforces this if a new variant is added.
   */
  private def engineErrorCode(err: io.sm8.core.engine.EngineError): Int = err match {
    case _: io.sm8.core.engine.EngineError.EngineUnavailable         => 503
    case _: io.sm8.core.engine.EngineError.QueryTimedOut            => 504
    case _: io.sm8.core.engine.EngineError.ConnectionFailed         => 502
    case _: io.sm8.core.engine.EngineError.ProviderInvocationFailed => 502
    case _: io.sm8.core.engine.EngineError.CancellationFailed       => 504
    case _: io.sm8.core.engine.EngineError.UnsupportedCapability    => 501
    case _: io.sm8.core.engine.EngineError.IncompatibleExprShape    => 422
    case _: io.sm8.core.engine.EngineError.DecimalOverflow           => 422
    case _: io.sm8.core.engine.EngineError.SourceSchemaChanged     => 409
    case _: io.sm8.core.engine.EngineError.AuditSinkUnavailable     => 503
    case _: io.sm8.core.engine.EngineError.FeatureDeferred          => 501
  }
}

