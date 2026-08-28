/*
 * SM8 in-memory Engine Provider — ServiceLoader descriptor
 * (PR-O4g parity, ADR-008-O).
 *
 * Mirrors the spark connector's `SparkEngineProviderDescriptor` pattern
 * (PR-O4g, ADR-008-O) and the trino descriptor added in PR-195. The
 * in-memory provider had been registered directly under SPI,
 * bypassing the descriptor indirection; this commit closes that gap.
 *
 * The in-memory provider is unique in that it's "always realized" —
 * there's no URL grammar, no Spark session, no remote client. The
 * descriptor's job here is therefore minimal:
 *   1. Declare the engine identity (`"in-memory"`) so ServiceLoader
 *      discoverers see a uniform shape.
 *   2. Forward `realizeTyped` directly to the underlying provider
 *      (already realized on the bare classpath; per RFC adapters.md
 *      Rule 4, in-memory has no URL grammar).
 *
 * PR-195 (Round 2 Review B/C): closes the descriptor-pattern gap.
 *
 * @see [[io.sm8.connectors.spark.SparkEngineProviderDescriptor]] for the
 *   shape this mirrors (ProviderDescriptor carries identity + realize();
 *   the heavy provider stays in the connector-specific class).
 */
package io.sm8.connectors.inmemory

import io.sm8.core.engine.{EngineError, EngineIdentity, EngineProvider, EngineUrl, TypedRealizationProvider}

class InMemoryEngineProviderDescriptor extends TypedRealizationProvider {

  override val identity: EngineIdentity =
    EngineIdentity(name = "in-memory", nativeVersion = "embedded", engineAdapterVersion = "0.1.0")

  /** In-memory has no URL grammar and no remote to set up, so the
    * descriptor is `available = true` immediately after the no-arg
    * constructor (matches the spark descriptor pattern where
    * `available = false` because it has no SparkSession; here
    * `available = true` because we have everything we need). */
  override val available: Boolean = true

  /** PR-O4g parity: the underlying provider is always realized, so
    * `realize` returns `Some(provider)` for any URL. (The legacy
    * `InMemoryEngineProvider.realize` returned `None` because it
    * considered the provider already realized; the descriptor
    * indirection exposes it explicitly here.) */
  override def realize(url: String): Option[EngineProvider] =
    Some(new InMemoryEngineProvider())

  /** PR-15 (ADR-008-Q §C2): typed realization. In-memory accepts any
    * `EngineUrl.InMemory` (returns `this` — already realized). Defense
    * in depth: rejects `Spark` / `Trino` URLs with typed
    * `ConnectionFailed`. */
  override def realizeTyped(parsedUrl: EngineUrl): Either[EngineError, EngineProvider] =
    parsedUrl match {
      case _: EngineUrl.InMemory => Right(new InMemoryEngineProvider())
      case other => Left(EngineError.ConnectionFailed(
        engine  = "in-memory",
        reason  = "unexpected EngineUrl case for in-memory descriptor",
        message = s"sm8: in-memory descriptor received non-InMemory EngineUrl: ${other.getClass.getSimpleName}"
      ))
    }

  override def query(
    model: io.sm8.core.model.Model,
    request: io.sm8.core.engine.QueryRequest,
    ctx: io.sm8.core.engine.EngineContext
  ): Either[EngineError, io.sm8.core.engine.PortableQueryResult] =
    new InMemoryEngineProvider().query(model, request, ctx)

  override def explain(
    model: io.sm8.core.model.Model,
    request: io.sm8.core.engine.QueryRequest,
    ctx: io.sm8.core.engine.EngineContext
  ): Either[EngineError, String] =
    Right(s"in-memory.explain(${model.name})")
}

object InMemoryEngineProviderDescriptor {
  def identity: EngineIdentity =
    EngineIdentity(name = "in-memory", nativeVersion = "embedded", engineAdapterVersion = "0.1.0")
}