/*
 * SM8 Server — EngineLoader (PR-15, ADR-008-Q §PR-15).
 *
 * Centralized engine discovery + realization helper. Per ADR-008-Q
 * §C11: the deployment module (sm8-server) owns the ServiceLoader +
 * URL realization orchestration; sm8-platform stays pure transport
 * with zero `ServiceLoader` references.
 *
 * Per [[karpathy-app-design-mindset]] §3.1 (Protocols before
 * implementations): the `EngineLoader` is a single function
 * (`discoverAndRealize`) that wraps 3 inner steps:
 *   1. Discover `EngineProvider` + `EngineUrlParser` via ServiceLoader.
 *   2. Parse the raw URL into a typed `EngineUrl` (per-connector grammar).
 *   3. Realize each provider against the typed URL with TYPED errors.
 *
 * Per [[scala-error-handlingmindset]] §1 (errors are data) + ADR-008-Q
 * §C1: this helper returns `List[Either[EngineError, EngineProvider]]`
 * — typed per-provider errors, not a silent `List[EngineProvider]`
 * that hides the failure mode.
 *
 * Per [[scala-jvm-safety-mindset]] §3 (long-lived state): NO static
 * ServiceLoader cache. The loader is per-call (no leak risk across
 * hot-reload); the caller (sm8-server Main.wire) calls this once at
 * boot.
 */
package io.sm8.server

import io.sm8.core.engine.{
  EngineError, EngineProvider, EngineUrl, EngineUrlParser,
  TypedRealizationProvider
}

import scala.jdk.CollectionConverters._

object EngineLoader {

  /**
   * Discover + realize engine providers against the given raw URL.
   *
   * Returns a `List[Either[EngineError, EngineProvider]]`:
   *   - `Right(provider)` for each successfully-realized provider.
   *   - `Left(engineError)` for each provider that could NOT be
   *     realized (typed error per ADR-008-Q §C1).
   *
   * Per [[scala-error-handlingmindset]] §1: the caller (Main.wire)
   * inspects the typed errors and surfaces them as boot failures
   * (fail-loud per design §4.1: misconfigured boots are loud at
   * startup, not silent at query time).
   *
   * Algorithm:
   *   1. Discover ALL `EngineProvider` instances on the classpath
   *      (no engine-name filter — we try each).
   *   2. If `rawUrl` is `None`, return providers unchanged (default
   *      behavior: keep stubs, the wire() filters by `available`).
   *   3. If `rawUrl` is `Some(url)`:
   *      a. Look up the parser for `engineName` via SPI.
   *         If not found, ALL providers get a `Left(EngineUnavailable)`
   *         (the engine name resolves to no parser).
   *      b. For each provider, call `realizeTyped(parsedUrl)` if it
   *         implements `TypedRealizationProvider`; otherwise the
   *         provider stays as-is (legacy `realize(url): Option` path).
   *
   * @param classLoader the ServiceLoader classloader (testable)
   * @param engineName  the engine name (e.g. "spark", "trino") used
   *                    to look up the per-connector URL parser
   * @param rawUrl      the raw URL from `--connector-url` (if absent,
   *                    providers are kept unchanged)
   * @return            per-provider typed realization result
   */
  def discoverAndRealize(
      classLoader: ClassLoader,
      engineName:  String,
      rawUrl:      Option[String]
  ): List[Either[EngineError, EngineProvider]] = {
    val discoveredProviders: List[EngineProvider] =
      java.util.ServiceLoader
        .load(classOf[EngineProvider], classLoader)
        .iterator()
        .asScala
        .toList

    rawUrl match {
      case None =>
        // No URL → keep all providers as-is (legacy default).
        discoveredProviders.map(Right(_))

      case Some(url) =>
        // 1. Look up the URL parser for this engine.
        EngineUrlParser.lookup(engineName, classLoader) match {
          case Left(parseErr) =>
            // No parser for this engine — all providers fail with the
            // same typed error (the engine is unavailable).
            discoveredProviders.map(_ => Left(parseErr))

          case Right(parser) =>
            // 2. Parse the raw URL into the typed EngineUrl.
            parser.parse(url) match {
              case Left(parseErr) =>
                discoveredProviders.map(_ => Left(parseErr))

              case Right(parsedUrl) =>
                // 3. Realize each provider against the typed URL.
                discoveredProviders.map { p =>
                  realizeOne(p, parsedUrl)
                }
            }
        }
    }
  }

  /**
   * Realize one provider against a typed `EngineUrl`. Per
   * ADR-008-Q §C7: single realization path (typed when available).
   *
   * Per [[scala-bug-hunting-mindset]] §3 (every match must be
   * exhaustive): the `instanceOf[TypedRealizationProvider]` check
   * is the binary-compat bridge between the new typed path and the
   * legacy `realize(url): Option` path.
   */
  private def realizeOne(
      provider:  EngineProvider,
      parsedUrl: EngineUrl
  ): Either[EngineError, EngineProvider] =
    provider match {
      case typed: TypedRealizationProvider =>
        typed.realizeTyped(parsedUrl)
      case other =>
        // Legacy provider (no TypedRealizationProvider subtrait):
        // best-effort realization via `realize(url)`. Returns Right if
        // Some(p), Left(ConnectionFailed) if None (silent-legacy
        // becomes typed-error here).
        other.realize(parsedUrl.raw) match {
          case Some(p)  => Right(p)
          case None     => Left(EngineError.ConnectionFailed(
            engine = parsedUrl.engineName,
            reason = "realize(url) returned None for legacy provider",
            message =
              s"sm8: ${parsedUrl.engineName} legacy engine provider " +
              s"rejected URL '${parsedUrl.raw}'"
          ))
        }
    }
}
