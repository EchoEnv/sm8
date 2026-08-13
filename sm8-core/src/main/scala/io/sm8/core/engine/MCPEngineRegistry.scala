package io.sm8.core.engine

/** MCPEngine-portable MCP engine-registry \u2014 Phase 2 contract.
  * Mirrors the design doc \u00a76.4 "MCPEngineRegistry".
  *
  * ==Why a registry (not direct provider use)==
  *
  * Per the design: "the registry's `select` filters availability".
  * Multiple providers (Spark, Trino, Databricks, ...) can be
  * registered; the MCP `query` tool routes to the chosen one
  * (via `request.engine` or the default). The registry centralizes
  * the lookup + availability check.
  *
  * ==Why `Map[String, MCPEngineProvider]`==
  *
  * Wire-stable engine names as map keys. The MCP `query` request
  * carries `engine: String` (the "13th property" per the design).
  * The registry maps that string to a provider.
  *
  * ==Why `require(engines(default).available)` at construction==
  *
  * Per the design (and the DE design §4.1): misconfigured
  * boots are loud at startup, not silent at query time. If the
  * default engine is unavailable at construction, the registry
  * throws \u2014 the MCP server fails to start with a typed error
  * instead of returning "engine unavailable" for every query.
  *
  * ==Why `select` returns `Either[EngineError, MCPEngineProvider]`==
  *
  * Per the design: "EngineUnavailable (wasDefault=true / false)"
  * \u2014 a typed error lets the MCP envelope surface the
  * unavailable-engine case distinctly. `Either` is the
  * engine-portable error shape (no exceptions for control flow). */
final class MCPEngineRegistry (
    private val engines: Map[String, MCPEngineProvider],
    val default:       String,
) extends Serializable {

  require(
    engines.contains(default),
    s"MCPEngineRegistry default '$default' is not in the engines map (${engines.keys.mkString(", ")})",
  )
  require(
    engines(default).available,
    s"MCPEngineRegistry default '$default' is registered but NOT available at startup (per design §4.1: misconfigured boots must fail loud)",
  )

  /** Select a provider by name. Returns:
    *   - `Right(provider)` if the name matches a registered AND
    *     available provider
    *   - `Left(EngineUnavailable(name, available, wasDefault))`
    *     if the name is unknown OR the provider is currently
    *     unavailable */
  def select(name: String): Either[EngineError, MCPEngineProvider] = {
    val available = availableProviders
    engines.get(name) match {
      case Some(p) if p.available => Right(p)
      case _                      => Left(EngineError.EngineUnavailable(
        engine     = name,
        available  = available,
        wasDefault = (name == default), message    = "engine unavailable: " + name,
      ))
    }
  }

  /** List the names of currently-AVAILABLE providers (per
    * design: "the registry's `select` filters availability"). */
  def availableProviders: List[String] =
    engines.filter { case (_, p) => p.available }.keys.toList.sorted

  /** Java-friendly alias for the `default` field.
    *
    * Scala generates `default()` as a Java method, but `default` is
    * a Java reserved keyword (used in `switch` labels and interface
    * default methods). Java callers cannot reference it directly.
    * This alias gives them a callable name.
    *
    * Scala callers should keep using `default` (the field) — it's
    * idiomatic. */
  def defaultEngine: String = default
}

object MCPEngineRegistry {

  /** Smart constructor with available-only filtering. Per design:
    * "the registry's `select` filters availability".
    *
    * @param engines  the provider map (name \u2192 provider)
    * @param default  the default provider name (used when
    *                 `request.engine` is absent)
    * @return          a registry; throws if `default` is
    *                 unregistered or unavailable at construction */
  def apply(
      engines: Map[String, MCPEngineProvider],
      default: String,
  ): MCPEngineRegistry = new MCPEngineRegistry(engines, default)
}