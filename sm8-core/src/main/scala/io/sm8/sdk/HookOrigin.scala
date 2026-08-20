/*
 * SM8 SDK — HookOrigin (RFC §8 conformance: priority-range reservation).
 *
 * 
 *
 * Per RFC §8 priority ranges:
 *
 *   | Range     | Owner                       | Origin tag  |
 *   |-----------|-----------------------------|-------------|
 *   | 0-99      | Core / built-in             | Core        |
 *   | 100-899   | First-party / official      | FirstParty  |
 *   | 900+      | Community / user           | Community   |
 *
 * sm8.sdk) so the SDK trait `HookManager` can import it.
 * Plugin authors reach it via the SDK import path
 * (`import io.sm8.sdk.HookOrigin`).
 *
 *  The impl throws
 * `IllegalArgumentException` AT THE BOUNDARY where the SDK contract
 * declares it (the SDK doc already declared the throw). The
 * validator's typed-Either shape lets callers test the boundary
 * without exception-catch.
 *
 * Per sm8-implementation-rules "type-class + data-driven
 * programming": HookOrigin is the discriminating tagged union.
 * The `validate` method is pure-function dispatch on the sealed
 * cases — no implicit evidence lookup, no Map tables, no reflection.
 */
package io.sm8.sdk

/**
 * Origin of a Hook (per RFC §8 reserved ranges). Three values,
 * sealed. Plugin authors declare which origin they belong to.
 */
sealed trait HookOrigin extends Product with Serializable

/** Companion for [[HookOrigin]] — sealed instances + the validator. */
object HookOrigin {

  /** Core / built-in hooks (priority 0-99). Reserved for `sm8-core`. */
  final case object Core extends HookOrigin

  /** First-party / official hooks (priority 100-899).
    * Reserved for `io.sm8.plugins.*` reference plugins. */
  final case object FirstParty extends HookOrigin

  /** Community / user hooks (priority 900+).
    * Reserved for third-party Plugin JARs. */
  final case object Community extends HookOrigin

  /** Inclusive lower bound of the origin's priority range. */
  def lowerBound(origin: HookOrigin): Int = origin match {
    case Core       => 0
    case FirstParty => 100
    case Community  => 900
  }

  /** Inclusive upper bound of the origin's priority range.
    * Per RFC §8: FirstParty is reserved "100-899" (NOT inclusive of
    * 900 — the Community band). Core is 0-99 (NOT inclusive of
    * 100). Community has no upper bound. */
  def upperBound(origin: HookOrigin): Int = origin match {
    case Core       => 99
    case FirstParty => 899
    case Community  => Int.MaxValue
  }

  /** Inclusive range check. Pure function. */
  def contains(origin: HookOrigin, priority: Int): Boolean =
    priority >= lowerBound(origin) && priority <= upperBound(origin)

  /** Validate that `priority` is non-negative AND inside the
    * declared origin's reserved range. Returns typed
    * `Either[String, Unit]` so callers can assert on the boundary
    * without try/catch.
    *
    * @param origin    the declared origin (typically the plugin
    *                  author's chosen range)
    * @param priority  the priority value passed to
    *                  `HookManager.registerPreHook` /
    *                  `registerPostHook`
    * @return          `Right(())` if valid; `Left(reason)` with a
    *                  human-readable explanation otherwise */
  def validate(origin: HookOrigin, priority: Int): Either[String, Unit] =
    if (priority < 0)
      Left(s"priority must be non-negative, got $priority")
    else if (!contains(origin, priority))
      Left(
        s"priority $priority is outside the reserved range " +
          s"[${lowerBound(origin)}, ${upperBound(origin)}] for origin $origin"
      )
    else Right(())
}
