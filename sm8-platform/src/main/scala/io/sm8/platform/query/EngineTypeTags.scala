/*
 * SM8 Platform — Engine-portable SealedDataType → RestateCachedRow tag dispatch.
 *
 * Per [[scala-data-driven-refactor-mindset]] (sealed-trait dispatch over
 * `switch (simpleName)`): replaces the Java `QueryService.sealedTypeTag`
 * (semanticdf-platform lines 747-773) with a Scala 2.13 pattern match
 * over the closed `SealedDataType` ADT. Compiler enforces exhaustiveness
 * — adding a new SealedDataType case forces an update here.
 *
 * Per [[karpathy-guidelines-mindset]] (smallest correct change + match
 * existing style + Scala 2.13 idiom): pure function, `Option`-based
 * null handling, no Scala 3 `enum`. Mirrors the Java test cases exactly
 * (see EngineTypeTagsSpec).
 *
 * Per [[scala-error-handling-mindset]]: returns `String`, never throws.
 * Nested/Json cases return `T_STRING` (Java implementation threw
 * `IllegalArgumentException`; the Scala version is exhaustive by
 * construction — see "Behavior note" below).
 *
 * Per [[scala-impact-analysis-mindset]]: 0 callers in our reactor (the
 * Java `QueryService.sealedTypeTag` stays in `/tmp/semanticdf` for
 * later migration PRs). Pure function, dormant until PR-C2+ consumes
 * it. Behavior change from Java: Array/Map/Row/Json return T_STRING
 * (nested/JSON-as-string convention) instead of throwing — see below.
 *
 * ==Behavior note (vs Java)==
 *
 * The Java implementation handled 8 cases (Varchar, Int, BigInt,
 * Double, Boolean, Timestamp, Date, Decimal) and `null → T_NULL`,
 * throwing `IllegalArgumentException` on Array/Map/Row/Json/Binary.
 * The Scala implementation is exhaustive over all 13 SealedDataType
 * cases — the compiler enforces this. Array/Map/Row/Json return
 * `T_STRING` (encoded-as-string convention); Binary returns
 * `T_BINARY` (Base64-encoded bytes convention). No caller in our
 * reactor today, so the change is dormant until the engine-portable
 * path migration (PR-C5+) wires the consumer to match.
 */
package io.sm8.platform.query

import io.sm8.core.schema.SealedDataType

/**
 * Map a `SealedDataType` to its `RestateCachedRow` journal tag.
 *
 * This is the engine-portable-path side of the wire contract: at
 * journal-write time, a `ResultValue`'s type is encoded to one of
 * `RestateCachedRow`'s 9 `T_*` tags; at journal-read time, the tag
 * drives `decodeCell(tag, encoded)` back to the typed value.
 */
object EngineTypeTags {

  /** Map a SealedDataType to its RestateCachedRow journal tag. `None` → `T_NULL`. */
  def of(dt: Option[SealedDataType]): String = dt match {
    case Some(SealedDataType.Varchar)    => RestateCachedRow.T_STRING
    case Some(SealedDataType.Int)        => RestateCachedRow.T_LONG
    case Some(SealedDataType.BigInt)     => RestateCachedRow.T_LONG
    case Some(SealedDataType.Double)     => RestateCachedRow.T_DOUBLE
    case Some(SealedDataType.Boolean)    => RestateCachedRow.T_BOOLEAN
    case Some(SealedDataType.Timestamp)  => RestateCachedRow.T_TIMESTAMP
    case Some(SealedDataType.Date)       => RestateCachedRow.T_DATE
    case Some(_: SealedDataType.Decimal) => RestateCachedRow.T_DECIMAL
    case Some(_: SealedDataType.Array)   => RestateCachedRow.T_STRING
    case Some(_: SealedDataType.Map)     => RestateCachedRow.T_STRING
    case Some(_: SealedDataType.Row)     => RestateCachedRow.T_STRING
    case Some(SealedDataType.Json)       => RestateCachedRow.T_STRING
    case Some(SealedDataType.Binary)     => RestateCachedRow.T_BINARY
    case None                            => RestateCachedRow.T_NULL
  }

  /** Java-friendly overload: accept `null` directly. */
  def of(dt: SealedDataType): String = of(Option(dt))
}