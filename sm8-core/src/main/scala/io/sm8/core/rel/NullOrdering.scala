/*
 * SM8 Core — NullOrdering (engine-portable null-ordering ADT).
 *
 * Per the v0.1.0 IR extension plan (ADR-007 + RFC §3): sort key
 * null ordering is part of the relational plan tree (`RelOp.Sort`).
 * Every SQL engine has the notion of "nulls first" vs "nulls last".
 *
 * is fixed at compile time (First / Last) → sealed ADT (NOT an Int
 * flag with named constants — silent defaulting on typo'd values).
 *
 * Per RFC §3: engine-portable; engine-specific compile lives in
 * the adapter (Spark's `.asc_nulls_first()` / `.asc_nulls_last()`,
 * Trino's `NULLS FIRST` / `NULLS LAST` keywords).
 *
 * `/tmp/semanticdf/semanticdf-core/src/main/scala/io/semanticdf/core/rel/NullOrdering.scala`
 * with the same 2 cases.
 *
 * contract: `grep -r 'org.apache.spark' sm8-core/src/main/scala/io/sm8/core/rel/NullOrdering.scala`.
 */
package io.sm8.core.rel

sealed trait NullOrdering extends Product with Serializable

object NullOrdering {

 /** Nulls sort BEFORE non-nulls. Maps to Spark `.asc_nulls_first()` /
 * `.desc_nulls_first()`, Trino `NULLS FIRST`, DuckDB `NULLS FIRST`. */
 case object First extends NullOrdering

 /** Nulls sort AFTER non-nulls. Maps to Spark `.asc_nulls_last()` /
 * `.desc_nulls_last()`, Trino `NULLS LAST`, DuckDB `NULLS LAST`.
 * This is the default in most SQL engines (ANSI SQL default). */
 case object Last extends NullOrdering
}
