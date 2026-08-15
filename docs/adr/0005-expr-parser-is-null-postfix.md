# ADR-005: `IS [NOT] NULL` postfix in `ExprParser`

**Status:** Accepted. **Date:** 2026-08-15. **Author:** SM8 agent.

## Context and Problem Statement

`ExprParser` (per ADR-004) is the typed-AST factory for the
engine-portable SQL subset. The previous PRs (PRs #46, #47, #50)
added support for:

- Literals: `IntValue`, `BoolValue`, `StringValue`, `DoubleValue`,
  `IntValue` (negative), etc.
- Arithmetic: `+`, `-`, `*`, `/`, `%` (left-associative, with
  precedence).
- Comparison: `=`, `!=`, `<`, `<=`, `>`, `>=` (non-associative).
- Boolean: `AND`, `OR`, `NOT` (case-insensitive; per PR #51).
- Parens for grouping.
- `name(args, ...)` → `Expr.FunctionCall(name, args)` (PR #47).
- `expr AS TYPE` → `Expr.Cast(expr, targetType)` (PR #50).

The case classes `Expr.IsNull(expr)` and `Expr.IsNotNull(expr)`
already exist in the `Expr` ADT (per the `Expr.scala` enum), but
**the parser did not produce them**. This gap meant:

1. A user filter like `age IS NULL` could not be expressed in a
   YAML manifest's `filters:` list (the loader's typed-filter path).
2. The `Expr` ADT was incomplete at the parser level — the type
   existed but the parser couldn't produce it.

This ADR documents the **decision to add `IS [NOT] NULL` postfix
support at the primary level** of `parseAsSuffix`, alongside the
existing `AS TYPE` postfix.

## Decision

The `parseAsSuffix` method now also handles `IS [NOT] NULL`:

- `expr IS NULL` → `Expr.IsNull(expr)`
- `expr IS NOT NULL` → `Expr.IsNotNull(expr)`
- `IS` keyword is **case-insensitive** (per PR #51's case-insensitive
  pattern: `startsWithWordCaseInsensitive("is")`).
- `NOT` is optional; `NULL` is required.
- After `IS` (or `IS NOT`), the next token MUST be `NULL`; otherwise
  a typed `UnexpectedToken` error surfaces (per
  `scala-error-handling-mindset`: errors are data).

The detection order is `AS` first, then `IS`. The postfix is
matched at the primary level only — real-world usage is
`column IS NULL` or `(expr) IS NULL`.

The case-insensitive consumption pattern is the same as
`startsWithWordCaseInsensitive` from PR #50 + the
`consumeWordCaseInsensitive` from PR #51: each character is
advanced individually (e.g. `advance(); advance()` for 2-char
`IS`), with skipWhitespace between.

## Consequences

**Positive:**
- The `Expr.IsNull` + `Expr.IsNotNull` case classes are now reachable
  from the parser, closing the gap in the typed-Expr family.
- A user filter like `age IS NULL AND active = true` is now parseable
  end-to-end (verified by `EndToEndPipelineSpec` tests).
- The pattern mirrors the existing `AS TYPE` postfix (same
  case-insensitive consumption + typed-error-on-mismatch).
- Connector layer (`spark-connector`'s `PortableExprCompiler`,
  per PR #41) compiles `Expr.IsNull` → Spark `df.filter(col.isNull)`
  at the driver side. No executor-side closure.

**Negative:**
- The parser grammar is now larger (1 more postfix case). The
  `parseAsSuffix` method has more branches.
- The case `IS` without `NULL` (e.g. `IS` as a column alias) is
  ambiguous with the postfix operator. Per the SQL standard, the
  `IS` keyword is reserved for `IS [NOT] NULL` + `IS DISTINCT FROM` +
  `IS OF TYPE`. SM8 supports the first; the others are deferred
  per plan.
- A user who writes `IS` as a column alias will get a typed
  `UnexpectedToken("expected 'NULL' or 'NOT NULL' after 'IS'")` —
  this is **per design** (per `scala-error-handling-mindset`: errors
  are data; fail-fast is preferred over silent acceptance).

**Reversibility:** N/A. The change is additive.

## RF References

- `semantic-layer-engine-architecture.md` §3 Core Boundary (line 25–34):
  the parser is in core (no data-source knowledge).
- `semantic-layer-engine-architecture.md` §7 Contracts (line 245–260):
  the `Engine.compile(model, ctx)` assumes a valid `Model`; boundary
  validation happens **before** this step.
- `hooks.md` line 111: "Validator: Inspects `context.request`/`context.meta`,
  raises or sets `stop` if invalid" — describes a **runtime** Validator
  hook, not the parser-level `IsNull` postfix.

## Plan References

- **Plan line 211** (predicate/ 1 file → re-homed): SM8 uses
  `core.expr.Expr` directly (PR #45 design choice in `FilterSpec.scala`).
  This ADR extends the typed-Expr family.
- **Plan line 195** (manifest/ IR move): completes the typed-shape
  chain.

## Skills Applied (per user directive)

- **karphyaguids-mindset** "smallest correct change": 1 new `else if`
  branch in `parseAsSuffix` + 3 new tests in `EndToEndPipelineSpec`.
  No production API changes.
- **karphyaguids-mindset** "name what done looks like":
  `parseExpr("age IS NULL") == Right(IsNull(FieldRef("age")))`.
  Round-trip proven by 3 new tests.
- **scala-data-driven-refactor-mindset** §1: shape (`IsNull` /
  `IsNotNull` case classes) and validity (the parser implementation)
  are separated.
- **scala-error-handling-mindset** "errors are data": unparseable
  `IS` returns typed `UnexpectedToken` with message
  "expected 'NULL' or 'NOT NULL' after 'IS'", never throws.
- **scala-impact-analysis-mindset** mantra 4: 0 production callers
  affected; the change is purely additive at the parser level.
- **debug-mantra-mindset** 5-step discipline: reproduce (the 3
  failing tests drove the diagnosis); bisect (the `parseAsSuffix`
  path was already correct, the gap was the `IS` branch missing);
  verify (10 tests pass, 0 regressions).
- **scala-spark-batch-bugs-mindset** mantras: the produced `Expr.IsNull`
  / `IsNotNull` are auto-serializable; the `PortableExprCompiler`
  (PR #41) compiles them into Spark `df.filter(col.isNull)` at
  the driver side. No executor-side closure captures the AST.

## Pre-commit Gates (per user directive)

- **Pre-flight**: 2.7 GB avail, 26 GB disk, 4 codegraph (healthy).
- **LSP diagnostic**: 1/1 touched file OK.
- **Codegraph pre-PR**: 0 external callers of the new `IsNull` /
  `IsNotNull` parse branch. **Gate PASS.**
- **Tests**: 442 reactor green (165 sm8-platform + 277 sm8-core),
  0 regressions.
- **Post-PR-push monitor rule**: PR status will be checked
  immediately after push (per standing user directive 2026-08-14).
