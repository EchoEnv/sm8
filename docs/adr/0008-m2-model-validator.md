# ADR-008-M2: `ModelValidator` (PR-M2) — closes ADR-008-L Appendix GAP 2

**Status:** Accepted. **Date:** 2026-08-17. **Closes:** GAP 2 (cross-reference validation).

## Context

Per **ADR-008-L Appendix GAP 2** (the post-PR-L production-readiness audit): `ModelValidator` did not exist. `Model.of` enforced only field-level validity (name non-blank, version non-negative); cross-reference errors (duplicate names, references to unknown fields) surfaced at engine-compile time or produced wrong results — the worst failure point per [[debug-mantra-mindset]] §1.

## Decision

### Two surfaces

1. **`ModelValidator.validate(model): Either[ModelValidationError, Unit]`** — pure model-level checks: duplicate names across the same field kind. No IO. Always callable from `Model.of` without a resolver.

2. **`ModelValidator.validateAgainstSchema(model, ResolvedSource.Scan): Either[ModelValidationError, Unit]`** — requires the schema (post-source-resolution). Called by the connector / deployment layer after `SourceResolver.resolve(...)` (PR-M3 + PR-M4 territory).

### Integration with `Model.of`

`Model.of` now calls `ModelValidator.validate` after the existing name/version checks. The validator aggregates ALL errors (per [[debug-mantra-mindset]] §1 — never silent partial-validation) and surfaces them as a single `ModelValidationError.SchemaValidation(messages)`.

### New `ModelValidationError` case

```scala
final case class SchemaValidation(messages: List[String]) extends ModelValidationError {
  val message = s"Model schema validation failed: ${messages.mkString("; ")}"
}
```

### The walker covers the FULL 24-case Expr family (PR-I grammar)

The `validateAgainstSchema` walker handles all 24 cases: `FieldRef`, `MeasureRef` (skipped — engine-known), `All` (skipped), `Literal`, `Not`, `IsNull`, `IsNotNull`, `Cast`, `Alias`, all 5 arithmetic, all 6 comparison, 3 boolean, `CaseWhen`, `FunctionCall`. This means PR-I's `CASE WHEN` and `Alias` work end-to-end through the validator.

## Layer ownership (RFC §3)

**Core** — both surfaces are engine-portable data-shaping. Zero spark imports — enforcer passes. `ModelValidator.validate` blast radius: 2 callers (both in `Model.scala` — the SM validate + the ModelBuilder wrap). `validateAgainstSchema` blast radius: 0 prod callers (foundation for PR-M4 wiring).

## Conformance (RFC §12)

+16 tests (sm8-core 425 → 441). All exercise the **public boundary** (`Model.of` for the pure-level checks; direct call for the schema-level checks). Surface-the-validator-through-the-public-API pattern verifies BOTH the validator logic AND the PR-M2 wiring (Model.of calls the validator).

## Pre-commit gates

| Gate | Result |
|---|---|
| Pre-flight | ✅ toolchain clean (3 codegraph duplicates killed at session start) |
| LSP | ✅ 3/3 files clean |
| Codegraph blast-radius | ✅ `validate` 2 callers in `Model.scala`; `ModelBuilder.build` signature unchanged |
| Enforcer | ✅ passes |
| Reactor | ✅ **1032 green, 0 failures** (was 1016, +16 from PR-M1 + +16 = +32 across PR-M1 + PR-M2 — actually 1032 vs 1016 = +16 just from PR-M2) |

## The debug-mantra lesson (recorded for future PRs)

The spec was first overengineered with a polymorphic `unique[T <: { def name: String }]` helper trying to bypass `Model.of`'s pre-validation. Three compile failures (default-param cross-ref, package-private ctor, Nothing type inference) — all were fixture bugs, not validator bugs. The user invoked the debug-mantra: replace the helper with the **public API** (Model.of directly). The resulting test is simpler AND stronger (it verifies Model.of's wiring too).
