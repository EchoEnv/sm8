# ADR-008-M1: `ExprParser` grammar extensions + `ModelLoader` joins/calculated_measures (PR-M1)

**Status:** Accepted. **Date:** 2026-08-16. **Author:** SM8 agent. **Closes:** ADR-008-L Appendix GAP 1 + GAP 4 (both core-layer per RFC §3).

## Context

ADR-008-L's production-readiness audit recorded 8 verified gaps. GAP 1: the `ExprParser` had no grammar for the PR-H/PR-I Expr cases (`CaseWhen`, `Alias`, `MeasureRef`, `All`) — YAML authors using those forms got a typed parse error (capability absent). GAP 4: `ModelLoader` never read the `joins` / `calculated_measures` YAML blocks — PR-J's Model fields were reachable only programmatically.

## Decision

### GAP 1 — three parser extensions

1. **`CASE WHEN c THEN v [WHEN c THEN v]* [ELSE e] END` → `Expr.CaseWhen(branches, otherwise)`.** Keywords case-insensitive (consistent with `AND`/`OR`/`NOT`/`AS`/`IS`). **Missing `ELSE` lowers to `Literal(NullValue, Varchar)`** per SQL semantics (no-ELSE yields NULL). Requires ≥1 `WHEN` branch (typed error otherwise).

2. **`expr AS name` disambiguation rule.** After `AS`: try `parseTypeName` first — a known type keeps the `Cast` reading. On type-parse failure, backtrack and read the token as a column alias → `Expr.Alias`. **Refinement (found by a legacy test):** if the token IS a type keyword (`DECIMAL` without `(p,s)` etc.), keep the original fail-loud cast error — a malformed cast must NOT silently degrade into a rename. Net rule: **type keywords keep the cast path (incl. its failures); non-type tokens become aliases.**

3. **`all(name)` / `measure(name)` rewrite.** Mirrors the legacy CalcExpr DSL (`DESIGN.md` §6.2): `FunctionCall("all", Seq(FieldRef(n)))` → `Expr.All(n)`; `measure(...)` → `Expr.MeasureRef(n)`. Single-arg only; multi-arg stays `FunctionCall` (no over-rewrite).

### GAP 4 — two loader extensions

4. **`parseJoins`**: YAML `joins:` block → `List[JoinSpec]`. Kind is case-insensitive with `outer` aliasing `Full`. **Multi-key pairs are preserved** (the loader does not reject; PR-K's compile step owns the single-key scope + typed rejection). Unknown kind → typed `ManifestError.ParseFailure`.

5. **`parseCalculatedMeasures`**: YAML `calculated_measures:` block → `List[CalculatedMeasure]` via `ExprParser.parseExpr` (now GAP-1-aware). Parse failure / missing `expr` → typed `ManifestError.ParseFailure`. Both blocks are absent-tolerant (`Nil`) for backward compat.

### One contract change (legacy test updated)

`amount AS NOTAREALTYPE` previously failed (`InvalidLiteral`); under the new contract it is `Expr.Alias("NOTAREALTYPE", amount)`. The 2 affected `ExprParserSpec` tests were rewritten: the non-type-token case now asserts the alias shape; the malformed-`DECIMAL` case still asserts fail-loud (kept working via the type-keyword refinement).

## Layer ownership (RFC §3)

Both gaps + fixes are **core** (engine-portable data-shaping; zero spark imports — enforcer passes). `parseExpr` blast radius: 5 callers, all in `ModelLoader`; `fromString` blast radius: 1 caller (`PlatformModelLoader`) — signature unchanged, additive fields only.

## Conformance (RFC §12)

+30 tests (16 parser + 11 loader + 3 rewritten/shifted). AST-shape assertions (typed equality), typed-error assertions at every boundary, backward-compat (absent block → `Nil`) for both extensions.

## Pre-commit gates

| Gate | Result |
|---|---|
| LSP | ✅ 4/4 files clean |
| Codegraph blast-radius | ✅ `parseExpr` 5 callers (all ModelLoader); `fromString` 1 caller — no signature change |
| Enforcer | ✅ passes |
| Reactor | ✅ **1016 green, 0 failures** (was 986, +30) |

## Sequence status

| Step | Status |
|---|---|
| PR-M1 (this) | GAP 1 + GAP 4 closed |
| PR-M2 `ModelValidator` | next (GAP 2) |
| PR-M3 `SparkSourceResolver` | queued (GAP 3) |
| PR-M4 production wiring | queued (GAP 5, 6, 7, 8) |
