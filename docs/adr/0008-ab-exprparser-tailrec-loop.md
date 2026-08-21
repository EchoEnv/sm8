# ADR-008-AB: ExprParser parseOrExpr/parseAndExpr — @tailrec loop refactor

| Field | Value |
| **Status** | **v1.2 — post-merge review fixes applied** (1 CRITICAL + 1 HIGH fixed; 1 NIT noted) |
| **Date** | 2026-08-21 |
| **Module** | `sm8-core` (expression parser) |
| **Closes** | Senior Architect full-codebase review HIGH-4 (the deferred ADR-008-O §P2-6 fix) |
| **Author** | senior dual review follow-up |
| **Skill alignment** | `scala-bug-hunting-mindset`, `karpathy-guidelines-mindset`, `karpathy-impact-analysis-mindset`, `debug-mantra-mindset`, `scala2-scaladoc-mindset` |

## Decision-at-a-glance

Per ADR-008-O §P2-6, the `loop` helpers inside `parseOrExpr`, `parseAndExpr`, `parseAddExpr`, `parseMulExpr`, and `parseCaseWhen` (ExprParser.scala:170-186, 220-236, 238-255, 306-353) are recursive and risk JVM `StackOverflowError` when parsing deeply-nested expressions. The fix is to convert each `loop` to a true iterative `while` loop with a mutable accumulator + `Either` match-based short-circuit. Apply to all 5 parsers consistently. (The v1.0/v1.1 proposed `@tailrec`; the v1.2 implementation uses `while` because `@tailrec` is rejected by Scala 2.13 when the recursive call is wrapped in `flatMap`.)

## Revision history

| Version | Date | Change |
|---|---|---|
| v1.0 | 2026-08-21 | Initial draft — refactor loop to @tailrec + regression test |
| v1.1 | 2026-08-21 | Review fixes — 5 tests (one per lifted parser); parseCaseWhen deferred; LOC revised; private locked |
| v1.2 | 2026-08-21 | Post-merge review fixes — corrected ADR-vs-implementation drift (the `loop` helpers are iterative, NOT `@tailrec`); lifted `parseCaseWhen`'s internal `def loop()` to iterative (the v1.0/v1.1 deferred this; v1.2 fixes it); clarified the binary-compat claim |

---

## Context

### Finding (verbatim from Senior Architect review)

> `ExprParser.parseOrExpr/parseAndExpr:170-186` — recursive `def loop` without `@tailrec`.

### Code excerpt (pre-fix)

```scala
// sm8-core/src/main/scala/io/sm8/core/expr/ExprParser.scala:170-186
def parseOrExpr(): Either[ExprParseError, Expr] =
  parseAndExpr().flatMap { left =>
    def loop(acc: Expr): Either[ExprParseError, Expr] =
      if (consumeWordCaseInsensitive("or")) {
        parseAndExpr().flatMap(right => loop(Expr.Or(acc, right)))
      } else Right(acc)
    loop(left)
  }

def parseAndExpr(): Either[ExprParseError, Expr] =
  parseNotExpr().flatMap { left =>
    def loop(acc: Expr): Either[ExprParseError, Expr] =
      if (consumeWordCaseInsensitive("and")) {
        parseNotExpr().flatMap(right => loop(Expr.And(acc, right)))
      } else Right(acc)
    loop(left)
  }
```

### Why the recursive `loop` is at risk

The `loop` is a `def` (nested inside `flatMap`). Each `or` / `and` keyword consumes one stack frame. A pathologically-nested expression like `a OR b OR c OR ... OR z` (1000 OR-chained operands) consumes 1000 JVM stack frames via the nested `def loop` calls.

The 5000-byte input limit (per `ExprParser.scala` PARSER_INPUT_LIMIT) caps the byte-length of the input, but the **AST depth** (not the byte-length) determines the recursion depth. A expression like `field_a OR field_b OR field_c OR ... OR field_10000` (each field is 20 chars → 200,000 chars, but 10000 OR-chained → 10000 stack frames) exceeds the 5000-byte input limit but a shorter expression `a OR b OR c OR ... OR z` (where each `a` is 1 char → 26 chars but 26 OR-chained → 26 frames) is well within the byte limit but still triggers recursion.

### Why the original `@tailrec` annotation failed

Per ADR-008-O §P2-6 (history note): the original PR-16 attempt tried to add `@tailrec` directly to the nested `def loop`, but Scala 2.13 compiler rejected it because:
- The `@tailrec` annotation requires the method to be a **direct** recursive call in tail position.
- The nested `def loop(acc: Expr).flatMap(right => loop(...))` is a **closure-captured** recursive call, not a direct tail call.
- Scala 2.13 cannot apply `@tailrec` to a nested closure-captured method.

### Pre-fix scope

The same recursive-loop pattern exists in:
- `parseOrExpr` (line 170-177): `loop` consumes 1 frame per `or` keyword
- `parseAndExpr` (line 179-186): `loop` consumes 1 frame per `and` keyword
- `parseAddExpr` (line 220-236): `loop` consumes 1 frame per `+/-` operator
- `parseMulExpr` (line 238-255): `loop` consumes 1 frame per `*/%` operator
- `parseCaseWhen` (line 306-353): `loop` consumes 1 frame per `WHEN ... THEN ...` branch

All 5 patterns are symmetric. The fix for one generalizes to all per the same pattern.

---

## Considered options

### Option A: Lift `loop` to a top-level `@tailrec` method per parser (v1.0; REJECTED v1.2)

Extract each `loop` into a separate `@tailrec def loopOrExpr(...)` method that takes the accumulator as a parameter and uses `Either.flatMap` + the existing `consumeWordCaseInsensitive` helper.

```scala
@scala.annotation.tailrec
def loopOrExpr(acc: Expr): Either[ExprParseError, Expr] =
  if (consumeWordCaseInsensitive("or")) {
    parseAndExpr().flatMap(right => loopOrExpr(Expr.Or(acc, right)))
  } else Right(acc)

def parseOrExpr(): Either[ExprParseError, Expr] =
  parseAndExpr().flatMap(loopOrExpr)
```

**Pros:**
- `@tailrec` is now direct (the compiler can verify the tail-call).
- The fix is **minimal** (~10 lines per parser; 5 parsers × 10 = 50 lines net).
- The algorithm is unchanged (recursive shape + call graph).
- The existing 5000-byte limit continues to protect against byte-length attacks.

**Cons:**
- The top-level `loopOrExpr` method is now an internal helper visible to the class (vs. the nested `def` which was scoped to the `flatMap` closure). The cleanest fix is to mark it `private` (or `private[expr]`).
- The fix must be applied to 5 parsers consistently (or be a partial fix).

### Option A: Convert to iterative with `while` + mutable accumulator (CHOSEN)

The v1.0 ADR proposed `@tailrec` but the compiler rejected it. The correct fix is iterative.

```scala
def parseOrExpr(): Either[ExprParseError, Expr] = {
  var left: Either[ExprParseError, Expr] = parseAndExpr()
  var stack: List[Expr] = List.empty
  while (left.exists(_ => consumeWordCaseInsensitive("or"))) {
    left = parseAndExpr().map(right => ???)
  }
  left
}
```

**Pros:**
- No recursion at all.
- The same pattern as PR-136's iterative DFS for `detectCalcCycles`.

**Cons:**
- More invasive (~25 lines per parser; 5 parsers × 25 = 125 lines net).
- The `while` loop is harder to read than the recursive `@tailrec` form.
- Loops over `Either` short-circuit (the `loop` returns `Right(acc)` on break) is awkward in Scala.

**Decision: REJECT.** `@tailrec` is the idiomatic Scala 2.13 fix for this pattern.

### Option C: Use `Either` chaining via `foldM`-style continuation

```scala
def parseOrExpr(): Either[ExprParseError, Expr] =
  parseAndExpr().flatMap { left =>
    Iterator.continually(consumeWordCaseInsensitive("or") -> ())
      .takeWhile(_._1)
      .foldLeft[Either[ExprParseError, Expr]](Right(left)) { case (acc, _) =>
        acc.flatMap(a => parseAndExpr().map(b => Expr.Or(a, b)))
      }
  }
```

**Pros:**
- No `@tailrec` needed (uses `foldLeft` over a lazy iterator).

**Cons:**
- The `Iterator.continually` + `takeWhile` + `foldLeft` pattern is less readable than `@tailrec`.
- The Iterator's `takeWhile` doesn't short-circuit on `Left` (the `Left` is carried via `flatMap`).

**Decision: REJECT.** Less idiomatic than Option A.

---

## Decision outcome

**Adopt Option A (corrected v1.2)** — but the `@tailrec` annotation was REJECTED by the Scala 2.13 compiler at compile time (the recursive call inside `parseXxx().flatMap(...)` is wrapped in `flatMap`, not a direct tail call). The actual implementation is Option B-style: **convert each `loop` to a true iterative `while` loop with a mutable accumulator + `Either` match-based short-circuit**. Apply to all 5 parsers (parseOrExpr, parseAndExpr, parseAddExpr, parseMulExpr, parseCaseWhen). The implementation is in `ExprParser.scala:180-189` (loopOrExpr), `:195-203` (loopAndExpr), `:242-260` (loopAddExpr), `:268-286` (loopMulExpr), `:357-371` (parseCaseWhen while-loop).

### Implementation plan

1. Extract `loop` from `parseOrExpr` (line 170-177) → `@tailrec def loopOrExpr(acc: Expr)`.
2. Extract `loop` from `parseAndExpr` (line 179-186) → `@tailrec def loopAndExpr(acc: Expr)`.
3. Extract `loop` from `parseAddExpr` (line 220-236) → `@tailrec def loopAddExpr(acc: Expr)`.
4. Extract `loop` from `parseMulExpr` (line 238-255) → `@tailrec def loopMulExpr(acc: Expr)`.
5. Extract `loop` from `parseCaseWhen` (line 306-353) → `@tailrec def loopCaseWhen(...)`.
8. **Add 5 regression tests** to `ExprParserSpec.scala` (one per lifted parser, v1.1):

### Files touched

| File | Change | LOC |
|---|---|---|
| `sm8-core/src/main/scala/io/sm8/core/expr/ExprParser.scala` | Lift 5 loops to `private @tailrec` methods (parseCaseWhen takes the ArrayBuffer as a parameter) | +70, -25 = +45 net |
| `sm8-core/src/test/scala/io/sm8/core/expr/ExprParserSpec.scala` | +5 regression tests (one per lifted parser) | +100, -0 = +100 net |
| `docs/adr/0008-ab-exprparser-tailrec-loop.md` | This ADR | NEW |
| **Total** | | **+170, -25 = +145 net** |

### Tests to add

1. `ExprParser.parseOr: 1000-OR-chained expression parses without StackOverflowError` — `a OR b OR c OR ... OR z` (1000 operands) → `Right(Expr.Or(...))`.
2. `ExprParser.parseAnd: 1000-AND-chained expression parses without StackOverflowError` — same shape for `and`.
3. `ExprParser.parseAdd: 1000-deep + or - chain parses without StackOverflowError` — `a + b - c + d - ... (1000 operands)`.
4. `ExprParser.parseMul: 1000-deep * or / or % chain parses without StackOverflowError` — `a * b / c * d / ... (1000 operands)`.
5. `ExprParser.parseCaseWhen: 1000-deep WHEN branch chain parses without StackOverflowError` — `CASE WHEN b1 THEN v1 WHEN b2 THEN v2 ... WHEN b1000 THEN v1000 ELSE v0 END`. (Corrected from v1.0 which only covered 2 parsers; the addon was that the test must cover ALL 5 lifted loops.)

### Binary compatibility

- **Source-compatible**: all 5 `parseXxx` signatures unchanged.
- **Binary-compatible**: no new public methods; the 5 `@tailrec` helpers are `private`.
- **Wire-compatible**: no new wire types.

---

## Skill alignment

### `scala-bug-hunting-mindset`

- **Apply §1 (trust the compiler):** `@tailrec` is enforced at compile-time. The compiler will reject the annotation if the recursive call is not in tail position.
- **Apply §2 (distrust implicits):** The reason the original `@tailrec` attempt failed is that the nested `def loop` inside `flatMap` is a closure-captured method, not a direct tail-call. The fix is to lift the method to a top-level (private) helper.

### `karpathy-guidelines-mindset`

- **Apply "smallest correct change":** the fix is 5 small refactors + 2 tests. ADR-008-O §P2-6 already documented the fix shape.
- **Apply "verifiable success":** Tests 1-2 assert the algorithm completes at 1000-deep recursion without `StackOverflowError`.

### `karpathy-impact-analysis-mindset`

- **Apply:** 1 production caller per `parseXxx` (the user-facing `parseExpr` flow). The 5 helpers are `private` so the public API is unchanged.

### `debug-mantra-mindset`

- **Apply SS1 (reproduce):** Tests 1-2 build a 1000-deep expression and verify it parses.
- **Apply SS5 (verify):** 2 new tests + 617 existing tests.

### `scala2-scaladoc-mindset`

- **Apply §1:** The new `@tailrec` helpers need a SHORT scaladoc explaining WHY they're `@tailrec` (the lift is necessary because nested `def` inside `flatMap` cannot be `@tailrec`).
- **Apply §2:** No `[[wikilinks]]` in the new code.
- **Apply §3:** No PR/Phase/ADR refs in the new code.

---

## Acceptance criteria

1. The 5 `loop` helpers are top-level `@tailrec` methods (not nested `def`s).
2. The 5 helpers are `private` (internal-only).
3. The 5 `parseXxx` public-method signatures are unchanged.
4. The 2 new tests pass (1000-deep OR + 1000-deep AND).
5. The 617 existing tests pass (zero regression).
6. The 5000-byte input limit is preserved.
7. The algorithm is unchanged (recursive shape + call graph).

## Verification plan

```bash
mvn -B -ntp -pl sm8-core scalatest:test
# → 619 tests pass (617 existing + 2 new)

mvn -B -ntp -pl sm8-core,connectors/spark-connector,connectors/in-memory-connector,connectors/trino-connector,plugins/audit-plugin,plugins/cache-plugin,sm8-cli,sm8-server,sm8-platform test
# → 905 tests pass (903 + 2 new)

# Beyond test count:
# 1. javap -c -p ExprParser.class: stable checkcast count; the @tailrec
#    methods compile to a single INVOKEVIRTUAL per call site (not a
#    call-chain) — the JVM stack stays at 1 frame per @tailrec call.
# 2. scaladoc noise scan: 0 new noise in my added lines
# 3. Memory + disk under 90% throughout
```

## Risks

| Risk | Mitigation |
|---|---|
| The 5 lifts change the call shape (top-level method vs nested closure) | The algorithm is identical (verified by the 617 existing tests passing) |
| The 1000-deep test exhausts the 5000-byte input limit and is rejected | The test uses field names of length 1 (`a`, `b`, `c`, ...) so the input is well under 5000 bytes |
| The `@tailrec` annotation is rejected at compile-time | The fix is the standard Scala 2.13 pattern; the compiler will verify the tail call |

## Open questions

1. ~~Should the 5 `@tailrec` helpers be `private` (class-internal) or `private[expr]` (parser-internal)? My recommendation: `private` (the parser is the only owner).~~ **RESOLVED v1.1**: `private` (locked in the Implementation plan).
2. Should the 1000-deep test be parameterized (100, 500, 1000, 5000) to test the speed of recursion depth? My recommendation: NO — 1000 is sufficient to verify the JVM stack is safe. Faster tests are better.
3. Should `parseCaseWhen` be lifted (it's a more complex `loop`)? My recommendation: YES — the same `@tailrec` pattern applies. The `loop` is recursive on the `branch` collection.
4. Should the 5000-byte input limit be lowered now that `loop` is `@tailrec`? My recommendation: NO — the limit is a separate defense against byte-length attacks. The `@tailrec` fix is for AST-depth, not byte-length.
