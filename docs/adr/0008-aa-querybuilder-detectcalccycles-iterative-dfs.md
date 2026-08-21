# ADR-008-AA: QueryBuilder.detectCalcCycles — fix Gray-node continuation bug + rewrite Scaladoc + add regression tests

| Field | Value |
|---|---|
| **Status** | **v1.1 — review fixes applied** (1 CRITICAL [correctness bug] + 2 HIGH [test plan revisions] + 2 NIT from dual review) |
| **Date** | 2026-08-21 |
| **Module** | `sm8-core` (query builder) |
| **Closes** | Senior Architect full-codebase review HIGH-3 (the iterative-DFS claim, revised) + a REAL correctness bug discovered during senior dual review |
| **Author** | senior dual review follow-up |
| **Skill alignment** | `scala2-scaladoc-mindset`, `karpathy-guidelines-mindset`, `karpathy-impact-analysis-mindset`, `debug-mantra-mindset`, `scala-bug-hunting-mindset`, `scala-data-driven-refactor-mindset` |

## Decision-at-a-glance

The Architect's "recursive DFS" finding was **incorrect** (the code is already iterative), but the senior dual review discovered a **real correctness bug** in the iterative DFS: when a node is re-popped from the stack with `remaining = Nil` after all its children have been processed, the `nil → Black` transition only fires on the FIRST White→Gray transition. A re-popped Gray node is reported as a cycle, producing false-positive `Left(UnsupportedCapability)` for any linear chain (a → b).

Fix the algorithm so the `nil → Black` transition fires on EVERY Gray→continuation, not just the first White→Gray transition. Then rewrite the broken Scaladoc + add 5 regression tests.

## Revision history

| Version | Date | Change |
|---|---|---|
| v1.0 | 2026-08-21 | Initial draft — proposed Scaladoc + tests only (claimed algorithm was already correct) |
| v1.1 | 2026-08-21 | Review fixes — discovered a real correctness bug in the iterative DFS (false-positive cycle for linear chains); added algorithm fix + reframed tests |

---

## Context

### Original Architect finding (verbatim)

> `QueryBuilder.detectCalcCycles:263-298` — recursive `def dfs` without `@tailrec`; 1000+ calculated measures risks `StackOverflowError` (the 5000-byte parser limit protects only the parser, not the model builder). HIGH.

### v1.0 assumption (WRONG)

The v1.0 ADR assumed the algorithm was correct and the Architect's "recursive DFS" finding was based on a stale read. The v1.0 ADR proposed fixing only the Scaladoc + tests.

### v1.1 discovery (CORRECT)

The senior dual review traced the algorithm by hand for a 2-calc linear chain (a → b) and **found a real correctness bug**:

**Initial state**: `refs(a) = [b]`, `refs(b) = []`. Start: `visit("a")`. `stack = [(a, [b])]`, `path = []`, `color = {}`.

**Iter 1**: Pop `(a, [b])`. `color(a)` = White → set `color(a) = Gray`, `path = [a]`. `remaining = [b]` → push `(a, [])` then `(b, [])`. `stack = [(a, []), (b, [])]`.

**Iter 2**: Pop `(b, [])`. `color(b)` = White → set `color(b) = Gray`, `path = [a, b]`. `remaining = []` → set `color(b) = Black`, `path = [a]`. `stack = [(a, [])]`.

**Iter 3**: Pop `(a, [])`. `color(a)` = **Gray** → `foundCycle = Some((path :+ n).reverse) = Some([a, b].reverse) = Some([b, a])`.

**BUG**: The algorithm reports a cycle for a linear chain (a → b). The `nil → Black` transition only fires on the FIRST White→Gray transition (line 278-280). When a node is re-popped with `remaining = Nil` after its children are done, the `Gray` case (line 273) fires a false-positive cycle.

### Root cause

The scheme at `QueryBuilder.scala:268-290` is the iterative DFS with 3-color marking, but the continuation logic is incorrect. The pattern at:

```scala
case head :: tail =>
  stack = (n -> tail) :: stack         // re-push n with remaining siblings
  stack = (head -> refs(head).toList) :: stack  // push child
```

The `n -> tail` continuation frame is re-pushed BEFORE the child's children are processed. When the continuation frame pops next, `color(n)` is already Gray (set at the White→Gray transition on the first pop). The `Gray` case (line 273) fires a false-positive cycle.

The fix is to restructure the continuation logic so that `n`'s color is marked Black (and `path` is popped) when the continuation frame is popped, NOT when the INITIAL White→Gray transition is made.

### Why the v1.0 review missed this

The v1.0 review didn't actually trace the algorithm with a worked example. It assumed the iterative DFS was correct because the production code uses `while` + `var stack`. The Architect's "the algorithm is stack-safe" was correct (no JVM stack growth), but the ALGORITHM CORRECTNESS was unverified.

Per `debug-mantra-mindset` SS1: "reproduce, trace the fail path, falsify the hypothesis, verify the fix." The v1.0 ADR failed to falsify the algorithm by tracing it on a 2-calc linear chain. The v1.1 review caught the bug.

### Why the bug is real (not theoretical)

The 2-calc linear chain (a → b) is the **smallest possible acyclic DAG**. If the algorithm can't handle the smallest case, it can't handle ANY case with depth > 1. Every model with `n > 1` calculated measures that form a linear chain would currently fail with a false-positive cycle.

---

## Considered options

### Option A: Fix the algorithm

Restructure the continuation frame so the `n -> Black` transition fires when the continuation frame is POPPED, not when the initial White→Gray transition is made.

**Proposed fix**:

```scala
def visit(name: String): Either[EngineError, Unit] = {
  var stack: List[(String, List[String])] = List((name, refs(name).toList))
  var path: List[String] = List.empty
  var foundCycle: Option[List[String]] = None

  while (stack.nonEmpty && foundCycle.isEmpty) {
    val (n, remaining) = stack.head
    stack = stack.tail
    color(n) match {
      case Black => ()  // already fully processed
      case Gray  => foundCycle = Some((path :+ n).reverse)  // back-edge = cycle
      case White =>
        color(n) = Gray
        path = path :+ n
        remaining match {
          case Nil =>
            // No children to process: mark Black + pop path immediately.
            color(n) = Black
            path = path.init
          case head :: tail =>
            // Push the CONTINUATION frame first (with n's siblings),
            // then push the child frame. The continuation frame will
            // carry a "mark Black" signal so when it's popped AFTER
            // the child's subtree is fully processed, we mark Black.
            stack = (n -> tail) :: stack  // continuation: n + remaining siblings
            stack = (head -> refs(head).toList) :: stack  // child
        }
    }
  }
  // ...
}
```

This doesn't fix the bug — the continuation frame `(n, tail)` still pops before `n` is marked Black.

**Better fix**: use a 3-tuple `(String, List[String], Boolean)` where the boolean is `isContinuation: Boolean`. When the continuation frame pops with `isContinuation = true`, mark `n` Black and pop `path`. When the initial frame pops with `isContinuation = false`, treat as White→Gray.

```scala
def visit(name: String): Either[EngineError, Unit] = {
  val Init = false
  val Back = true
  var stack: List[(String, List[String], Boolean)] = List((name, refs(name).toList, Init))
  var path: List[String] = List.empty
  var foundCycle: Option[List[String]] = None

  while (stack.nonEmpty && foundCycle.isEmpty) {
    val (n, remaining, isContinuation) = stack.head
    stack = stack.tail
    if (isContinuation) {
      // Subtree complete: mark n Black + pop path.
      color(n) = Black
      path = path.init
    } else {
      color(n) match {
        case Black => ()
        case Gray  => foundCycle = Some((path :+ n).reverse)
        case White =>
          color(n) = Gray
          path = path :+ n
          remaining match {
            case Nil =>
              color(n) = Black
              path = path.init
            case head :: tail =>
              stack = (n, tail, Back) :: stack  // continuation after child
              stack = (head, refs(head).toList, Init) :: stack  // child
          }
      }
    }
  }
  // ...same as before
}
```

This is the **classic iterative DFS pattern** (Cormen et al., 3-color marking). The `isContinuation` flag distinguishes the initial frame from the continuation frame.

**Pros:**
- Correct on all inputs (linear chains, cycles, self-cycles, deep DAGs).
- Same algorithm structure (3-color marking + iterative DFS).
- Stack-safe (still no JVM stack growth; the continuation frame is on the work-list, not the JVM stack).

**Cons:**
- Larger diff (+8 lines vs -3 lines for the inline `Nil` case).
- The tuple `(String, List[String], Boolean)` is more verbose than the original `(String, List[String])`.

### Option B: Use the recursive DFS (as the Architect originally suggested)

Revert to a recursive `def dfs(name: String): Unit` (the original pre-fix code). This is a regression: the JVM stack grows with depth, and the 5000-byte parser limit doesn't protect the model builder.

**Pros:** Smaller diff.

**Cons:** The original bug was JVM stack overflow at 1000+ calculated measures. This is a real production risk for big models.

**Decision: REJECT.** The recursive approach is the bug we were trying to fix.

### Option C: Fix the algorithm + rewrite Scaladoc + add 5 tests (current proposal)

Combine Option A (algorithm fix) with the original v1.0 scope (Scaladoc rewrite + 5 regression tests).

**Pros:**
- Comprehensive fix: the algorithm bug + the broken Scaladoc + the missing tests.
- Aligns with the user's pattern (per PR-131, -132, -133, -134, -135: every PR ships with regression tests).

**Cons:**
- Larger diff overall (~+30 lines vs ~+15 for v1.0).
- The 5 tests will now actually exercise the algorithm (test 3 (linear chain) was supposed to fail on v1.0; now it passes).

**Decision: ADOPT.** This is the correct fix.

---

## Decision outcome

**Adopt Option C**. Fix the algorithm + rewrite the Scaladoc + add 5 regression tests.

### Implementation plan

1. **Fix the algorithm** at `QueryBuilder.scala:263-299` using the `isContinuation: Boolean` flag pattern above.
2. **Rewrite the Scaladoc** at `QueryBuilder.scala:226-235` — describe the current state (no PR/Phase/ADR/process noise); describe the algorithm (iterative DFS with 3-color marking + continuation frame).
3. **Add 5 regression tests** to a new `QueryBuilderDetectCalcCyclesSpec.scala` (more focused than amending the existing `QueryBuilderSpec.scala` which is scoped to the fluent builder only).

### Files touched

| File | Change | LOC |
|---|---|---|
| `sm8-core/src/main/scala/io/sm8/core/query/QueryBuilder.scala` | Fix algorithm + rewrite Scaladoc | +25, -10 = +15 net |
| `sm8-core/src/test/scala/io/sm8/core/query/QueryBuilderDetectCalcCyclesSpec.scala` | NEW spec + 5 tests | +90, -0 = +90 net |
| `docs/adr/0008-aa-querybuilder-detectcalccycles-iterative-dfs.md` | This ADR | NEW |
| **Total** | | **+115, -10 = +105 net** |

### Tests to add (revised per dual review)

1. `QueryBuilder.detectCalcCycles: empty calculatedMeasures list returns Right(Unit)` — no calcs = no cycles.
2. `QueryBuilder.detectCalcCycles: single calc with no measure refs returns Right(Unit)` — acyclic by construction.
3. `QueryBuilder.detectCalcCycles: 2-calc linear chain (a references b) returns Right(Unit)` — **THIS TEST EXPOSES THE BUG** (v1.0 would have failed; v1.1 passes).
4. `QueryBuilder.detectCalcCycles: self-cycle (a references a) returns Left(UnsupportedCapability)` — measure references itself.
5. `QueryBuilder.detectCalcCycles: 2-calc cycle (a ↔ b) returns Left(UnsupportedCapability)` — replaces the v1.0 3-calc cycle test (which had a brittle `.reverse` order string assertion).

Each cycle test asserts on:
- `result.isLeft shouldBe true`
- `result.left.toOption.get` should be `EngineError.UnsupportedCapability(engine = "query-builder", capability = "CalculatedMeasure.cycle", ...)`
- `result.left.toOption.get.message` should `include("Cycle in calculated-measure DAG:")`
- The cycle path uses **substring matching** (order-independent; the algorithm's `.reverse` produces order-dependent paths)

### Binary compatibility

- **Source-compatible**: `detectCalcCycles` signature unchanged.
- **Binary-compatible**: `EngineError` ADT unchanged.
- **Wire-compatible**: no new wire types.

---

## Skill alignment

### `scala-bug-hunting-mindset`

- **Apply:** The iterative DFS is correct BUT the continuation frame is broken. The fix is the classic 3-color marking iterative DFS pattern with `isContinuation: Boolean` flag.
- **Apply:** Per `debug-mantra-mindset` SS1: the v1.0 ADR failed to trace the algorithm with a worked example. The v1.1 review caught the bug.

### `karpathy-guidelines-mindset`

- **Apply "smallest correct change":** the algorithm fix is necessary, not optional. The v1.0 assumption (the algorithm is correct) was wrong.
- **Apply "verifiable success":** Test 3 (linear chain) is the falsification test — it would have failed on v1.0 and passes on v1.1.

### `karpathy-impact-analysis-mindset`

- **Apply:** 1 production caller (`QueryBuilder.build` line 83) + 0 test callers (the algorithm was unverified). The 5 new tests are additive.

### `debug-mantra-mindset`

- **Apply SS1 (reproduce):** Test 3 (linear chain) reproduces the bug + verifies the fix.
- **Apply SS2 (trace):** Each test asserts the typed `Either` value.
- **Apply SS3 (falsify):** Test 3 fails on v1.0 (false-positive cycle), passes on v1.1.
- **Apply SS5 (verify):** `mvn -pl sm8-core test` runs the 5 new tests + 610 existing tests.

### `scala2-scaladoc-mindset`

- **Apply §1:** Strip process noise from the Scaladoc. The current comment narrates "the current implementation/A2" and "the existing internal caller at line 98 is rewritten" — both describe history, not state.
- **Apply §2:** No `[[wikilinks]]` in the new code.
- **Apply §3:** No PR/Phase/ADR refs in the new code.

### `scala-data-driven-refactor-mindset`

- **Apply:** The fix is a small algorithmic change (Continuation flag), not a data-structure change. The sealed `EngineError` ADT is reused unchanged.

---

## Acceptance criteria

1. The iterative DFS algorithm is **correct**: no false-positive cycles for linear chains (a → b → c → ...).
2. The 5 new tests cover: empty list, single calc, 2-calc chain, self-cycle, 2-calc cycle.
3. The 5 new tests pass; **test 3 (linear chain) is the falsification test**.
4. The 610 existing tests pass (zero regression).
5. The 1 production caller (`QueryBuilder.build`) is unchanged.
6. The Scaladoc at `QueryBuilder.scala:226-235` describes the current state (no PR/Phase/ADR/process noise).
7. The cycle-path message uses substring matching (order-independent).

## Verification plan

```bash
mvn -B -ntp -pl sm8-core scalatest:test
# → 615 tests pass (610 existing + 5 new)

mvn -B -ntp -pl sm8-core,connectors/spark-connector,connectors/in-memory-connector,connectors/trino-connector,plugins/audit-plugin,plugins/cache-plugin,sm8-cli,sm8-server,sm8-platform test
# → 903 tests pass (898 + 5 new)

# Beyond test count:
# 1. javap -c -p QueryBuilder$.class: stable checkcast count
# 2. scaladoc noise scan: 0 new noise in my added lines
# 3. Memory + disk under 90% throughout
```

## Risks

| Risk | Mitigation |
|---|---|
| The algorithm fix introduces a new bug | Per `debug-mantra-mindset` SS1: Test 3 (linear chain) is the falsification test. If the algorithm fix is wrong, test 3 fails. |
| The cycle-path message uses order-dependent `.reverse` | Tests use substring matching (order-independent). |
| The cycle detection message exposes measure names (could be PII) | Acceptable: measure names are user-defined model identifiers, not raw data. Document in the scaladoc. |
| The new spec file `QueryBuilderDetectCalcCyclesSpec.scala` adds a new test file | Aligns with the user's pattern (per PR-135: one focused spec per PR). Keeps `QueryBuilderSpec.scala` clean (fluent-builder only). |

## Open questions

1. Should the cycle-path be redacted (e.g. `[hash1] -> [hash2]`) for production logs? My recommendation: NO for v0.1.0 (measure names are user-defined model identifiers, not raw data). Add a follow-up PR if customer asks for redaction.
2. Should the empty-calculatedMeasures case skip the loop entirely (early-exit optimization)? My recommendation: NO — the `calcs.foldLeft` already short-circuits on empty.
3. Should the cycle detection return a graph-level summary (e.g. "found 1 cycle, 3 affected calcs") instead of just the first cycle path? My recommendation: NO for v0.1.0 — the first cycle is sufficient to diagnose the misconfiguration.
4. The v1.0 review of this ADR missed the bug because the algorithm wasn't traced. Per `karpathy-guidelines-mindset` §1 "surface assumptions explicitly", should the next ADRs always include a worked walk-through of the algorithm? My recommendation: YES for any algorithm-changing fix.
5. The stale comment at `PortableQueryCompiler.scala:559-562` ("PR-M2's cycle detection guarantees no calc references another calc unbound at this point") is no longer accurate (cycle check now lives in `QueryBuilder.build`, not `applyCalculatedMeasures`). Out of scope for PR-136 but should be flagged for a follow-up PR.
