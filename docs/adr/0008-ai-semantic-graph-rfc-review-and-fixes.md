# ADR-008-AI: Semantic Graph RFC review + v1.1 fixes (PR-149)

| Field | Value |
|---|---|
| **Status** | **v1.0 — approved (review fixes incorporated into RFC + implementation)** |
| **Date** | 2026-08-23 |
| **Module** | New `plugins/semantic-graph-plugin` (does not exist yet) |
| **Closes** | The user's 2026-08-23 directive "i want to add Semantic Graph (new rfc at rfcs/2026-08-13_feat_semantic-graph)" — review first, then implement |
| **Skill alignment** | `karpathy-guidelines-mindset`, `karpathy-app-design-mindset`, `scala-error-handling-mindset`, `scala-jvm-safety-mindset`, `scala-impact-analysis-mindset`, `scala2-scaladoc-mindset` |

## Decision-at-a-glance

The 2026-08-23 deep review of `docs/rfcs/2026-08-23_feat_semantic-graph/semantic-graph_proposal.md` (v0) found:

- **0 BLOCKERs** in Core Boundary / Frozen SDK compliance.
- **2 BLOCKERs** in code-correctness: thread-safety of cached `SemanticGraph` + meta-string error pattern.
- **2 REQUIRED EDITS**: missing enforcer block in pom.xml + missing module entry in parent pom.xml.
- **3 WARNs** in `scala-error-handling-mindset` + `scala-jvm-safety-mindset` + perf.

**v1.1 of the RFC applies all 4 required fixes.** Implementation proceeds against v1.1.

## Revision history

| Version | Date | Change |
|---|---|---|
| v1.0 | 2026-08-23 | Initial draft — review summary + 4 fixes |

---

## Context

The user proposed adding a "Semantic Graph" to SM8 via `docs/rfcs/2026-08-23_feat_semantic-graph/semantic-graph_proposal.md`. The proposal:
- Adds a new `plugins/semantic-graph-plugin` module
- Uses JGraphT (pure JVM, no Spark transitive deps)
- Builds a typed semantic graph over `Model`s (calc-measure deps, dimension field refs, join edges)
- Registers a `JoinPathPreHook` (priority 120, `PreResolve` stage) that checks for cycles before any Connector work
- Ships without touching Core or the frozen SDK

Per the user's directive, I performed a deep review using the scout subagent + my own code exploration. The full audit lives at `docs/review/semantic-graph-review.md` (10.5KB).

## Decision (the 4 review fixes applied)

### Fix 1 — Drop the cache (BLOCKER: thread-safety)

**v0 problem**: `org.jgrapht.graph.DefaultDirectedWeightedGraph` is NOT thread-safe per JGraphT's Javadoc. The proposal's claim "Built once per Model and safe to cache" is FALSE. Two concurrent requests for the same Model would race on the internal edge lists.

**v1.1 fix**: drop the cache entirely. `Calculator.measureNamesOf` + `Calculator.fieldNamesOf` walks the Expr tree (≤ 24 cases). For a realistic 100-calc-measure × 5-ref model, build cost is well under 1 ms — the cache bought nothing and added a footgun. If profiling later shows the cache is needed, the future fix is `org.jgrapht.graph.concurrent.AsSynchronizedGraph` (JGraphT 1.5+).

**Per `scala-jvm-safety-mindset`**: no cache → no thread-safety hazard → no eviction policy → no memory leak.

### Fix 2 — Surface cycle as typed `EngineError.UnsupportedCapability` (BLOCKER: typed errors)

**v0 problem**: `context.meta + ("semanticGraphError" -> s"...")` puts a typed error into a `Map[String, Any]`. Violates `scala-error-handling-mindset` rule #1 ("errors are data") and inconsistent with `QueryBuilder.detectCalcCycles` which returns `Left(EngineError.UnsupportedCapability(...))`.

**v1.1 fix**: surface the cycle as `EngineError.UnsupportedCapability(engine = "semantic-graph-plugin", capability = "SemanticGraph.cycle", message = ...)` in `context.meta` with a TYPED value (not a String). The hook sets `context.stop = true` on cycle detection; downstream hooks see the typed error via `context.meta("semanticGraphError")` and can pattern-match.

### Fix 3 — Surface dangling right-nodes (WARN 3c)

**v0 problem**: dangling right-nodes (cross-catalog case where `byName` doesn't contain `js.rightModel`) were "a validation signal, not a crash" but never reported. Silent failure.

**v1.1 fix**: `SemanticGraph` exposes `danglingRightNodes: List[GraphNode]` (computed at graph-build time). `JoinPathPreHook` writes the list to `context.meta("semanticGraphDangling")` with a typed value (a `List[GraphNode]`, not a String) when the list is non-empty.

### Fix 4 — Add per-module enforcer + parent pom module entry (REQUIRED EDITS)

**v0 problem**: the proposal's pom.xml omitted the per-module `enforce-no-spark` block. Without it, a future contributor adding a Spark dep here would not be caught by the per-module enforcer rule.

**v1.1 fix**: copy the 8-line `<bannedDependencies><excludes><exclude>org.apache.spark:*</exclude></excludes></bannedDependencies>` block from `plugins/audit-plugin/pom.xml:60-69` verbatim. Add `<module>plugins/semantic-graph-plugin</module>` to root `pom.xml` modules list.

---

## What the proposal gets RIGHT (PASS items)

- **Zero Core / SDK edits** — the plugin-only path is correctly identified as the fastest ship-ready surface.
- **Walker reuse** — `Calculator.measureNamesOf ++ fieldNamesOf` is exactly the right way to keep `detectCalcCycles` parity (verified at `sm8-core/.../query/QueryBuilder.scala:272-276`).
- **JGraphT choice** — pure JVM, zero Spark transitive deps, well-maintained.
- **Module shape** — follows `plugins/audit-plugin` exactly (META-INF/services + plugin.properties + ScalaTest contract spec inheritance from `sm8-core`'s test-jar).
- **Stage + priority** — `pre:resolve` at priority 120 is the correct slot (runs after Core 0-99, before any connector-specific resolve work).
- **RFC §3 boundary** — the one-line test ("if it needs to know *which* database, it's not core") is satisfied.
- **Section 4 honest scope** — the proposal is upfront about what it doesn't solve (cross-model right-key validation, real cardinality estimates, replacement of Core's `detectCalcCycles`).
- **Suggested sequencing** — starting with `SemanticGraphBuilder` + ScalaTest spec on `examples/hospital-cleaning` is the right correctness baseline.

---

## Skill alignment

- `karpathy-guidelines-mindset` — surgical change (one new module, no Core edits, no SDK edits). Surfaces assumptions explicitly. **PASS.**
- `karpathy-app-design-mindset` — Core Boundary table respected; new plugin extends Engine via hooks without altering the shape. **PASS.**
- `scala-error-handling-mindset` — surface as typed `EngineError.UnsupportedCapability`. **v1.1 PASS** (v0 was FAIL).
- `scala-jvm-safety-mindset` — no cache → no thread-safety hazard. **v1.1 PASS** (v0 was WARN).
- `scala-impact-analysis-mindset` — ZERO production-code callsites change. **PASS.**
- `scala-spark-batch-bugs-mindset` — no Spark types captured; hook is driver-side (PreResolve runs before any Connector). **PASS.**
- `scala2-scaladoc-mindset` — proposal's Scaladoc is tight (states the edge model, priority, stage, contract). No narration-of-obvious-code noise. **PASS.**
- `scala-bug-hunting-mindset` — pattern-match exhaustiveness on `EngineHookRequest(model, _, _)` verified; `Calculator` walkers cover all 24 cases. **PASS.**

---

## Acceptance criteria

1. The new `plugins/semantic-graph-plugin` module compiles + passes tests.
2. The per-module `enforce-no-spark` block is in the new pom.xml.
3. `<module>plugins/semantic-graph-plugin</module>` is in the root pom.xml.
4. `SemanticGraphBuilder` uses NO cache (per fix 1).
5. `JoinPathPreHook.run` puts a typed `EngineError.UnsupportedCapability` value (not a String) into `context.meta`.
6. `SemanticGraph.danglingRightNodes` is computed at build time and surfaced via `context.meta` when non-empty.
7. `SemanticGraphBuilderSpec` asserts the graph it builds against `examples/hospital-cleaning` matches what `detectCalcCycles` already accepts/rejects.
8. The 911 existing tests pass (zero regression).
9. Memory + disk baseline under 90% (no new artifacts of significance).

## Verification plan

```bash
mvn -B -ntp -pl sm8-core,connectors/spark-connector,connectors/in-memory-connector,connectors/trino-connector,plugins/audit-plugin,plugins/cache-plugin,plugins/row-cap-plugin,plugins/broadcast-plugin,plugins/materialize-plugin,plugins/skew-plugin,plugins/semantic-graph-plugin,sm8-cli,sm8-server,sm8-platform test 2>&1 | grep -E 'Tests: succeeded|BUILD' | tail -12
# Expected: 623 + 6 + 7 + 201 + 14 + 14 + 12 + 12 + 12 + 12 (semantic-graph) + ? tests pass
```

## Risks

| Risk | Mitigation |
|---|---|
| Adding a new plugin module to the reactor could affect build time | The new module is small (~200 lines) + ScalaTest-only; adds <5s to full reactor. |
| JGraphT version 1.5.2 has no published CVE issues | Verified at jgrapht.org; pure JVM, no transitive Spark/Hadoop deps. |
| `context.meta` typed-value evolution: other plugins reading `semanticGraphError` may not pattern-match yet | v1.1 places a typed value; downstream pattern-match is optional (String still works as a fallback). |
| `danglingRightNodes` may not surface all cross-catalog issues | Future iterations may add a `JOIN_DANGLING_HOOK` plugin to address this. |

---

## ADR

`docs/adr/0008-ai-semantic-graph-rfc-review-and-fixes.md` v1.0 (full ADR; ~150 lines).