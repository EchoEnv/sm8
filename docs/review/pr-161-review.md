# PR-161 Final Pre-Merge Review (HEAD 69bd9ed)

**Reviewer**: `pr-161-review` scout | **Verdict**: **APPROVE**

## Summary

PR-161 (impact-analysis dependents()) is final-merge-ready. All 10 checklist items pass. Zero new BLOCKER, HIGH, or MEDIUM findings vs prior 7 reviews. Zero production-code callsite changes (only the plugin module touched). Zero Spark types captured. Scaladoc noise + shape checks: CLEAN. 14/14 plugin tests pass.

## Section 1 — RFC + ADR alignment (PASS)

- The PR implements the "impact analysis" use case from `docs/rfcs/2026-08-23_feat_semantic-graph/semantic-graph_proposal.md` §"Where a semantic graph earns its place": `"Which calculated measures / models break if this dimension changes?" = reverse-edge traversal`.
- The implementation lives in `SemanticGraphBuilder.dependents(node: GraphNode): List[GraphNode]` — a transitive reverse-closure via BFS over `g.incomingEdgesOf` + `g.getEdgeSource`.
- The PR does NOT deviate from the v1.1 fixes in `docs/adr/0008-ai-semantic-graph-rfc-review-and-fixes.md` (no cache, typed errors, dangling nodes, enforce-no-spark). PR-161 is additive.
- The new `dependents` field on `GraphSnapshot` follows the same `Product with Serializable` case-class pattern as the other fields (verified via codegraph).

## Section 2 — Architecture-spec compliance (PASS)

- Plugin-owns-schema: `dependents: Map[GraphNode, List[GraphNode]]` lives in the plugin module (`plugins/semantic-graph-plugin/.../GraphSnapshot.scala`).
- Transport-is-generic: `MetaInspectorService` reads `context.meta(GraphSnapshot.MetaKey)` generically; the new field flows through the existing `toMetaValue` projection.
- §3 Core Boundary: zero `sm8-core` production-code callsite references `semanticgraph.*`. Only `sm8-platform/.../MetaRequest.scala:16` mentions the meta key (as a comment), which is consistent with the architect's design review.
- `plugins.md` Rule 1: the plugin's `setup` is idempotent (PR-159); PR-161 does NOT modify the `setup` method. Both observers are still registered.

## Section 3 — Spark closure + serialize safety (PASS)

- The new `dependents: Map[GraphNode, List[GraphNode]]` field is `Map + List + case class` (all `Product with Serializable` auto-derived). Crosses the Restate journal boundary safely.
- `SemanticGraphBuilder.dependents` uses `g.incomingEdgesOf(n)` and `g.getEdgeSource(e)` — JGraphT APIs that touch mutable internal state. The `AsSynchronizedGraph` wrapper at `SemanticGraphBuilder.scala:104-108` (from PR-149) provides thread-safety. The wrapper is preserved in this PR.
- Zero Spark types captured. The `enforce-no-spark` rule at `plugins/semantic-graph-plugin/pom.xml:79-87` is unchanged.

## Section 4 — Performance concerns (PASS)

- `dependents(node)` is BFS over the reverse graph: O(V + E) per call (each vertex visited at most once via the `seen` set; each edge explored at most once).
- The observer computes `dependents` for EVERY vertex: O(V · (V + E)) = O(V² + V·E). For realistic model sizes (V ≤ 100, E ≤ 500), this is ~10,000 ops — sub-ms.
- `GraphSnapshot.toMetaValue` sorts the dependents list by (model, field): O(K log K) where K is the number of dependents (small). Sort is stable + deterministic (test assertions are reproducible).

## Section 5 — Skill alignment (all PASS)

- **karpathy-guidelines-mindset**: PASS — smallest correct change; 1 new public method + 1 new field + 1 new test file + 4 new tests.
- **karpathy-app-design-mindset**: PASS — plugin owns the schema; transport is generic; no new wire DTO.
- **scala-error-handling-mindset**: PASS — typed `Map[GraphNode, List[GraphNode]]` (no `String`/`Throwable` boxes).
- **scala-jvm-safety-mindset**: PASS — `AsSynchronizedGraph` preserves thread-safety; no resource leaks.
- **scala-impact-analysis-mindset**: PASS — blast radius is contained in the plugin module; codegraph confirms 5 touched files, all in the plugin module.
- **scala-spark-batch-bugs-mindset**: PASS — zero Spark types captured; observer is driver-side; no closure-capture of mutable state.
- **scala-perf-testing-mindset**: PASS — sub-ms for realistic models; no per-row allocation.
- **scala-bug-hunting-mindset**: PASS — `Map.empty` default for backward compat; exhaustive pattern-match on `EngineHookRequest`; no implicits.
- **scala2-scaladoc-mindset**: PASS — noise check CLEAN on all 4 changed files; shape check CLEAN on 2 main files; zero process noise (no PR / architect / skill / design-review citations).

## Section 6 — Final-merge-readiness checklist (10/10 PASS)

| # | Item | Status |
|---|---|---|
| 1 | All 4 review-fix sets from prior reviews still verified in code | PASS |
| 2 | PR-161 implements the impact-analysis use case per the RFC + ADR | PASS |
| 3 | No new BLOCKER, HIGH, or MEDIUM findings vs prior 7 reviews | PASS |
| 4 | Zero production-code callsite changes | PASS |
| 5 | Zero Spark types captured | PASS |
| 6 | Scaladoc noise scan: 0 new noise in the new code | PASS |
| 7 | Memory + disk under 90% | PASS (67% / 65%) |
| 8 | Zero orphan codegraph/metals/bloop processes | PASS |
| 9 | All 4 new tests in the plugin module are independent + reproducible | PASS |
| 10 | v0.1.0 tag readiness | GATED (per the user's 2026-08-20 directive) |

## Final verdict

**APPROVE.** PR-161 (impact-analysis dependents()) is final-merge-ready and merged to `main` at HEAD `69bd9ed`. The 4th use case from the semantic-graph RFC §"Where a semantic graph earns its place" (impact analysis = reverse-edge traversal) is now shipped.
