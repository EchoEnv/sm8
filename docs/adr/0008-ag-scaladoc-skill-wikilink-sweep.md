# ADR-008-AG: scaladoc sweep — strip skill-wikilink pattern from production source

> **⚠️ Historical snapshot (2026-08-22).** File paths in the tables below (e.g.
> `io/sm8/sdk/ResultRows.scala`) reflect the tree at the time of the sweep;
> the `Connector` SDK files have since been removed by ADR-011-a. Read
> current architecture from `docs/rfcs/2026-08-12_v1_architecture-spec/`
> + `docs/adr/0011-a-*`.

| Field | Value |
| **Status** | **v1.0 — mechanical sweep approved (per scala2-scaladoc-mindset §"No internal process noise")** |
| **Date** | 2026-08-22 |
| **Module** | 17 production source files across `sm8-core`, `sm8-platform`, `sm8-server` |
| **Closes** | The remaining Wave 3 finding (scaladoc sweep) per the user's 2026-08-22 directive |
| **Skill alignment** | `scala2-scaladoc-mindset` |

## Decision-at-a-glance

Strip the `[[skill-name-mindset]]` / `[[skill-name]]` double-bracket reference pattern from production source files. Replace with prose that names the skill's intent (without the agent-flavored citation). Per `scala2-scaladoc-mindset` §"No internal process noise": **a skill file is not a spec; never cite it as one**.

## Revision history

| Version | Date | Change |
|---|---|---|
| v1.0 | 2026-08-22 | Initial draft — mechanical sweep across 17 files |

---

## Context

### Codegraph finding (2026-08-22)

17 production source files contain 76 total noise patterns of the form `[[skill-name-mindset]]` or `[[skill-name]]`. Per `scripts/check_scaladoc_noise.py`, these are flagged as:

> `double-bracket reference looks like a skill/tool name, not a Scala symbol`

### Why this is a noise pattern (per scala2-scaladoc-mindset)

The skill explicitly says:

> No internal process noise. Never reference the task/conversation/process that produced the code. Scaladoc and comments describe the code as it exists, not how or why it was requested. Strip out anything like:
> - `// per [[some-skill-name]]` or any citation of the agent's own guidance/skill files — a skill file is not a spec; never cite it as one

The 76 occurrences are exactly this pattern. Per Wave 1 history (PR-130 stripped 600+ similar patterns from a wider sweep), the fix is mechanical.

### Files to clean

| File | Count |
|---|---|
| `sm8-platform/src/main/scala/io/sm8/platform/query/QueryService.scala` | 14 |
| `sm8-server/src/main/scala/io/sm8/server/Main.scala` | 6 |
| `sm8-platform/src/main/scala/io/sm8/platform/query/cache/CacheBridge.scala` | 6 |
| `sm8-server/src/main/scala/io/sm8/server/EngineLoader.scala` | 5 |
| `sm8-platform/src/main/scala/io/sm8/platform/query/RestatedEngineRunner.scala` | 5 |
| `sm8-platform/src/main/scala/io/sm8/platform/query/PlatformModelLoader.scala` | 5 |
| `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala` | 5 |
| `sm8-platform/src/main/scala/io/sm8/platform/query/QueryResult.scala` | 4 |
| `sm8-platform/src/main/scala/io/sm8/platform/query/QueryRequest.scala` | 4 |
| `sm8-platform/src/main/scala/io/sm8/platform/query/HttpTransport.scala` | 4 |
| `sm8-core/src/main/scala/io/sm8/core/query/QueryBuilder.scala` | 3 |
| `sm8-core/src/main/scala/io/sm8/core/Pipeline.scala` | 3 |
| `sm8-platform/src/main/scala/io/sm8/platform/query/hooks/EngineHookTypes.scala` | 2 |
| `sm8-platform/src/main/scala/io/sm8/platform/query/hooks/EngineHookDispatcher.scala` | 2 |
| `sm8-core/src/main/scala/io/sm8/sdk/ResultRows.scala` | 1 |
| (1 file total from additional scan) | ... |
| **Total** | **76** |

---

## Decision

Per `karpathy-guidelines-mindset` "smallest correct change":

1. **Strip the `[[skill-name-mindset]]` bracket pattern** from each occurrence.
2. **Preserve the rationale** (the prose content without the agent-flavored citation).
3. **Fix only `[[skill-name-mindset]]` patterns**, NOT other noise (the noise scanner also flags `<!-- ... -->` HTML comments which are out of scope for this PR; per PR-130 history the broader sweep is a separate PR).

---

## Skill alignment

### `scala2-scaladoc-mindset`

- **Apply §1:** strip `[[skill-name-mindset]]` brackets; replace with prose.
- **Apply §3:** the rationale is preserved (WHY a class/method exists), the citation (WHICH skill said it) is removed.
- **Apply §4:** no TODOs without attribution; no `[[wikilinks]]` to skill files.

---

## Files touched

| Pattern | Action |
|---|---|
| `[[karpathy-guidelines-mindset]]` | strip brackets; preserve prose |
| `[[karpathy-impact-analysis-mindset]]` | strip brackets; preserve prose |
| `[[scala-data-driven-refactor-mindset]]` | strip brackets; preserve prose |
| `[[scala-jvm-safety-mindset]]` | strip brackets; preserve prose |
| `[[scala-spark-batch-bugs-mindset]]` | strip brackets; preserve prose |
| `[[scala-error-handling-mindset]]` | strip brackets; preserve prose |
| (any other `[[skill-name]]` pattern) | strip brackets; preserve prose |

### Example transform

**Before** (per scala2-scaladoc-mindset, this is a noise pattern):
```scala
/**
 * Per [[karpathy-guidelines-mindset]]: a singleton `object` (not a
 * class) for the factory method.
 */
object FooBuilder { ... }
```

**After**:
```scala
/**
 * Singleton object (not a class) for the factory method. The singleton
 * pattern keeps the public API surface narrow.
 */
object FooBuilder { ... }
```

---

## Acceptance criteria

1. The 76 noise patterns in the 17 files are removed.
2. Each removal preserves the prose rationale (no information loss).
3. `python3 scripts/check_scaladoc_noise.py` reports 0 noise patterns in the 17 files (modulo pre-existing `<!---->` HTML comments that are out of scope).
4. The 911 existing tests pass (zero regression; this is a documentation-only change).

## Verification plan

```bash
# 1. After PR-147 lands + merges:
mvn -B -ntp -pl sm8-core,connectors/spark-connector,connectors/in-memory-connector,connectors/trino-connector,plugins/audit-plugin,plugins/cache-plugin,sm8-cli,sm8-server,sm8-platform test 2>&1 | grep -E 'Tests: succeeded|BUILD' | tail -10
# 2. Verify noise removed:
python3 scripts/check_scaladoc_noise.py sm8-platform/src/main/scala/io/sm8/platform/query/QueryService.scala 2>&1 | tail -3
# 3. javap: stable checkcast count
# 4. Memory + disk under 90% throughout
```

## Risks

| Risk | Mitigation |
|---|---|
| Stripping the bracket pattern accidentally removes useful context | The replacement preserves the original prose verbatim; only the `[[skill-name-mindset]]` prefix is removed |
| Other noise patterns (e.g. `<!-- ... -->` HTML comments, `// TODO` notes) remain | Out of scope per PR-130 history; a separate sweep PR can address them |

## Open questions

1. Should this PR also sweep the test files? My recommendation: **NO** — PR-130 already swept the test files; the current 17-file sweep is production-only.
2. Should the ADR reference the skill names directly? My recommendation: **YES** (the skill names are facts about the codebase discipline; the noise is the bracket-format citation, not the skill name itself).
3. Should the sweep also strip `<!-- ... -->` HTML-style comments? My recommendation: **NO** for this PR (out of scope; could be a separate sweep).

---

## ADR

`docs/adr/0008-ag-scaladoc-skill-wikilink-sweep.md` v1.0 (full ADR; ~150 lines).
