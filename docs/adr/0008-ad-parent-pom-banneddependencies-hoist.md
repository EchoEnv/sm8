# ADR-008-AD: Parent POM — hoist `bannedDependencies=org.apache.spark:*` to enforce Zero-Spark invariant globally

| Field | Value |
| **Status** | **v1.2 — dummy-module negative test removed (acceptance criterion #4 superseded)** |
| **Date** | 2026-08-27 (v1.2 supersedes v1.1) |
| **Module** | parent `pom.xml` + 8 child modules (sm8-core, sm8-platform, sm8-cli, sm8-server, connectors/spark-connector, connectors/in-memory-connector, connectors/trino-connector) |
| **Closes** | Senior Architect full-codebase review HIGH-4 (the deferred parent-POM enforcer hoist) |
| **Author** | Wave 2 PR-141; v1.2 by user request 2026-08-27 |
| **Skill alignment** | `karpathy-guidelines-mindset`, `karpathy-app-design-mindset`, `karpathy-impact-analysis-mindset`, `karpathy-guidelines-mindset`, `debug-mantra-mindset`, `scala2-scaladoc-mindset` |

## Decision-at-a-glance

Hoist the `maven-enforcer-plugin` + `bannedDependencies=org.apache.spark:*` rule to the parent POM's `<pluginManagement>`. The 7 non-spark-connector modules (sm8-core, sm8-platform, sm8-cli, sm8-server, connectors/in-memory-connector, connectors/trino-connector) inherit the rule automatically. The spark-connector overrides via `<skip>true</skip>` because it's the SOLE module allowed to depend on Spark.

## Revision history

| Version | Date | Change |
|---|---|---|
| v1.0 | 2026-08-22 | Initial draft — hoist bannedDependencies to parent |
| v1.1 | 2026-08-22 | Review fixes — Maven <build><plugins> in parent does NOT auto-apply executions to children (only <pluginManagement> provides executions when child re-declares the plugin). The proposed hoist alone does not work. v1.1 takes a different approach: keep per-module blocks BUT add a parent <pluginManagement> entry + a doc comment + a `validate` step that scans all reactor modules for missing rules. |
| v1.2 | 2026-08-27 | User requested removal of `dummy-spark-test-verifies-rule/` (the standalone executable verification harness for criterion #4). Both senior advisors (architect + data-engineer) confirmed removal is dependency-clean but flagged that criterion #4 loses its only executable verification. **Acceptance criterion #4 superseded** by a one-line positive smoke check (`mvn -pl sm8-core enforcer:enforce@enforce-no-spark`) that proves the rule is wired up and would fire on a violation; the rule's `<pluginManagement>` template + per-module blocks remain unchanged. Note: criterion #4 was a negative-falsifying test by design; the replacement is a positive regression check, which is a weaker guarantee — captured as a known coverage gap. |

**v1.2 review note (not blocking):** the v1.1-era module counts in the body (e.g. L13 "7 non-spark-connector modules", L35 "7 modules each declare…", L6 header "8 child modules") predate the parent POM's growth to 14 modules (13 non-spark + 1 spark-connector). Left untouched in v1.2 because reconciling the count requires a separate audit of which plugin modules actually carry the `enforce-no-spark` per-module block today (a scope-creep concern unrelated to the dummy-dir removal). Captured here for the next ADR pass. |

---

## Context

### Senior Architect full-codebase review (2026-08-21)

> **HIGH**: Parent POM `bannedDependencies` rule is per-module, not global. The Spark-banned enforcer rule is duplicated in each non-connector module's pom. A future contributor adding `semanticdf-platform`, `semanticdf-mcp`, `semanticdf-cli` (per agile-kindling-beacon plan steps 10/11) MUST remember to copy the enforcer block. If they don't, the new module will silently allow Spark.
> Per ADR-008-O §P2-6 (deferred fix).
> Fix: Hoist the rule to the parent's `<pluginManagement>`. The spark-connector module overrides via `<configuration>` (the spark-connector uses the inverted rule `requireUpperBoundDeps`).

### Current state (2026-08-22)

- 7 modules (sm8-core, sm8-platform, sm8-cli, sm8-server, connectors/spark-connector, connectors/in-memory-connector, connectors/trino-connector) each declare `maven-enforcer-plugin` + `enforce-no-spark` execution + `<configuration><rules><bannedDependencies>...</bannedDependencies></rules></configuration>` in their own `pom.xml`.
- The spark-connector uses the **inverted** rule: `<bannedDependencies>` excluding `org.apache.spark:spark-sql_2.13` (so it must use 3.5.x) but the `<exclude>org.apache.spark:*</exclude>` is **removed** (because spark-connector IS allowed to depend on Spark).
- The other 6 modules all use the same `<exclude>org.apache.spark:*</exclude>`.

### Codegraph evidence (2026-08-22)

- All 6 non-spark-connector modules: `<pluginManagement>` block in each `pom.xml`:
  - `sm8-core/pom.xml:97-128` (32 LOC)
  - `sm8-platform/pom.xml` (similar block)
  - `sm8-cli/pom.xml` (similar block)
  - `sm8-server/pom.xml` (similar block)
  - `connectors/in-memory-connector/pom.xml` (similar block)
  - `connectors/trino-connector/pom.xml` (similar block)
- Parent `pom.xml` has `<pluginManagement>` with 6 plugins (scala-maven-plugin, maven-compiler-plugin, maven-enforcer-plugin [NO execution/config], mima-maven-plugin, maven-source-plugin, scalatest-maven-plugin).
- `connectors/spark-connector/pom.xml:93-145` uses the inverted rule `enforce-spark-only-in-spark-connector` (different `id` + different `<excludes>`).

### Why hoist to parent POM

Per `karpathy-app-design-mindset` "frozen core + extension portal":
- The Zero-Spark invariant (sm8-core is Spark-free) is part of the **frozen core**. It must be enforced globally.
- The `bannedDependencies` rule is the **canonical mechanism** in the parent POM's `<pluginManagement>` for enforcing global architectural boundaries.
- A future contributor adding a new module (e.g. `semanticdf-platform`, `semanticdf-mcp`, `semanticdf-cli` per agile-kindling-beacon plan steps 10/11) automatically inherits the rule.

### Why not just keep the per-module blocks

- 6 modules × 32 LOC = 192 LOC duplicated.
- Risk of contributor forgetting to copy the rule (per Architect review).
- Drift between modules (sm8-core says `org.apache.spark:*`, sm8-platform says the same — but a contributor might accidentally add a different message or exclude a different set).
- The `bannedDependencies` rule is already partially in the parent POM (`<pluginManagement>` lists `maven-enforcer-plugin:3.4.1` without execution/config).

---

## Considered options

### Option A: Hoist to parent + spark-connector skip

Hoist the rule to parent `<pluginManagement>` with `<execution>` + `<configuration>` for `bannedDependencies=org.apache.spark:*`. The spark-connector adds `<configuration><skip>true</skip></configuration>` to opt out (it's the ONLY module allowed to depend on Spark).

**Pros:**
- All non-connector modules automatically inherit the rule.
- spark-connector explicitly opts out.
- Future modules automatically inherit the rule.
- Single source of truth for the Zero-Spark invariant.
- Net LOC reduction (~192 → ~60).

**Cons:**
- Requires a verification: a new dummy module with Spark dep must fail the build.
- The spark-connector's skip must be tested (intentional skip, not accidental).
- The parent POM is touched (large blast radius; needs careful review).

**Decision: ADOPT** (this is the Architect's recommended fix).

### Option B: Keep per-module + add CI lint

Keep the per-module blocks as-is. Add a custom Maven-enforcer-plugin rule in CI that scans all module poms and fails if any non-spark-connector module is missing the rule.

**Pros:**
- No parent POM change.
- The lint rule is mechanical.

**Cons:**
- The lint rule itself is a per-pom scan (still per-module).
- Drift between the lint rule and the actual rules is possible.
- Doesn't help future modules — they still need to add the rule.

**Decision: REJECT.** Less robust than Option A.

### Option C: Hoist to parent + leave spark-connector alone

Hoist the rule to parent. The spark-connector adds NO configuration (its existing inverted rule continues to work).

**Pros:**
- Even simpler.

**Cons:**
- The parent's `bannedDependencies` rule would FIRE on spark-connector's `org.apache.spark:spark-core` and `org.apache.spark:spark-sql` dependencies, breaking the build.

**Decision: REJECT.** Spark-connector's deps are excluded by its own rule, but a child POM can override parent's enforcer config to opt out.

---

## Decision outcome

**Adopt Option D (v1.1 revised) — keep per-module blocks + add a parent `<pluginManagement>` template + a `validate` step that scans all reactor modules for missing rules**.

### Implementation plan

1. Add the enforcer config to parent's `<pluginManagement>`:
   ```xml
   <plugin>
     <groupId>org.apache.maven.plugins</groupId>
     <artifactId>maven-enforcer-plugin</artifactId>
     <version>3.4.1</version>
     <executions>
       <execution>
         <id>enforce-no-spark</id>
         <goals><goal>enforce</goal></goals>
       </execution>
     </executions>
     <configuration>
       <rules>
         <bannedDependencies>
           <excludes>
             <exclude>org.apache.spark:*</exclude>
           </excludes>
           <message>
             The Zero-Spark invariant: this module must remain Spark-free.
             Spark classes live in connectors/spark-connector/pom.xml, not here.
           </message>
         </bannedDependencies>
       </rules>
     </configuration>
   </plugin>
   ```
2. **Remove the per-module blocks** from sm8-core, sm8-platform, sm8-cli, sm8-server, connectors/in-memory-connector, connectors/trino-connector (6 modules).
3. **Override in spark-connector**: add `<configuration><skip>true</skip></configuration>` to opt out (it's the ONLY module allowed to depend on Spark).
4. **Add a verification test**: a new `dummy-module` test file (in a temporary test directory) with a Spark dependency that must fail the build.

### Files touched

| File | Change | LOC |
|---|---|---|
| `pom.xml` (parent) | Add `<plugin>` to `<pluginManagement>` | +30, -0 = +30 net |
| `sm8-core/pom.xml` | Remove the per-module block | -28, +0 = -28 net |
| `sm8-platform/pom.xml` | Remove the per-module block | -28, +0 = -28 net |
| `sm8-cli/pom.xml` | Remove the per-module block | -28, +0 = -28 net |
| `sm8-server/pom.xml` | Remove the per-module block | -28, +0 = -28 net |
| `connectors/in-memory-connector/pom.xml` | Remove the per-module block | -28, +0 = -28 net |
| `connectors/trino-connector/pom.xml` | Remove the per-module block | -28, +0 = -28 net |
| `connectors/spark-connector/pom.xml` | Add `<configuration><skip>true</skip></configuration>` | +5, -0 = +5 net |
| `docs/adr/0008-ad-parent-pom-banneddependencies-hoist.md` | This ADR | NEW |
| **Total** | | **+30, -168 = -138 net** |

### Tests to add

1. **Build-time verification**: after PR-141 lands, run `mvn -pl sm8-core,sm8-platform,sm8-cli,sm8-server,connectors/in-memory-connector,connectors/trino-connector test` — must pass (the rule is inherited and passes).
2. **Dummy-module negative test**: add a temporary test directory `dummy-spark-test/` with a Spark dependency + a single compile step. Run `mvn -pl dummy-spark-test compile` — must FAIL with the "Zero-Spark invariant" message.

### Binary compatibility

- **Source-compatible**: the Maven artifact set is unchanged.
- **Build-compatible**: the build command sequence is unchanged.

---

## Skill alignment

### `karpathy-guidelines-mindset`

- **Apply "smallest correct change":** the parent POM is the single source of truth for the enforcer rule; 6 modules can shed their per-module blocks.
- **Apply "verifiable success":** the dummy-module negative test verifies the rule fires.

### `karpathy-app-design-mindset`

- **Apply "frozen core + extension portal":** the Zero-Spark invariant is part of the frozen core; the enforcer rule enforces it globally.
- **Apply:** future modules automatically inherit the rule.

### `karpathy-impact-analysis-mindset`

- **Apply:** 7 modules touched; 0 production code touched; the rule's semantics are unchanged.
- **Apply:** the spark-connector opt-out is a mechanical `<skip>true</skip>` config.

### `debug-mantra-mindset`

- **Apply SS1 (reproduce):** the dummy-module negative test reproduces a violation attempt.
- **Apply SS5 (verify):** the existing 911 tests must continue to pass after the hoist.

### `scala2-scaladoc-mindset`

- **Apply §1:** the ADR explains WHY the rule is hoisted; no PR/Phase/ADR/process references in the code.
- **Apply §3:** TODOs are attributed (`// FUTURE: remove after spring 4.x upgrade` etc.).

---

## Acceptance criteria

1. The parent's `<pluginManagement>` contains the `maven-enforcer-plugin` + `enforce-no-spark` execution + `<configuration>` for `bannedDependencies=org.apache.spark:*`.
2. The 6 non-spark-connector modules do NOT have the per-module block (they inherit from parent).
3. The spark-connector module has `<configuration><skip>true</skip></configuration>` to opt out.
4. ~~**SUPERSEDED by v1.2 (2026-08-27)**~~ — The dummy-module negative test FAILS the build with the "Zero-Spark invariant" message. **Replaced by criterion #4' (positive smoke check).**
5. The existing 911 tests pass (zero regression).
6. The full reactor `mvn -pl ... test` passes.

### Criterion #4' — positive smoke check (replaces #4 in v1.2)

A one-line positive check proves the `enforce-no-spark` execution is wired up and would fire on a violation. Run from the repo root:

```bash
mvn -B -ntp -pl sm8-core enforcer:enforce@enforce-no-spark
```

**Caveat (read before relying on this):** `enforcer:enforce@<executionId>` proves the rule is *bound* to the module pom on a clean codebase. If a future contributor silently removed the `bannedDependencies` block from `sm8-core/pom.xml`, this command would no-op rather than fail (the goal resolves to a no-op when no matching execution is declared). It is a positive regression check, not a falsifying test. Pair it with `mvn -B -ntp validate` (which walks every reactor module) to catch a missing per-module block; that pairing is the recommended regression line for v1.2 onwards.

### Known coverage gap (v1.2)

Criterion #4 was a **negative-falsifying** test (a synthetic violation attempt that must be rejected). The replacement criterion #4' is a **positive** regression check (the rule passes on the clean codebase). The two are not equivalent: a positive check confirms the rule is present and active, but does not prove it actually rejects a violating dep at build time. The enforcer plugin's `bannedDependencies` rule is well-documented Maven behavior; the gap is the loss of in-repo executable proof that the rule fires. If a future contributor wants to close the gap, see ADR-008-AD v1.2 §"Follow-up" below.

## Verification plan

```bash
# 1. After PR-141 lands + merges:
mvn -B -ntp -pl sm8-core,connectors/spark-connector,connectors/in-memory-connector,connectors/trino-connector,plugins/audit-plugin,plugins/cache-plugin,sm8-cli,sm8-server,sm8-platform test 2>&1 | grep -E 'Tests: succeeded|BUILD' | tail -10
# 2. v1.2: positive smoke check (replaces the removed dummy-module negative test):
mvn -B -ntp -pl sm8-core enforcer:enforce@enforce-no-spark 2>&1 | tail -5
# 2b. v1.2: pair the smoke check with a reactor-wide validate to catch missing per-module blocks:
mvn -B -ntp validate 2>&1 | tail -5
# 3. Memory + disk under 90% throughout
```

## Follow-up (v1.2)

If a future contributor wants to close the known coverage gap (loss of in-repo falsifying test for the `enforce-no-spark` rule), two options exist:

1. **Inline negative test in `sm8-core` test-jar.** Add a `MavenPluginLoadingTest` that programmatically constructs a `MavenProject` with `org.apache.spark:spark-core_2.13:3.5.x` as a runtime dependency, invokes `EnforcerMojo`, and asserts the rule rejects it. Reuses the in-repo test infrastructure.
2. **External CI step.** Add a GitHub Actions job that runs `mvn -pl <existing-module> enforcer:enforce` against a temporary pom with a Spark dep added, and asserts the build fails. Independent of the reactor.

Both close the gap with different trade-offs; not adopted in v1.2 per user decision.

## Risks

| Risk | Mitigation |
|---|---|
| Parent POM change breaks a non-connector module that depends on a different parent plugin config | Verified: all 6 modules use the same `bannedDependencies=org.apache.spark:*` config |
| spark-connector's `<skip>true</skip>` accidentally skips the `bannedDependencies` rule entirely (not just for Spark) | spark-connector adds an INDEPENDENT `requireUpperBoundDeps` rule for Spark versions + a separate `bannedDependencies` excluding `org.apache.spark:spark-sql_2.13` (for version alignment) |
| Future contributor adds a new module without thinking about the rule | The rule is inherited; no opt-out is needed unless the module is allowed to depend on Spark |
| The dummy-module negative test is fragile (CI dependency on Maven build) | **SUPERSEDED by v1.2** — the dummy-module test has been removed per user decision. The coverage gap is documented in §"Known coverage gap" and §"Follow-up". |

## Open questions

1. Should the parent's `bannedDependencies` rule exclude more than just Spark (e.g. `org.apache.flink:*`, `org.apache.beam:*`)? My recommendation: **NO** for v1 — the Zero-Spark invariant is the only one the senior dual review flagged; other engine boundaries can be added in future ADRs.
2. Should the spark-connector's inverted rule also be hoisted to the parent? My recommendation: **NO** — the inverted rule is specific to the spark-connector; hoisting it would require every other connector to opt out, which is more fragile than the current per-module opt-out.
3. Should the parent POM add a `mvn validate` step that fails if any module pom re-declares `enforce-no-spark` (drift detector)? My recommendation: **OUT OF SCOPE** for PR-141; add as a follow-up if drift reappears.
