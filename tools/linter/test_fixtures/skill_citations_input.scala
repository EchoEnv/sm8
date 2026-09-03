/*
 * Linter fixture: examples that should be STRIPPED by
 * tools/linter/apply_linter_skill_citations.py.
 *
 * Run:
 *   python3 tools/linter/apply_linter_skill_citations.py tools/linter/test_fixtures/skill_citations_input.scala
 *
 * Expected output: 9 replacements (per the per-line fixtures below).
 * Expected content: tools/linter/test_fixtures/skill_citations_expected.scala
 */

// Bare form (no -mindset suffix):
 * Per scala-jvm-safety: this object has no mutable state.

// Bare form with §N:
 * Per scala-spark-batch-bugs §2: every assertion is on the evaluated result.

// -mindset suffix (correct):
 * Per scala-data-driven-refactor-mindset §1: data in core, behavior in adapters.

// -mindset suffix with mantra #N:
 * Per scala-jvm-safety-mindset mantra #3: no static / ThreadLocal state.

// Typo form (missing hyphen between handling and mindset):
 * Per scala-error-handlingmindset §1: errors are data.

// Lowercase "per":
 * per scala-perf-testing: warm the JIT before measuring.

// Bracket form (also stripped per PR-288):
 * Per [[scala-jar-packaging-mindset]] §1: tooling artifacts.

// Typo bracket form:
 * Per [[karphyaguidsmindset]]: typo.

// Typo bare form:
 * Per scala-data-driven-refacer: typo.
