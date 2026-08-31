/*
 * Linter fixture: examples that should be REWRITTEN by
 * tools/linter/apply_linter_skill_citations.py.
 *
 * Run:
 *   python3 tools/linter/apply_linter_skill_citations.py tools/linter/test_fixtures/skill_citations_input.scala
 *
 * Expected output: 6 replacements.
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