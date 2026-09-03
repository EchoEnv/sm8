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
 * this object has no mutable state.

// Bare form with §N:
 * every assertion is on the evaluated result.

// -mindset suffix (correct):
 * data in core, behavior in adapters.

// -mindset suffix with mantra #N:
 * no static / ThreadLocal state.

// Typo form (missing hyphen between handling and mindset):
 * errors are data.

// Lowercase "per":
 * warm the JIT before measuring.

// Bracket form (also stripped per PR-288):
 * tooling artifacts.

// Typo bracket form:
 * typo.

// Typo bare form:
 * typo.
