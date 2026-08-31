/*
 * Linter fixture: examples that should be FLAGGED by the linter
 * extensions added in PR-246.
 *
 * Each comment below matches one of the new patterns:
 *   1. Mixed-letter PR handle (e.g. PR-O4g, PR-2A)
 *   2. Bare skill-citation in .scala source (e.g. "per scala-jvm-safety §3")
 *   3. Stable-doc PR handle with process-narration context
 *      (e.g. "was added in PR-O4g", "introduced in PR-NNN")
 *
 * Run `python3 tools/linter/check_scaladoc_noise.py tools/linter/test_fixtures/should_be_flagged.scala`
 * and expect exit 1 + findings for each comment.
 */

// 1. Mixed-letter PR handle (was PR-O4g, the original C4 T6 finding)
 * See PR-O4g for the duckdb connector shape.

// 1b. Other mixed-letter form (digits-then-letters)
// Per PR-2A for the test fixture.

/* 2. Bare skill-citation in .scala source. The linter extension
 * added in PR-246 catches the bare form (without the `[[...]]`
 * wiki-link brackets). The bracket form is OK. */

// 2b. Other bare skill-citation forms (matches the broader regex).
// Per scala-spark-batch-bugs §1 we mirror the bare form.
// Per scala-error-handling we also flag it.
// Per scala-data-driven-refactor §2 to prove the typo fix.
// Per scala-2-scaladoc §3 to prove the 2-scaladoc form is caught.

// 2c. Skill wiki-links (carve-out from pattern [27] — MUST NOT be flagged).
// * Per [[scala-jvm-safety-mindset]]: this is intentional.
// * Per [[scala-perf-testing-mindset]] §3.
// * Per [[scala-data-driven-refactor]]: bare skill wiki-link also ok.

/* 3. Stable-doc PR handle with process-narration context
 * (this is the .md linter's scope, but a process-narration
 * phrase + PR handle in a .scala comment should also be caught
 * by the .scala linter's existing rules). */

/* 3b. Other "fixed in PR-NNN" form */
class Foo
