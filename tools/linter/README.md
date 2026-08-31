# Linter extensions for SM8 (PR-246)

Two scripts in this directory extend the
`scala2-scaladoc` skill (which lives at `~/.agents/skills/scala2-scaladoc/scripts/`)
with patterns that the C4 second-wayfinder audit (map #237, closed)
flagged as missing.

## `check_scaladoc_noise.py` (modified copy of the skill's original)

Modifications from the upstream skill:

1. **Mixed-letter PR handle** — the original `\bpr[\s#-]*\d+\b` only catches
   digit-only PR handles (`PR-123`, `PR-244`). The C4 audit
   flagged `PR-O4g` (mixed letter+digit) as a gap. New pattern:
   `\bpr-\w*(?=[a-zA-Z]\w*\d|\d\w*[a-zA-Z])\w*` matches mixed-letter
   handles like `PR-O4g`, `PR-2A` while correctly rejecting
   digit-only `PR-123`.
2. **Bare skill-citation in .scala source** — original
   `\[\[[a-z0-9]+(-[a-z0-9]+)+\]\]` only catches the `[[...]]`
   wiki-link form. C4 T6 flagged that the bare form
   (e.g. `Per scala-jvm-safety §3` without brackets) bypasses the
   linter. New pattern:
   `\bper\s+scala-(?:jvm-safety|spark-batch-bugs|error-handling|data-drivenrefactor|jar-packaging|perf-testing|scala2-scaladoc)(?:\s+§\d+|\b)` —
   catches the bare form. Bracket form remains OK.

## `check_md_doc_narration.py` (new file, sibling)

A new linter for stable documentation files (RFCs, design docs, cross-engine
matrices). The original skill is .scala-only; this script is .md-only and
catches process narration in stable docs that the original couldn't reach.

Patterns:

- `was added in PR-NNN` / `introduced in PR-NNN` / `added in PR-NNN` /
  `merged in PR-NNN` / `fixed in PR-NNN` /
  `See PR-NNN` (standalone, no `in`) /
  `See PR-NNN in ...` / `sprints in PR-NNN` — stable-doc
  PR handle with process-narration context. (Plain `PR-123` mentions in
  cross-references are OK; only the narration phrasing is flagged.)
- Mixed-letter PR handle with process-narration context
  (bullet starting with mixed-letter handle + colon/period): same
  `(?=[a-zA-Z]\w*\d|\d\w*[a-zA-Z])\w*` lookahead as the .scala linter
  extension.

## Test fixtures

- `test_fixtures/lint_clean.scala` — synthetic .scala with
  legitimate (non-narration) comments. Linter should report
  "Clean — no noise patterns found."
- `test_fixtures/should_be_flagged.scala` — synthetic .scala with
  examples of the new patterns. Linter should report exit 1 + each
  comment flagged.
- `test_fixtures/should_be_flagged.md` — synthetic .md with examples
  of the stable-doc narration patterns (digit-only + mixed-letter + standalone
  `See PR-NNN`). Linter should report exit 1 with 9 findings (3 digit-only
  "added/introduced/merged in PR-NNN", 2 mixed-letter "added/fixed in PR-X",
  2 standalone "See PR-NNN", 2 mixed-letter list-item prefixes). Section D
  (legitimate cross-references without narration) MUST remain unflagged.

## Usage

```bash
# Original (use from the global skill or copy in your repo)
python3 ~/.agents/skills/scala2-scaladoc/scripts/check_scaladoc_noise.py <file_or_dir>

# PR-246 modified copy (in this repo; same usage)
python3 tools/linter/check_scaladoc_noise.py <file_or_dir>

# PR-246 new sibling (in this repo; .md only)
python3 tools/linter/check_md_doc_narration.py <file_or_dir>

# CI integration (suggested)
mvn -q \
  -pl none \
  -DskipTests \
  exec:exec \
  --non-interactive \
  -Dexec.executable=python3 \
  -Dexec.args="tools/linter/check_scaladoc_noise.py connectors/ plugins/ sm8-core/src/main"
```

(The CI integration example isn't wired up in this PR — that's a
follow-up if the team wants lint-as-CI.)
