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
2. **Bare + bracket skill-citation in .scala source** — the original
   `\[\[[a-z0-9]+(-[a-z0-9]+)+\]\]` only catches the `[[...]]`
   wiki-link form. PR-246 added a bare-form catch. PR-288 reverses
   PR-247's bracket carve-out: BOTH bare and bracket forms are
   noise. The new bare-form rule catches typo / drift forms
   (`karphyaguids`, `scala-data-driven-refacer`, `mindset-…`, drift
   like `scala-jvm-safety-typo`). Bare valid skill names (the 10
   `scala-*` skills + `karpathy-guidelines` + `karpathy-app-design`
   + `debug-mantra`) are allowed by the linter because the bracket
   wiki-link form is the noise form the linter focuses on; the
   stripper (`apply_linter_skill_citations.py`) catches bare forms at
   rewrite time. The correct Scaladoc per the scala2-scaladoc skill
   cites no internal skill at all.

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

- `test_fixtures/lint_clean.scala` — clean control fixture. Plain
  prose .scala comments with no flagged noise patterns. Linter should
  report "Clean — no noise patterns found."
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

# PR-288 strip helper: remove `Per scala-X` skill-citations from
# .scala source (both bare form and bracket wiki-link form). Per the
# scala2-scaladoc skill: no internal process noise — skill names,
# whether bare or bracketed, are internal process metadata. PR-247 had
# converted bare form to `[[scala-X-mindset]]`; PR-288 inverts that:
# the correct Scaladoc describes behavior without citing an internal skill.
# Rewrites in place; verify the diff before committing.
python3 tools/linter/apply_linter_skill_citations.py <file_or_dir>

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

## PR-288: skill-citation stripping (replaces PR-247's bracket-rewrite)

Per the scala2-scaladoc skill ("no internal process noise"), skill
names are internal process metadata regardless of form. Both the
bare form (`Per scala-jvm-safety §3`) and the bracket wiki-link form
(`Per [[scala-jvm-safety-mindset]] §3`) reference internal skills by name.

PR-247's approach was to convert bare → bracket form (treating the
bracket form as canonical sm8 convention). PR-288 reverses that
decision: skill citations are stripped entirely. The correct Scaladoc
describes the behavior or links to an external symbol; it never cites
an internal skill by name.

Changes from PR-247:

- `check_scaladoc_noise.py` — the bracket-form carve-out for the 7 known
  `scala-*` skill names is **removed**. Every `[[scala-X-mindset]]` /
  `[[karphyaguidsmindset]]` / etc. is now flagged as internal noise.
- The bare-form rule is updated: it now flags BOTH valid skill names
  (the 7 in the allowlist) AND typo skill names
  (`karphyaguids*`, `scala-data-driven-refactor`).
- `apply_linter_skill_citations.py` — instead of converting bare → bracket
  form, it now **strips** the leading citation clause entirely. Bare
  form, bracket form, and typo forms are all stripped.

### PR-288 fixtures

- `test_fixtures/skill_citations_input.scala` — input fixture with 9 variants
  (bare, `§N`, `-mindset`, `-mindset mantra #N`, typo, lowercase `per`,
  bracket form, typo bracket form, typo bare form).
- `test_fixtures/skill_citations_expected.scala` — expected output
  (all citations stripped, header comments preserved).
- `test_fixtures/should_be_flagged.scala` — updated to flag the bracket
  form as noise (PR-247 originally carved it out).
