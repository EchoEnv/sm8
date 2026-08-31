---
title: Linter fixture — should be FLAGGED by check_md_doc_narration.py
---

# Section A — process narration with digit-only PR handles (catches the "was added in PR-NNN" form)

The connector was added in PR-235 to add DuckDB support.
The example plugin template was introduced in PR-232.
Conformance suite coverage was merged in PR-234.

# Section B — process narration with mixed-letter PR handles (C4 audit gap)

The README hook-contracts section was added in PR-O4g.
The matrix document was fixed in PR-2A.
The scanner improvement shipped in PR-C2.

# Section C — standalone "see PR-NNN" reference (must catch without the trailing "in")

See PR-244 for the process-narration cleanup details.
See PR-245 for the matrix doc accuracy fixes.

# Section D — legitimate cross-references that MUST NOT be flagged

PR-244 stripped narration lines from PR-235 + PR-236 (this is a stable spec section explaining cross-references).
We backport fixes from PR-234 to older branches when needed.
See [the migration guide](https://example.com/migrate-v2) for upgrade paths.

# Section E — list item starting with mixed-letter PR handle (process narration)

- PR-O4g: hook contracts documentation
- PR-2A: example plugin template scaffold