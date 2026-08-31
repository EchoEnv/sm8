#!/usr/bin/env python3
"""
check_md_doc_narration.py — flags internal process-narration in stable
.md documentation files (architecture specs, design docs, cross-engine
matrices, RFCs).

A heuristic linter — not a compiler check. It will have false
positives on legitimate text that happens to match a pattern. Review
flagged lines; don't blindly delete.

This is a sibling of `check_scaladoc_noise.py` but scoped specifically
to .md files: the patterns are tuned for stable documentation (RFC
specs, design docs, cross-reference matrices) where a process handle
like `(was added in PR-NNN)` or `(introduced in PR-NNN)` is narration
rather than context.

Usage:
    python3 check_md_doc_narration.py <file_or_dir> [<file_or_dir> ...]
    python3 check_md_doc_narration.py docs/rfcs/2026-08-12_v1_architecture-spec/

Exit code: 0 if clean, 1 if any noise found.
"""

import re
import sys
from pathlib import Path

# (pattern, human-readable reason)
NOISE_PATTERNS = [
    (r"\b(?:was added|introduced|added|merged|fixed|see|sprints?)\s+in\s+pr-?\d+\b", "stable-doc PR handle (was added in PR-NNN) — process narration"),
    (r"\b(?:was added|introduced|added|merged|fixed|see|sprints?)\s+in\s+pr-\w*(?=[a-zA-Z]\w*\d|\d\w*[a-zA-Z])\w*\b", "stable-doc PR handle with process-narration context (was added in / introduced in PR-O4g) — process narration"),
    (r"^\s*[-*]\s*PR-\w*(?=[a-zA-Z]\w*\d|\d\w*[a-zA-Z])\w*[:.]", "list item starting with mixed-letter PR handle — process narration"),
]

COMPILED = [(re.compile(p, re.IGNORECASE | re.MULTILINE), reason) for p, reason in NOISE_PATTERNS]


def check_file(path: Path):
    findings = []
    text = path.read_text(encoding="utf-8", errors="replace")
    for line_no, line in enumerate(text.splitlines(), start=1):
        for pattern, reason in COMPILED:
            for m in pattern.finditer(line):
                snippet = line.strip()[:100]
                findings.append((line_no, reason, snippet))
    return findings


def collect_md_files(paths):
    files = []
    for p in paths:
        pp = Path(p)
        if pp.is_dir():
            files.extend(sorted(pp.rglob("*.md")))
        elif pp.suffix == ".md":
            files.append(pp)
    return files


def main(argv):
    if not argv:
        print(__doc__)
        return 1

    files = collect_md_files(argv)
    if not files:
        print("No .md files found in given paths.")
        return 0

    total_findings = 0
    for f in files:
        findings = check_file(f)
        if findings:
            print(f"\n{f}")
            for line_no, reason, snippet in findings:
                print(f"  line {line_no}: {reason}")
                print(f"    > {snippet}")
            total_findings += len(findings)

    if total_findings:
        print(f"\n{total_findings} potential narration comment(s) found across {len(files)} file(s).")
        return 1
    else:
        print(f"Clean — no narration found in {len(files)} file(s).")
        return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
