#!/usr/bin/env python3
"""
check_scaladoc_noise.py — flags internal process noise in Scala comments.

Scans .scala files for comment content (// lines and /* */ / /** */ blocks)
and flags patterns that reference internal process artifacts (phases, PRs,
tickets, design docs, audits, skill files) or narrate a diff ("was X, now Y")
instead of describing the code's current state.

This is a heuristic linter, not a compiler check — it will have false
positives on legitimate text that happens to match a pattern (e.g. a genuine
external RFC citation). Review flagged lines; don't blindly delete.

Usage:
    python3 check_scaladoc_noise.py <file_or_dir> [<file_or_dir> ...]
    python3 check_scaladoc_noise.py .                     # scan whole repo
    python3 check_scaladoc_noise.py src/main/scala/Foo.scala

Exit code: 0 if clean, 1 if any noise found (suitable for CI / pre-commit).
"""

import re
import sys
from pathlib import Path

# (pattern, human-readable reason)
NOISE_PATTERNS = [
    (r"\bper\s+phase\s*#?\d*\b", "references an internal phase/step"),
    (r"\bphase[\s#-]+\d+\b", "bare phase reference (Phase 1 / phase-2 / Phase #3)"),
    (r"\bstep\s+\d+\s*:", "uses internal step numbering as a comment label"),
    (r"\bper\s+design\s+doc\b", "cites an internal design doc"),
    (r"\bper\s+spec\s+section\b", "cites an internal spec section"),
    (r"\bper\s+adr\b", "cites an internal ADR (architecture decision record)"),
    (r"\(this\s+pr\)", "references \"this PR\""),
    (r"\bper\s+pr\s*#?\d+\b", "references a PR number"),
    (r"\bfrom\s+pr\s*#?\d+\b", "references a PR number"),
    (r"\bpr[\s#-]*\d+\b", "bare PR number reference (PR-123 / PR #123 / PR#123)"),
    (r"\bpr-\w*(?=[a-zA-Z]\w*\d|\d\w*[a-zA-Z])\w*", "mixed-letter PR handle reference (PR-O4g / PR-2A) — internal process artifact"),
    (r"\badr[\s#-]*\d+(?:-[a-z]{1,2})?\b", "bare ADR number reference (ADR-008 / ADR-008-AI) — internal design record"),
    (r"\badr\s*§\s*[a-zA-Z0-9]+", "bare ADR section reference (ADR §C1)"),
    (r"\brfc\s*(?:§|ss)\s*\w+", "internal RFC section reference (RFC §3 / RFC SS3) — external RFC-<digits> standards kept allowed"),
    (r"\bwas\s+added\s+in\s+pr\s*#?\d+\b", "stable-doc PR handle reference (was added in PR-NNN) — internal process artifact"),
    (r"\bintroduced\s+in\s+pr\s*##?\d+\b", "stable-doc PR handle reference (introduced in PR-NNN) — internal process artifact"),
    (r"\bas\s+reviewed\s+in\s+pr\b", "references a PR review"),
    (r"\baudit\s+fix\b", "references an internal audit"),
    (r"\bper\s+the\s+audit\b", "references an internal audit"),
    (r"\bper\s+ticket\b", "references a ticket outside a TODO/FIXME line"),
    (r"\bas\s+discussed\b", "references an internal conversation"),
    (r"\bper\s+our\s+conversation\b", "references an internal conversation"),
    (r"\bas\s+requested\b", "references a request rather than stating the reason"),
    (r"\bper\s+instructions\b", "references instructions rather than stating the reason"),
    (r"\(was\s+a\b", "narrates a diff (\"was a ...\") instead of describing current state"),
    (r"\bis\s+replaced\s+with\b", "narrates a diff (\"replaced with\") instead of describing current state"),
    (r"\bearlier\s+internal-only\s+version", "references internal version history"),
    (r"\[\[[a-z0-9]+(-[a-z0-9]+)+\]\]", "double-bracket reference looks like a skill/tool name, not a Scala symbol"),
    (r"\bper\s+scala-(?:jvm-safety|spark-batch-bugs|error-handling|data-driven-refactor|jar-packaging|perf-testing|2-scaladoc)(?:\s+§\d+|\b)", "bare skill-citation in .scala source (per scala-X §N) — should use `[[scala-X]]` wiki-link form instead"),
]

COMPILED = [(re.compile(p, re.IGNORECASE), reason) for p, reason in NOISE_PATTERNS]

# Lines that are TODO/FIXME are allowed to reference tickets.
TODO_LINE = re.compile(r"//\s*(TODO|FIXME)\b", re.IGNORECASE)

LINE_COMMENT = re.compile(r"//(.*)$")
BLOCK_COMMENT = re.compile(r"/\*(.*?)\*/", re.DOTALL)


def extract_comment_spans(text: str):
    """Yield (start_line, comment_text) for every // and /* */ comment."""
    for m in BLOCK_COMMENT.finditer(text):
        start_line = text.count("\n", 0, m.start()) + 1
        yield start_line, m.group(1)
    for i, line in enumerate(text.splitlines(), start=1):
        m = LINE_COMMENT.search(line)
        if m and not TODO_LINE.search(line):
            yield i, m.group(1)


def check_file(path: Path):
    findings = []
    text = path.read_text(encoding="utf-8", errors="replace")
    for start_line, comment in extract_comment_spans(text):
        for pattern, reason in COMPILED:
            for m in pattern.finditer(comment):
                # Approximate line number within multi-line block comments.
                offset_line = comment.count("\n", 0, m.start())
                line_no = start_line + offset_line
                snippet = comment.splitlines()[offset_line].strip() if "\n" in comment else comment.strip()
                findings.append((line_no, reason, snippet[:100]))
    return findings


def collect_scala_files(paths):
    files = []
    for p in paths:
        pp = Path(p)
        if pp.is_dir():
            files.extend(sorted(pp.rglob("*.scala")))
        elif pp.suffix == ".scala":
            files.append(pp)
    return files


def main(argv):
    if not argv:
        print(__doc__)
        return 1

    files = collect_scala_files(argv)
    if not files:
        print("No .scala files found in given paths.")
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
        print(f"\n{total_findings} potential noise comment(s) found across {len(files)} file(s).")
        return 1
    else:
        print(f"Clean — no noise patterns found in {len(files)} file(s).")
        return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
