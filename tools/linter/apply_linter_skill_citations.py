#!/usr/bin/env python3
"""
Strip leading `Per <skill-name>` clauses in .scala source.

The leading clause names an internal skill; per the scala2-scaladoc skill,
Scaladoc describes behavior without citing an internal skill name. Both
bare form (`Per scala-X §N`) and bracket form (`Per [[scala-X-mindset]]`)
are noise and are dropped wholesale. Typo drift forms (`karphyaguids*`,
`scala-data-driven-refacer`) are dropped too.

Rules (replacement is the empty string):

  Per scala-X                    -> ''
  Per scala-X-mindset            -> ''
  Per scala-X §N                 -> ''
  Per [[scala-X-mindset]] ...    -> ''
  Per [[karphyaguidsmindset]]    -> ''
  Per scala-data-driven-refacer  -> ''
  Per karpathy-guidelines-mindset (bare or bracketed) -> ''
  Per debug-mantra (bare or bracketed) -> ''

The trailing whitespace after the dropped clause is also collapsed.
Only the lines that contained a stripped clause are touched — Scaladoc
indentation in `@param` lists, `* Build...` continuations, and aligned
column text elsewhere in the file is preserved byte-for-byte.

Reads a list of files (or directory trees of .scala) on argv, rewrites in place.
"""
import os
import re
import sys

# Pattern: catch the leading `Per <skill-ref>` clause.
# The skill-ref may be:
#   - bare:   `Per scala-jvm-safety`, `Per scala-spark-batch-bugs §2`
#   - bracketed: `Per [[scala-jvm-safety-mindset]]`, `Per [[karphyaguidsmindset]]`
# We accept BOTH forms (linter flags both as noise).
# The matched clause ends at the first `:` (which starts the actual
# Scaladoc sentence), OR at a sentence-final punctuation if there's
# no colon.
SKILL_PATTERN = re.compile(
    r"\b(?P<per>per)\s+"
    r"(?:\[\[)?"
    r"scala-"
    r"(?P<skill>jvm-safety|spark-batch-bugs|error-handling|data-driven-refactor|"
    r"jar-packaging|perf-testing|2-scaladoc|"
    r"bug-hunting|chaos-testing|impact-analysis|spark-streaming-bugs)"
    r"(?P<suffix>-(?P<mindset>mindset)|(?P<typo_mindset>mindset))?"
    r"(?P<bracket_close>\]\])?"
    r"(?P<trailing>\s+(?:§\s*\d+|mantra\s*#\s*\d+|section\s+\d+))?"
    r"\s*:?\s*",
    re.IGNORECASE,
)

# Also catch the typo forms (`karphyaguids*`, `scala-data-driven-refacer`)
# which are not in the allowlist and drift.
TYPO_PATTERN = re.compile(
    r"\b(?P<per>per)\s+"
    r"(?:\[\[)?"
    r"(?P<typo>karphyaguids(?:mindset)?|scala-data-driven-refacer)"
    r"(?P<bracket_close>\]\])?"
    r"(?P<trailing>\s+(?:§\s*\d+|mantra\s*#\s*\d+|section\s+\d+))?"
    r"\s*:?\s*",
    re.IGNORECASE,
)

# Catch other internal-skill citations that survived through previous
# rewrites. The bracket form (with or without -mindset) for the 7
# valid scala-* skills is also noise per the new convention.
ANY_BRACKET_PATTERN = re.compile(
    r"\b(?P<per>per)\s+"
    r"\[\[(?:"
    r"scala-(?:jvm-safety|spark-batch-bugs|error-handling|data-driven-refactor|"
    r"jar-packaging|perf-testing|2-scaladoc|"
    r"bug-hunting|chaos-testing|impact-analysis|spark-streaming-bugs)(?:-mindset)?"
    r"|karphyaguids(?:mindset)?"
    r"|scala-data-driven-refacer"
    r"|debug-mantra(?:-mindset)?"
    r"|karpathy-(?:guidelines|app-design)(?:-mindset)?"
    r")\]\]"
    r"(?P<trailing>\s+(?:§\s*\d+|mantra\s*#\s*\d+|section\s+\d+))?"
    r"\s*:?\s*",
    re.IGNORECASE,
)

# Bare form of skills that don't start with `scala-` (karpathy-* and
# debug-mantra). Without this, `Per karpathy-guidelines-mindset` (no
# brackets) escapes the strip and lingers in Scaladoc.
OTHER_BARE_PATTERN = re.compile(
    r"\b(?P<per>per)\s+"
    r"(?:karpathy-(?:guidelines|app-design)(?:-mindset)?"
    r"|debug-mantra(?:-mindset)?)"
    r"(?P<trailing>\s+(?:§\s*\d+|mantra\s*#\s*\d+|section\s+\d+))?"
    r"\s*:?\s*",
    re.IGNORECASE,
)

# Drift forms: a valid scala-* skill name followed by an additional
# hyphenated suffix that's NOT `-mindset` (e.g. `scala-jvm-safety-typo`,
# `scala-bug-hunting-whatever`). Without this, SKILL_PATTERN matches
# only the valid prefix and leaves the `-typo:` residue. Listed
# before SKILL_PATTERN so the drift case wins. The drift class
# `[a-z][a-z0-9-]*` allows multi-segment drift like
# `scala-jvm-safety-typo-foo`.
DRIFT_PATTERN = re.compile(
    r"\b(?P<per>per)\s+"
    r"(?:\[\[)?"
    r"scala-(?:jvm-safety|spark-batch-bugs|error-handling|data-driven-refactor|"
    r"jar-packaging|perf-testing|2-scaladoc|"
    r"bug-hunting|chaos-testing|impact-analysis|spark-streaming-bugs)"
    r"(?<!-mindset)-(?P<drift>[a-z][a-z0-9-]*)"
    r"(?P<bracket_close>\]\])?"
    r"(?P<trailing>\s+(?:§\s*\d+|mantra\s*#\s*\d+|section\s+\d+))?"
    r"\s*:?\s*",
    re.IGNORECASE,
)

# Catch patterns in priority order: drift first (to grab the full
# `scala-X-typo` form), then the rest.
ALL_PATTERNS = [DRIFT_PATTERN, SKILL_PATTERN, TYPO_PATTERN, ANY_BRACKET_PATTERN, OTHER_BARE_PATTERN]


def rewrite(text: str) -> tuple[str, int]:
    """Return (rewritten_text, count_of_replacements).

    Only the lines whose `Per <skill>...` clause was actually stripped
    get whitespace cleanup. Lines that did not match a pattern are
    returned byte-for-byte, so Scaladoc indentation (`@param` lists,
    `* Build...` continuations, aligned columns) is preserved.
    """
    # Apply patterns sequentially (each pattern strips its own clauses).
    # Sequential application avoids group-name collisions when patterns
    # are combined via `|` alternation. Collect the set of 0-indexed line
    # numbers whose contents were touched by any pattern; whitespace
    # cleanup is restricted to those lines.
    touched: set[int] = set()
    count = 0
    for pattern in ALL_PATTERNS:
        chunks: list[str] = []
        last_end = 0
        for m in pattern.finditer(text):
            # Record every line that the match span intersects.
            start_line = text.count("\n", 0, m.start())
            end_line = text.count("\n", 0, m.end())
            for ln in range(start_line, end_line + 1):
                touched.add(ln)
            chunks.append(text[last_end:m.start()])
            last_end = m.end()
            count += 1
        chunks.append(text[last_end:])
        text = "".join(chunks)

    # Whitespace cleanup is scoped to touched lines so untouched
    # Scaladoc blocks (indented `*`, `@param` alignment, etc.) keep
    # their column positions.
    if touched:
        lines = text.split("\n")
        for ln in touched:
            if ln < len(lines):
                # Collapse double-spaces left at the seam where the
                # dropped clause used to be. The seam is identified
                # by a non-space char followed by 2+ spaces followed
                # by another non-space char (i.e. mid-line whitespace,
                # NOT leading indent).
                lines[ln] = re.sub(r"(\S) {2,}(?=\S)", r"\1 ", lines[ln])
                # Trim leading whitespace inside the `//` (or `/*`) of
                # the touched comment line, restoring single space.
                lines[ln] = re.sub(
                    r"(?m)^([ \t]*/+[ \t]+)[ \t]+(?=[A-Za-z(])",
                    r"\1",
                    lines[ln],
                )
        text = "\n".join(lines)

    return text, count


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: apply_linter_skill_citations.py <file-or-dir> ...", file=sys.stderr)
        sys.exit(2)

    targets: list[str] = sys.argv[1:]
    files: list[str] = []
    for t in targets:
        if os.path.isdir(t):
            for root, _, fs in os.walk(t):
                for f in fs:
                    if f.endswith(".scala"):
                        files.append(os.path.join(root, f))
        elif os.path.isfile(t):
            files.append(t)

    total_files = 0
    total_replacements = 0
    for path in sorted(files):
        with open(path, encoding="utf-8") as fh:
            src = fh.read()
        new_src, n = rewrite(src)
        if n:
            total_files += 1
            total_replacements += n
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(new_src)
            print(f"{path}: {n} replacement(s)")
    print(f"\nTotal: {total_replacements} replacement(s) across {total_files} file(s)")


if __name__ == "__main__":
    main()