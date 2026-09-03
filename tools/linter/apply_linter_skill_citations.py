#!/usr/bin/env python3
"""
Strip bare `Per scala-X` skill-citations in .scala source.

Replaces PR-247's previous behavior (which converted bare form to the
`[[scala-X-mindset]]` wiki-link form). Per the scala2-scaladoc skill:
no internal process noise — skill names, whether bare or bracketed, are
internal process metadata. The correct Scaladoc describes the behavior
without citing an internal skill name.

Rules (replacement is the empty string, dropping the leading clause):

  Per scala-X                    -> '' (empty)
  Per scala-X-mindset            -> ''
  Per scala-X §N                 -> ''
  Per scala-X-mindset §N         -> ''
  Per scala-X-mindset mantra #N  -> ''
  Per scala-X section N          -> ''
  Per [[scala-X-mindset]] ...     -> '' (the bracket form is also noise)
  Per [[karphyaguidsmindset]] ... -> '' (typo names)
  Per [[scala-data-driven-refacer]] -> '' (typo names)

The trailing whitespace after the dropped clause is also collapsed.

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
    r"jar-packaging|perf-testing|2-scaladoc)"
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
    r"jar-packaging|perf-testing|2-scaladoc)(?:-mindset)?"
    r"|karphyaguids(?:mindset)?"
    r"|scala-data-driven-refacer"
    r"|debug-mantra(?:-mindset)?"
    r"|karpathy-(?:guidelines|app-design)(?:-mindset)?"
    r")\]\]"
    r"(?P<trailing>\s+(?:§\s*\d+|mantra\s*#\s*\d+|section\s+\d+))?"
    r"\s*:?\s*",
    re.IGNORECASE,
)

ALL_PATTERNS = [SKILL_PATTERN, TYPO_PATTERN, ANY_BRACKET_PATTERN]


def rewrite(text: str) -> tuple[str, int]:
    """Return (rewritten_text, count_of_replacements)."""
    # Apply patterns sequentially (each pattern strips its own clauses).
    # Sequential application avoids group-name collisions when patterns
    # are combined via `|` alternation.
    count = 0
    for pattern in ALL_PATTERNS:
        chunks: list[str] = []
        last_end = 0
        for m in pattern.finditer(text):
            chunks.append(text[last_end:m.start()])
            last_end = m.end()
            count += 1
        chunks.append(text[last_end:])
        text = "".join(chunks)
    # Collapse runs of 3+ spaces into a single space (defensive: in case
    # the dropped clause left double-spaces around the colon).
    text = re.sub(r"   +", " ", text)
    # Collapse runs of leading non-letter punctuation on comment lines
    # whose first non-whitespace, non-`/` char is now the start of the
    # actual Scaladoc sentence (not the dropped `Per X:` prefix). This
    # trims the `//   this object has no mutable state.` residue back to
    # `// this object has no mutable state.`.
    text = re.sub(
        r"(?m)^([ \t]*/+[ \t]+)[ \t]+(?=[A-Za-z(])",
        r"\1",
        text,
    )
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