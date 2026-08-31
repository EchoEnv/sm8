#!/usr/bin/env python3
"""
PR-247 helper: convert bare `Per scala-X` skill-citations in .scala source to
`Per [[scala-X-mindset]]` wiki-link form (sm8 convention).

Rules:
  Per scala-X                    -> Per [[scala-X-mindset]]
  Per scala-X-mindset            -> Per [[scala-X-mindset]]
  Per scala-X §N                 -> Per [[scala-X-mindset]] §N
  Per scala-X-mindset §N         -> Per [[scala-X-mindset]] §N
  Per scala-X-mindset mantra #N  -> Per [[scala-X-mindset]] mantra #N
  Per scala-X section N          -> Per [[scala-X-mindset]] section N

Reads a list of files (or directory trees of .scala) on argv, rewrites in place.
"""
import os
import re
import sys

SKILL_PATTERN = re.compile(
    r"\b(?P<per>per)\s+scala-"
    r"(?P<skill>jvm-safety|spark-batch-bugs|error-handling|data-driven-refactor|"
    r"jar-packaging|perf-testing|2-scaladoc)"
    r"(?P<suffix>-(?P<mindset>mindset)|(?P<typo_mindset>mindset))?"
    r"(?P<trailing>\s+(?:§\s*\d+|mantra\s*#\s*\d+|section\s+\d+))?",
    re.IGNORECASE,
)


def rewrite(text: str) -> tuple[str, int]:
    """Return (rewritten_text, count_of_replacements)."""
    chunks: list[str] = []
    last_end = 0
    count = 0
    for m in SKILL_PATTERN.finditer(text):
        per_word = m.group("per")
        # Preserve the case of the original "per"/"Per" word
        if text[m.start()].isupper():
            per_out = "P" + per_word[1:].lower()
        else:
            per_out = per_word.lower()
        skill = m.group("skill")
        trailing = m.group("trailing") or ""
        replacement = f"{per_out} [[scala-{skill}-mindset]]{trailing}"
        chunks.append(text[last_end:m.start()])
        chunks.append(replacement)
        last_end = m.end()
        count += 1
    chunks.append(text[last_end:])
    return "".join(chunks), count


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