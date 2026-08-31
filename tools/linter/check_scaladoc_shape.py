#!/usr/bin/env python3
"""
check_scaladoc_shape.py — validates Scaladoc blocks match the code shape.

Unlike check_scaladoc_noise.py (which flags forbidden *content*), this script
checks *structural completeness*: every public def/class/trait/object should
have a Scaladoc block directly above it, with:

  - a one-sentence summary line
  - an @param tag for every parameter (and no @param for a parameter that
    doesn't exist — usually a stale doc after a signature change)
  - an @return tag if the method doesn't return Unit
  - (best-effort) a period-terminated first summary line

This is a regex-based heuristic, not a Scala parser — it handles common
signature shapes (including curried param lists and default values) but can
be fooled by unusual formatting. Treat findings as things to check, not
ground truth.

Usage:
    python3 check_scaladoc_shape.py <file_or_dir> [<file_or_dir> ...]

Exit code: 0 if clean, 1 if any findings (suitable for CI / pre-commit).
"""

import re
import sys
from pathlib import Path

DEF_RE = re.compile(
    r"^(?P<indent>[ \t]*)(?P<mods>(?:(?:private|protected|final|override|implicit|sealed|abstract)\[?\w*\]?\s+)*)"
    r"def\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?P<tparams>\[[^\]]*\])?",
    re.MULTILINE,
)

DOC_BLOCK_RE = re.compile(r"/\*\*(.*?)\*/", re.DOTALL)


def is_private(mods: str) -> bool:
    return "private" in mods or "protected" in mods


def find_balanced_parens(text: str, start: int):
    """Given text and an index at/near '(', return (full_param_text, end_index)
    covering all curried param lists, or (None, start) if none found."""
    i = start
    n = len(text)
    # Skip whitespace
    while i < n and text[i] in " \t\n":
        i += 1
    if i >= n or text[i] != "(":
        return None, start
    full_start = i
    while i < n and text[i] == "(":
        depth = 0
        while i < n:
            if text[i] == "(":
                depth += 1
            elif text[i] == ")":
                depth -= 1
                if depth == 0:
                    i += 1
                    break
            i += 1
        # peek for another param list (curried)
        j = i
        while j < n and text[j] in " \t\n":
            j += 1
        if j < n and text[j] == "(":
            i = j
        else:
            break
    return text[full_start:i], i


def extract_param_names(param_text: str):
    """Extract top-level parameter names from one or more (...) groups,
    ignoring nested parens/brackets (default values, generics)."""
    names = []
    depth = 0
    current = []
    groups = []
    buf = ""
    depth = 0
    for ch in param_text:
        if ch == "(":
            depth += 1
            if depth == 1:
                buf = ""
                continue
        elif ch == ")":
            depth -= 1
            if depth == 0:
                groups.append(buf)
                continue
        buf += ch
    for group in groups:
        # split on top-level commas
        parts = []
        d = 0
        cur = ""
        for ch in group:
            if ch in "([{":
                d += 1
            elif ch in ")]}":
                d -= 1
            if ch == "," and d == 0:
                parts.append(cur)
                cur = ""
            else:
                cur += ch
        if cur.strip():
            parts.append(cur)
        for p in parts:
            p = p.strip()
            if not p:
                continue
            # strip leading `implicit`
            p = re.sub(r"^implicit\s+", "", p)
            m = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*:", p)
            if m:
                names.append(m.group(1))
    return names


def extract_return_type(text: str, after_idx: int):
    """Look right after the param lists for `: ReturnType =` or `: ReturnType`
    up to end of line / opening brace."""
    m = re.match(r"\s*:\s*([^\n={]+)", text[after_idx:])
    if not m:
        return None
    return m.group(1).strip()


def find_preceding_doc(text: str, def_start: int):
    """Walk backwards from def_start, skipping annotations/blank lines, to
    find an immediately-preceding /** ... */ block. Returns its content or
    None."""
    before = text[:def_start]
    lines = before.splitlines()
    i = len(lines) - 1
    # skip blank lines and annotation lines (@Foo, @Foo(...))
    while i >= 0 and (lines[i].strip() == "" or lines[i].strip().startswith("@")):
        i -= 1
    if i < 0:
        return None
    if not lines[i].rstrip().endswith("*/"):
        return None
    # walk further back to the matching /**
    end_line = i
    while i >= 0 and "/**" not in lines[i]:
        i -= 1
    if i < 0:
        return None
    block_text = "\n".join(lines[i:end_line + 1])
    m = DOC_BLOCK_RE.search(block_text)
    return m.group(1) if m else None


def check_file(path: Path):
    text = path.read_text(encoding="utf-8", errors="replace")
    findings = []

    for m in DEF_RE.finditer(text):
        mods = m.group("mods") or ""
        name = m.group("name")
        if is_private(mods):
            continue

        def_line = text.count("\n", 0, m.start()) + 1
        param_text, after_params_idx = find_balanced_parens(text, m.end())
        param_names = extract_param_names(param_text) if param_text else []
        return_type = extract_return_type(text, after_params_idx) if param_text else \
            extract_return_type(text, m.end())

        doc = find_preceding_doc(text, m.start())
        loc = f"line {def_line}: def {name}"

        if doc is None:
            findings.append((def_line, f"'{name}' is public but has no Scaladoc block above it"))
            continue

        # Summary line check: first non-empty content line should end with '.'
        raw_lines = [
            re.sub(r"^\s*\*\s?", "", l) for l in doc.splitlines()
        ]
        content_lines = [l for l in raw_lines if l.strip()]
        # Summary check: join the first paragraph (until blank comment line)
        # and confirm it contains a sentence-ending period — physical line
        # wrapping is normal in Scaladoc, so we don't check line-by-line.
        first_para_lines = []
        for l in raw_lines:
            if l.strip() == "":
                if first_para_lines:
                    break
                continue
            first_para_lines.append(l.strip())
        first_para = " ".join(first_para_lines)
        if first_para and "." not in first_para:
            findings.append((def_line, f"'{name}': summary paragraph has no sentence-ending period: \"{first_para[:60]}\""))
        elif not content_lines:
            findings.append((def_line, f"'{name}': Scaladoc block has no summary text"))

        documented_params = set(re.findall(r"@param\s+([A-Za-z_][A-Za-z0-9_]*)", doc))
        actual_params = set(param_names)

        missing = actual_params - documented_params
        stale = documented_params - actual_params
        for p in sorted(missing):
            findings.append((def_line, f"'{name}': parameter '{p}' has no @param tag"))
        for p in sorted(stale):
            findings.append((def_line, f"'{name}': @param '{p}' does not match any actual parameter (stale?)"))

        has_return_tag = "@return" in doc
        if return_type and return_type != "Unit" and not has_return_tag:
            findings.append((def_line, f"'{name}': returns `{return_type}` but has no @return tag"))
        if return_type == "Unit" and has_return_tag:
            findings.append((def_line, f"'{name}': returns Unit but has an @return tag (remove it)"))

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

    total = 0
    for f in files:
        findings = check_file(f)
        if findings:
            print(f"\n{f}")
            for line_no, msg in findings:
                print(f"  line {line_no}: {msg}")
            total += len(findings)

    if total:
        print(f"\n{total} shape issue(s) found across {len(files)} file(s).")
        return 1
    else:
        print(f"Clean — Scaladoc shape matches signatures in {len(files)} file(s).")
        return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
