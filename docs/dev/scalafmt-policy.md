# Style policy (SM8)

## scalafmt (format-on-touch)

The project ships `.scalafmt.conf` at the repo root (scala213 dialect,
v3.10.7) and a `fmt-maven-plugin` entry in the parent POM
(com.spotify.fmt:2.29, `<skip>true</skip>` by default).

**Policy**: format-on-touch, not format-everything. Existing code is
intentionally left unformatted — 97% of `.scala` files don't conform
to `.scalafmt.conf` today (verified 2026-09-03 via `scalafmt --list`).
A whole-codebase format would produce ~30k lines of diff and is
un-reviewable. Instead, only files you actually touch get reformatted
on commit, via the `.githooks/pre-commit` hook.

### Setup (one-time per clone)

```bash
git config core.hooksPath .githooks
cs install scalafmt:3.10.7   # if scalafmt is not already in PATH
```

After setup, verify the version matches `.scalafmt.conf`:

```bash
scalafmt --version   # must print "scalafmt 3.10.7"
```

If your version is older (e.g., 3.8.0 from a stale coursier install),
upgrade explicitly:

```bash
cs uninstall scalafmt
cs install scalafmt:3.10.7
```

An older scalafmt binary will refuse to read `.scalafmt.conf` (it
errors out with "version 3.10.7 is not supported by binary 3.8.0").
The hook catches the error and proceeds with unformatted code (warn
+ skip), so a stale binary silently disables formatting without
breaking the commit. Check the version before assuming the hook is
working.

The hook:
- Reads `git diff --cached --name-only` for staged `.scala` files.
- Runs `scalafmt --config .scalafmt.conf <file>` on each.
- Re-stages the formatted version.

If scalafmt is missing, the hook warns and continues (does not block
the commit). The commit proceeds with unformatted code; CI is not
configured to fail on formatting (no `.github/workflows/`).

### Manual formatting (when you want to format a file you're not committing)

```bash
scalafmt --config .scalafmt.conf <path/to/Foo.scala>
```

### Check formatting without modifying

```bash
scalafmt --test --config .scalafmt.conf   # exits 1 if any file is mis-formatted
scalafmt --list --config .scalafmt.conf  # lists files that need reformatting
```

### Why a plugin in the POM if it's `<skip>true</skip>`?

The plugin declares the convention (version + config path) so any
`mvn scalafmt:format` invocation from a child module uses the right
config. The `<skip>true</skip>` keeps it OFF by default — devs who
want to format manually can opt in per module without it running
unconditionally on every `mvn validate`.

## Reviewer checklist for scalafmt-related PRs

- A PR that reformats a non-touched file is suspect — the developer
  likely ran `mvn scalafmt:format` on the whole module instead of
  just the file they changed. Ask why.
- A PR that introduces new lines violating the existing module's
  indentation is suspect — the dev probably skipped the pre-commit
  hook setup. Politely remind them of `git config core.hooksPath
  .githooks`.
