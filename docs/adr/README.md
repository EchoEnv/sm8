# Architecture Decision Records (ADRs)

ADRs document significant architectural decisions made in this repo:
their context, the options considered, and the consequences of the chosen path.

## Conventions

- File naming: `NNNN-<kebab-case-slug>.md`, monotonically increasing `NNNN`
- Status values: `Proposed`, `Accepted`, `Superseded`, `Deprecated`
- Each ADR has sections: **Context and Problem Statement**, **Decision**, **Consequences**, **Alternatives Considered**, **References**
- ADRs are immutable once accepted; supersede via a new ADR that links back

## Current ADRs

| # | Title | Status |
|---|---|---|
| [0001-0004](0001-0004-engine-portable-architecture.md) | Engine-portable + manifest-validator + plugin-portal + typed-expr-parser (combined) | Accepted |
| [0005](0005-expr-parser-is-null-postfix.md) | ExprParser IS [NOT] NULL postfix | Accepted |
| [0006](0006-step-11-sm8-mcp-server.md) | Step 11 — SM8 MCP server integration (Post-#65 Refinement) | Accepted |
| [0007](0007-v0.1.0-cut-plan.md) | v0.1.0 cut plan + RFC §12/§13 conformance gaps | Accepted |

## Tools

`adr-tools`, `log4brains`, and similar tools expect `./docs/adr/` at the repo root —
that's where we are.
