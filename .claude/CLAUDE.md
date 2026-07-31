# CLAUDE.md

## What This Repo Is

`execution-service` is the Temporal-backed runtime for the BPMN Workflow Engine platform: it starts, tracks, and drives workflow instances to completion, dispatching tasks to human assignees and other services along the way. It's the execution-time counterpart to `definition_service`, which owns design-time authoring and compilation of workflow templates.

## Getting Started

Run `make help` for all available targets, or see [README.md](../README.md) for the full setup walkthrough. Use `make test`, `make lint`, `make arch-lint`, `make check` rather than calling `go test`/`golangci-lint`/`go-arch-lint` directly — the Makefile targets carry the flags (coverage, race, package scope) CI actually enforces.

## Critical Rules — Never Break These

- **Never add a `replace` directive** pointing at a local path in `go.mod`.
- **Never import from a local path** in source code — always the full `github.com/BCBP-SOLUTIONS-FZC-LLC/...` module path.
- do not bloat this file with code specifics, this should only contain guidance. update this when needed.

## Coding Conventions

- Prefer self-documenting code (clear naming, small functions, obvious control flow) over comments. Add a comment only when the WHY is genuinely non-obvious from reading the code — a hidden constraint, a deliberate scope limit, a workaround. Don't restate what the next line already says.
- When a comment needs to cite rationale that's written down elsewhere, reference the design doc by name and section (e.g. "execution LLD §2.5") rather than restating it — design docs may only exist as shareable documents outside this repo, so name them, don't path them.
- keep functions and files small, readable and as little complex as makes sense.
- At the end of a task, update README.md and this file if the change affects what they document — a new package, a new Makefile target, a changed convention. Don't leave docs describing a state that no longer exists.
- RLS tenant scoping: a repo method that takes an explicit `tenantID` sets the `app.tenant_id` GUC itself, per call — but that only covers its own call. It does not propagate to a sibling repo call sharing the same `Transactor.RunInTx`/`RunInTxWithRetry` transaction, since those acquire the connection using whatever ctx they're given *before* their callback runs. Any caller composing multiple repo calls inside one transaction must set the GUC on ctx before calling `RunInTx`, not rely on a repo method to do it from inside the callback.
- Coverage is gated per-package, not by one flat global number (`make cover-check-pkg`, floors in the Makefile's `COVER_PKG_FLOORS`). Packages not listed must clear the global `COVER_THRESHOLD`; only add a package-specific floor for code that's genuinely hard to exercise at that bar (documented inline in the Makefile), never to paper over a gap in new code.

## Local Development Context

See `.claude/CLAUDE.local.md` (gitignored, not committed) for local reference paths — e.g. sibling repos and design docs on this machine.
