# CLAUDE.md

## What This Repo Is

`execution-service` is the Temporal-backed runtime for the BPMN Workflow Engine platform: it starts, tracks, and drives workflow instances to completion, dispatching tasks to human assignees and other services along the way. It's the execution-time counterpart to `definition_service`, which owns design-time authoring and compilation of workflow templates.

Currently at Tier-0: a bootstrapped skeleton (build/lint/test tooling, DB schema, proto contracts). No business logic yet.

## Getting Started

Run `make help` for all available targets, or see [README.md](../README.md) for the full setup walkthrough.

## Critical Rules — Never Break These

- **Never add a `replace` directive** pointing at a local path in `go.mod`.
- **Never import from a local path** in source code — always the full `github.com/BCBP-SOLUTIONS-FZC-LLC/...` module path.
- do not bloat this file with code specifics, this should only contain guidance. update this when needed.

## Coding Conventions

- Metrics, tracing-init, and Temporal Search-Attribute helpers are centralized in `internal/observability` — one `Register()` function for all Prometheus metrics (mirrors `iam-user-profile`'s `internal/adapter/outbound/metrics` pattern: nil-until-`Register()` vars, nil-guard at call sites), not scattered `promauto` calls per package. New metrics belong there, not inline in whichever handler first needs one.
- `go-arch-lint` peer-leaf components (like `internal/workflow` and `internal/observability`) that both get imported by other layers *and* have their own external (`_test` suffix) test package need a self-reference in `deps:` (`mayDependOn: [componentname]`), the same way `port` already does for its `mocks` sub-package — otherwise arch-lint flags the test file's own import of the package it's testing.
- Prometheus `*Vec` collectors (CounterVec/GaugeVec/HistogramVec) with open-ended labels emit nothing at all in a scrape — not even HELP/TYPE — until something calls `WithLabelValues`. Only pre-initialize label combinations in `Register()` when *every* label on that metric has a closed, LLD-documented vocabulary; don't assert on zero-value scrape output for metrics that don't qualify.

## Local Development Context

See `.claude/CLAUDE.local.md` (gitignored, not committed) for local reference paths — e.g. sibling repos and design docs on this machine.
