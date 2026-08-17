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
- RLS tenant scoping: a repo method that takes an explicit `tenantID` sets the `app.tenant_id` GUC itself, per call — but that only covers its own call. It does not propagate to a sibling repo call sharing the same `Transactor.RunInTx`/`RunInTxWithRetry` transaction, since those acquire the connection using whatever ctx they're given *before* their callback runs. Any caller composing multiple repo calls inside one transaction must set the GUC on ctx before calling `RunInTx`, not rely on a repo method to do it from inside the callback. `internal/core/service` cannot import the postgres package's own `withTenantGUC` (arch-lint's service/adapter dependency direction), so it carries its own copy — every `RunInTx`/`RunInTxWithRetry` call site in that package must call it explicitly on ctx first.
- Coverage is gated per-package, not by one flat global number (`make cover-check-pkg`, floors in the Makefile's `COVER_PKG_FLOORS`). Packages not listed must clear the global `COVER_THRESHOLD`; only add a package-specific floor for code that's genuinely hard to exercise at that bar (documented inline in the Makefile), never to paper over a gap in new code.
- Metrics, tracing-init, and Temporal Search-Attribute helpers are centralized in `internal/observability` — one `Register()` function for all Prometheus metrics (mirrors `iam-user-profile`'s `internal/adapter/outbound/metrics` pattern: nil-until-`Register()` vars, nil-guard at call sites), not scattered `promauto` calls per package. New metrics belong there, not inline in whichever handler first needs one.
- `go-arch-lint` peer-leaf components (like `internal/workflow` and `internal/observability`) that both get imported by other layers *and* have their own external (`_test` suffix) test package need a self-reference in `deps:` (`mayDependOn: [componentname]`), the same way `port` already does for its `mocks` sub-package — otherwise arch-lint flags the test file's own import of the package it's testing.
- Prometheus `*Vec` collectors (CounterVec/GaugeVec/HistogramVec) with open-ended labels emit nothing at all in a scrape — not even HELP/TYPE — until something calls `WithLabelValues`. Only pre-initialize label combinations in `Register()` when *every* label on that metric has a closed, LLD-documented vocabulary; don't assert on zero-value scrape output for metrics that don't qualify.
- **Temporal Activity idempotency**: an Activity that inserts a new row (`CreateTaskActivity`, `DeferTask`'s regression-task helper) must derive that row's ID deterministically, not `uuid.New()` — a lost-ack retry under Temporal's at-least-once execution otherwise creates a second real row. Derive from stable inputs the caller controls (`internal/adapter/outbound/temporal/helpers.go`'s `deterministicTaskID`/`deterministicRegressionTaskID`), and classify the resulting primary-key conflict (`mapErr`) as "already done," not an error. If the same business key can be legitimately revisited within one workflow execution (an exclusive-gateway back-edge, an admin force-back) — not just retried — a business key alone isn't enough: thread an interpreter-side counter (`internal/workflow`'s `taskVisits`) into the ID derivation to distinguish a genuine revisit from a retry of the same call. An Activity's own `context.Context` is not a real Temporal activity context in this package's unit tests (`context.Background()`), so `activity.GetInfo(ctx)` panics there — don't reach for it as a source of per-invocation uniqueness in this codebase's Activities.
- An Activity that mutates no status a retry could gate on (an audit-only write like `RecordSLAWarning`/`RecordSLABreach`) can't rely on "is it already in the target state" for retry-safety — check `OutboxRepository.ExistsForTask` (or an equivalent existing-record lookup) before enqueueing instead.

## Local Development Context

See `.claude/CLAUDE.local.md` (gitignored, not committed) for local reference paths — e.g. sibling repos and design docs on this machine.
