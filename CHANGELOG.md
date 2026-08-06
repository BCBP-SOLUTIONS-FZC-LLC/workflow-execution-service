# Changelog

All notable changes to `execution-service` are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- **`internal/workflow`** — the Temporal workflow-function interpreter over the compiled BPMN DSL (`workflow-models`'s `pkg/dsl`): recursive step dispatch (`Sequential`/`Parallel`/`Exclusive`/`SubWorkflow`/`CallPool`), the binary exclusive-gateway comparator, boundary-event racing (timer/message/error), the intra-pool message buffer, `completedNodes` force-back history (including the active-parallel-gateway extension), the `DEGRADED` park/respawn state machine, SLA timer racing, the `get-workflow-status` Query handler, and the `workflow.GetVersion` replay-safety convention.
- `go.temporal.io/sdk` v1.46.0 and `github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models` v1.0.0 dependencies.
- Minimal `internal/core/domain` (`InstanceStatus`/`TaskStatus`/`AssignmentStatus`/`NodeKey`) and `internal/core/port` (Activity name constants + input/output shapes) surface the interpreter calls through — a soft coordination point with whichever sibling task lands `internal/core/service`.
- `test/workflow/` — `testsuite.WorkflowTestSuite` end-to-end coverage (dispatch, boundary events, force-back, DEGRADED, SLA timers, the status query) alongside white-box unit tests for the pure-function pieces; 92%+ package coverage.

### Fixed

- `make test` now includes `test/workflow/` in its coverage run — previously omitted, invisible until now since this is the first PR to populate that directory with real tests.

---

## [T0] — 2026-07-28

Initial scaffold (`chore/t0-bootstrap`, PR #1).

**Infrastructure:**

- Clean Architecture with `domain ← port ← service ← adapter` (+ `internal/workflow` as a peer of `adapter`) enforced by `go-arch-lint`
- Go 1.26, Gin HTTP server (`:8080`) + gRPC server (`:9090`), Temporal Worker process (`cmd/worker`)
- PostgreSQL adapter via `platform-pgcommon` (pgx/v5, RLS via `app_tenant_id()`, transactional outbox)
- Valkey adapter, LocalStack for SNS/SQS, PgBouncer, Temporal dev server (docker-compose)
- golang-migrate schema migrations, sqlc query generation, `buf` proto generation, GoMock stubs

**Domain:**

- `workflow_execution` schema: `workflow_instance`, `workflow_task`, `workflow_task_assignment`, `assignee_overrides`, `active_task_queues`, `workflow_data_keys`, outbox tables

**API:**

- Proto contracts scaffolded (`execution_service.proto`, `definition.proto`)

**CI/CD:**

- `ci.yml` GitHub Actions workflow (generate/lint/arch-lint/test jobs; build-image/trivy/smoke-test/push jobs stubbed `if: false`, deferred to a later Tier-1 task)
- golangci-lint v2, go-arch-lint, govulncheck
