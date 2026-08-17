# Changelog

All notable changes to `execution-service` are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- **`cmd/connector-worker`** — the third binary that actually executes connector-typed tasks (`design/LLD/workflow_connectors.md`): consumes a Valkey Stream `cmd/server`'s `internal_events.go` handler pushes connector-typed `workflow.task.created` events onto, resolves each `secret_ref` input's OpenBao path to its real credential (`internal/adapter/outbound/openbao`, mirroring `definition_service`'s write-side path convention and path-traversal guard), dispatches to a real `workflow-connectors.Connector` under a per-connector-type bounded pool with the ratified per-type retry/backoff policy, and reports the outcome via a new pair of internal endpoints. Never imports the Temporal SDK — see the new `.claude/CLAUDE.md` bullet.
- **New completion path**: `POST /internal/connector-tasks/:id/{complete,fail}` (`internal/adapter/inbound/http/handler/connector_task_completion.go`), backed by a new `ConnectorTaskService` (deliberately separate from the human `TaskService`, whose `checkHumanActionable` gate rejects connector tasks). Idempotent under `cmd/connector-worker`'s own Stream-redelivery-driven retries via a terminal-status check plus a short-TTL `CacheStore.SetNX` dedup key.
- `internal/adapter/outbound/valkeystream` — Stream producer/consumer-group wrapper (`XADD`/`XREADGROUP`/`XACK`/`XAUTOCLAIM`) and the `port.ConnectorEventPublisher` implementation `internal_events.go`'s new `workflow.task.created` handler calls.
- `internal/adapter/outbound/openbao` — the OpenBao KV-v2 **read** counterpart to `definition_service`'s write-only `SecretsClient`.
- `WorkflowTaskCreatedPayload.OutputMapping` — the compiled stage's `IOMapping.Outputs` was computed but never persisted or forwarded; `CreateTaskActivity` now stashes it into `ExtrasJSON` and the new completion endpoint applies the source→target rename before signaling, so the connector-worker↔execution_service contract stays "send whatever `Execute()` returned, verbatim."
- New config: Valkey Stream key/group/consumer/timeouts, OpenBao addr/token/mount, the static alias-registry path, per-connector-type pool size/timeout, and the completion-callback target — see `.env.example`. Connector-worker-only fields are deliberately not enforced in `Config.validate()`, mirroring the existing `DefinitionServiceAddr` precedent; `cmd/connector-worker` checks its own required fields at startup.
- **Connector-worker Temporal Activity support** — all 15 catalogue Activities (`internal/adapter/outbound/temporal/`) plus `cmd/worker` registration: `CreateTaskActivity` resolves a connector-typed stage's `IOMapping.Inputs` against the task's context and skips assignee-row creation entirely (connector tasks are fully automation-only); `workflow_task` gained `connector_type`.
- `port.OutboxRepository.ExistsForTask` — lets an audit-only Activity (no status to gate a retry on) check whether it already recorded a given event for a task before enqueueing a second one.
- `internal/workflow`'s interpreter now tracks a per-`NodeKey` visit counter, threaded into `CreateTaskInput.VisitCount` — see Fixed below for why.
- **`internal/workflow`** — the Temporal workflow-function interpreter over the compiled BPMN DSL (`workflow-models`'s `pkg/dsl`): recursive step dispatch (`Sequential`/`Parallel`/`Exclusive`/`SubWorkflow`/`CallPool`), the binary exclusive-gateway comparator, boundary-event racing (timer/message/error), the intra-pool message buffer, `completedNodes` force-back history (including the active-parallel-gateway extension), the `DEGRADED` park/respawn state machine, SLA timer racing, the `get-workflow-status` Query handler, and the `workflow.GetVersion` replay-safety convention.
- `go.temporal.io/sdk` v1.46.0 and `github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models` v1.0.0 dependencies.
- Minimal `internal/core/domain` (`InstanceStatus`/`TaskStatus`/`AssignmentStatus`/`NodeKey`) and `internal/core/port` (Activity name constants + input/output shapes) surface the interpreter calls through — a soft coordination point with whichever sibling task lands `internal/core/service`.
- `test/workflow/` — `testsuite.WorkflowTestSuite` end-to-end coverage (dispatch, boundary events, force-back, DEGRADED, SLA timers, the status query) alongside white-box unit tests for the pure-function pieces; 92%+ package coverage.

### Fixed

- **Pause/Resume/Cancel/Reassign activities could retry forever**: each reused a `RecordVersion` captured at signal-send time; a Temporal at-least-once retry after a lost ack replayed that now-stale version, hit an unclassified version-conflict error, and retried unboundedly. Now refetch current state and no-op once already applied.
- **CreateTask/DeferTask had no idempotency key**: a retry created a second real task/assignment row. `task.ID` is now deterministic (`instanceID`+`NodeKey`+`VisitCount` — see Added above); a primary-key conflict on retry is treated as "already created," not an error. A first attempt at this fix collided on a legitimate node revisit (an exclusive-gateway back-edge or admin force-back) before `VisitCount` was added to disambiguate it from a retry of the same call.
- **`TaskAssignmentRepository.Complete`/`SetLead` had no optimistic-concurrency guard at all** — the LLD frames the task, not the assignment, as claim/complete's contested resource. Added a `workflow_task.record_version` bump as the guard, plus retry-idempotency at each call site.
- The `stage-fail` signal reused `stage-transition`'s wire shape, which didn't match the LLD's documented payload — split into its own struct with the LLD's exact field names, safe since no sender exists anywhere yet.
- `UpdateInstanceStatusActivity`'s `Degraded` transition emitted no event at all, and neither did the `Degraded`→`Running` recovery — both now build/enqueue their documented events.
- `RecordSLAWarning`/`RecordSLABreach` could double-emit their event on retry (audit-only, nothing to gate a retry on) — now checked against the outbox first.
- The start-instance HTTP handler only checked `len(context_json) > 0`, not that it's actually a JSON object — a bare string/number/array would only fail later, deep inside a live workflow's first connector stage.
- `make test` now includes `test/workflow/` in its coverage run — previously omitted, invisible until now since this is the first PR to populate that directory with real tests.
- `TaskAssignmentRepository`/`InstanceRepository` gained `GetByID`/`Complete`/`SetLead` and `UpdateCurrentNodeKeys` — the 4 repository methods T1.3's activity catalogue (`CompleteAssignmentActivity`, `ClaimAssignmentActivity`, `UpdateInstanceNodesActivity`) structurally needs; previously missing, blocking that work.

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
