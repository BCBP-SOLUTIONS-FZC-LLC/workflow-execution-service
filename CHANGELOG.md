# Changelog

All notable changes to `execution-service` are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- `POST /instances/:id/nodes/:node/complete` and `.../boundaries/:boundary/fire`: an outside party's answer completes a receive task, or fires a message boundary, that no send task of the plan feeds (`dsl.ExternalWaits`). Open to an admin and to a user assigned to a task of the instance; `WAIT_NOT_FOUND` (404) and `NOT_WAITING` (409) are new. The `instance-external-answer` signal and the `waiting-external` query carry it (change id `external-wait-by-key`).
- Completed tasks' results and outside answers merge into the instance's variables, which each task is created with (change id `task-result-variables`).

### Changed

- **Execution names the connector actor.** The connector callbacks (`POST /api/v1/internal/connector-tasks/:id/complete` and `/fail`) no longer take `actor_user_id`: any caller holding the internal token could record any actor, and a new tenant needed a configuration change and a restart. The service resolves the tenant's `platform-automation` principal from the Token Service (`TOKEN_SERVICE_BASE_URL`, required outside dev; `TOKEN_SERVICE_CLIENT_TIMEOUT`), caches it for five minutes, records its `principal_id` as the actor, refuses to put a connector job on the stream for a tenant with none, and answers a callback for such a tenant `409 NO_AUTOMATION_PRINCIPAL`. `CONNECTOR_ACTOR_USER_IDS` and the chart's `connectorWorker.actorUserIDs` are gone (LLD rev 1.70).
- **Each binary checks only the settings it uses.** `config.Load` parses the environment and checks value ranges; the API, the Worker, the connector-worker and the `migrate` subcommand each check their own required settings. Before this, every binary demanded `DATABASE_URL`, the relay DSN and the SNS and Glue settings, so the connector-worker and the migrate Job could not start from the chart's environment. The chart now gives the API `GLUE_REGISTRY_NAME`, `ELIGIBILITY_BASE_URL` and `OUTBOX_RELAY_DATABASE_URL` (required outside dev), requires `SNS_TOPIC_ARN` while `awsUseStub` is false, and no longer passes the Worker an `INTERNAL_API_TOKEN` it never reads (LLD rev 1.69).
- An `IOMapping` input is `=name` for an instance variable or any other text for a literal, and an output's `source` names a key of the connector's result with or without `=` (change id `io-input-rule` on call steps). An authored `=name` used to read a variable called `=name`, and a call input `=x` to `x` set `x` to nothing.
- The connector worker reads each secret field of its connector type from the tenant's stored credential, `connectors/<tenant>/<type>/<field>`, and discards any value a plan carries for one. A field with no stored credential is left out for the connector to refuse; `secret_ref_invalid` is no longer raised for an authored path.

- An exclusive branch with `Steps` runs them (workflow-models `v1.4.0-rc.1`); a call or subprocess on a branch had never run.
- A branch that leads to an end event ends its path: the instance completes and its status is written. It ended as `TERMINATED` with no status write (change id `exclusive-end-completes-path`).
- The parallel join counts branches by their place in the step; two branches with one `DeptID` hung it (change id `parallel-join-by-branch`).
- A force-route resumes inside an exclusive or parallel branch's `Steps` at the department it names.

### Removed

- The call-pool step and its admin stub task (`runCallPool`): a document runs one pool, and outside parties' pools produce nothing (workflow-models `v1.4.0-rc.2` removes `CallPoolStep`, `CompiledPlan.Ignored`, `CompiledCollaboration.Messages` and `DepartmentDef.Ignore`).

### Added

- `GET /api/v1/internal/workflows/{workflow_id}/active-instances` and `POST /api/v1/internal/users/{user_id}/pause-tasks`, the Definition Service's archive guard and membership-revocation pause, replacing the gRPC `CheckActiveInstances`/`PauseUserTasks`.
- `api/internal-openapi.yaml`, the authority for every `/api/v1/internal` route (moved out of the public spec). `test/contract` validates the real router's responses against it, and the definition client's requests against the copy of the Definition Service's spec in `api/contracts/`.
- `make contract-breaking` (oasdiff against `origin/main`, in `make check` and CI, replacing `buf breaking`) and `make contract-check` (the copy against its source branch, in CI).

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
- `go.temporal.io/sdk` v1.46.0 and `github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models` v1.1.0 dependencies.
- Minimal `internal/core/domain` (`InstanceStatus`/`TaskStatus`/`AssignmentStatus`/`NodeKey`) and `internal/core/port` (Activity name constants + input/output shapes) surface the interpreter calls through — a soft coordination point with whichever sibling task lands `internal/core/service`.
- `test/workflow/` — `testsuite.WorkflowTestSuite` end-to-end coverage (dispatch, boundary events, force-back, DEGRADED, SLA timers, the status query) alongside white-box unit tests for the pure-function pieces; 92%+ package coverage.

### Changed

- **The Definition Service link is internal HTTP; gRPC is removed.** `internal/adapter/outbound/http.DefinitionClient` reads compiled plans for `cmd/server`, `cmd/worker` and the connector-worker's alias fetch, with `DEFINITION_SERVICE_URL`/`DEFINITION_SERVICE_TOKEN` replacing `DEFINITION_SERVICE_ADDR` and `DEFINITION_SERVICE_INTERNAL_HTTP_ADDR`. `port.ErrDefinitionRejected`/`ErrDefinitionUnavailable` replace the gRPC adapter's sentinels. `GRPC_PORT`, the protos, buf and port 9090 are gone.
- Helm: the Worker gets the Definition Service URL, token and egress; the API and Worker get `INTERNAL_API_TOKEN`; the Secret refuses to render without both tokens outside dev; the gateway routes only the public routes; `make helm-lint` renders to a file first, since a failed render had passed through the pipe.
- A failed compiled-plan read answers `503 UPSTREAM_UNAVAILABLE` with a fixed detail; the cause, which names the Definition Service's URL and the tenant, is logged instead.
- Helm: in a dev install the API's and Worker's `DEFINITION_SERVICE_TOKEN` and `INTERNAL_API_TOKEN` references are optional, so pods start without them. The internal-call alert selects on `route` (`method` is the HTTP verb) and reports per route.
- `make contract-check` and `make contract-breaking` fail on a repo, ref or `CONTRACT_SRC_DIR` they cannot read, instead of reporting it as pending; a source file missing from a real branch is still pending.
- CI's contract copy check reads the sibling repo with `GO_PRIVATE_TOKEN`. The release deploys with `secret.existingSecretName=workflow-execution-service-secrets`, a Secret created in the cluster, instead of `PROD_*` secrets.
- A plan that cannot be read is logged once, with its cause and version, by the shared plan cache; callers log only what they skip. `cmd/connector-worker` drops its `INTERNAL_API_TOKEN` check, which `config.Load` already makes outside dev.
- Node keys and the human-task rule come from workflow-models' `dsl.NodeKey` and `StageDef.CreatesHumanTask`, which Definition's node read uses too. The key format is unchanged, so no Temporal version gate is needed.

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
