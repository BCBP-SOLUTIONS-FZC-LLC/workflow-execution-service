# Changelog

All notable changes to `execution-service` are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Fixed

- Instance start read a version's status and validity from this service's one-hour plan cache, so after the Definition Service invalidated the version (a membership revocation) or archived it, a start within the hour answered `201` and the instance then failed. Start now always reads the Definition Service, which evicts its own copy on both events, and answers `409 VERSION_INVALID` or `409 VERSION_NOT_PUBLISHED`. The cache is still written by each start and serves outside answers for running instances.
- A force-forward that cancelled a task stage while it was still creating its task or recording its node failed the stage with the cancelled activity's error, so a parallel join counted the branch as failed; CI caught it as an intermittent `TestRunTaskStage_AbandonedOnCancel` failure. The stage is abandoned, and a task it created is superseded (change ID `abandoned-mid-activity`).
- CI's first full run on this branch (earlier runs skipped every job on a draft) failed in three places: the tidy check ran without the gitignored mocks and dropped `go.uber.org/mock`, and the Trivy and smoke jobs rebuilt the images without the sqlc output. The quality job generates the mocks first, and both image jobs generate the sqlc code.
- An instance started on a version the Definition Service had already marked invalid (after a membership revocation) ran: the API's start reads a plan cached for up to an hour, and `GetCompiledPlanActivity` did not check the status or validity it fetched. The activity now refuses, without retry, a version that is not `PUBLISHED` or not valid, so the instance fails and runs no step.
- A receive task's wait did not record its node, so an instance waiting on a message or an outside answer showed an empty `current_node_keys`; a Definition plan whose main pool starts at a message wait showed no node at all. The node is recorded when the wait begins (change ID `receive-records-node`).
- The chart rendered without `VALKEY_ADDR` in its Secret, giving pods that referenced a missing key and could not start. `VALKEY_ADDR` is now required with the other Secret values, and a test renders the chart without it. The Makefile tool downloads (hadolint, Trivy, kubeconform) use `curl -f`.
- The AsyncAPI document names two inbound messages, `MembershipRevoked` and `TenantMembershipsPurged`, that had no schema file, so the schema registry workflow failed its AsyncAPI-to-schema coverage pass. Both files are added (open schemas, as the other inbound ones), and a test fails when any message lacks its file.
- A connector callback for a paused instance returns 409 `INVALID_INSTANCE_STATE` instead of success; the workflow dropped the signal, so the task never completed after resume. The connector worker redelivers until the instance runs. A callback for an instance that is over is acknowledged with no signal.
- A task failed through `UpdateTaskStatus` vacates its active assignments, as a cascade-failed task does, so the automation principal no longer holds a failed task.

### Changed

- The image build and the Trivy scan are single jobs named `Build image (cache)` and `Trivy CVE scan`, as the branch ruleset requires; as matrices they reported per image and the required checks never appeared. The GHCR push and release image jobs generate the sqlc code before building.
- A delegation scope this service has no case for (anything but `all`, `department` and `tender`) matches nothing and is logged and counted; it was compared with the instance's `subject_id`, which could reroute the wrong tasks when IAM adds a scope.
- The LLD residual-risk row says the connector callbacks answer `204`, as built.
- The public spec follows the handlers: a task's assignment record has no `task_id`, the active-assignment item is the seven fields the handler sends, and the outside-answer routes declare `413` and `415`. Contract tests send the instance and task routes through the router and fail on any response field the spec omits.
- `workflow-connectors/v2` is v2.1.0. The connector worker's Valkey ACL user needs `+asking`, the startup document-reference check puts and deletes a probe object (the worker needs `s3:PutObject` and `s3:DeleteObject` on the prefix), and an unknown key in the alias file fails startup.
- `WorkflowTaskFailed` carries an optional `error_class` when a stage fails (the connector's error class); a cascade failure carries none. The payload schema, the AsyncAPI document and the LLD describe it, and the message no longer claims a task only fails through a cascade.
- Branch conditions are parsed and matched by `workflow-models` `dsl.ParseCondition`: `= decision = "bid"` evaluates as documented, a literal that contains `==` is no longer mis-split, numbers compare as numbers, and an unsupported form fails the gateway.
- The internal spec declares 502 `EVENT_DECODE_FAILED` on the four event category routes. The copy of the Definition Service's internal spec is refreshed and `SOURCES` names `feat/service`.

### Added

- `make test-compat` (`test/compat`, build tag `compat`) builds the Definition Service from a checkout (`DEFINITION_SERVICE_DIR`, default `../definition_service`) and runs it against this service's real API, worker activities, Postgres, Valkey and Temporal. It publishes every standalone Definition fixture and starts it here, runs a two-stage template and a library-module call to completion, and checks the archive guard, the pause after a department or tenant revocation, both tokens, the refusal codes and the connector-alias decode. CI runs it in a `compat` job against the Definition Service's `feat/service` branch (a `compat_peer_ref` input of `validate-test.yml`), and `make test-e2e`, which ran in no job before, in its own `e2e` job.
- A retention sweep in the API process, once a day and at start: published `outbox_events` older than the 7-year audit floor are pruned in batches through the outbox runner, and `processed_event` rows older than 7 days are pruned (the table has no row-level security). Settings `OUTBOX_RETENTION`, `PROCESSED_EVENT_RETENTION`, `RETENTION_SWEEP_INTERVAL` and `RETENTION_SWEEP_BATCH`; the counter `workflow_execution_service_retention_rows_deleted_total` counts rows by table.

- Contract tests send the event category routes and the connector-task complete and fail callbacks through the real router against `api/internal-openapi.yaml`.

### Added

- `workflow_execution_service_*` collectors built through `platform-gincommon`, with the `platform_*` series from the platform libraries in every scrape; `make metrics-lint`, a generated `docs/observability/metric-registry.md`, a scrape test, and a test that every metric an alert rule names is exported.
- The metrics the design listed and nothing recorded are recorded: task-signal and instance-start durations, activity durations, version conflicts, plan-cache hits and misses, upstream dependency errors, SLA breaches, DEGRADED entries, workflow panics, sampled RLS violations (`rls_violation_log` exporter), and the DEGRADED and READY-task gauges (`OPERATIONAL_STATS_INTERVAL`).
- A Temporal worker interceptor for activity duration and workflow panics; the worker process installs the Temporal tracing interceptor; every outbound HTTP call carries the trace context.
- Route parity tests for the public and internal OpenAPI and AsyncAPI parity tests for produced and handled event types; a chart test that every Deployment sets the environment its binary requires.
- `ARCHITECTURE.md`, `CONTRIBUTING.md`, `VERSIONING.md`, `.editorconfig`; a changelog gate on the release workflow.
- `RLS_VIOLATION_EXPORT_INTERVAL`, `RLS_VIOLATION_RETENTION`, `OPERATIONAL_STATS_INTERVAL`, and `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_INSECURE`, `OTEL_TRACES_SAMPLER_RATIO` in the chart.
- `POST /instances/:id/nodes/:node/complete` and `.../boundaries/:boundary/fire`: an outside party's answer completes a receive task, or fires a message boundary, that no send task of the plan feeds (`dsl.ExternalWaits`). Open to an admin and to a user assigned to a task of the instance; `WAIT_NOT_FOUND` (404) and `NOT_WAITING` (409) are new. The `instance-external-answer` signal and the `waiting-external` query carry it (change id `external-wait-by-key`).
- Completed tasks' results and outside answers merge into the instance's variables, which each task is created with (change id `task-result-variables`).

### Changed

- The connector worker runs `workflow-connectors/v2` v2.0.0 in full. A failed call is retried only when the library's retry decision allows it, up to `CONNECTOR_RETRY_MAX_ATTEMPTS`, with exponential backoff and jitter; before, every error was retried without limit. Each decision is logged and counted by `workflow_execution_service_connector_task_failures_total`.
- Every connector call carries its tenant. Document references (S3 bucket `CONNECTOR_DOCS_BUCKET`, metadata in Valkey), the Google Drive document registry and send-email duplicate protection (PostgreSQL, under a tenant policy added by migration `000009`) are wired; the migrate step applies the library's schema first. The worker needs `DATABASE_URL`, and the chart gains the bucket, retry and retention values and the egress to PostgreSQL and HTTPS.
- `/api/v1/internal/connector-tasks/:id/fail` accepts the connector's output map (a send-email delivery outcome, a rest-call status), whose keys the service logs.
- The worker drops cached provider clients on the alias refresh period and prunes Drive audit rows past `CONNECTOR_AUDIT_RETENTION` daily.
- The inbound queue set follows IAM's AsyncAPI: `DelegationStarted` and `DelegationEnded` come from `iam.delegation.events` through their own queue, `MembershipRevoked` and `TenantMembershipsPurged` are on the membership topic, and each queue's subscription carries the producer's filter policy. LLD rev 1.76 and the AsyncAPI (1.1.0) carry the correction; no code changes.
- `workflow-models/v2` v2.0.2 (import path `…/workflow-models/v2/pkg/…`): every plan expansion passes a stage budget of 1000, the limit Definition enforces at publish, because the library refuses a zero budget. A stored plan that expands past it is an invalid version and fails the compiled-plan activity without retry.
- **Platform libraries** `platform-events/v2` v2.1.0, `platform-pgcommon/v2` v2.0.1, `platform-gincommon` v1.6.0, `workflow-models/v2` v2.0.2, `workflow-connectors/v2` v2.0.0; Go 1.26.9. The service name is `workflow-execution-service` for all three binaries and every service metric is prefixed `workflow_execution_service_`. Alerts use the platform metric names; `tenant_id` is no longer a metric label.
- `APP_ENV` must be one of `local`, `dev`, `test`, `staging`, `prod`; `AWS_USE_STUB` must be `false` outside dev and local; the connector worker requires `OPENBAO_TOKEN` outside dev; the chart sets `appEnv: prod` and the OTLP settings, and the lint values cover every required secret.
- The internal OpenAPI drops the retired catch-all `/api/v1/internal/events` and lists `/events/workflow-task`; the AsyncAPI documents `MembershipRevoked` and `TenantMembershipsPurged`.
- Servers set read-header timeouts, the health servers set read, write and idle timeouts, and the request timeout runs inside the observability middleware.
- Non-test code carries no comments.
- The outbox existence check test runs under the tenant's context, as the code does.
- **A defer sends the instance one completed step back** (change id `defer-routes-back`), as the public API always said: the deferred task is closed `DEFERRED` naming the earlier node, and that step runs again through the force-back path. It made a copy of the deferred task at the same node before, so nothing went back. `DeferTask` returns no output and the regression-task helper is gone; deferring with no completed step is `409 INVALID_TASK_STATE`, and an instance that is not running `INVALID_INSTANCE_STATE`. Nothing is recorded as an admin route (LLD rev 1.73).
- **An admin force-route names a node key and nothing else** (change id `force-route-by-node-key`): the department and the branch come from the key. Force-forward in a parallel step and in a DEGRADED instance, and force-back in a DEGRADED instance, were dropped because the HTTP path never set a department. `POST /instances/:id/force-back` takes an optional `target_node_key` naming a failed branch; a branch that fails again after a respawn keeps its steps and its last completed node.
- **A fired boundary runs its whole path.** An interrupting boundary runs its `Steps` in the host's place and ends its path, so nothing the host would have gone on to runs; a non-interrupting one runs them beside the host. A boundary with no `Steps` behaves as before (workflow-models `v1.4.0-rc.4`, LLD rev 1.72).
- **A stage that fails inside a call or subprocess raises its error class to the call's error boundaries** (change id `subworkflow-error-raised`): the path naming the class, else the catch-all, runs; with none the instance fails as before. Error boundaries are interrupting.
- **A task open in a cancelled scope is superseded** (change id `abandoned-task-superseded`): an interrupted call's open tasks are marked `SUPERSEDED`, their assignments vacated and `WorkflowTaskSuperseded` enqueued, its actor the nil user identifier. The `UpdateTaskStatus` activity no longer supersedes a task that is already completed, failed or superseded.
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
