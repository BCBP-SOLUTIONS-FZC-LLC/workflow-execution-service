# Execution Service — Low-Level Design

> This is the in-repo copy of the LLD (`docs/lld/`), kept content-identical to the design repo's published copy. The two are intentionally allowed to differ only on embed-vs-link mechanics (e.g. this copy may link to this repo's own `api/asyncapi.yaml` instead of embedding it) — never on content.

The **Workflow Execution Service** is the runtime control plane for the BPMN-driven Workflow Engine. It instantiates and drives compiled workflow instances to completion on top of a shared Temporal cluster, handling instance/task lifecycle, admin overrides, retry/failure semantics, and the full domain event surface. Execution Service owns runtime orchestration state. It is the only service in the platform with a Temporal dependency.

---

## Table of Contents

1. Overview & Scope
2. DSL & Temporal Semantics
3. Temporal Operations
4. Data Model
5. API Design
6. Events & Integration Topology
7. Testing & Infrastructure Strategy
8. Worked Example: Tender Lifecycle Walkthrough
9. Security
10. Non-Functional Requirements

- Appendix A: Design Decisions
- Appendix B: Open Items
- Appendix C: Database Migration Scripts
- Appendix D: Glossary
- Appendix E: Coding Style
- Revision history

---

## 1. Overview & Scope

### 1.1 Service Definition and Platform Role

Execution Service is the runtime that instantiates and drives BPMN-derived workflow instances to completion. It is one of three services in the Workflow Engine platform, all sharing a single Temporal cluster:

- **Definition Service** — BPMN ingestion, structural/semantic validation, DSL compilation, DRAFT → PUBLISHED → ARCHIVED versioning.
- **Execution Service** — instance/task API, signal forwarding to Temporal, the subject of this document.
- **Temporal Workers** — the workflow function and its activities;

Execution API and Temporal Workers are *one logical domain* split across two deployable processes (`cmd/server`, `cmd/worker`) for independent scaling and rollback, not two services — both connect to the same `workflow_execution` database.

Execution Service hosts a domain-generic orchestration engine: today's only tenant workload is tender approval, but the engine must not bake tender-specific concepts into its own schema, API, or event contracts. `subject_id` and `business_key` are opaque, domain-scoped identifiers for this reason; a future domain (e.g. PMS) can be a first-class citizen of the same engine. The engine reads neither: the subject is the entity the calling domain says the instance is about, and the key identifies one instance within that subject.

### 1.2 Architectural Principles

- **Identity-agnostic.** No local auth, no user/department storage — only UUID references. Every request carries gateway-injected headers (`x-tenant-id`, `x-user-id`, `x-tenant-roles`, `x-departments`, `x-plan`, `x-feature-flags`). Trial tenants share a Keycloak realm, so `x-tenant-id` is the real isolation boundary.
- **Signal-Only API.** The HTTP layer validates and forwards to Temporal as a signal; it never writes the DB directly. Temporal Activities are the sole DB writers, avoiding the dual-write problem between orchestration state and relational state.
  - **Documented exception.** Bulk tenant/delegation events (`TenantStateChanged`, `DelegationStarted`, etc.) write the DB directly in one transaction, then signal Temporal in a loop — routing thousands of per-instance signals through a single SQS-handler invocation would blow the visibility timeout.
- **Transactional outbox.** State change and domain event commit in one transaction; a background runner publishes to SNS after commit (§6).
- **Triple-layer tenant isolation.** Gateway header (rejected before DB on mismatch) → Postgres RLS (`tenant_id = current_setting('app.tenant_id')::uuid`, GUC per-transaction, no `BYPASSRLS`) → Temporal task-queue partitioning (`wf-queue-<tenant_uuid>`) for enterprise/noisy-neighbor isolation. The Temporal Web UI/cluster itself is not a fourth tenant-isolation layer and doesn't need to be one: it's never exposed to any tenant-facing surface, only to internal ops/cluster-operator access. An admin who needs to scope a query to one tenant filters or groups by the `TenantId` Search Attribute (§3.6) — no additional isolation layer is required.
- **Optimistic locking.** `workflow_task.record_version` and `workflow_instance.record_version` are bumped on state-changing writes; every admin signal is version-checked against them.

### 1.3 Cross-Service Integration

Execution Service depends on Definition Service for compiled plans and validates against it at two points:

- **The Definition Service's compiled-plan read, `GET /api/v1/internal/workflow-versions/{version_id}/compiled-plan?tenant_id=` → `{workflow_id, version_id, version_number, status, is_valid, compiled_plan}`** (§5.3) — called at instantiation (`POST /instances`) and re-validated authoritatively inside `GetCompiledPlanActivity`; rejects a `DRAFT`/`ARCHIVED` version (`409 VERSION_NOT_PUBLISHED`) or an invalid one (`409 VERSION_INVALID`). Instance start always reads it live, so a version the Definition Service has invalidated or archived is refused at once: the Definition Service evicts its own copy on both events. Each read is written through to a Valkey cache keyed by `(tenant_id, version_id)` (shared across API replicas), which serves only reads for running instances (an outside answer), since a version's plan body never changes; that read is cache-aside and fail-open on a miss or an unreachable cache.
- **`GET /api/v1/internal/workflows/{workflow_id}/active-instances?tenant_id=` → `{has_active, count}`** — the archive guard the Definition Service calls on Execution's internal API before allowing `POST /workflows/:id/archive`; blocks the archive if any instance is `RUNNING` or `PAUSED`. Archiving a template version never affects instances already running against it — they hold their compiled plan from the moment they started and never re-fetch it.

`POST /api/v1/internal/users/{user_id}/pause-tasks` is the safety-net call the Definition Service makes on `DepartmentMembershipRevoked`/`MembershipRevoked` — Execution runs no separate subscription for these.

Beyond Definition Service, Execution integrates with:

- **Org & Membership / User Profile (IAM)** — inbound events (delegation, tenant lifecycle, user lifecycle, availability) and outbound synchronous eligibility calls (§6, §5).
- **The browser** — calls Execution's business-action and read endpoints (claim/complete/defer/reassign/overrideNodeAssignee, list/detail) directly through the platform's API gateway, the same trust model any other caller uses (gateway-injected headers, §5.6/§5.7). No intermediary service sits in this path (Appendix A.2, decisions 19/20/31). UUID-to-display-name enrichment is likewise not Execution's or any backend's concern: the browser composes Execution's raw-UUID responses with IAM's `/users:batch`/`/departments` directly (§5.9).
- **Dashboard Stream Gateway** — a separate SSE fan-out service (not Execution's), consuming Execution's outbound event stream and routing per-user pushes. Its own subscription mechanism, connection-auth ticket, and backfill design are owned by that service.
- **Tender Service** — consumes `WorkflowInstanceFinished` as a post-hoc finalization notification only. For the approval/signing flow itself, Tender's own backend calls Execution's Complete endpoint directly, synchronously, immediately after its own signing step succeeds (§3.5) — the same caller-agnostic contract any direct caller uses, not a call routed through an intermediary.

### 1.4 Document Map

| § | Content |
| --- | --- |
| 2 | DSL & Temporal semantics — the compiled-plan constructs the workflow function executes |
| 3 | Temporal operations — signals, activities, retry/failure semantics |
| 4 | Data model |
| 5 | API design — HTTP endpoints, errors |
| 6 | Events & integration topology |
| 7 | Testing & infrastructure strategy |
| 8 | Worked example — tender lifecycle walkthrough |
| 9 | Security |
| 10 | Non-functional requirements |

### 1.5 Architecture Diagram

```mermaid
---
config:
  layout: elk
---
flowchart TB
 subgraph Clients["Clients"]
    direction TB
        Admin["Tenant Admin / Browser"]
  end
 subgraph Execution["Execution Service"]
    direction TB
        API["API process<br>cmd/server<br>HTTP"]
        Worker["Worker process<br>cmd/worker<br>Temporal Workers"]
        Outbox["Outbox Relay<br>runs in API process"]
  end
 subgraph Runtime["Runtime Infrastructure"]
    direction TB
        Valkey[("Valkey<br>compiled-plan cache")]
        PG[("Postgres<br>workflow_execution")]
        Temporal[("Temporal Cluster")]
  end
 subgraph Dependencies["Definition & Identity"]
    direction TB
        Def["Definition Service"]
        IAM["Org &amp; Membership<br>User Profile"]
  end
 subgraph Consumers["Workflow Event Consumers"]
    direction TB
        Dash["Dashboard Stream Gateway"]
        Tender["Tender Service"]
        LLM["LLM Service"]
        Notif["Notification"]
        Audit["Audit Log"]
  end
    Worker -- poll task queues --> Temporal
    Admin -- HTTP --> API
    Tender -- HTTP sync, Complete-after-signing --> API
    API -- Start / Signal / Query --> Temporal
    API -- cache read/write --> Valkey
    Worker -- Activities read/write --> PG
    Worker -- enqueue outbox event<br>same transaction --> PG
    API -- compiled-plan read (internal HTTP) --> Def
    API -- eligibility checks --> IAM
    Def -- active-instances / pause-tasks (internal HTTP) --> API
    IAM -- Delegation / Tenant / User events --> Consumer["Shared Workflow-Events Consumer"]
    Consumer -- POST /api/v1/internal/events --> API
    Outbox -- poll outbox_events --> PG
    Outbox -- publish --> SNS[/"SNS<br>wf-workflow-events"/]
    SNS --> Dash & Tender & LLM & Notif & Audit
```

### 1.6 Technical Stack

- **Language**: Go 1.26
- **HTTP Framework**: Gin (REST APIs), used identically to Definition Service for the `cmd/server` API process.
- **Workflow Orchestration**: Temporal Go SDK (`go.temporal.io/sdk`) Execution Service is the only service in the Workflow Engine platform that talks to the shared Temporal cluster; it owns the workflow function (`runSteps`, boundary-event handling, park/resume mechanism, §2/§3) and every Activity implementation.
- **Internal HTTP API**: the two-way contract with the Definition Service — Execution reads compiled plans from its internal API and serves the active-instances and pause-tasks endpoints on its own, each specified in an OpenAPI document both sides test against (§5.3).
- **Shared Middleware Library**: `platform-gincommon v1.6.0` — the same correlation-ID, OTel HTTP/gRPC span, Prometheus metrics, structured Zap logging, panic recovery, gateway header authentication, tenant context propagation, per-route RBAC, per-request timeout, and graceful telemetry shutdown middleware Definition Service uses, wired identically in `cmd/server`. `cmd/worker` uses only the subset that applies to a process with no business HTTP surface — OTel init, Zap logger, and the minimal health/metrics HTTP server (§7.5/§7.6) — not the gateway-header or RBAC middleware, since the Worker receives no gateway traffic.
- **Database**: PostgreSQL (schema: `workflow_execution`), physically separate from Definition Service's `workflow_definition` schema (§4.1), shared between the API process and the Worker process as a documented same-domain exception to database-per-service (§5.1).
- **Database Driver**: `platform-pgcommon/v2 v2.0.0` — the same pgx/v5 wrapper Definition Service uses: RLS GUC injection (`app.tenant_id`, `app.user_id`, `app.tenant_roles`), slow-query logging, OTel tracing, Prometheus pool metrics, transaction helpers, pool health check, and `migrate.Runner`. Both processes' pools are instrumented identically (§7.6).
- **Database Tooling**: `sqlc` for type-safe query generation, `platform-pgcommon/v2` `migrate.Runner` for schema migrations (one shared migration Helm hook runs before either Deployment rolls, §7.5).
- **Distributed Cache / Store**: Valkey 8.0 (Redis-compatible), via `github.com/redis/go-redis/v9`. Used by the API process (`cmd/server`) for the compiled-plan cache (§1.3, §6.2), written through by every instance start and read, cache-aside and fail-open, by running-instance reads such as an outside answer, falling through to the Definition Service's compiled-plan read — the Temporal Worker process (`cmd/worker`) has no Valkey dependency at all (§7.6). `cmd/connector-worker` (`workflow_connectors.md` §6.1) is a third, separate process with its own, new Valkey dependency — a Stream consumer for connector-typed `WorkflowTaskCreated` events (`workflow_connectors.md` §6.5), unrelated to either of the above two usages.
- **Event Bus (Outbound)**: `platform-events` — typed event envelopes, SNS publisher, transactional outbox runner, all mirroring Definition Service's shipped configuration (§6.6). The single outbox relay runs in the API process only; the Worker process enqueues via Activities but never publishes.
- **Prometheus**: collectors built through `platform-gincommon` as `workflow_execution_service_*`, served on a dedicated metrics port by every binary (§7.6).
- **OpenTelemetry**: tracing initialised through `platform-gincommon` in all three binaries, with the Temporal tracing interceptor on the API client and the worker and trace context injected on every outbound HTTP call (§7.6).
- **Structured Logging**: `go.uber.org/zap` via `platform-gincommon/pkg/logger`, used identically by both processes.
- **Mocks**: `go.uber.org/mock` (GoMock / `mockgen`) — generates mocks for all `core/port/` interfaces, the same convention as Definition Service (§1.5.3).

**Three binaries, one module.** Execution Service ships **three binaries from one Go module**. `cmd/server` is the API process: HTTP `:8080` (public routes and `/api/v1/internal`), the outbox relay, and the Temporal client that issues `SignalWorkflow`/`StartWorkflow`/`QueryWorkflow`. `cmd/worker` is the Temporal Worker process: it polls task queues and hosts the workflow function and its Activities, with no business HTTP surface. `cmd/connector-worker` is the connector dispatch process, a Valkey Stream consumer with no inbound business surface (`workflow_connectors.md` §6). The three share `go.mod`, `internal/core/domain` and `internal/core/port`. Each is built, containerized, deployed, scaled and rolled back on its own (§7.5). Together they are one logical domain running as three deployable processes (§1.1). Load testing decides whether any of them warrants promotion to a service of its own (§7.5).

### 1.7 Internal Package Layout

The service follows the same clean architecture Definition Service established: nothing in `core/` imports from `adapter/`, and nothing in `core/` or `internal/workflow/` imports from `adapter/`. Concrete adapters are wired to interfaces in two places instead of one — `cmd/server/main.go` for the API process's dependencies, `cmd/worker/main.go` for the Worker process's.

- **`workflow-execution-svc/`** (Project Root)
  - **`cmd/server/`**
    - `main.go` — API process bootstrap: starts the Gin HTTP server (the internal routes included) and the outbox relay; wires the Temporal client adapter and the Definition Service HTTP client adapter.
  - **`cmd/worker/`**
    - `main.go` — Temporal Worker process bootstrap: connects to the Temporal frontend, registers the workflow function and every Activity against the default task queue plus every tenant-isolated queue in the registry (`active_task_queues`, §4.6), runs the dynamic queue-registration loop. No outbox relay, no gateway-header middleware, no `INTERNAL_API_TOKEN` validation (§7.6).
  - **`internal/`**
    - **`core/`** — fully decoupled from external libraries and from `internal/workflow/`.
      - `domain/` — entities (`WorkflowInstance`, `WorkflowTask`, `WorkflowTaskAssignment`, `WorkflowEvent`, the `CompiledCollaboration` DSL types consumed from Definition Service, §2.1), sentinel errors.
      - `port/` — interface contracts: `Transactor`, `OutboxRepository`, `InstanceRepository`, `TaskRepository`, `TaskAssignmentRepository`, `DefinitionServiceClient` (the compiled-plan read), `IAMClient` (eligibility checks), `TemporalClient` (the `SignalWorkflow`/`StartWorkflow`/`QueryWorkflow` contract the API process's Temporal adapter implements), `CacheStore`.
      - `port/mocks/` — GoMock-generated mocks for all port interfaces. Gitignored, regenerated via `make mock`.
      - `service/` — business logic orchestrating use cases: `InstanceService` (instantiate, admin signals), `TaskService` (claim/complete/reassign), `DelegationHandler`, `TenantLifecycleHandler`, `UserLifecycleHandler` — imports `domain` + `port` only, exactly Definition Service's rule.
    - **`workflow/`** — **the one new architectural layer, with no analogue in Definition Service.** Contains the Temporal workflow function itself (`runSteps`: stage dispatch, boundary-event `Selector` handling, force-back/`DEGRADED` park-and-respawn, SLA timer registration, §2.5–§2.9) and every Activity implementation (`GetCompiledPlanActivity`, DB-writing Activities for task/assignment/event rows, §3.1). This package imports `core/domain` + `core/port` only — an Activity is, from `core/`'s perspective, just another caller of `core/service`/`core/port` interfaces, same as an HTTP handler is. It does **not** import from `adapter/`.
      - **How it gets wired without an inverted dependency.** `internal/workflow` is never imported by `adapter/outbound/temporal` (the API process's outbound Temporal client wrapper) or vice versa — the two packages are peers, not a dependency pair. The connection between them is runtime registration, not a Go import: `cmd/worker/main.go` imports `internal/workflow` directly and calls `worker.RegisterWorkflow`/`worker.RegisterActivity` against a `worker.Worker` instance obtained from the Temporal SDK; `adapter/outbound/temporal` (used only by `cmd/server`) never references `internal/workflow` at all — it only issues `SignalWorkflow`/`StartWorkflow`/`QueryWorkflow` calls by workflow-type string and task-queue name, which the Temporal frontend routes to whichever Worker process has registered that type. The two processes are decoupled at the Temporal wire protocol, not at the Go module graph.
    - **`adapter/`** — outward-facing transport and driver implementations, imports `port` + `domain` only.
      - `inbound/http/` — Gin handlers, routes, and middleware (API process only), mirroring Definition Service's `inbound/http` shape.
      - `outbound/postgres/` — sqlc-generated DB layer (`db/`, gitignored) and hand-written repo adapters implementing `InstanceRepository`/`TaskRepository`/etc. Used by both `core/service` (via the API process, for reads) and by Activities in `internal/workflow` (for writes) — the same repository adapters are wired into both processes' DI graphs.
      - `outbound/temporal/` — the Temporal client wrapper the API process uses to make `SignalWorkflow`/`StartWorkflow`/`QueryWorkflow` calls; implements `port.TemporalClient`. API-process-only; the Worker process never calls this package, since Activities talk to Postgres directly, not back through a Temporal client.
      - `outbound/valkey/` — `CacheStore` implementation (API process only, §1.6).
      - `outbound/http/definition_client.go` — the Definition Service client, implementing `port.DefinitionServiceClient` (the compiled-plan read), used both by the API process at instantiation and by `GetCompiledPlanActivity` in `internal/workflow` for the authoritative re-fetch (§3.1) — this is the one outbound adapter both processes wire independently into their own DI graphs.
    - **`config/`** — env var loading into a typed `Config` struct; each binary checks only the settings it uses (§7.6).
  - **`test/fixtures/`, `test/unit/`, `test/integration/postgres/`, `test/integration/temporal/`, `test/e2e/`, `test/workflow/`** — as already described in §7 (Testing & Infrastructure Strategy) of this document.

**Import-direction rule, restated for this service's shape:** `domain ← port ← service`. `adapter/*` depends on `port` + `domain` only. `internal/workflow` depends on `domain` + `port` only — it is architecturally a peer of `core/service`, not a child of `adapter/`, even though it is only ever *invoked* (via Temporal's own registration/dispatch mechanism, not a Go call) from a binary (`cmd/worker`) that also wires up `adapter/outbound/postgres`. Nothing in `core/` or `internal/workflow` imports from `adapter/`. The only two places concrete adapters are wired to interfaces are `cmd/server/main.go` and `cmd/worker/main.go` — each binary's own composition root, matching the one-composition-root convention Definition Service already established, doubled for the two-process shape.

---

## 2. DSL & Temporal Semantics

### 2.1 Compiled-Plan DSL Shape

Execution consumes a `CompiledCollaboration` — the JSON artifact Definition Service's compiler emits and hands over via its compiled-plan read and `GetCompiledPlanActivity` (§3.1). It is not a single flat script; it is a small, typed DSL whose top-level shape drives everything the Temporal workflow function does.

```go
CompiledCollaboration{ main_plan, plans[]CompiledPlan, schema_version }
CompiledPlan{ name, task_queue?, departments[]DepartmentDef, execution ExecutionPlan, visual_elements[]? }
DepartmentDef{ id, label, iam_department_id?, props{}, stages[]StageDef }
StageDef{ type, activity, node_id, role, default_assignees[], due_date, follow_up_date,
          boundary_timer?, boundary_message?, engine_note, extras{}, is_zeebe_user_task }
ExecutionPlan{ steps[]ExecutionStep }
ExecutionStep{ sequential[]?, parallel[]ParallelBranch?, exclusive[]ExclusiveBranch?,
               sub_workflow?SubWorkflowStep, call_plan?CallPlanStep, io_mapping?IOMapping,
               extras{}, message_paths[]? }
```

Each `ExecutionStep` carries **exactly one** populated variant field — `sequential`, `parallel`, `exclusive`, `sub_workflow`, or `call_plan` — and `runSteps` (§2.5) dispatches purely on which one is set. A `call_plan` never reaches it: every stored plan is expanded when decoded, turning each call into a `sub_workflow` (§2.3). `ParallelBranch` (`dept`, nested `steps[]`), `ExclusiveBranch` (nested `steps[]` and target/revert field groups, §2.6), and `SubWorkflowStep` (a nested `ExecutionPlan` plus its own `error_paths`/`timer_paths`/`message_paths`) are the three step bodies. `TargetDept`/`RevertToDept` (on `ExclusiveBranch`) are already resolved, at compile time, to the concrete department the boundary/path's outgoing flow lands in — the workflow function never has to re-derive routing from raw BPMN flow edges.

`StageDef.Extras` and `DepartmentDef.Props` are opaque passthrough bags of Zeebe custom properties — treated as forward-compatible metadata unless a specific key is given defined meaning in the shared DSL contract, **with one production exception**: `StageDef.Extras["message"]` is load-bearing, not opaque (§2.5). `CompiledPlan.VisualElementDef` entries (e.g. `dataStoreReference`) and `CompiledCollaboration.Messages` (the `bpmn:messageFlow` catalogue) are both diagram-only — the workflow function ignores them entirely; real cross-task correlation runs through `Extras["message"]`, never through `Messages`.

`IOMapping` (`Inputs`/`Outputs`, each an ordered `{Source, Target}` pair list) sits on a `callActivity`'s call step and on a connector stage. An input's `Source` is `=name`, which reads the instance variable `name`, or any other text, which is a literal (`IOVar.InputValue`, workflow-models LLD §2.5); an output's `Source` names a key of the connector's result (`IOVar.OutputKey`). On a call step the workflow function applies the inputs on entry: each is resolved and copied into `context_json` under its `Target` before the call runs, and outputs are not separately re-applied on exit, since the call's own steps write `context_json` directly as they execute. A connector stage's inputs are resolved the same way against `context_json` when its task is created, and its outputs when the worker reports the result (§7). The input rule applies under change id `io-input-rule`; before it, a source was always a variable name.

**Instance variables.** `context_json` is the instance's variables: one flat map, shared by every branch. It starts as the start request's context and gains the keys of every completed task's `result_json` (a connector's is its mapped outputs) and of every outside answer (§5.11); a later write to a key replaces the earlier. Each task is created with the map as it stands. Task results merge under change id `task-result-variables`; before it, only the start context and call inputs wrote it. Exclusive-gateway conditions still read only the last completed task's `result_json` (§2.6).

### 2.2 Boundary Events

Boundary events (timer, error, message) have no dedicated JSON wrapper of their own — they are flattened into the attached node's step at compile time, and where they land depends on the host element:

| Host element | Timer | Error | Message |
| --- | --- | --- | --- |
| Plain task (`userTask`/`sendTask`/`receiveTask`) | `StageDef.BoundaryTimer` (singular) | Not supported — a compile-time rejection | `StageDef.BoundaryMessage` (singular) |
| `subProcess` | `SubWorkflowStep.TimerPaths[]` | `SubWorkflowStep.ErrorPaths[]` | `SubWorkflowStep.MessagePaths[]` |
| `callActivity` | `CallPlanStep.TimerPaths[]`, a `SubWorkflowStep`'s once expanded | `CallPlanStep.ErrorPaths[]` | `CallPlanStep.MessagePaths[]` |

A plain task carries at most one timer boundary (compiler-enforced). Definition Service's validator rejects a task with more than one message boundary event at compile time, the same enforcement the timer boundary already has (tracked as an implementation TODO in Definition Service's own repo). `ErrorPath.ShortCircuit` is declared in the struct but never populated by the compiler and carries no runtime meaning until one is assigned.

**Runtime handling.** Before entering any step that carries boundary data, the workflow function registers one `workflow.Selector` case per boundary alongside the case awaiting the host's own completion:

1. **Timer boundary** → `workflow.NewTimer(ctx, duration)`. The duration is a Go duration (`72h`) or an ISO 8601 period in the form Definition's validator accepts: weeks alone, or days and a time part (`P2W`, `P5D`, `PT1H30M`). An ISO period is read under change id `iso-timer-duration`; before it, one was dropped with a warning.
2. **Message boundary** → a signal channel scoped to the boundary's message name.
3. **Error boundary** (`subProcess` and `callActivity`) → a dedicated `workflow.Channel` that the body pushes to when a stage fails inside it. The error class the stage failed with, a connector's `error_class` for one, is matched against the boundary's error paths: the path naming that class wins, else the catch-all (a path with no code). An error boundary is interrupting. A failure no path matches, and any other error, fails the instance as before. This applies under change id `subworkflow-error-raised`; before it, every failure inside a call failed the instance.
4. `Selector.Select` blocks until exactly one case fires. If the host's own completion fires first, any pending timer is cancelled and an unfired non-interrupting boundary is simply abandoned.
5. If a boundary fires first: an **interrupting** boundary cancels the in-flight host and runs its path in the host's place; a **non-interrupting** boundary lets the host keep running while its path also runs — both continue independently. The path is the boundary's `Steps`, run in order like the steps of an exclusive branch; an interrupting boundary then ends its path, the steps of the plan level holding the host stopping as for a boundary that terminates (rule 6), so nothing the host would have gone on to runs. A boundary with no `Steps`, which is one recorded before paths were carried or one whose path continues into the main flow, runs the department `TargetDept` names alone. Every task still open inside a cancelled host is marked `SUPERSEDED` (§4.3).
6. **A boundary that terminates** (`Terminates`, set when its path leads straight to an end event) ends that path when it fires. Interrupting, the steps of the plan level holding the host stop there, so a boundary inside a module ends the module's own flow and its caller carries on, a top-level one completes the instance, and a parallel branch settles as done at its join. Non-interrupting, it does nothing. A force-back that re-runs the department before a fork, whose boundary then ends that path, leaves the fork in place: the branches resume and the join proceeds. The interpreter reads it as an empty `TargetDept`, which is the same boundary: every boundary sets exactly one of `TargetDept` and `Terminates`, and `dsl.ExpandCalls` refuses a plan with one that sets both or neither, so such a plan never starts (`VERSION_INVALID`) and never reaches the interpreter. Reading the empty target keeps an instance whose plan was recorded without `Terminates` running unchanged; such a stored plan starts no new instance. This applies under change id `boundary-no-target-ends-path`; before it, such a boundary failed the instance. 

### 2.3 subProcess and callActivity Constructs

Two BPMN constructs nest process fragments:

1. **`subProcess`** compiles to `ExecutionStep.SubWorkflow`. The workflow function interprets it **inline/recursively, inside the same Temporal workflow execution** — never as a child workflow.
2. **`callActivity`** (calling a reusable module, or another process in the same file) compiles to `ExecutionStep.CallPlan`, naming a plan of the collaboration that was compiled once, with its department bindings, call-site default assignees and boundary paths (definition LLD §4.1.2; workflow-models LLD §2.6). Execution expands every call with the shared `dsl.ExpandCalls` wherever it decodes a stored plan: the `GetCompiledPlan` activity, instance start (override-map and eligibility checks) and the service's compiled-plan cache (reassign and delegation level lookups). Each call becomes the `SubWorkflowStep` the interpreter already runs, with the called plan's departments cloned under call-scoped IDs (`<callNodeID>::<department>`, nested calls composing the prefix), bound to the caller's IAM departments. A module task's node key is therefore `<callNodeID>::<department>/<task>`, which is what `override_map` names. The expansion happens inside the activity, so the plan the workflow records and replays is the expanded one, and a later change to `ExpandCalls` never alters a running instance; a plan without calls expands to itself, so no `GetVersion` gate is needed. A plan that cannot be expanded fails the activity as `DefinitionServiceClientError`, non-retryable.

Practical consequence: `sub_workflow` always recurses inline, and the interpreter has no `callActivity` code — by the time a plan reaches it, every call is an ordinary `sub_workflow`. A BPMN document runs one pool; outside parties' pools and lanes produce nothing (definition LLD §4.1.2), so no step crosses into another pool.

### 2.4 Stage-Type Dispatch

`StageDef.Type` is not uniform across task kinds — the workflow function needs four distinct dispatch branches:

| `Type` | Compiled fields | Dispatch |
| --- | --- | --- |
| `prep` / `review` / `approve` | `Activity` = registered Temporal activity name | `CreateTaskActivity` + claim/complete signal wait — a human claims, works, and submits `result_json`. LLM assistance during `prep` drafting is entirely Frontend ↔ LLM Service ↔ User Profile, outside Execution's call chain; Execution's only touchpoint is the pre-existing one-way async cache pre-warm event. |
| Unrecognized `Type` | `EngineNote` set to a "not a defined class" string | **Does not fail** — the intended forward-compat path for a task type Definition does not define. Dispatched exactly like the row above, using the raw `Activity` string as a generic label; `EngineNote` is logged for audit, never used to fail the instance. |
| `send_task` | `Extras["message"]` = resolved message name | Fires the message named `Extras["message"]`; proceeds immediately, no wait. |
| `receive_task` | `Extras["message"]` = resolved message name | Waits (a `Selector` case) for an inbound signal matching `Extras["message"]` before proceeding. A receive task no send task of the main plan feeds is an **outside wait**: it proceeds only on an outside party's answer addressed by its node key (§5.11). |
| Connector-typed (`connector:<name>`) | `connector_type` = the connector's name | `CreateTaskActivity` + signal wait — dispatched exactly like the first row above, except the wait resolves via one of two signals, `stage-transition` on success or the new `stage-fail` (§3.1) on an exhausted/failed call or an unregistered connector type, both raised by `ConnectorTaskService` on `cmd/connector-worker`'s HTTP callback (`POST /api/v1/internal/connector-tasks/:id/complete`\|`/fail`, §5.2) — never a human, and never a direct Temporal signal from `cmd/connector-worker` itself, which has no Temporal SDK dependency. See `workflow_connectors.md` for the full design. |

**Intra-pool message correlation** — a `sendTask` and `receiveTask` inside the same Temporal execution — has no Temporal signal to ride on, so the workflow function maintains a **workflow-local message buffer**: a `map[string][]nodeKey`, message name → an ordered list of node keys that fired an as-yet-unconsumed send.

1. **Scope: instance-wide**, shared across every `Parallel` branch and every inline `SubWorkflow` recursion — not per branch. A send in one parallel branch and a receive in a sibling branch of the same gateway is an ordinary concurrent-lane pattern; a per-branch buffer would leave the sibling's receive blocked forever with no error or timeout.
2. **`send_task` fires (node N).** If a `receiveTask` is currently blocked on this message name (any branch's), deliver directly and skip the buffer. Otherwise append `N` to `buffer[messageName]` and proceed immediately.
3. **`receive_task` reached.** If `buffer[messageName]` is non-empty, pop the oldest entry (FIFO) and proceed immediately — the popped entry may have been fired by any branch, which is exactly the cross-sibling correlation the instance-wide scope enables. Otherwise block on the signal channel.
4. **Unconsumed entries** persist harmlessly for the rest of the execution and are discarded with the execution's own state at completion — fire-and-forget, matching a dangling cross-pool send.
5. **Reset on regression.** A force-back (§2.7) or a DEGRADED force-back respawn (§3.3) that rewinds a branch past a `send_task`'s node removes, from `buffer[messageName]`, only the **unconsumed** entries whose firing node key falls in that branch's rewound span — never a sibling's. Consumed entries are already gone (step 3 popped them); a send a sibling already consumed is not undone.

**Outside waits.** A receive task or message boundary that no send task of the main plan feeds (`dsl.ExternalWaits`, workflow-models LLD §2.6) can only be completed from outside, so it does not wait in this buffer. The interpreter registers it under its wait key, its node key for a receive task and `<host>#<boundary id>` for a boundary, and an `instance-external-answer` signal (§3.1) naming that key completes it: a receive task proceeds, a boundary fires as any message boundary does (§2.2). The answer's variables are merged first (§2.1). An answer for a key nothing is waiting in is dropped, never kept for a later visit. This applies under change id `external-wait-by-key`; before it, such a wait sat in the buffer and nothing could complete it.

Because Temporal workflow goroutines are cooperatively scheduled on a single thread, this shared map needs no mutex and stays replay-safe by construction. It is a distinct mechanism from Temporal's own native signal-channel buffering (§3.3) — that is the SDK buffering a signal delivered before its `Selector` case is live; this is an application-level map for the case where no Temporal signal exists between the two tasks at all.

### 2.5 Workflow-Function Execution Algorithm

1. On workflow start, the workflow function calls `GetCompiledPlanActivity` and holds the resulting `CompiledCollaboration` for the workflow's lifetime; it resolves `main_plan` and begins interpreting that plan's `Execution.Steps` via `runSteps`.
2. `runSteps` iterates the step slice in array order, dispatching on the populated variant:
   1. **`Sequential`** — for each department ID in order, dispatch each `StageDef` under it per §2.4, advancing only once the current stage resolves.
   2. **`Parallel`** — spawn one `workflow.Go` goroutine per branch, each independently calling `runSteps` for its own department; the parent blocks on a `Selector`/aggregation channel until every branch signals completion.
   3. **`Exclusive`** — evaluated per §2.6.
   4. **`SubWorkflow`** — recurse via `runSteps` on the nested plan, inside the same Temporal execution, registering the step's `ErrorPaths`/`TimerPaths`/`MessagePaths` as concurrent `Selector` watches (§2.2) active only while the recursive call is in flight.
3. When `runSteps` returns with no `Terminates`/revert pending, the workflow function calls `UpdateInstanceStatusActivity` with a terminal `COMPLETED` status.

**DSL-shape backward compatibility for in-flight instances.** Step 1's "fetched once, held for the workflow's lifetime" behavior is what makes an in-flight instance immune to a later re-publish of the same template: once `GetCompiledPlanActivity` returns a `CompiledCollaboration`, that struct — not a live pointer to Definition Service's current data — is what `runSteps` interprets for the rest of the execution, including across every replay. A DRAFT → PUBLISHED → ARCHIVED transition on `version_id` after instantiation has no effect on a running instance; it only changes what a *future* `StartWorkflow` resolves. The one case where this immunity has a hard edge is the DSL schema itself: `GetCompiledPlanActivity`'s fail-closed check (§3.1/§3.3) rejects a `CompiledCollaboration` whose `schema_version` major version the running Worker binary does not understand, and that rejection is deliberately **non-retryable** (§3.3's retry-policy table) rather than backed off — a major-version bump is a breaking shape change no amount of waiting resolves, so the instance fails fast into `FAILED`/`DEGRADED` (per branch scope, §8.2.6) instead of retrying against a plan it can never correctly interpret. Minor/patch schema revisions are additive by convention and never trigger this check.

`GetCompiledPlanActivity` (`internal/adapter/outbound/temporal/plan.go`) fetches the plan and expands its calls (§2.3) only when it is of the current major; any other major reaches `Execute` as stored. Expansion is bounded: a plan may hold at most 1000 stages and calls once expanded, the limit Definition enforces at publish, and a plan past it is an invalid version, fails the activity without retry and starts no instance. The fail-closed check and strategy resolution run in the workflow function (`Execute`), immediately after the activity returns — the same placement `Execute` already uses for its `mainPlan`-not-found check. A strategy for an older major therefore sees a plan that has not been expanded.

**Multi-version compatibility: Factory/Strategy layer.** A `SchemaVersion` major mismatch does not always mean rejection. `internal/workflow/compat.go` holds a registry, `map[int]schemaStrategy`, keyed by `SchemaVersion`'s major value; each entry normalizes that major's `CompiledCollaboration` shape into whatever `runSteps` expects. One running Worker binary interprets every major it has a registered strategy for — old in-flight instances alongside new ones — no second deployment. A major with no registered strategy fails closed per the paragraph above. An entry is deleted once its last in-flight instance completes.

**Points of change.** Other shared contracts can break in-flight instances the same way the DSL schema can:

| Point of change | Location | Compatibility posture |
| --- | --- | --- |
| `pkg/dsl` struct JSON shape | `workflow-models` `pkg/dsl` | Additive only; a JSON-tag change is a MAJOR bump (`workflow-models` `VERSIONING.md`). |
| Compiler output semantics | Definition `internal/bpmn_compiler` | No structural versioning; a compiler behavior change alters interpretation of newly-compiled plans without a struct-shape change. |
| NodeKey encoding | workflow-models' `dsl.NodeKey` (workflow-models LLD §2.3) | Persisted in Postgres task rows and Temporal signal payloads; an unrecognized `StageDef.Type` already passes through rather than erroring. |
| Signal payloads (`adminSignal`, `stageTransitionSignal`, `stageDeferSignal`) | `internal/workflow/signals.go` | Sent by external callers; no versioning. |
| Internal HTTP contracts | Each service's `api/internal-openapi.yaml`; the caller's copy in `api/contracts/` | `make contract-breaking` (oasdiff) refuses a breaking change against `main`; `make contract-check` refuses a stale copy (§5.3). |
| DB status-string encodings | `internal/core/domain` | Persisted as strings; no pressure point today. |

Only the DSL schema has a concrete compatibility mechanism today (the Factory/Strategy layer above). Everything else is additive-by-default; a genuinely breaking change at any of these points needs the same treatment before it ships.

### 2.6 Exclusive-Gateway Evaluation

Scoped to the dominant real-world shape: a binary yes/no check after a completed user task, deciding forward progress vs. a revert loop. This is the permanent scope: richer boolean/arithmetic condition expressions have no real BPMN fixture that needs them today, and no third-party expression-evaluation dependency is adopted. Revisit only if a real workflow needs more than a single equality/inequality check.

1. For each `ExclusiveBranch`, in array order: if `ConditionExpression` is non-empty, evaluate it as one comparison, `[= ] <field> (=|==|!=) <literal>` with a double-quoted string, a number, `true` or `false` as the literal, against the just-completed task's `result_json`. The expression is parsed and matched by the shared condition parser of `workflow-models`, the parser Definition Service's validator refuses other forms with, so a published condition is always evaluable; a field absent from the result is unequal to every literal, and a number compares as a number. An expression the parser refuses fails the gateway. No third-party expression-evaluation dependency is adopted.
2. The first branch whose condition evaluates true wins; no further branches are evaluated.
3. If no branch's condition evaluates true, fall back to the one branch (if present) with an empty `ConditionExpression` — the implicit else. Definition Service's validator enforces that exactly one outgoing flow per exclusive gateway may be conditionless (sourced from BPMN's own `default` flow marker on the gateway, not inferred), rejecting zero-or-multiple-default configurations at compile time (tracked as an implementation TODO in Definition Service's own repo).
4. The winning branch decides what happens next. A forward branch with `Steps` runs them, the branch's own work up to the gateway's join, and then, when it also has `Terminates: true`, ends its path. `Terminates: true` alone ends the path, as the end event it leads to does: at the top level the instance completes, in a parallel branch the branch is done, in a subprocess the subprocess is. This applies under change id `exclusive-end-completes-path`; before it, such a branch ended the instance as `TERMINATED` and wrote no instance status. A populated `RevertTo*` group transfers backward — structurally the same as a force-back (§2.7), condition-triggered rather than signal-triggered. A `Target*` group without `Steps` transfers forward to that department. An empty `Target` routes nowhere of its own: the path that branch selects is compiled as the steps that follow, so matching it resolves the step and `runSteps` advances.
5. **A revert re-enters the gateway.** Once the reverted-to work completes, steps 1–4 run again against *its* result, so the Nth rejection of the same node sends work back again and a BPMN rework loop (`Prepare → Review → Approve → "No" → Prepare`) behaves as drawn. Definition Service's compiler flattens the back-edge into a one-shot `RevertTo*` field plus a separate later step, so the compiled plan carries no loop construct for the interpreter to follow on its own.
6. **One gateway reverts at most `maxRevertCycles` (50) times**, then the instance fails with a diagnosable error. Every cycle is gated on a human completing a real task, so the bound exists because each cycle appends to Temporal's finite event history, not to catch a runaway loop. It is a compile-time constant rather than configuration: a workflow reading mutable config mid-execution replays non-deterministically if that value changes. Exceeding the bound never falls through to the forward branch the condition did not select.
7. **A re-evaluation matching nothing resolves the step.** On the first evaluation, no matching branch and no implicit else is a malformed plan (step 3). On a re-evaluation after a revert it means the rework loop has finished and the reverted-to work produced a result this gateway has no opinion on, so `runSteps` advances to the forward continuation the compiler emitted as the next step.

Empty `Target` is the shape Definition Service emits for a gateway with one forward exit and one or more send-back edges (`handleSingleForwardWithReverts`): the branch exists to tell "go forward" apart from "send back", and the forward path is already the following steps. Naming a `Target` there as well dispatches that department twice. The split shapes carry each branch's work as `Steps` instead, a call, a subprocess, an inner gateway or several departments included, since their branches diverge and their continuation begins at the join; their `Target*` group names the branch's first department and node for display.

`ExclusiveBranch`'s eight target/revert fields are two mutually exclusive four-field groups — every compiler construction site populates at most one:

| Field (forward / revert) | Role |
| --- | --- |
| `Target` / `RevertToDept` | Department/lane routing key — the coarse destination. |
| `TargetStage` / `RevertToStage` | A stage-type string, copied verbatim from the target's stage type (`prep`, `review`, `approve`, `sub_workflow`, `send_task`, `receive_task`, or empty). |
| `TargetNodeID` / `RevertToNodeID` | Precise, machine-addressable routing — populated only when dept+stage alone would be ambiguous; left empty for plain `prep`/`review`/`approve` stages. |
| `TargetName` / `RevertToName` | Human-readable label for audit/UI, always paired with the NodeID field, never used for routing. |

A branch's `Steps` take precedence over its `Target*` group. Otherwise dispatch uses `TargetNodeID`/`RevertToNodeID` when present, falling back to `Target`+`TargetStage`. Two branch kinds populate neither group: a branch that goes straight to its join or end event, and the single-forward branch of step 4, whose path is the following steps.

### 2.7 Force-Back and the Parallel-Gateway History Model

The workflow function maintains `completedNodes` — an append-only history stack of node keys, one pushed as each node completes. On `instance-force-back`, entries are popped down to the admin-chosen target, the in-flight task is regressed, and `runSteps` re-enters at the target.

The regressed task is closed out, not abandoned. Both force directions read the nodes that are pending *at the moment the signal arrives* — the live registry of in-flight stage calls, captured before the cancelled run unwinds and removes its own entry — and pass them to `RecordForceRoute`, which supersedes each one, vacates its assignments, and emits `workflow.task.superseded`. `completedNodes` is the wrong source for this: it holds nodes that have already finished, so a task named from it is never in a supersedable status. Force-back and force-forward differ only in which node they resume at and in the `direction` the audit event records (§6.4).

**Defer is the assignee's own one-step force-back.** `stage-defer` closes the deferring assignment, marks the task DEFERRED, enqueues `workflow.task.deferred` with the earlier node as `deferred_to_node_key`, and then routes exactly as `instance-force-back` does: the most recently completed node is popped from `completedNodes`, the message buffer is reset over the popped span, and `runSteps` re-enters at that node, whose department creates an ordinary new task (`workflow.task.created`). The deferred task is already closed, so no `workflow.task.superseded` is written for it, and no `workflow.instance.force-routed` is, since no admin routed anything. A task with no completed step before it cannot be deferred: `409 INVALID_TASK_STATE`, checked synchronously, as an admin force-back at the first step is `409 INVALID_INSTANCE_STATE`. Inside a parallel step the same routing applies as to an admin force-back, below.

Force-back **is** supported while a parallel gateway is active (`current_node_keys` has multiple entries):

1. The target node is popped from `completedNodes` pre-fork — the entry recorded before the parallel branches began, not any single branch's in-flight position.
2. Each parallel branch's goroutine still active at the time of the force-back is signalled to **save** its position rather than being cancelled — a paused flag the goroutine checks between steps, keeping its `workflow.Go` state valid for later resume.
3. The workflow function proceeds exactly as in the non-parallel base mechanism for the popped node.
4. Once the force-back resolves, each saved sibling branch **resumes from its saved position** — it does not restart — so side-effecting steps the branch already completed are never re-fired.

### 2.8 SLA Semantics: `DueDate`/`FollowUpDate`

`StageDef.DueDate`/`FollowUpDate` are real, populated compiled-plan fields, sourced verbatim from a BPMN task's `dueDate`/`followUpDate` XML attributes — raw strings, not typed dates, since Camunda/Zeebe allows either a plain ISO-8601 date-time or a FEEL expression there. Definition Service makes no attempt to parse or validate either field; it is a raw passthrough.

`dueDate` is the "this task should be done by" convention; `followUpDate` an earlier "check in on this" checkpoint. `CreateTaskActivity`'s input carries both fields when present, exactly like every other compiled field — this document fixes only that they flow through; the runtime SLA-timer mechanism itself is designed in §3.3/§3.4.

**Accepted scope limit:** only a plain ISO-8601 date-time is supported for `DueDate`/`FollowUpDate`. A FEEL expression is out of scope, consistent with §2.6's binary-only condition-expression evaluator — no real fixture needs richer date logic today.

---

## 3. Temporal Operations

### 3.1 Signal, Activity & Query Catalogue

Every signal name is either part of the Definition↔Execution DSL contract (`stage-transition`/`stage-defer`, and the `PrepActivity`/`ReviewActivity`/`ApproveActivity` names the compiler writes) or is entirely Execution's own design space — Definition Service has no stake in naming an admin/lifecycle signal the compiled DSL never encodes. Execution designs `instance-reassign` as its own mechanism, independent of whether `assignee-override` is itself DSL-addressed.

**Signals:**

| Signal | Payload | Precondition |
| --- | --- | --- |
| `stage-transition:{instanceID}` | `{dept_id, to_stage, node_id?, user_id, result_json, record_version}` | Addressed `(dept_id, to_stage)` must match a currently-active step. Realizes `Exclusive`'s forward branch and the plain `Sequential` advance. |
| `stage-fail:{instanceID}` | `{dept_id, node_id, connector_type, error_class, record_version}` | Addressed node must be a currently-active connector-typed task. Feeds the same `FAILED`/`DEGRADED` transition logic a non-retryable Activity error already drives — `FAILED` on the main/Sequential path (§3.3), contributes to `DEGRADED` inside a `Parallel` branch (§2.7). New signal, `workflow_connectors.md` §10 Decision #9 — `cmd/connector-worker` is its only caller; no human ever sends it. |
| `stage-defer:{instanceID}` | `{dept_id, from_stage, reason, user_id, record_version, task_id, assignment_id}` | The assignee's own one-step force-back (§2.7): closes the deferring assignment, then routes as `instance-force-back` does, naming the assignee rather than an admin. |
| `task-claim:{taskID}` | `{user_id, assignment_id, record_version}` | Only meaningful for multi-assignee (`assignee_mode='all'`) tasks — establishes the lead assignment. No-op for single-assignee tasks. |
| `instance-pause:{instanceID}` | `{admin_user_id, reason?, record_version}` | Instance must be RUNNING or DEGRADED. In DEGRADED the pause is recorded and takes effect when the park ends (§8.2.6). |
| `instance-resume:{instanceID}` | `{admin_user_id, initiator, record_version}` | Instance must be PAUSED or DEGRADED, and `initiator` must match the initiator that paused it. An `admin` resume lifts any pause; every other initiator lifts only its own (§6.2). In DEGRADED it clears a recorded pause under the same rule and is dropped, logged, when none is recorded. |
| `instance-cancel:{instanceID}` | `{admin_user_id, reason, record_version}` | RUNNING, PAUSED, or DEGRADED. Graceful cancel: marks active tasks FAILED, then exits. |
| `instance-force-forward:{instanceID}` | `{admin_user_id, target_node_key, record_version}` | RUNNING or DEGRADED. Admin-invoked jump beyond the compiled graph's explicit edges. Inside a parallel step or a DEGRADED instance, the department of `target_node_key` names the branch it applies to. |
| `instance-force-back:{instanceID}` | `{admin_user_id, target_node_key?, record_version}` | RUNNING or DEGRADED. Pops `completedNodes` (§2.7), extended for the active-parallel-gateway case. In a DEGRADED instance with more than one failed branch, `target_node_key` names a node of the branch to resume; with one failed branch it may be omitted. |
| `instance-external-answer:{instanceID}` | `{key, variables?}` | RUNNING or DEGRADED, and the interpreter waiting in the outside wait `key` (§2.4). The signal is dropped otherwise. |
| `instance-reassign:{taskID}` | `{admin_user_id, old_user_id, new_user_id, record_version}` | Task READY/IN_PROGRESS, new user ≠ current. Execution's own mechanism regardless of `assignee-override`'s status. |

`record_version` is present on every admin signal, guarding the same stale-client-view race that task-level signals already guard against. The bulk-signal-loop paths (`TenantStateChanged`, pause-tasks) are the one exception: a version mismatch there is logged and skipped per-instance rather than aborting the whole bulk operation, since they iterate many instances from a single event with no per-instance human review.

**Direct client call:** `instance-terminate` uses `TemporalClient.TerminateWorkflow` directly — immediate cutoff, no in-workflow cleanup. Because this bypasses the workflow entirely, the service layer writes the terminal DB state itself in the same request, after the execution is closed: close the workflow execution first, then mark `workflow_instance` TERMINATED, mark every currently-active `workflow_task` FAILED and vacate their open assignments in one transaction. A terminate call on an execution that is already closed or does not exist succeeds. If closing the execution fails, nothing is written and the request fails, so a retry runs the whole sequence again; if the DB write fails after the execution was closed, the instance stays non-terminal, the request fails, and a retry completes it. The execution is closed once more after the DB write, and instance creation re-reads the row once its workflow exists and closes that execution if the row is already terminal. A terminate that lands while a start is still being retried therefore leaves no live execution under a TERMINATED row, whichever of the two happens first, unless the later close itself fails, which is logged. The start answers with the instance's current status. The DB write re-reads the instance, so a write that landed while the execution was being closed does not fail the termination.

**Retired:** `admin-route` as a distinct name — its `"goto"` case is `instance-force-forward`; its `"terminate"` case is the direct client call above, not a signal at all.

**Activities:**

| Activity | Inputs → Outputs | DB writes | Used by |
| --- | --- | --- | --- |
| `GetCompiledPlanActivity` | `tenantID, versionID → CompiledPlan` | None (read-only HTTP call) | First activity, always. Refuses, without retry, a version that is no longer `PUBLISHED` or that the Definition Service has marked invalid, so the instance fails and runs no step: the API's start reads a plan cached for up to an hour and can accept such a version. Expands the plan's calls when its schema major is current; the fail-closed check runs in `Execute` (§2.5). |
| `CreateTaskActivity` | `instanceID, tenantID, nodeKey, compiledNode, contextJSON, overrideMap → taskID` | Inserts `workflow_task` (READY) + N `workflow_task_assignment`; enqueues `TaskCreated`. Refuses, without retry, an instance that is terminated, failed or completed, so an activity still in flight when its instance is terminated leaves no READY task behind | `prep`/`review`/`approve`, unrecognized-`Type` passthrough, connector-typed automatic tasks (`workflow_connectors.md` §5.1). Also where `DueDate`/`FollowUpDate` SLA-timer setup starts, if present. |
| `UpdateInstanceNodesActivity` | `instanceID, tenantID, nodeKeys[] → —` | Updates `workflow_instance.current_node_keys` | Every step transition, a receive task's wait included: the node is recorded when the wait begins, so an instance waiting on a message or an outside answer names that node. |
| `ClaimAssignmentActivity` | `assignmentID, tenantID, userID, recordVersion → —` | Sets `is_lead=true` on the claiming assignment; bumps `workflow_task.record_version` (the contested resource is the task, not the assignment, matching `CompleteAssignmentActivity`'s own scope); enqueues `TaskClaimed` | `task-claim`. |
| `CompleteAssignmentActivity` | `assignmentID, tenantID, resultJSON, record_version → allDone` | Sets `claimed_at`/`completed_at`; bumps `record_version`; enqueues `TaskCompleted` | `stage-transition`, admin-stub completion, Tender's post-signing Approve call. |
| `DeferTaskActivity` | `taskID, tenantID, userID, assignmentID, reason, deferredToNodeKey, recordVersion → —` | Marks task DEFERRED; sets `completed_at` on the deferring assignment(s); enqueues `workflow.task.deferred` naming the earlier node the instance goes back to. Creates no replacement task: the earlier node's task is created when its department runs again (§2.7) | `stage-defer`. |
| `UpdateInstanceStatusActivity` | `instanceID, tenantID, status, completedAt → —` | Updates `workflow_instance.status`; enqueues `WorkflowFinished` on terminal status. On `FAILED` specifically, also marks any still-open task(s) FAILED and vacates their assignments. | Instance completion, `DEGRADED` transition, `FAILED` transition. |
| `PauseInstanceActivity` / `ResumeInstanceActivity` | `instanceID, tenantID, adminUserID, recordVersion → —` | Version-checked status update; writes event | Admin lifecycle signals. |
| `CancelInstanceActivity` | `instanceID, tenantID, adminUserID, recordVersion → —` | Marks active tasks FAILED and vacates assignments, then updates status to TERMINATED; writes both event classes | `instance-cancel`. |
| `ReassignAssignmentActivity` | `taskID, tenantID, oldUserID, newUserID, adminUserID, recordVersion → —` | Vacates old assignment, inserts new; writes `TASK_REASSIGNED` | `instance-reassign`. |
| `RecordForceRouteActivity` | `instanceID, tenantID, oldNodeKeys[], targetNodeID, direction, adminUserID, recordVersion → —` | Marks the superseded task(s) SUPERSEDED and vacates assignments; writes both a `FORCE_ROUTED` (carrying `direction`) and a `TASK_SUPERSEDED` event | `instance-force-forward` and `instance-force-back`, paired with `UpdateInstanceNodesActivity` (must read `oldNodeKeys` before that activity overwrites them). |
| `RecordSLAWarningActivity` / `RecordSLABreachActivity` | `instanceID, tenantID, taskID, nodeKey → —` | Audit-only `outbox_events` row (§4.5), no status change | `FollowUpDate`/`DueDate` timer firing. |

**Explicitly removed:** `CreateInstanceRecordActivity` — the instance record is written synchronously by the API service layer, before the Temporal workflow starts, not as the workflow's first activity.

**Explicitly out of scope here:** the code that actually executes a connector-typed automatic task (fetching from storage, sending an email) is never part of Execution's own workflow interpreter — it runs in `cmd/connector-worker`, a separate binary inside this same repo (`workflow_connectors.md` §6.1), and never imports the Temporal SDK itself: it calls back over HTTP instead, `POST /api/v1/internal/connector-tasks/:id/complete` on success or `/fail` on an exhausted/failed call or an unregistered connector type (§5.2), guarded by the same `RequireInternalToken` convention as every other `/api/v1/internal/*` route (§9.2). `ConnectorTaskService`'s handler for those two endpoints is what actually signals the running workflow into this catalogue's signal path — `stage-transition` on `complete`, the new `stage-fail` above on `fail` — so `cmd/connector-worker` completes/fails a task through an HTTP callback that triggers the signal, not by signaling Temporal directly. See `workflow_connectors.md` for the full design; this document only carries the footprint changes that design needs (`stage-fail` above, the dispatch-table row in §2.4, the two HTTP endpoints in §5.2, and `connector_type` in §4.3/§6.4).

**Query:**

| Query | Returns | Notes |
| --- | --- | --- |
| `get-workflow-status` | `{status, current_node_keys, active_tasks[], saved_sibling_branches[]}` | The one handler, by design — `saved_sibling_branches` folds in force-back's saved-but-not-restarted parallel branches rather than a separate query. |
| `waiting-external` | `string[]`: the keys of the outside waits the instance is waiting in now | Read by the service layer before it accepts an outside answer (§5.11). |

**Claim, formalized.** Claim exists only for multi-assignee (`assignee_mode='all'`) tasks, to establish a lead assignee — the one who can act/complete — versus read-only co-assignees. Single-assignee tasks skip claim entirely; `stage-transition`/`stage-defer` are valid directly against the sole assignment.

The lead is a property of the assignment row, so every path that replaces that row carries it: the event-driven delegation reroute, the synchronous delegate swap, and an admin reassign (§5, §5.8). A claimed task stays claimed by whoever now holds it. Dropping the flag would leave an `assignee_mode='all'` task with no lead at all, which `Complete` and `Defer` refuse for everyone until someone claims it again.

Two consequences of that rule are deliberate. Ending a delegation returns the claim to the delegator even where the delegate was the one who made it — the work reverts with it, and the alternative leaves the task lead-less. And where the delegate already holds an assignment on the same task, that row stands and inherits the claim rather than a second row being inserted, which the active-assignment uniqueness rule would reject.

### 3.2 Worker Topology & Task-Queue Registration

Temporal Workers run in a **separate deployment/repository** from the HTTP API — the two are one logical domain (both connect to the same `workflow_execution` database, a documented exception to database-per-service) but never call each other directly; all coordination is mediated by the Temporal cluster and the shared database. `InstanceService.Start`'s flow is: validate → re-validate node defaults → write `workflow_instance` (transaction, enqueues `WorkflowStarted`) → `StartWorkflow` (`WorkflowIDReusePolicy: AllowDuplicate`) → record the run id → return. `GetCompiledPlanActivity` is the workflow function's first activity, run after all of this. A start that fails after the row is written leaves a `RUNNING` row with no run id; the same request sent again completes it (§5.2).

**Task-queue registration is dynamic, not static.** The compiled plan names the shared default queue (`wf-queue-default`) and no tenant; the Execution Service chooses the queue at instance start: the tenant's own queue (`wf-queue-<tenant_uuid>`) when the registry below holds one for the tenant, the plan's queue otherwise. A static, redeploy-to-add queue list is operationally wrong — a tenant upgrading tier shouldn't require a Workers redeploy.

1. Workers maintain an `active_task_queues` registry, populated by Execution's tenant-lifecycle handler when a `plan` delta arrives: upsert the tenant's isolated queue and mark it routable on upgrade, and mark it no longer routable on downgrade. A row is never deleted.
2. Each Worker process polls this registry on a fixed interval (proposed: 60s) and, for every queue not yet being polled, starts an additional `worker.Worker` instance against it — the Go SDK supports multiple `Worker` instances per process, started at runtime, without a restart. `wf-queue-default`'s worker is always started unconditionally at process boot.
3. A tenant's isolated queue stays in the registry, and its worker keeps polling it, for as long as the row exists, and a row is never deleted while the tenant has an instance (only the tenant's purge, §4.13, removes it, after every instance is gone): a running workflow execution is permanently bound to the task queue it started on, and a Worker that restarts must still find that queue. A downgrade only stops new instances from starting on it, at once and without waiting for running ones, so a tier change affects instances started after the change and a start racing a downgrade cannot strand an instance.
4. **Per-queue concurrency, not just per-queue registration.** Registering a separate `worker.Worker` per queue is necessary but not sufficient for the priority-isolation goal: each `worker.Worker` instance is constructed with its own `WorkerOptions{MaxConcurrentActivityExecutionSize, MaxConcurrentWorkflowTaskExecutionSize}`, not a shared process-wide default — otherwise a busy default-queue tenant can starve an enterprise tenant's nominally dedicated queue.
5. **Not built: volume-triggered isolation, and a cap with sharding of queues across Worker replicas.** Every Worker replica polls every row of the registry, and only the enterprise plan puts a tenant on its own queue. Both mechanisms are designed when the number of isolated tenants makes one process's pollers a problem; until then the queue health signals in §7.6 show a queue nobody polls.

### 3.3 Retry, Failure, and the `DEGRADED` Status

**Per-activity-class retry policy:**

| Activity class | `StartToCloseTimeout` | `RetryPolicy` | `NonRetryableErrorTypes` |
| --- | --- | --- | --- |
| DB-writing activities | 30s | `InitialInterval: 1s, BackoffCoefficient: 2.0, MaximumInterval: 60s, MaximumAttempts: 0` (unlimited) | `ValidationError`, `NotFoundError` |
| External-call activities (`GetCompiledPlanActivity`) | 10s | `InitialInterval: 500ms, BackoffCoefficient: 2.0, MaximumInterval: 30s, MaximumAttempts: 0` (unlimited) | Definition Service 4xx-class errors, DSL schema-version major mismatch |
| Workflow-level | `WorkflowExecutionTimeout`: none (unbounded-duration business processes) | `WorkflowTaskTimeout`: 10s | — |

`ScheduleToCloseTimeout` is deliberately unset on every class — it bounds the *sum* of all retry attempts, which would cap the unlimited-retry-with-backoff intent `MaximumAttempts: 0` already commits DB-writing activities to. No activity uses `RecordHeartbeat` today; every activity in the catalogue (§3.1) is a fast, bounded DB write or HTTP call, so the `StartToCloseTimeout`s above are sufficient. Revisit criterion: a future activity whose expected duration can legitimately exceed its `StartToCloseTimeout` due to external latency (not retry) adds `RecordHeartbeat` calls with a `HeartbeatTimeout` ≈ its own polling interval, with `StartToCloseTimeout` widened accordingly so the heartbeat becomes the liveness signal.

**Instance-level `FAILED` vs. `DEGRADED` — scoped deliberately, never overlapping:**

- **`FAILED`** is the outcome of a non-retryable activity failure **outside** a `Parallel` branch — the main/Sequential path (including `SubWorkflow` recursion). There is no sibling branch to preserve, so it is a clean terminal transition: the workflow function catches the error, calls `UpdateInstanceStatusActivity(FAILED)`, and returns.
- **Branches are told apart by their place in the step**, not by `DeptID`: two branches that start with a call both carry an empty one, and the join waits for every branch. This applies under change id `parallel-join-by-branch`; before it, two branches with one `DeptID` never joined. An admin force-forward reaches the branch keyed by the signal's department, or else the one whose steps run it.
- **`DEGRADED`** is scoped strictly to a `Parallel` branch's activity exhausting retries on a non-retryable error. That branch's goroutine returns an error; **sibling branches are not cancelled** — they keep running, consistent with force-back's "save, don't discard" philosophy. Surviving siblings progress between the first failure and the aggregation point; the instance is still `RUNNING` during that window. `DEGRADED` is set only once every branch has completed or failed. It requires admin intervention (force-back, force-forward, or terminate) to resolve — it is never auto-recovered.

Auto-failing the whole instance on one branch's error would discard completed sibling work; silently ignoring it would hide a real failure from the audit trail. `DEGRADED` makes the failure visible without being destructive. Detection is via the `InstanceStatus` Search Attribute (§3.6) plus an activity-failure-count metric on the `UpdateInstanceStatusActivity(DEGRADED)` call, labelled by tenant, as the alertable paging signal.

**Park/resume mechanism — one end-to-end procedure:**

1. A branch's activity exhausts retries on a non-retryable error. That branch's goroutine returns an error; siblings are not cancelled. Nothing observable happens yet — status is still RUNNING.
2. The parallel aggregator waits for every branch to settle (complete or fail) — this can take arbitrarily long if siblings still have real work in flight.
3. Once all branches have settled, the aggregator checks for any failure. None → normal completion, no `DEGRADED` transition at all. At least one → the aggregator records every branch's outcome in workflow-local state: completed branches' results, and for each failed branch its identity and last-completed node (the same saved-branch structure force-back's paused siblings use), extended with a `failed` variant carrying the branch's error.
4. `UpdateInstanceStatusActivity(DEGRADED)` is called — the first externally-observable effect: the DB row, the `INSTANCE_DEGRADED` event, and the Search Attribute all flip together.
5. The workflow function **parks**: it blocks on a `Selector` whose only cases are `instance-force-forward`, `instance-force-back`, and `instance-cancel` (`instance-terminate` bypasses the workflow entirely and lands as termination directly). No other signal case is registered — anything else arriving here is rejected at signal validation before reaching this Selector. `instance-pause` and `instance-resume` are the exception: they are recorded at signal validation, not parked on (§8.2.6).
6. A resolution signal arrives and is validated (`record_version` check) before the exit path below runs.
7. **Exit, by signal:**
   - **Force-forward** — the failed branch's bypassed task is marked SUPERSEDED and its open assignments vacated; completed-branch results are merged; this branch is resolved.
   - **Force-back** — the failed branch is **respawned as a new `workflow.Go` goroutine** starting at the admin-chosen prior node, seeded from the saved branch context. The original goroutine already returned its error and cannot be resumed — respawn is the only mechanism available for a *failed* branch, distinct from a *paused* sibling's resume (§2.7). Task creation for the respawned node goes through `CreateTaskActivity` again from scratch, so any SLA timers are freshly set up, not resumed. When the branch's own `steps[]` runs more than one department in sequence (§2.5), respawn does not restart the whole branch from its first department: it resumes at the failed branch's last-completed node and continues that branch's remaining steps from there, so an earlier department's already-completed work within the same branch is never re-run. Only the branch's structurally-nested shape (a `SubWorkflow` step with no finer-grained resume pointer) falls back to a full branch re-run.
   - **Cancel/terminate** — the existing cascades apply unchanged and exit the whole instance regardless of how many failed branches remain unresolved.
8. **Multiple failed branches:** each signal addresses exactly one failed branch; the park loop re-enters after each resolution and does not exit until every failed branch is resolved. Cancel/terminate short-circuits regardless of how many remain.
9. The park loop exits; status returns to `RUNNING`. For a respawned branch, aggregation waits on it exactly as it waited on the original — if it fails unrecoverably again, step 3 re-applies and the instance parks in `DEGRADED` again, with no cap on repeated respawn attempts.

A respawned branch also gets its instance-wide message buffer entries reset by firing-node key (§2.8) — only that branch's own unconsumed fires are dropped, never a sibling's.

```mermaid
sequenceDiagram
    participant Sib as Sibling Branch (goroutine)
    participant Fail as Failed Branch (goroutine)
    participant WF as Workflow Function (aggregator)
    participant Temporal as Temporal Cluster
    participant DB as Postgres (workflow_execution)
    participant Admin as Admin (force-back signal)

    Fail->>Fail: Activity exhausts retries<br/>(non-retryable error)
    Fail-->>WF: goroutine returns error
    Note over Sib: Sibling keeps running,<br/>not cancelled
    Sib-->>WF: goroutine completes normally
    WF->>WF: Aggregation point:<br/>all branches settled, one failed
    WF->>Temporal: UpdateInstanceStatusActivity(DEGRADED)
    Temporal->>DB: workflow_instance.status = DEGRADED<br/>+ INSTANCE_DEGRADED event
    WF->>WF: Park in admin-signal-only Selector loop
    Admin->>Temporal: instance-force-back {record_version}
    Temporal->>WF: signal delivered to parked Selector
    WF->>WF: Validate record_version
    WF->>WF: Respawn failed branch as new workflow.Go<br/>at admin-chosen node (saved context)
    WF->>Temporal: CreateTaskActivity (fresh SLA timers,<br/>message-buffer reset for this branch)
    WF->>WF: Park loop exits — status RUNNING
    WF->>Temporal: UpdateInstanceStatusActivity(RUNNING)
    Temporal->>DB: workflow_instance.status = RUNNING
```

### 3.4 State Machines

**Instance status** (`RUNNING | PAUSED | COMPLETED | TERMINATED | FAILED | DEGRADED`):

| From | To | Trigger |
| --- | --- | --- |
| RUNNING | PAUSED | `instance-pause` |
| PAUSED | RUNNING | `instance-resume` |
| RUNNING | COMPLETED | `runSteps` returns cleanly, no pending revert/terminate |
| RUNNING/PAUSED | TERMINATED | `instance-terminate` (direct client call) or graceful `instance-cancel` |
| RUNNING/PAUSED | FAILED | A non-retryable activity failure on the main/Sequential path — terminal, no sibling work to preserve |
| RUNNING | DEGRADED | A `Parallel` branch's activity exhausted retries on a non-retryable error; set at the aggregation point once every branch has completed or failed |
| DEGRADED | RUNNING | Admin force-forward/force-back resolves every failed branch with no pause held — the park loop exits |
| DEGRADED | PAUSED | The park loop exits while a pause is held, recorded during the park or in force when the branch failed |
| DEGRADED | TERMINATED | `instance-terminate`/`instance-cancel`, same as RUNNING |

`RUNNING → COMPLETED` completes implicitly the moment `runSteps` reaches the terminal node — no explicit admin sign-off gate is adopted.

**`workflow_task` status** (`READY | IN_PROGRESS | COMPLETED | DEFERRED | FAILED | SUPERSEDED`):

| From | To | Trigger |
| --- | --- | --- |
| *(created)* | READY | `CreateTaskActivity` |
| READY | IN_PROGRESS | `task-claim` (multi-assignee only; optional, never automatic) |
| READY/IN_PROGRESS | COMPLETED | `stage-transition`, `allDone` gate satisfied |
| READY/IN_PROGRESS | DEFERRED | `stage-defer` (closes the deferring assignment(s)) |
| READY/IN_PROGRESS | FAILED | Cascaded from `instance-cancel`, `instance-terminate`, or instance `FAILED` — never an independent per-task failure |
| READY/IN_PROGRESS | SUPERSEDED | Cascaded from `instance-force-forward` or `instance-force-back`, closing out the node those signals leave behind (§2.7) |
| READY/IN_PROGRESS | SUPERSEDED | An interrupting boundary event (§2.2) cancels the host task, or a call or subprocess holding open tasks, and its path runs instead; each open task is superseded, its assignments vacated, and `WorkflowTaskSuperseded` is enqueued carrying the nil user identifier as its actor, since no user abandoned the task. A task already completed, failed or superseded is left as it is, since the cancellation can race the completion that settled it. An admin force-route supersedes the tasks it leaves behind itself, naming the admin. This applies under change id `abandoned-task-superseded`. |

`SUPERSEDED` exists precisely because a force-routed task is neither a failure nor the assignee's own send-back — a distinct, dashboard-worth-distinguishing outcome. The interrupting-boundary case is the same distinction for a different trigger: the host task didn't fail and wasn't the assignee's own choice to defer — a boundary event just moved execution on without it. Unlike `workflow_instance.status`, there is no independent per-task failure mode: `FAILED` here means "swept up in the parent instance's terminal transition," never "this task failed on its own."

**`workflow_task_assignment` status** (Created → Claimed → Completed *or* Vacated):

| From | To | Trigger |
| --- | --- | --- |
| *(created)* | Created | `CreateTaskActivity` |
| Created | Claimed | `task-claim` |
| Created/Claimed | Completed | `CompleteAssignmentActivity` (normal path) or `DeferTaskActivity` (assignee's own action) |
| Created/Claimed | Vacated | `ReassignAssignmentActivity`, or cascaded from `instance-cancel`/`instance-terminate`/instance `FAILED`/`instance-force-forward` — something *other* than the assignee's own action closed it |

The naming rule this encodes: Completed = the assignee acted (including choosing to defer); Vacated = something external acted instead.

**SLA timer procedure**, running once per non-empty `DueDate`/`FollowUpDate` alongside a task's normal completion wait:

1. At task creation, for each non-empty date field parsing to a future timestamp, `workflow.NewTimer` starts and is added as an additional `Selector` case alongside the task's own resolution wait.
2. Whichever case fires first wins. Task resolution (completion, defer, force-forward supersession, `FAILED` via any cascade or via its `Parallel` branch failing unrecoverably, or a force-back regression) → cancel any pending timer. A timer firing first → call `RecordSLAWarningActivity` (`FollowUpDate`) or `RecordSLABreachActivity` (`DueDate`) — audit-only, no status change — and loop back to the wait.
3. A branch respawned after `DEGRADED` re-enters at step 1 with fresh timers; this procedure never resumes a cancelled timer's remaining duration.

### 3.5 Approval/Signing Sequencing

The adopted ordering is **Tender-first-synchronous** (Appendix A.2 decisions 19/20; matches `IAM/approver-approval-signature-workflow.md`'s own sequence diagram):

1. The approver clicks Approve in the UI, which calls Tender Service (not Execution).
2. Tender Service re-verifies assignee eligibility and MFA freshness (can reject with `409`), signs, and stores the artifact.
3. Only once that succeeds does Tender Service's own backend — directly, no intermediary — call Execution's Complete-task endpoint over HTTP, synchronously, the same endpoint a browser-originated Complete would hit — reusing `CompleteAssignmentActivity`, no new activity or signal.

The signed artifact precedes workflow progression, not the reverse — this is required for the non-repudiation guarantee the signing step exists to provide. Tender Service is not treated as an autonomous backend actor for this call — it forwards the approver's `x-user-id`/`x-tenant-id` gateway headers exactly as a direct browser call would, validated against the task's actual assignee exactly as any caller would be — no bypass path, no separate internal-service credential (§5.6/§5.7). Tender Service must fetch the task's current `record_version` immediately before calling Complete, the same requirement any caller has. Direct HTTP (not gRPC, not an event) is used specifically because the approval-race edge case requires a synchronous `409` back to the caller on a completed/reassigned-task race — an async event cannot deliver that.

Tender Service is the direct caller of Execution for this flow, the same way the browser is the direct caller for every other business action — no third-party intermediary sits between either of them and Execution. This generalizes beyond Tender: whichever domain service performs its own domain-specific precondition before a task can be completed calls Execution's Complete endpoint itself, immediately afterward, rather than routing through a shared intermediary. No invariant in this section requires a third party — every property above holds identically whether Tender calls Complete itself or a third service does, and collapsing signer-and-caller into one service removes a crash window between "signed" and "completed" rather than adding one.

**Synchronous `409` delivery under the Signal-Only API pattern.** This flow requires a synchronous `409` on a version/eligibility mismatch, but the Signal-Only pattern means the API layer forwards to Temporal as a signal and returns `202` once the signal call itself succeeds — before the version-checking activity has actually run. Per §5.10, every signal-forwarded mutating endpoint does its own synchronous HTTP-layer pre-check (mirroring node-override's pattern) before forwarding the signal, so the `409` is produced by that pre-check, not by waiting on the activity.

### 3.6 Search Attributes & Temporal Web UI Visibility

Five custom Search Attributes are registered — `TenantId`, `InstanceStatus`, `WorkflowVersionId`, `SubjectId`, `BusinessKey` (all Keyword) — set via `workflow.UpsertSearchAttributes` on workflow start and on every status transition, including `DEGRADED`. Subject and key are both set: the key identifies an instance within its subject, so filtering on it alone spans every subject, and an operator looking for one entity's runs filters on `SubjectId`. Their purpose is Temporal Web UI operational visibility (e.g., "every RUNNING workflow for tenant X"), independent of and complementary to the Postgres dashboard's own query path; `DEGRADED` instances are found the same way, via `InstanceStatus = "DEGRADED"`. This requires Temporal's Advanced Visibility (Elasticsearch-backed) store, not the default SQL-backed visibility — a real infra dependency this decision introduces. The Temporal Web UI is internal-ops-only, never tenant-facing, so cross-tenant visibility there is not a tenant-isolation gap; `TenantId` is exactly the filter an admin uses to scope a query to one tenant when needed.

The five key constants and an `UpsertInstanceSearchAttributes` helper live in `internal/observability`, using the Temporal Go SDK's newer typed `SearchAttributeKeyKeyword`/`UpsertTypedSearchAttributes` API rather than the older untyped `map[string]interface{}` form — compile-time key/type safety for a fixed, closed set that changes only by deliberate revision. The helper itself is only callable from workflow-context code (Temporal permits Search Attribute upserts only from inside a running workflow); wiring the actual call site into the workflow function's start and status-transition points is a separate task's job.

### 3.7 Worker Runtime: Instantiation-to-Execution Flow

This section specifies what the workflow function itself receives as `StartWorkflow`'s own input, and gives the end-to-end instantiate-through-first-task path a complete account, extending §3.2's instantiation-flow steps.

**Instantiation input contract.** `StartWorkflow`'s input argument is `{instanceID, tenantID, workflowVersionID, businessKey, overrideMap, contextJSON}` — exactly the columns `workflow_instance` already carries at the moment the API writes that row (§4.2). The compiled plan itself never crosses this wire: the workflow function fetches it via `GetCompiledPlanActivity(tenantID, workflowVersionID)`, its own first activity, using only the identifiers from this input — keeping the instantiation payload small and letting §2.5's `runSteps` dispatch proceed once the plan is in hand.

| Field | Source |
| --- | --- |
| `instanceID` | Pre-generated by the API service layer before either the DB write or `StartWorkflow` (the same pre-generated-ID idempotency pattern Decision 22 uses) |
| `tenantID`, `subjectID`, `businessKey` | Request context; the subject and key together compose the deterministic `workflowID` (`{tenantID}:{subjectID}:{businessKey}`) |
| `workflowVersionID` | Request — which published template version to instantiate; the identifier `GetCompiledPlanActivity` uses, not the plan itself |
| `overrideMap`, `contextJSON` | Request, validated at `Start` time (§5.5) |

`TaskQueue` on the same `StartWorkflowOptions` is `workflow_instance.task_queue` — read back from the row just written in the same request, snapshotted once at instantiation (§4.2): the tenant's registered queue when the tenant has one, `CompiledPlan.TaskQueue` otherwise. A failed registry read fails the start; it never falls back to the plan's queue. This is what connects §3.2's dynamic queue registration to a specific instance's `StartWorkflow` call: the queue an instance runs on is fixed at instantiation, from the same registry Workers poll to decide which queues to serve.

**One owner at a time; replay on takeover.** Exactly one Worker replica actively executes a given run at any moment. If that replica dies mid-execution, Temporal does not hand off in-memory state — it replays the recorded event history (every signal, activity result, timer fire so far) against the workflow function's code on whichever replica next picks up the task queue, deterministically reconstructing the same in-memory state before continuing. This is why `workflow.GetVersion` patching (Decision 14) matters for any workflow-function code change: a replay must reproduce the same decisions a past execution made even after the code has moved on, and an un-gated change would replay differently — Temporal detects the resulting non-determinism as a fatal workflow error, never a soft warning.

`SignalWithStartWorkflow` (the SDK's atomic start-or-signal call) is not used anywhere in this flow (Decision 26): instantiation and every signal are always issued from a code path that already knows which situation it's in — the atomic combined call solves a problem this design doesn't have.

**Edge case: a `StartWorkflow` call targets a task queue with zero polling Worker replicas.** Temporal persists the scheduled workflow task on that queue indefinitely — no cluster-level timeout fires, and the caller's `202` succeeds regardless, since scheduling and execution are separate Temporal-side steps. A misconfigured or not-yet-scaled-up tenant-isolated queue strands an instantiated instance with no caller-visible error until some Worker replica starts polling it. §3.2's guarantee that a queue row is never deleted protects an instance already running on a queue; it does not guarantee a queue is polled by at least one replica at the moment a new instance's `StartWorkflow` call targets it. The `InstancesOnUnpolledQueue` alert (§7.6) fires when instances are non-terminal on a queue that no Worker has polled for five minutes.

---

## 4. Data Model

### 4.1 Entity Inventory

Execution Service owns its own Postgres schema (`workflow_execution`), physically separate from Definition Service's `workflow_definition` schema — cross-schema access is forbidden even between the two services sharing one RDS instance. The schema comprises five domain tables plus three platform-convention tables reused from the shared pattern already established by Definition Service and IAM. There is no dedicated `workflow_event` table — the full internal audit trail lives in `outbox_events`' `payload` column instead (§4.5, §4.10).

| Entity | Purpose | Owner |
| --- | --- | --- |
| `workflow_instance` | One row per running/completed workflow instance — the dashboard-facing projection of Temporal's authoritative execution state | Domain (service-authored) |
| `workflow_task` | One row per dispatched stage/task (`prep`/`review`/`approve`, unrecognized-`Type` passthrough, connector-typed automatic tasks) | Domain (service-authored) |
| `workflow_task_assignment` | One row per assignee on a task; carries claim/completion/reassignment state | Domain (service-authored) |
| `active_task_queues` | Registry of tenant-isolated Temporal task queues | Domain (service-authored) |
| `tenant_retention` | One row per offboarded tenant: when it left and when its history is purged (§4.13) | Domain (service-authored) |
| `tenant_state` | The tenant's last applied lifecycle status, read by the start gate (§5.5) | Domain (service-authored) |
| `assignee_overrides` | One row per admin node-override action — the API process's own synchronous audit record, written before the Temporal signal (§4.12, §5.4) | Domain (service-authored) |
| `outbox_events` / `outbox_dead_letters` | Transactional outbox envelope and dead-letter tables | `platform-events` (library-owned migration, no local `CREATE TABLE`) |
| `processed_event` | Consumer-idempotency dedup table for inbound events (§6) | Domain (service-authored, following the platform-wide convention) |

Not modeled here: Definition Service's own tables (`workflow`, `workflow_version`, `workflow_node_assignee`) — out of Execution's database entirely, per the database-per-service principle. The one documented exception to that principle is that the Execution API process and the Temporal Worker process share this one schema (§5.1) — that is a same-domain exception, not a cross-service one.

Two carried-forward entities from the legacy LLD — `workflow_comment` and `workflow_resource_link` — are removed from Execution Service's scope in this rework. Neither has a Stage 1/2 mechanic driving it, and no code exists yet to migrate; if the removal is reversed, the original design (including its RLS/soft-delete treatment) is preserved for reference rather than re-derived from scratch.

#### Entity-Relationship Summary

| Parent | Child | Relationship | FK column |
| --- | --- | --- | --- |
| `workflow_instance` | `workflow_task` | one-to-many | `workflow_task.workflow_instance_id` |
| `workflow_task` | `workflow_task_assignment` | one-to-many | `workflow_task_assignment.task_id` |
| `workflow_task` | `workflow_task` | self-reference (legacy, not written) | `workflow_task.deferred_from_task_id` |
| `workflow_instance` | `assignee_overrides` | one-to-many | `assignee_overrides.workflow_instance_id` |
| `workflow_instance` | `outbox_events` | one-to-many, **logical only** — a JSONB key lookup (`payload -> 'data' ->> 'workflow_instance_id'`), not a DB-enforced FK | — |
| `workflow_task` | `outbox_events` | one-to-many, nullable, **logical only** — same JSONB-key relationship, absent for instance-level events | — |

### 4.2 `workflow_instance`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | Application-generated before either the DB write or `StartWorkflow`, never DB-defaulted (§4.9). |
| `tenant_id` | `uuid` NOT NULL | RLS boundary (§4.8). |
| `workflow_id` | `uuid` NOT NULL | Definition Service's `workflow.id` — lineage/reporting join, no dedicated index (§4.9). |
| `workflow_version_id` | `uuid` NOT NULL | The specific published version this instance runs. |
| `subject_id` | `text` NOT NULL | The entity this instance is about, opaque and domain-scoped (a `tender_id` in the tender domain, a `project_id` under a future PMS deployment). Addressable on its own: it is what a subject-scoped delegation resolves against (§5) and what "every live instance for this subject" filters on (§5.2). |
| `business_key` | `text` NOT NULL | Identifies one instance within its subject, opaque in the same way (a stage or phase identifier in the tender domain). The pair is the idempotent-instantiation natural key. A domain running one instance per subject sets this to a constant of its own choosing. |
| `temporal_workflow_id` | `text` NOT NULL | `{tenantID}:{subjectID}:{businessKey}` — needed to issue `SignalWorkflow`/`TerminateWorkflow` against the right execution. |
| `temporal_run_id` | `text` | Recorded once `StartWorkflow` returns, by a write that leaves `record_version` alone so a concurrent status change never conflicts with it. Every close of the execution names this run when one is recorded, so a close issued after the instance's key was reused cannot end the newer execution. A row whose start never returned has none, and its closes address the current execution of the identifier. |
| `status` | `enum` NOT NULL | `RUNNING \| PAUSED \| COMPLETED \| TERMINATED \| FAILED \| DEGRADED`. `DEGRADED` is the one new enum value this schema introduces; Cancel and Terminate share `TERMINATED` rather than a separate `CANCELLED` value. |
| `current_node_keys` | `text[]` NOT NULL | Query-facing projection of every currently-active leaf node; updated at every step transition. |
| `saved_node_keys` | `text[]` NOT NULL DEFAULT `'{}'` | Force-back's paused-but-not-restarted sibling branches — distinct from `current_node_keys` so a status read can tell "actively running" apart from "parked, awaiting resume." |
| `context_json` | `jsonb` | The instance's variable/context store that `IOMapping` reads and writes against at runtime. The workflow function applies an `IOMapping` on entry to the step that carries it, a call's (§2.1). |
| `override_map` | `jsonb` | The instantiation-time non-default-assignee map, captured once at `InstanceService.Start` and never mutated afterward. |
| `task_queue` | `text` NOT NULL | `wf-queue-default` or `wf-queue-<tenant_uuid>`, snapshotted once at instantiation: the tenant's routable registered queue, else `CompiledPlan.TaskQueue`. |
| `started_by_user_id` | `uuid` NOT NULL | Domain-specific actor column; no generic `created_by` exists anywhere in this schema (§4.9). |
| `started_at` | `timestamptz` | |
| `completed_at` | `timestamptz` | Set by the system-driven `COMPLETED` status write, the instant the workflow function returns cleanly at a BPMN end node — not gated on any downstream service's own business-completion action on its own data. |
| `record_version` | `bigint` NOT NULL DEFAULT 1 CHECK (`record_version > 0`) | Optimistic-lock token for every instance-scoped admin signal (`pause`/`resume`/`cancel`/`force-forward`/`force-back`). `instance-reassign` is the one instance-addressed exception: it is task-addressed, so its version check runs against `workflow_task.record_version`, not this column. |
| `created_at` / `updated_at` | `timestamptz` | Bumped per-statement in application SQL, not by a database trigger — every version-checked `UPDATE` sets `updated_at = now()` in the same statement as its `record_version` bump (§4.11). |

Unique index: `(tenant_id, subject_id, business_key)` — the idempotent-instantiation key. This index is *partial*, to allow key reuse once an instance reaches a terminal state: `WHERE status NOT IN ('COMPLETED', 'TERMINATED', 'FAILED')` — a `DEGRADED` instance still needs admin resolution, so its key is not yet reusable (Appendix C's DDL uses this predicate).

One subject holds as many concurrent instances as it has distinct keys, which is the property the column exists for: a domain whose entity runs several workflows at once cannot express that under a single key, since the second would collide with the first and an instance left paused or degraded would block every later one indefinitely.

"Every live instance for this subject" needs no index of its own: the uniqueness index leads on `(tenant_id, subject_id)` over the identical predicate, so it already serves that read.

### 4.3 `workflow_task`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | Application-generated. |
| `tenant_id` | `uuid` NOT NULL | |
| `workflow_instance_id` | `uuid` NOT NULL FK | `ON DELETE RESTRICT` (§4.9). |
| `node_key` | `text` NOT NULL | `{deptID}/{stage node_id}` addressing scheme (the stage type when a stage has no node id). A module task's `deptID` is call-scoped, `<callNodeID>::<department>`, nested calls composing the prefix (§2.3). |
| `department_id` | `uuid` NOT NULL | Snapshotted from the compiled plan at task creation. It is the `IAMDepartmentID` of the task's `DepartmentDef`, the `dept_id` of the task's lane; `DepartmentDef.ID` is a lane segment key (definition LLD §4.2), never an IAM department UUID. |
| `status` | `enum` NOT NULL | `READY \| IN_PROGRESS \| COMPLETED \| DEFERRED \| FAILED \| SUPERSEDED`. `SUPERSEDED` is the newest value — an `instance-force-forward` bypassing this task, distinct from both `FAILED` (a real failure) and `DEFERRED` (the assignee's own choice). |
| `record_version` | `bigint` NOT NULL DEFAULT 1 CHECK (`record_version > 0`) | Optimistic lock guarding concurrent claim/complete races across *all* of a task's assignments at once — the contested resource is "who acts on this task next," not any single assignment row, so no second lock token lives on `workflow_task_assignment`. |
| `assignee_mode` | `text` NOT NULL | `single` \| `all`. |
| `connector_type` | `text`, nullable | Set only for a connector-typed automatic task (`workflow_connectors.md` §5.2), mirroring `workflow-models`' `StageDef.ConnectorType` (`workflow_models_lib.md` §2.3) — a real column, not folded into `extras_json`, since `cmd/connector-worker` needs to filter/query tasks by connector type efficiently, the same reason `department_id` is a real column. Null for every other task type. |
| `extras_json` | `jsonb` | Compiled `StageDef.Extras`/dynamic `Activity` label snapshot — needed so unrecognized-`Type` passthrough tasks render correctly. |
| `deferred_from_task_id` | `uuid` FK, self-ref, nullable | No longer set: a defer re-runs the earlier node, whose new task is an ordinary one (§2.7). The column and the `deferred_from_task_id` response fields stay until the next schema revision drops them (Appendix B). |
| `due_at` / `follow_up_at` | `timestamptz`, both nullable | Parsed from `StageDef.DueDate`/`FollowUpDate` at task creation, used to seed Temporal SLA timers. Purely audit/display columns — the SLA mechanism itself is a Temporal-native timer, not DB-polled, so neither column is queried by any Execution-side mechanism. |
| `created_at` / `updated_at` / `completed_at` | `timestamptz` | `updated_at` is a generic last-touched marker distinct from `completed_at`'s specific-event meaning — the row updates in place across several statuses before ever reaching `COMPLETED`. |

Indexes: `(tenant_id, department_id, status)` for dashboard/admin filtering; `(workflow_instance_id, status)` for the status-query's active-task lookup; `(tenant_id, created_at DESC, id DESC)` for keyset-ordered tenant-wide task listing (§4.9, §5.9); a partial `(tenant_id, connector_type, status) WHERE connector_type IS NOT NULL` for `cmd/connector-worker`'s own connector-task lookup (`workflow_connectors.md` §5.2).

### 4.4 `workflow_task_assignment`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | |
| `tenant_id` | `uuid` NOT NULL | |
| `task_id` | `uuid` NOT NULL FK | `ON DELETE RESTRICT`. |
| `user_id` | `uuid` NOT NULL | Bare IAM UUID — no accompanying display-name column anywhere in this schema (§4.9). |
| `assigned_by` | `uuid` | Admin user ID for reassignment; null for the original compiled-default assignment. |
| `reason` | `text` | A plain-string convention, not a structured column — carries `delegation:<id>` when the assignment arose from an out-of-office delegation reroute, and `automation` on the row a connector callback creates for the acting principal (§5.7). **Disambiguation:** this is a different field from `stage-defer`'s own signal-payload `reason` (why an assignee deferred), which is instead carried in the `TASK_DEFERRED` `outbox_events.payload` row as an audit fact, never written to this column. |
| `is_lead` | `bool` NOT NULL DEFAULT `false` | The `assignee_mode='all'` claim mechanism's lead marker. `Complete` and `Defer` require an active assignment carrying it and belonging to the caller, so a task with no lead row refuses both for everyone until someone claims it again; admin reassign is gated on admin authority and does not read it (§3.1, §5). Carried across a delegation reroute (§5). **Disambiguation:** unrelated to IAM's own `dept_memberships.is_lead` (a department-lead concept, obsolete on IAM's side) — same word, two different tables, two different systems. |
| `is_active` | `bool` NOT NULL DEFAULT `true` | Together with `vacated_at`, this table's own domain-specific equivalent of a `deleted_at` soft-delete — more precise than a generic flag since it says exactly "no longer the live assignment." |
| `assigned_at` | `timestamptz` | |
| `claimed_at` | `timestamptz` | Only ever populated for multi-assignee (`assignee_mode='all'`) tasks — meaningless, left null, for single-assignee tasks that complete directly. |
| `completed_at` | `timestamptz` | |
| `result_json` | `jsonb` | Per-assignee completion payload — stored per assignee, not per task, since a multi-assignee task can have divergent per-assignee results. |
| `vacated_at` | `timestamptz` | Set on reassignment; a row is never deleted on reassignment, only vacated and superseded by a fresh insert. |
| `updated_at` | `timestamptz` | Bumped at claim/complete/vacate via the same statement that performs the write — application SQL, not a database trigger (§4.11). |

Unique index: `(task_id, user_id) WHERE is_active` — a correctness constraint that also serves as a performance index: nothing else stops two active rows existing for the same `(task_id, user_id)` pair from a retried activity insert or two independent reassignment paths targeting the same user. This same constraint is the actual backstop against duplicate assignment under redelivered delegation events (§6).

### 4.5 The audit trail: `outbox_events`, no dedicated `workflow_event` table

There is no local `workflow_event` table. Every domain event Execution records — the full internal audit trail — is written as a single `outbox_events` row (§4.10); the row's `payload jsonb` column (library-owned, genuinely arbitrary JSON, no schema change required to use it) carries every field a dedicated audit table would otherwise have needed as its own columns:

| Payload key | Notes |
| --- | --- |
| `workflow_instance_id` | Always present. |
| `task_id` | Present only for task-scoped events; absent for instance-level ones (`WorkflowStarted`, `WorkflowFinished`, `FORCE_ROUTED`). |
| `actor_user_id` | Present only for user-driven events; absent for system-driven ones. |
| `node_key` | Present where the event is node-scoped. |
| every other event-specific field | Same shape as the published envelope's `data` (§6.4) — the audit copy and the published payload are the same JSON, not two independently-maintained structures. |

`outbox_events.event_type` (already `text`, not a DB `enum`, matching the library's own column) and `outbox_events.created_at` (the row's own insert timestamp, which is also "when this happened" — insert and the triggering domain transition commit in the same transaction) stand in for what would otherwise have been dedicated `event_type`/`occurred_at` columns.

`outbox_events` itself is `platform-events`-library-owned — Execution authors no `CREATE TABLE` for it (§4.7) — but a service can still add its own indexes on top of a library-owned table via a service-authored migration, exactly as this schema already does for that table's RLS policy (§4.8). Two such indexes exist for audit/dashboard query access, both JSONB expression indexes: `(tenant_id, (payload -> 'data' ->> 'workflow_instance_id'), created_at DESC, id DESC)` for instance-timeline reads, carrying the keyset-ordering columns pagination needs in one composite; `((payload -> 'data' ->> 'task_id')) WHERE payload -> 'data' ->> 'task_id' IS NOT NULL` for task-scoped audit reads. The `data` hop matters: `outbox.Enqueue` (`platform-events`) `json.Marshal`s the whole CloudEvents-shaped envelope into `payload` — `{id, type, source, tenant_id, data: {...}, time, ...}` — so business fields the schema needs to filter/index on live under the envelope's own `data` key, never at `payload`'s top level.

This table is insert-only in effect for audit purposes — no row's `payload` is ever mutated after insert — but it is not schema-immutable the way a dedicated table would be: `published_at`/`attempts`/`last_error` are written by the outbox runner itself as it works the row toward publish. That's the library's own concern, not a violation of the audit trail's own append-only semantics.

### 4.6 `active_task_queues`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` NOT NULL PRIMARY KEY | Application-generated, matching every other table's PK convention (§4.11). |
| `tenant_id` | `uuid` NOT NULL | Recorded for the consumer's own upsert/lookup and operational debugging — not an RLS boundary (this table has none, see below). |
| `queue_name` | `text` NOT NULL UNIQUE | `wf-queue-<tenant_uuid>`. |
| `routable` | `boolean` NOT NULL | Whether new instances of the tenant start on this queue. True on registration and re-registration; false after a downgrade. Workers poll every row regardless. |
| `registered_at` | `timestamptz` NOT NULL | |
| `updated_at` | `timestamptz` NOT NULL | Single-writer-per-tenant upsert, no real concurrent-edit race, but the column is still carried for consistency. |

**No RLS on this table**, despite the `tenant_id` column, and its rows are never deleted while the tenant has an instance (the tenant's purge, §4.13, removes the row): Workers need to read every queue across every tenant in one query to compute their own registration set, which would be backwards to force through a per-tenant GUC context switch for what is fundamentally an operational/infra table, not tenant business data. No tenant-facing code path ever queries this table directly — only the tenant-events consumer writes it, only Workers read it, roughly every 60 seconds. A row is hard-deleted once the zero-active-instances downgrade check passes (§4.9); no soft-delete column is needed since nothing is left worth retaining at that point.

Index: `(queue_name)` unique — the registry's own upsert key. No other index is needed; the table stays small (bounded by the number of tenants that were ever on the enterprise plan, since a row stays until the tenant's purge) and is read in full by every Worker regardless. A worker polls a queue for as long as its row exists, so the poller count of a Worker replica only grows; removing the rows of offboarded tenants belongs to the offboarding purge (Appendix B).

### 4.7 Platform Tables

**`outbox_events` / `outbox_dead_letters`** are created entirely by `platform-events`' own embedded migrations — Execution Service authors no `CREATE TABLE` for either. The service only ever calls `outbox.Enqueue(ctx, tx, envelope)` inside the same transaction as the triggering domain write; `outbox_dead_letters` is written exclusively by the library's own relay on retry exhaustion. The one thing the service does author for these tables is the RLS policy migration (§4.8) — the schema itself is not local.

**`processed_event`** is a service-authored migration, following the same platform-wide consumer-idempotency convention already used by Definition Service and IAM's `iam-user-profile`:

| Column | Type | Notes |
| --- | --- | --- |
| `event_id` | `uuid` NOT NULL | Matches Definition Service's shape (IAM's own equivalent table uses `text` here — a minor cross-service divergence that doesn't affect dedup semantics). |
| `consumer` | `text` NOT NULL | |
| `event_type` | `text` | |
| `processed_at` | `timestamptz` NOT NULL DEFAULT now() | |

Composite PK: `(event_id, consumer)` — the dedup key is the event envelope's ID per consuming handler, so two different consumers can each process the same event exactly once. No `tenant_id` column and no RLS: the envelope ID is globally unique, and the table is infrastructure dedup state, not tenant business data. Which `consumer` values this table actually carries — the delegation pair and the `TenantStateChanged` tenant-lifecycle relay off `iam.membership.events`, plus `iam.user.events`, all forwarded via the shared Workflow-Events Consumer — is designed in §6, not here; this section fixes only the shape.

### 4.8 Row-Level Security

Every tenant-scoped table (`workflow_instance`, `workflow_task`, `workflow_task_assignment`) carries the same policy already run by Definition Service: `tenant_id = current_setting('app.tenant_id')::uuid`, the GUC set per-transaction via `pgcommon.WithGUCSet`/`WithValidatedGUCSet`. This RLS policy itself doesn't special-case any role — §9.2/§9.7's outbox-relay role is a `BYPASSRLS` exception at the connection-role level (the one process needing a cross-tenant scan), not a carve-out in the policy expression above.

Two named exceptions, both deliberate:

- **`active_task_queues`** — no RLS policy at all (§4.6): an operational/infra table read cross-tenant by design, never queried by a tenant-facing code path.
- **`processed_event`** — no `tenant_id` column and no RLS: infrastructure dedup state keyed by globally-unique envelope IDs (§4.7).

`outbox_events`/`outbox_dead_letters` **do** get RLS, but the policy is itself a service-authored migration (not library-provided) — matching Definition Service's own `000005_outbox_rls` pattern exactly: `current_setting('app.tenant_id', true)` (the missing-OK variant), plain text comparison with no `::uuid` cast, since that table's `tenant_id` column is `text`.

RLS is strictly a tenant-isolation boundary, not a soft-delete visibility mechanism — a distinction that matters if `workflow_comment`/`workflow_resource_link` ever return to scope, since their soft-delete convention would need its own, separate application-layer enforcement.

### 4.9 Indexing Strategy

Every index below is derived from a stated access pattern, not added speculatively:

| Index | Table | Access pattern |
| --- | --- | --- |
| `(tenant_id, department_id, status)` | `workflow_task` | Dashboard/admin filtering by tenant + department + status |
| `(workflow_instance_id, status)` | `workflow_task` | Status-query's "every active task for this instance" lookup |
| `(tenant_id, created_at DESC, id DESC)` | `workflow_task` | Tenant-wide task listing in keyset order (§5.9) |
| `(task_id) WHERE is_active` | `workflow_task_assignment` | Claim/complete/reassign's "current live assignment(s) for this task" lookup |
| `(task_id, user_id) WHERE is_active` UNIQUE | `workflow_task_assignment` | Same lookup shape, doubling as the correctness constraint (§4.4) |
| `(tenant_id, user_id) WHERE is_active` | `workflow_task_assignment` | **"Show me my active tasks"** — the single most common query a dashboard runs |
| `(tenant_id, subject_id, business_key)` UNIQUE | `workflow_instance` | The idempotent-instantiation upsert key, partial over non-terminal instances |
| `(tenant_id, status)` | `workflow_instance` | Instance-level (not task-level) dashboard views filtering by tenant + status |
| `(workflow_version_id) WHERE status IN ('RUNNING','PAUSED','DEGRADED')` | `workflow_instance` | The archive-guard query behind the active-instances endpoint — the Definition Service calls this before archiving a version; a partial index keeps the count from scanning terminal history. A race between this count and a concurrently-completing instance is an inherent TOCTOU gap in any check-then-act archive guard, not something this index resolves — the archive operation itself would need to re-verify at archive time for a stronger guarantee. |
| `(tenant_id, (payload -> 'data' ->> 'workflow_instance_id'), created_at DESC, id DESC)` | `outbox_events` | Instance-timeline/audit queries, carrying the keyset ordering columns — a service-authored JSONB expression index on a library-owned table (§4.5, §4.10) |
| `((payload -> 'data' ->> 'task_id')) WHERE payload -> 'data' ->> 'task_id' IS NOT NULL` | `outbox_events` | Task-scoped audit reads |
| `(event_type, (payload -> 'data' ->> 'task_id')) WHERE event_type IN ('WorkflowTaskSlaWarning','WorkflowTaskSlaBreached')` UNIQUE | `outbox_events` | DB-level backstop under `ExistsForTask`'s app-level SLA-event dedup check (§4.5) — not a query-serving index, a correctness constraint against two overlapping activity attempts both passing that check under READ COMMITTED |
| `(queue_name)` UNIQUE | `active_task_queues` | The registry's own upsert key — declared via the column's own `UNIQUE` constraint (§4.6, Appendix C), not a separate index |
| `(workflow_instance_id, node_key)` | `assignee_overrides` | "Override history for this node" — the read pattern §4.12's own audit purpose exists to serve |
| `(processed_at)` | `processed_event` | The 7-day TTL prune sweep's own delete-by-age query (§6.8) |

**Considered, not added:** `workflow_instance (tenant_id, started_by_user_id)` for a "workflows I started" view — no stated access pattern currently justifies it; noted here rather than silently never considered. `workflow_id` on `workflow_instance` deliberately gets no index — it exists for lineage/reporting joins with no current query-driven access pattern.

**Pagination is keyset, not offset**, on `workflow_task`/`outbox_events` — the two highest-cardinality, highest-write-volume tables in the schema. Offset pagination degrades linearly with page depth on tables expected to grow large fast; keyset pagination stays constant-cost provided an index leads with the filter column(s) and continues with the keyset ordering columns, which is exactly why the `outbox_events` instance-timeline expression index and the `workflow_task` tenant-listing index above are shaped the way they are (§5.9 carries the HTTP-level contract this enables).

**Aggregates are computed live, not stored.** Dashboard counts (e.g. "active tasks per department") run through the existing `(tenant_id, department_id, status)` index rather than a stored/materialized counter table — no measured performance need justifies the added write-path complexity and staleness risk of a counter.

### 4.10 One table, not two: why there's no dedicated `workflow_event`

`outbox_events` is a generic envelope — `{id, event_type, payload jsonb, tenant_id, trace_id, attempts, last_error, created_at, scheduled_at, published_at}`. There is no internal-only/externally-published split among event classes: every event class Execution records is treated as outbox-eligible, and consumers that don't care about a given `event_type` filter it out on their own subscription (a standard SNS/SQS filter-policy pattern) rather than Execution deciding on their behalf which classes leave the service.

**Decision, adopted here: one table, not two.** There is no dedicated `workflow_event` table. Every activity writes its audit/dashboard event directly as one `outbox.Enqueue` call — the row *is* the audit record and the to-be-published envelope at once, not two independently-written structures kept in sync by convention. What a dedicated table would have needed as typed columns (`workflow_instance_id`, `task_id`, `node_key`, `occurred_at`) lives inside `payload jsonb` instead, which is genuinely arbitrary JSON — no `platform-events` schema change is required to put them there. Query access comes from two service-authored JSONB expression indexes on top of the library-owned table (§4.5, §4.9), the same pattern this schema already uses for that table's RLS policy (§4.8) — adding an index to a library-owned table has never required the library's own migration to change. This is fully buildable today, with no cross-team dependency.

**When a separate `workflow_event` table would become necessary again.** Two conditions would tip the balance back: (1) audit/dashboard query needs diverge far enough from what a JSONB expression index can serve efficiently — e.g. a query shape needing to join or aggregate across many typed columns at once, where an expression index per field stops scaling; or (2) `platform-events` changes `outbox_events`' own shape in a way that stops fitting this use (for instance, tightening the pruning contract in a way that can no longer be deferred per-service, §4.11). Neither condition holds today; revisit if either does.

### 4.11 Retention, Partitioning, and Operability

**Provisional partitioning/retention plan** for `outbox_events` — now the schema's highest-write-volume table, accumulating a row per state transition across every tenant, on every instance, in addition to its own transactional-outbox role (§4.10). Following Definition Service's own precedent (its `audit_events` retention is plan-dependent, 1/3/7 years, with a hard 7-year floor for regulatory approvals with signatures): `outbox_events` is **range-partitioned by `created_at`, monthly**, from the first migration — not deferred, since this table's write rate is high enough by design (a row per signal, per task transition, per SLA timer fire) that retrofitting partitioning after the fact on a live table is the more expensive path. Retention mirrors Definition's rule: kept for 7 years for any instance whose approval flow carries a signature (the compliance floor), older partitions archived to cold storage rather than dropped. `workflow_instance`, `workflow_task`, and `workflow_task_assignment` grow roughly one row per instance/task/assignment rather than one row per transition, so they are **not partitioned initially** — the same precedent (no partitioning for the equivalent lower-write-rate tables in Definition Service's own schema) applies. **These are estimated numbers, not measured ones** — no production data exists yet since Execution Service has no code (Appendix B) — revisit the partition interval and the archival/cold-storage mechanics once real volume is observed.

**`PrunePublished` and the audit floor.** `platform-events`' `Runner.PrunePublished(ctx, olderThan, limit)` is an opt-in maintenance call, not automatic — the library's own guidance suggests `olderThan = 7 days` for a pure relay table, but nothing requires that number. Because this table now also serves as the audit trail, Execution calls `PrunePublished` with `olderThan` set to the same 7-year compliance floor as the rest of this section, not the library's short-lived-relay suggestion — a published row that's also an audit record is never eligible for deletion before that floor regardless of how long ago it was actually delivered to SNS.

**Backup/restore/DR** (RTO/RPO, restore testing) is likewise managed-infrastructure territory, not an app-repo schema-design decision — the same one-line deferred treatment already given to Temporal's own cluster DR elsewhere in this document (§3).

**Schema-wide conventions carried without re-litigation per table:**

- Every PK is application-generated, never DB-side-defaulted — load-bearing for `ON CONFLICT DO NOTHING` retry-safety under Temporal's at-least-once activity execution.
- No hard deletes among audit-bearing entities; every relevant FK is `ON DELETE RESTRICT`, not `CASCADE`. The only deletion is the tenant data purge (§4.13), which runs child-first after the retention period.
- `READ COMMITTED` isolation is sufficient throughout — the one race this schema needs to guard (concurrent claim/complete) is handled by the `record_version`-checked `UPDATE` pattern itself, not by a stronger isolation level.
- Timestamps are `timestamptz`, UTC throughout — a deliberate deviation from older, timezone-less precedent elsewhere in the platform.
- `text` columns compare case-sensitively at cluster-default collation; nullable `text` columns use `NULL` for "not applicable," never empty string.
- No generic `created_by`/`updated_by` anywhere — every table needing an actor column has a domain-specific one instead (`started_by_user_id`, `assigned_by`, `actor_user_id`).
- No display-name columns anywhere — every UUID reference is a bare IAM UUID; UUID-to-display-name enrichment is a read-API-layer concern (§5.8), not a schema one.

**`record_version`/`updated_at`/`deleted_at`, applied need-based:**

| Table | `record_version` | `updated_at` | `deleted_at` |
| --- | --- | --- | --- |
| `workflow_instance` | Yes | Yes | Skipped — `COMPLETED`/`TERMINATED`/`FAILED` are the domain's own terminal markers |
| `workflow_task` | Yes | Yes | Skipped — same reasoning |
| `workflow_task_assignment` | Skipped — the contested resource is the task, not any one assignment | Yes | Skipped — `vacated_at`/`is_active` is this table's own, more precise equivalent |
| `active_task_queues` | Skipped | Yes | Skipped — a row is removed only by the tenant's purge (§4.13) |
| `assignee_overrides` | Skipped | Skipped | Skipped — insert-only, immutable audit record; same reasoning `outbox_events` itself has always followed for its audit-bearing rows |

`record_version`/`updated_at` are bumped per-statement in application SQL, not via a database trigger — every version-checked `UPDATE` does `SET status = $2, updated_at = now(), record_version = record_version + 1 WHERE id = $1 AND record_version = $3` in one statement (e.g. `internal/adapter/outbound/postgres/db/workflow_instance.sql.go`, `workflow_task.sql.go`), so the bump and the optimistic-lock check are the same conditional `UPDATE`, not two separate mechanisms. System-driven transitions with no external caller (`DEGRADED`/`FAILED`/`COMPLETED`) still bump via their own equivalent unconditional `UPDATE` — there's no trigger backstopping either path.

### 4.12 `assignee_overrides`

The node-override endpoint's own audit record (§5.4) — one row per admin action, written by the API process itself in the same transaction as step 3 of the endpoint's validate-then-persist contract, *before* the Temporal signal in step 4. This is a documented same-class exception to "Activities are the sole DB writers" (§1.2), alongside the existing Terminate exception (§3) — the row exists specifically to prove an override decision was made and why, independent of whatever the eventual `instance-reassign` signal's own `ReassignAssignmentActivity` later writes to `outbox_events`/`workflow_task_assignment` once Temporal processes it. It is not a duplicate of that audit trail: the `outbox_events` row for `TASK_REASSIGNED` records that a reassignment happened to the running workflow; `assignee_overrides` records the admin decision and its eligibility check outcome that triggered it, queryable directly by node without parsing a generic `payload`.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `uuid` PK | Application-generated, never DB-defaulted (§4.9). |
| `tenant_id` | `uuid` NOT NULL | RLS boundary (§4.8). |
| `workflow_instance_id` | `uuid` NOT NULL REFERENCES workflow_instance(id) | `ON DELETE RESTRICT`, matching every other audit-bearing FK in this schema (§4.11). |
| `node_key` | `text` NOT NULL | The node whose target assignee was overridden. |
| `previous_user_id` | `uuid` NOT NULL | The assignee in effect immediately before this override. |
| `new_user_id` | `uuid` NOT NULL | The eligibility-checked replacement assignee (§5.4 step 2). |
| `reason` | `text` | Optional, admin-supplied free text explaining the override decision (§9.6). |
| `actor_user_id` | `uuid` NOT NULL | The admin who performed the override. |
| `created_at` | `timestamptz` NOT NULL DEFAULT `now()` | Insert-only; no `updated_at` exists to bump. |

**Insert-only, immutable** — no `record_version`, `updated_at`, or `deleted_at`, the same reasoning already established for every audit-bearing row in this schema (§4.5, §4.11): a row never updated after creation has nothing for those columns to guard. **RLS:** tenant-scoped like every other domain table (§4.8). **Retention:** same 7-year audit floor as `outbox_events` (§4.11) — this is audit content, not operational state.

---

### 4.13 Tenant data purge

An offboarded tenant keeps its work history for a retention period and is then purged.

- **Recording.** When the offboarding event is applied (`TenantMembershipsPurged`, or `TenantStateChanged` to `offboarded`) and the tenant's instances are terminated, the tenant is recorded in `tenant_retention`: tenant id, the offboarding time, `history_purge_after` (the offboarding time plus the configured history retention, 7 years by default) and `purged_at` (null until purged). A redelivered event keeps the first recorded times.
- **Purge.** A sweep in the API process, once the retention period has passed, deletes the tenant's operational rows in batches as the tenant, under its own row-level security (the purge needs no `BYPASSRLS` role), children first: assignee overrides, assignments, tasks (first clearing the reference between tasks), instances, then the connector document, attempt and send-intent rows, the tenant's queue registry row and its recency rows. Its recorded status stays as a marker (§5.5), so the start gate still sees an offboarded tenant and a late event cannot make it look new. It sets `purged_at` only after a check across every purged table finds no row left for the tenant: a pass that deleted nothing can mean another replica took the rows, so it counts as a failure and the next run retries. A run that stops part-way resumes where it left off, a tenant already purged is ignored, and other tenants' rows are never touched.
- **Not deleted.** `outbox_events` and `outbox_dead_letters`, the audit trail, which keep their own 7-year floor (§4.11); `processed_event`, which has no tenant. The stored objects behind connector documents, Temporal histories and Valkey stream entries are not deleted by this service; their retention is infrastructure configuration.
- **Configuration.** The retention period is a setting, so the compliance rule changes a value, not code. The sweep can be switched off. Nothing is deleted for a tenant before its period has passed, and nothing for a tenant that was not offboarded.

## 5. API Design

### 5.1 Deployment and Communication Topology

The Execution API process and the Temporal Worker process are separate deployments; no direct RPC exists between them. Both are independent clients of the Temporal Cluster's gRPC frontend and share the one `workflow_execution` Postgres schema described in §4:

- **API process**: on `POST /instances`, writes the `workflow_instance` row in its own transaction, then calls `StartWorkflow`. This is the one place the API writes the DB directly, ahead of any Activity. It also calls the Definition Service's compiled-plan read synchronously, as an early pre-check, before committing.
- **Worker process**: polls task queues (registry-driven off `active_task_queues`, §4.6), executes Activities. `GetCompiledPlanActivity` reads the compiled plan again, authoritatively, closing the gap between the API's pre-check and actual execution. Every other DB write happens inside a Worker-executed Activity.
- Coordination between the two processes is mediated entirely by (a) the Temporal Cluster — `SignalWorkflow`/`QueryWorkflow` calls issued by the API reach the workflow function running in a Worker, a Temporal-internal mechanism, not a service contract — and (b) the shared Postgres schema, which the API reads directly and the Worker's Activities write.

**Task Projection ownership.** Dashboard/list endpoints (§5.9) read the `workflow_execution` schema directly via the API process's own connection pool — no RPC or Worker-mediated indirection. Dashboard/list reads go against the primary, with no read-replica routing — the same posture Definition Service's own schema uses; keyset pagination (§5.9) already keeps these reads cheap regardless of table size. Revisit only if measured dashboard-query contention against the Worker's write-heavy pool materializes. The push side (Server-Sent Events to the browser) is a separate Dashboard Stream Gateway service that subscribes to Execution's outbox-published events; Execution's obligation is limited to emitting well-formed events (§6), not building the gateway.

### 5.2 HTTP Endpoint Catalogue

Execution's HTTP API uses the `/api/v1` prefix, matching Definition Service's existing shipped convention.

| Method | Path | Purpose | Transaction shape |
| --- | --- | --- | --- |
| GET | `/healthz` | Liveness | — |
| GET | `/readyz` | Readiness (DB + Temporal) | — |
| GET | `/metrics` | Prometheus scrape | — |
| GET | `/swagger/` \| `/api/openapi.yaml` \| `/asyncapi` | Doc surface (Swagger UI dev-only; OpenAPI/AsyncAPI specs) | — |
| POST | `/instances` | Start instance | Exception: writes `workflow_instance` synchronously, then `StartWorkflow` |
| GET | `/instances` | List instances (keyset-paginated, filterable; `subject_id` filters to one subject's instances) | Read-only |
| GET | `/instances/:id` | Instance detail and tasks, including `override_map`; scoped to assignee/department/admin (§9.2) | Read-only |
| POST | `/instances/:id/pause` \| `/resume` \| `/cancel` \| `/terminate` \| `/force-forward` \| `/force-back` | Admin lifecycle signals | Signal-only (Terminate is the one exception — direct DB write then `TerminateWorkflow`, §3) |
| POST | `/instances/:id/nodes/:node/complete` | An outside party's answer completes a receive task (§5.11) | Signal-only; query-checked first |
| POST | `/instances/:id/nodes/:node/boundaries/:boundary/fire` | An outside party's message fires a message boundary (§5.11) | Signal-only; query-checked first |
| POST | `/instances/:id/nodes/:node/override` | Node-override (§5.4) | 4-step ordered: validate + version-check → eligibility check → persist → signal |
| GET | `/instances/:id/events` | Cursor-paginated activity log | Read-only |
| GET | `/tasks` | List tasks (keyset-paginated; filterable by `status`, `department_id`, `instance_id`, `assignee_user_id`, `due_before`) | Read-only |
| GET | `/tasks/:id` | Task detail and assignments (including vacated history); scoped to assignee/department/admin (§9.2) | Read-only |
| POST | `/tasks/:id/claim` \| `/complete` \| `/defer` \| `/reassign` | User/admin task actions | Signal-only; `/complete` per §5.6's caller-agnostic contract |
| GET | `/workflows/active-by-user` | Admin/reconciliation query, shared `ActiveUserTask` type | Read-only |
| GET | `/api/v1/internal/workflows/delegate-impact` | Preview of a delegate-reroute's affected-row set (§5.8) | Read-only |
| POST | `/api/v1/internal/workflows/reassign-delegate` \| `/cancel-by-delegate` | `port.WorkflowClient` family (§5.8) | Exception: bulk-write-then-signal |
| POST | `/api/v1/internal/connector-tasks/:id/complete` \| `/fail` | `cmd/connector-worker`'s HTTP callback for a connector-typed task's dispatch outcome (§3.1). The body names no actor: the service records the tenant's automation principal, resolved from IAM (§5.7). A failure carries the connector's output map when the connector produced one (a send-email call's delivery outcome, a rest-call's status), whose keys the service writes to its failure log | `ConnectorTaskService` signals `stage-transition`/`stage-fail`; idempotent under retried callback (terminal-status check + `Cache.SetNX`). A callback for a `PAUSED` instance is refused with `409 INVALID_INSTANCE_STATE` and writes nothing, so the worker leaves its entry unacknowledged and the redelivery after resume completes the task; a callback for an instance that is over (`COMPLETED`, `TERMINATED`, `FAILED`) is acknowledged with no signal |

Every mutating operation accepts an optional `Idempotency-Key` header (§5.9). Twenty base operations, plus the three cross-service additions this stage designs (node-override, the `WorkflowClient` family, and the delegate-impact preview) close every Execution-facing requirement raised by the cross-service workflow documents consulted for this design — no further endpoint is required at this stage.

Two carried-forward gaps are closed here: `override_map` is added to the instance-detail response (it was stored but never returned), and a single shared `ActiveUserTask` domain type serves both the `active-by-user` endpoint and an internal event-handler call site, rather than two divergent shapes. Instance-level comments and a task-scoped resource-links list, considered at an earlier point, are removed from scope along with the `workflow_comment`/`workflow_resource_link` tables (§4.1).

Starting an instance supplies the version to run, the subject the instance is about, the business key identifying it within that subject, and an optional per-node assignee override map:

```json
{
  "workflow_version_id": "8fa88cde-824c-47bc-836b-665cd42c2222",
  "subject_id": "TND-2026-04471",
  "business_key": "stage-prequalification",
  "override_map": {
    "review_finance": "4da18bde-7244-47ac-986c-665cd42caaaa"
  }
}
```

Both are required. A domain running one instance per subject sets the key to a constant of its own choosing; the pair is what must be unique, and the engine supplies no default for either.

**Retrying a start.** A start whose subject and business key match a live instance is the same start sent again when that instance is `RUNNING`, runs the same version, was started by the same caller and carries the same context and override map. The match is made before the compiled plan is read or the assignees are checked, so a version archived or an assignee changed since the first attempt does not strand the instance. When no run id is recorded the service issues `StartWorkflow` for it again, an execution already running under that identifier counts as success, the run id is recorded, and the existing instance is returned with no second row and no second `started` event. A match that has a run id recorded is returned unchanged and issues nothing. Every other match, an instance of another version, started by another caller, carrying another context or override map, or no longer `RUNNING`, is `409 DUPLICATE_BUSINESS_KEY`. A start that fails after the row is written answers `5xx`, and the client's retry is what completes it. A `RUNNING` row with no run id whose execution was closed outside this service is started again as a new execution; the service does not look for closed executions.
On success, the instance is created and `StartWorkflow` has been issued against the Temporal Cluster:

```json
{
  "id": "b6e2b6f0-1c3e-4a2a-9c6d-2f0a7e9b5d11",
  "subject_id": "TND-2026-04471",
  "business_key": "stage-prequalification",
  "workflow_version_id": "8fa88cde-824c-47bc-836b-665cd42c2222",
  "status": "RUNNING",
  "temporal_workflow_id": "7ca648b2-b432-4744-884c-35fd556a310c:TND-2026-04471:stage-prequalification",
  "current_node_keys": ["intake"],
  "record_version": 1,
  "created_at": "2026-07-20T09:14:02.331Z"
}
```

If the bulk node-eligibility re-validation (§5.5) finds a stale default assignee, the request is rejected before `workflow_instance` is written or `StartWorkflow` is called:

```json
{
  "type": "https://errors.bcbp.io/execution/assignee-ineligible",
  "title": "Assignee Ineligible",
  "status": 422,
  "detail": "One or more default assignees no longer satisfy their node's eligibility requirement.",
  "instance": "/api/v1/instances",
  "code": "ASSIGNEE_INELIGIBLE"
}
```

### 5.3 Calls between this service and the Definition Service

The two services call each other over HTTP on their internal route groups (`/api/v1/internal`), which the public gateway does not route.

**Contracts.**
- This service's internal endpoints are specified in `api/internal-openapi.yaml`, with a design-repo copy, `execution_internal_openapi.yaml`.
- `api/contracts/definition-internal-openapi.yaml` is a byte-identical copy of the Definition Service's internal document, and `api/contracts/SOURCES` names the commit it was taken from. `make contract-check` fails when the copy differs from that file.
- `test/contract` sends requests through the real router and validates each response against this service's document. It also validates every request the definition client sends against the copy, and parses the copy's response examples (definition LLD §3.4).

**Authentication.** A caller sends the callee's token in `x-internal-token`. This service compares it with its own `INTERNAL_API_TOKEN` in constant time (`RequireInternalToken`), and answers a missing or wrong token with `401` and a problem body. Calling the Definition Service, this service sends `DEFINITION_SERVICE_TOKEN`; the Definition Service sends this service's token as `EXECUTION_SERVICE_TOKEN`.

| Direction | Endpoint | Caller | Notes |
| --- | --- | --- | --- |
| Served | `GET /api/v1/internal/workflows/{workflow_id}/active-instances?tenant_id=` → `{has_active, count}` | Definition Service | The archive guard, backed by the partial index in §4.9 |
| Served | `POST /api/v1/internal/users/{user_id}/pause-tasks`, body `{tenant_id}` → `204` | Definition Service | The membership-revoked safety-net pause: each RUNNING instance holding an active assignment for the user gets a pause signal with initiator `safety_net`; a failure to signal one instance is logged and skipped |
| Called | `GET /api/v1/internal/workflow-versions/{version_id}/compiled-plan?tenant_id=` | Definition Service | From instance start (§5.1), always live; from running-instance reads through the compiled-plan cache; and from the Worker's `GetCompiledPlanActivity`, which is authoritative |

**The definition client** (`internal/adapter/outbound/http/definition_client.go`, configured by `DEFINITION_SERVICE_URL` and `DEFINITION_SERVICE_TOKEN`):
- It makes 3 attempts, each bounded by `DEFINITION_CLIENT_TIMEOUT`, and stops when its context is done.
- It retries a transport error or a `5xx` after 50ms, then 100ms, capped at 500ms. After the last attempt it returns `port.ErrDefinitionUnavailable`, which `GetCompiledPlanActivity` retries.
- A `4xx`, or a response that breaks the contract (a body that does not decode, or identifiers that are not UUIDs), is `port.ErrDefinitionRejected`, which the activity fails as non-retryable `DefinitionServiceClientError`.
- The plan arrives as a JSON object, and the client keeps its bytes as `CompiledWorkflow.CompiledPlanJSON`. The plan's own `schema_version` is the DSL schema major version that §2.5's compatibility layer acts on.
- `cmd/server`, `cmd/worker` and `cmd/connector-worker` all use `DEFINITION_SERVICE_URL`; the connector-worker reads the alias registry from it (`workflow_connectors.md` §6.2).

### 5.4 Node-Override Endpoint

`POST /api/v1/instances/:id/nodes/:node/override` lets an admin reassign a single node's target user, per the cross-service assignee-override contract owned by Execution (Org & Membership does not model this). The request body carries the node's target `new_user_id`, an optional `reason`, and the caller's current view of `record_version` for the task at that node — the same optimistic-lock discipline every mutating endpoint in this design follows.

The endpoint implements a **validate-then-persist** contract: no override row may ever exist for a request that is later rejected on eligibility grounds.

1. **Validate and version-check.** Look up the `workflow_task` row at the target node. If it has already resolved (completed, superseded, etc.), short-circuit with `409` before any further work — the cheapest possible rejection point. If it is still open, compare the caller-supplied `record_version` against the row's current value; a mismatch returns `409 RECORD_VERSION_CONFLICT`, the same response every version-checked endpoint gives a stale-view caller.
2. **Check assignee eligibility.** Call the domain's assignee-eligibility endpoint — today, Org & Membership's internal eligibility check — supplying `{new_user_id, department_id, required_level, actor_id}`. Execution supplies the node's `(department, level)` eligibility requirement, since the upstream service does not derive it independently. On ineligibility, return `422 ASSIGNEE_INELIGIBLE`; on unavailability or an unrecognized response shape, return `503 UPSTREAM_UNAVAILABLE` — fail closed rather than guess at intent. Either way, nothing is persisted and no signal is sent.
3. **Persist the override**, an `assignee_overrides` row (`workflow_instance_id`, `node_key`, `previous_user_id`, `new_user_id`, `reason`, `actor_user_id`), only on step 2's success.
4. **Signal Temporal** (`instance-reassign`) after step 3's commit, carrying the same `record_version` step 1 validated — the signal-level check is redundant-but-consistent with the HTTP-level one, not a second independent gate that could disagree with the first.

Step 1's checks resolve two races without any bespoke locking, both via the ordinary `409` family: a concurrent `instance-force-forward` on the same task either marks it `SUPERSEDED` first (caught by the already-resolved check) or is itself caught by force-forward's own signal-level version check if the override commits first; a concurrent delegate-reroute on the same task is caught symmetrically by the `record_version` mismatch, since neither operation marks the task resolved. Two admins submitting competing overrides concurrently both pass the initial checks and both call the eligibility endpoint (a real, if wasteful, double external call), but only the first to persist commits — the loser's persist fails on its now-stale `record_version`, returning `409 RECORD_VERSION_CONFLICT` rather than a silent overwrite; the loser's already-succeeded eligibility check is not undone, a minor wasted-authorization side effect.

If the eligibility check and the persist both succeed but the subsequent Temporal signal fails, both durable stores have recorded the change but the running workflow has not been told — an accepted non-atomic gap of the same class as Terminate's own DB-then-RPC shape (§3), flagged as an open reconciliation item rather than designed further here.

`POST /api/v1/instances/:id/nodes/:node/override` carries the target user, an optional reason, and the caller's view of the task's `record_version`:

```json
{
  "new_user_id": "c1a9e6b0-5f34-4e2a-9d11-8a2c4e7b9911",
  "reason": "primary reviewer on leave, reassigning to backup",
  "record_version": 4
}
```

On success (step 2's eligibility check passed, the override row is persisted, and the `instance-reassign` signal was sent):

```json
{
  "workflow_instance_id": "b6e2b6f0-1c3e-4a2a-9c6d-2f0a7e9b5d11",
  "node_key": "review_finance",
  "previous_user_id": "4da18bde-7244-47ac-986c-665cd42caaaa",
  "new_user_id": "c1a9e6b0-5f34-4e2a-9d11-8a2c4e7b9911",
  "reason": "primary reviewer on leave, reassigning to backup",
  "actor_user_id": "9e21c8a0-6b45-4a10-8e3f-2d6c1a9b4400",
  "record_version": 5
}
```

If the caller's `record_version` is stale — the task moved on since the caller last read it — step 1's version-check rejects before the eligibility call is ever made:

```json
{
  "type": "https://errors.bcbp.io/execution/record-version-conflict",
  "title": "Record Version Conflict",
  "status": 409,
  "detail": "The task at node 'review_finance' has been modified since record_version=4 was read.",
  "instance": "/api/v1/instances/b6e2b6f0-1c3e-4a2a-9c6d-2f0a7e9b5d11/nodes/review_finance/override",
  "code": "RECORD_VERSION_CONFLICT"
}
```

### 5.5 Bulk Node-Eligibility Re-validation at Instantiation

**Tenant status and new instances.** `TenantStateChanged` (§6.2) pauses or terminates instances that already exist, and the status it carries is recorded in `tenant_state` (the latest change by its own time wins, so a delayed older event never undoes a newer one). Starting an instance consults that status only for the statuses named in `START_REFUSED_STATUSES`, which is empty by default, so by default no start is refused on a tenant's status. A start for a tenant whose recorded status is in the list is refused before anything is read or written, with `409 TENANT_NOT_ACTIVE`; a tenant with no recorded status is allowed, and a failure to read the status fails the start. The platform covers part of the gap on its own: a `cancelled` tenant's writes are denied by the authorization layer and a `suspended` tenant's realm is disabled, which stops new tokens; it states nothing for `trial_expired`, and a token issued before a suspension stays valid for a few minutes. The status is recorded before the transition is applied, so a suspended tenant stops starting at once even while a failed pause is retried. Which statuses to refuse is a product decision the setting carries.

On `InstanceService.Start`, before writing `workflow_instance`, Execution also re-validates every node's default assignee — not only what appears in the request's `override_map` — for both existence and current `(department, level)` eligibility, since a compiled plan's default assignees can go stale between authoring time and instantiation time (a membership change, a level demotion), and a version published before Definition required a default assignee may name nobody at all.

1. **Resolve every node that will become a human task.** A node's assignees are its `override_map` entry when the request carries one, and its compiled `DefaultAssignees` otherwise: for a module task, the call site's `Assignees` entry, else the task's own `candidateUsers`, as `dsl.ExpandCalls` writes them. A module task's node is its call-scoped key (§2.3). Stages that create no task are skipped, and the skip list mirrors the interpreter's own dispatch (§2.6) rather than restating it: connector-typed stages, whose worker resolves them, and `send_task`/`receive_task`, which resolve through the message buffer. Departments marked `ignore` are skipped whole.
2. **A node resolving to nobody is ineligible, without asking IAM.** A node whose assignees are absent, or whose only entries do not parse, would otherwise produce a task with no assignment rows and `assignee_mode = "single"` — a shape the claim path refuses on both counts (§5.6), so the task could be neither claimed nor completed and the instance would never advance. Definition refuses to publish a workflow in which any such task has no default user (`definition_service.md` §3.1.4 code 8, `CANDIDATE_USERS_EMPTY`), so this check is the backstop: for a default whose entries do not parse, and for a version published before that rule.
3. **Batch the distinct requirements.** For the nodes that do resolve to somebody, group by distinct `(department_id, required_level)` pairs rather than issuing one call per node — this is what makes the check scale to large templates instead of firing N synchronous calls per instantiation.
4. **Call the membership endpoint once per distinct pair**, checking whether the currently-assigned default still satisfies that node's `(department, level)` requirement.
5. **Hard-block on any failure.** If any node resolved to nobody, or any default assignee no longer satisfies their node's requirement, `POST /instances` returns `422 Unprocessable Entity` with code `ASSIGNEE_INELIGIBLE`, naming every offending node in one payload so the caller resolves all of them before a single retry — never silently substituting a replacement the caller never confirmed, since this is a compliance-sensitive workflow. One code covers both conditions: from the caller's side each is the same problem, a named node no one can act on.

This re-validation is synchronous and blocking, performed in the same request that later writes `workflow_instance` and calls `StartWorkflow`. The `422` status aligns with the equivalent eligibility failure returned by node-override's own check (§5.4) and by Definition Service's analogous business rule — one code covers instantiation-time and node-override eligibility failures across every service in this contract.

### 5.6 Complete-Task's Caller-Agnostic Contract

`POST /api/v1/tasks/:id/complete` is called directly by the browser for an ordinary task completion, and directly by Tender Service (or any other domain service with its own precondition to satisfy first, §3.5) immediately after that precondition succeeds — never through a third-party intermediary. Any caller authenticates and forwards identity the same way (gateway headers, not the internal-service token), so from the endpoint's own perspective the contract is identical either way (Appendix A.2 decisions 19/20, §3.5). The endpoint:

- Forwards `x-user-id`/`x-tenant-id` — the acting approver's identity, never the calling service's own credential — and validates it against the task assignment's actual assignee, returning `403 NOT_ASSIGNEE` on a mismatch.
- Requires the caller to have fetched the task's current `record_version` immediately before calling, satisfying the standard optimistic-lock pattern with no special case carved out for service-to-service callers.

The endpoint's own shape doesn't depend on who the caller is: identity-forwarding plus version-check is all it requires, and that's true whether the caller is the browser or a domain service completing its own precondition.

Separately, `get-workflow-status` — the one Temporal Query this schema defines — is deliberately **not** exposed via any HTTP endpoint. `GET /api/v1/instances/:id` serves purely from the Postgres projection (§4), avoiding a second, Temporal-Cluster-latency-bound read path for data the dashboard already gets from Postgres; the Query is reserved for internal reconciliation and test tooling only.

`POST /api/v1/tasks/:id/complete` — the caller (browser, or a domain service like Tender forwarding the approver's own identity after its own precondition succeeds) submits the assignee's decision payload plus the version-checked lock token:

```json
{
  "result_json": {
    "decision": "approve",
    "comments": "figures reconcile against the Q2 tender ledger"
  },
  "record_version": 2
}
```

Success response — the mutated task itself, not a `completed_by_user_id`-bearing shape and not a minimal `SignalAccepted` message either: `claim`/`complete`/`defer`/`reassign` all share the same `TaskSummary` shape `GET /tasks`/`GET /tasks/:id` return, per §5.10:

```json
{
  "id": "d4f2a911-7c3b-4e8a-9b21-6f5a8e0c3d77",
  "workflow_instance_id": "b6e2b6f0-1c3e-4a2a-9c6d-2f0a7e9b5d11",
  "tenant_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "node_key": "review_finance",
  "task_type": "userTask",
  "department_id": "9b1c1e2a-...-finance",
  "status": "COMPLETED",
  "record_version": 3,
  "assignee_mode": "single",
  "assignee_count": 1,
  "due_at": "2026-07-21T09:14:02.331Z",
  "created_at": "2026-07-20T09:14:02.331Z",
  "completed_at": "2026-07-20T09:31:47.902Z"
}
```

(`deferred_from_task_id` is omitted here: no task is created by a defer any more, so the column is never written.)

### 5.7 Internal-Service Authentication

The HTTP API layer's default is Signal-Only: it validates requests and forwards them to Temporal as signals, never writing the DB directly in the request path. The one documented exception is the bulk SQS-driven tenant/delegation event handlers (§5.8), which write the DB directly and then signal in a loop — a deliberate, reasoned exception, not something to "fix."

Node-override's outbound call to the domain's eligibility endpoint, and the inbound `port.WorkflowClient` family (§5.8), are service-to-service, not browser-driven through the gateway. **Inbound internal-service authentication adopts Definition Service's shipped convention**: a `RequireInternalToken`-style middleware validating an `x-internal-token` header against an `INTERNAL_API_TOKEN` environment value (required outside dev, optional in dev), applied to the `/api/v1/internal` route group — exactly how Definition Service already guards its own `POST /api/v1/internal/events`. For the outbound eligibility call, the authentication requirement is the upstream service's to specify; Execution supplies whatever credential their contract names, not an Execution-side decision.

**Scope, stated explicitly: the internal-service token is for non-user-driven automation only.** It authenticates the shared Workflow-Events Consumer, the Definition Service's calls, and Org & Membership's `port.WorkflowClient` calls — none of these have a live human user behind them at the moment of the call. Business-action calls are a different case entirely: whether the caller is the browser or a domain service like Tender acting on an explicit, in-the-moment user action (§5.4, §5.6), it authenticates the same way a direct browser call would — ordinary gateway-injected headers (`x-tenant-id`/`x-user-id`/etc., §1.2), not the internal-service token. `x-internal-token` is never used to call Execution's task/instance mutation endpoints.

**Trust boundary, stated explicitly.** A valid `x-internal-token` authenticates that the *caller* is a legitimate internal service — it does not scope *which* tenant that caller may act on. Any request bearing a valid token can specify any `tenant_id` in its body; the only tenant-consistency check performed is `TENANT_MISMATCH` (a `delegation_id` belonging to a different tenant than the request's own `tenant_id` parameter), not a check that the calling service is "allowed" to touch that particular tenant. This is a deliberate trust boundary — per-tenant authorization for a given caller's requests is the upstream service's own responsibility, upstream of ever calling this endpoint. A required ingress `NetworkPolicy` restricts `/api/v1/internal/*` at the network layer to the specific pod identities allowed to call it — the shared Workflow-Events Consumer and Definition Service — as defense-in-depth alongside the token (§9.2). `INTERNAL_API_TOKEN` stays a single static secret per environment, rotated only as an ordinary deployment/secrets-management operation — no time-limited or automated refresh flow is adopted.

**Automation carries an actor, not only a token.** A connector-driven completion authenticates with the internal token and, until this change, carried no identity at all — the platform could establish that a call came from inside the cluster and not on whose behalf. Authentication without attribution is enough for a call and not enough for a record, and downstream domains retain records of whether an actor was a person or the system.

Execution names the tenant's `platform-automation` principal as the actor of every connector-driven completion and failure, and the callbacks carry no actor. The principal is a real per-tenant client the identity subsystem mints and custodies (its Token Service holds the key material, its Realm Provisioner mints the client); Execution names it and never authenticates as it. One automation identity serves the platform rather than each surface inventing its own. A workflow template names a kind of connector and never a user, so no automation identity is resolved at design time.

Execution resolves the principal through the Token Service's per-tenant read (`GET /api/v1/internal/tenants/:id/service-accounts/platform-automation`), caches the answer per tenant for a short period, and records the principal's `principal_id`, which is stable across the re-mint at tenant conversion, never its `sub`, which changes. It resolves when a connector task is put on the stream, so a tenant with no principal is refused before any side effect and the event consumer retries the dispatch, and again on each callback, where a tenant with no principal answers `409`. A principal whose status is not `active` counts as no principal, and neither that answer nor a missing principal is cached, so a tenant minted later is found at once. Because no request names an actor, no caller can name a wrong one.

The read is made by the API service, not by the connector worker. The identity subsystem admits callers to it by namespace and by mesh identity, so the namespace the Execution Service runs in is named on the identity side's workflow list, and the API service is a mesh peer; that admission is an onboarding step per environment. Execution does not subscribe to the principal's registration event: it keys on `principal_id`, which a re-mint leaves unchanged, so a cached entry can never name a wrong actor, and a tenant that is offboarded stops resolving once the cache lifetime passes.

The principal is a non-member by construction on the identity side — it cannot be granted a department membership at all, a bar enforced structurally rather than by configuration. It therefore satisfies no eligibility requirement this service checks, which is what makes it safe to hand a task: automation changes who drafts, never who accepts. Execution records that dependency and does not re-check it, since a second enforcement path here would only drift from the structural one.

**The principal holds a real assignment row.** Both callbacks create an assignment for the actor before signalling, so a connector task resolves through the same path a human's does: the actor is retained where a human actor is retained, and the task's completion event names it. Connector completion stops being a path with its own rules. The row's identifier is derived from the task and user, so a retried callback re-creates the same row rather than a second one.

**Trust boundary, unchanged for now.** The actor arrives as a request field, and the mesh plus `x-internal-token` is what vouches for the caller. Verifying the principal's own signed assertion at this hop is the open item (Appendix B); until then this hop trusts the worker to state its own identity truthfully, which is the same trust the token already implies.

### 5.8 `port.WorkflowClient` Family

Execution is the server, Org & Membership is the caller — mirroring Org & Membership's own client-side interface of the same name.

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/v1/internal/workflows/delegate-impact` | Query params `tenant_id`, `delegate_user_id`, `delegation_id` — previews how many/which active assignments a delegate change touches |
| POST | `/api/v1/internal/workflows/reassign-delegate` | Body `{tenant_id, old_delegate_id, new_delegate_id, delegation_id?}` |
| POST | `/api/v1/internal/workflows/cancel-by-delegate` | Body `{tenant_id, delegate_user_id, delegation_id?}` |

`delegation_id` is optional; when present it filters via the `reason="delegation:<id>"` tag (§4.4), reusing the same scope-aware filter rather than re-deriving scope at query time. Omitted, it falls back to a `reason`-prefix match only, for backward compatibility. A call for a delegate with zero active tasks returns `{reassigned: 0}`, not an error — an empty affected-set is a valid outcome.

**Scope-aware filter algorithm**, shared by both mutating methods and the read-only preview:

1. **Establish the starting set.** List every currently active assignment for the delegator within the tenant.
2. **Filter by `scope`.** A scope of `all` passes every row through unfiltered. A scope of `department` keeps only rows whose task's `department_id` matches the given scope value, which is what lets a delegator who holds assignments across several departments hand over one department's work alone. A scope of `tender` is compared with the instance's `subject_id`, since the identity subsystem mints it as the bare subject identifier and §4.2's `subject_id` holds exactly that; a future domain's subject-scoped value gets its own case beside it. Any other scope value matches nothing, is logged and counted (§11), and leaves the delegator's assignments where they are so an addition upstream is visible rather than silently treated as subject-scoped. The value itself is constrained upstream: Org & Membership rejects anything outside its own enum at creation, so a misspelling never reaches this service.
3. **Re-check delegate eligibility** for each surviving row's node before acting — routing never implies eligibility. Rows whose delegate is ineligible for the node stay held at the delegator rather than being rerouted to an unqualified assignee. For eligible rows, `DelegationStarted` vacates the delegator's assignment and inserts one for the delegate, carrying the vacated row's `is_lead` across so a task the delegator had claimed stays claimed by the delegate, and tagging `reason="delegation:<delegation_id>"`; `DelegationEnded` reverses it, finding active assignments tagged with that same `delegation_id` and restoring the original assignee. `ReassignDelegate` additionally checks the new delegate's liveness via the upstream IAM contract before this loop begins, fail-open (§5.10) — a per-call check, since it applies uniformly to every task the call would otherwise reassign, not per-row.

Because both mutating methods act on potentially dozens of a delegate's active tasks at once, they use the same bulk-write-then-signal transaction shape as the SQS-driven bulk handlers (§5.7): the affected-row set is written in one transaction, then each affected instance is signaled individually, with partial signal failure handled by the existing retry/idempotency pattern rather than a new mechanism.

`POST /api/v1/internal/workflows/reassign-delegate`:

```json
{
  "tenant_id": "7ca648b2-b432-4744-884c-35fd556a310c",
  "old_delegate_id": "4da18bde-7244-47ac-986c-665cd42caaaa",
  "new_delegate_id": "c1a9e6b0-5f34-4e2a-9d11-8a2c4e7b9911",
  "delegation_id": "e9a71c40-8b3d-4f11-9c2e-5a6b7d8e9f01"
}
```

Response — the count of rows the scope-aware filter actually rerouted, `0` on a delegate with no active tasks being a valid, non-error outcome:

```json
{
  "reassigned": 7
}
```

### 5.9 Idempotency and Pagination Conventions

**`Idempotency-Key`.** An optional `Idempotency-Key` UUID header is accepted on every mutating endpoint, cached in Valkey under `idem:<tenantID>:<routePath>:<idempotency-key>` with a **24-hour TTL** — not invalidated on read, expires naturally — with the request body's SHA-256 hash stored alongside; this is the identical key shape, TTL, and cache-set-failure behavior Definition Service already ships, adopted unchanged rather than re-derived. A replay with the same key and the same body returns the cached response verbatim without re-executing the handler; a replay with the same key and a different body returns `409 IDEMPOTENCY_KEY_REPLAY`. Only `2xx` responses are cached, so a failed attempt can be retried under the same key. **Valkey unreachable at cache-set or cache-lookup time is fail-open**, matching Definition Service's own posture: the failure is logged at `WARN` and the request proceeds without caching or dedup — duplicate-protection is degraded for the outage's duration, not the request itself blocked, since `Idempotency-Key` is a safe-retry convenience layered on top of the endpoint's own mutation, not this schema's correctness backstop (§4.9's own DB-level uniqueness constraints and `record_version` remain the correctness guarantees regardless of Valkey's availability). The resulting window — a duplicate mutation slipping through while Valkey is down — is bounded strictly by the outage's own duration, the same trade-off Definition Service's design already accepts. This is a distinct concern from `record_version` optimistic locking (which protects against concurrent conflicting writes) and from Temporal's own activity-retry safety (which protects against at-least-once activity execution) — `Idempotency-Key` specifically protects a client safely retrying one logical HTTP request after a network failure.

**Pagination is keyset, not offset**, on every list endpoint — a deliberate deviation from Definition Service's own shipped `page`/`limit`/`{page, limit, total}` offset contract, justified by `workflow_task`/`outbox_events`' write volume (§4.9). The request carries an opaque `cursor` (encoding the previous page's last `(created_at, id)` pair) and a `limit` (default 25, max 100 — deliberately kept under the eligibility-hydration endpoint's own per-call ID-batch limit, so downstream UUID-to-display-name enrichment composes one batch call per page). The response carries `items[]` plus a `next_cursor`, omitted on the final page — no `total` field, since a count would re-scan exactly what keyset pagination exists to avoid.

Explicit edge-case decisions: an unparseable or tampered `cursor` returns `400 BAD_REQUEST` rather than silently starting over, which would mask a client-side bug as an empty result set; a `limit` above 100 is clamped rather than rejected (a soft ceiling, not a security boundary); a `limit` ≤ 0 is rejected, since there is no sane clamp direction for a non-positive page size. A filter or cursor matching zero rows returns an empty page with no error — the `*_NOT_FOUND` codes are reserved for single-resource `:id` lookups, never for a list endpoint's filtered-to-nothing result. Requesting a single-resource `:id` endpoint for an ID that exists but belongs to another tenant returns the same `*_NOT_FOUND` code as a nonexistent ID, never `403` — Postgres RLS filters the row out at the query layer itself, so the handler cannot distinguish "doesn't exist" from "exists, wrong tenant."

No `ETag`, `Cache-Control`, or conditional-GET behavior is defined — task/instance state changes too frequently for cache staleness to be safe, matching Definition Service's own convention. No `Retry-After` header is defined on `503` responses, and no HTTP-layer rate limiting is designed — both are candidate future additions.

**API versioning: path-based `/api/v2`, matching Org & Membership's own established platform convention.** A breaking change (field removal/rename, type change, required-ness tightened) to any endpoint gets a parallel `/api/v2/...` route for the affected resource; `/api/v1` stays fully functional until its traffic drops to zero, then retires — the same producer-then-consumer coexistence shape the event side's `.v2` mechanism already uses, just at the HTTP-route level instead of the wire-type level. An additive change (new optional field, widened enum, new endpoint) never requires a version bump, matching the event side's own additive-first convention. No sub-resource-level versioning (e.g. versioning just one endpoint's response shape independently) — a `/api/v2` bump is whole-API, matching IAM's own precedent, simpler to reason about than partial per-route versioning even though it means duplicating unaffected routes' definitions into the new version.

**UUID-to-display-name enrichment** is explicitly out of Execution's own contract: every list/detail endpoint returns raw UUID columns only (`user_id`, `department_id`, `assigned_by`, etc.), matching this schema's identity-agnostic, no-display-name-column design (§4.11). The browser itself composes Execution's paginated responses with IAM's own `/users:batch`/`/departments` endpoints directly — no backend enrichment layer sits between Execution and the browser for this — Execution's list endpoints' bounded page size (§5.9 above) is specifically sized to make that composition cheap regardless of who performs it, but the composition itself is not Execution's responsibility.

`GET /api/v1/instances` returns a keyset page — `items[]` plus an opaque `next_cursor`, omitted on the final page. `?subject_id=` narrows the page to one subject, which is how a calling domain reads back every instance it is running for one of its own entities; combined with `?status=`, how it reads back the live ones. The filter composes with the department/assignee scoping every list read already applies, so it widens nobody's visibility.

```json
{
  "items": [
    {
      "id": "b6e2b6f0-1c3e-4a2a-9c6d-2f0a7e9b5d11",
      "business_key": "TND-2026-04471",
      "workflow_version_id": "8fa88cde-824c-47bc-836b-665cd42c2222",
      "status": "RUNNING",
      "current_node_keys": ["review_finance"],
      "created_at": "2026-07-20T09:14:02.331Z"
    },
    {
      "id": "a3c7d1e2-9f45-4b6a-8c12-3e9b7d4f6a01",
      "business_key": "TND-2026-04512",
      "workflow_version_id": "8fa88cde-824c-47bc-836b-665cd42c2222",
      "status": "DEGRADED",
      "current_node_keys": ["review_legal", "review_finance"],
      "created_at": "2026-07-20T08:52:11.045Z"
    }
  ],
  "next_cursor": "eyJjcmVhdGVkX2F0IjoiMjAyNi0wNy0yMFQwODo1MjoxMS4wNDVaIiwiaWQiOiJhM2M3ZDFlMi05ZjQ1LTRiNmEtOGMxMi0zZTliN2Q0ZjZhMDEifQ=="
}
```

`GET /api/v1/instances/:id` returns the detail view, including `override_map` and `current_node_keys`:

```json
{
  "id": "b6e2b6f0-1c3e-4a2a-9c6d-2f0a7e9b5d11",
  "business_key": "TND-2026-04471",
  "workflow_version_id": "8fa88cde-824c-47bc-836b-665cd42c2222",
  "status": "RUNNING",
  "current_node_keys": ["review_finance"],
  "saved_node_keys": [],
  "override_map": {
    "review_finance": "c1a9e6b0-5f34-4e2a-9d11-8a2c4e7b9911"
  },
  "temporal_workflow_id": "7ca648b2-b432-4744-884c-35fd556a310c:TND-2026-04471",
  "record_version": 5,
  "created_at": "2026-07-20T09:14:02.331Z"
}
```

### 5.10 Error Code Catalogue

Execution's HTTP API uses its own `problemDetails` envelope (RFC 9457): `{type, title, status, detail, instance, code, invalid_params?}`, served as `application/problem+json`, with `type` URIs namespaced under `https://errors.bcbp.io/execution/<slug>`. This deliberately does not claim identity with Definition Service's own convention (`application/json; charset=utf-8`, `https://api.workflow.platform/errors/`) — the two services' envelopes share the same RFC 9457 shape and error-code granularity philosophy, not byte-identical wire details. `invalid_params` exists on the struct (reserved for validation errors naming more than one offending field/node, e.g. bulk node-eligibility failures) but is not yet populated by any handler — the bulk node-eligibility re-validation it's meant for isn't built yet (Appendix B). The catalogue assigns one code per distinct business rule, matching Definition Service's own granularity:

| Code | HTTP | Meaning |
| --- | --- | --- |
| `BAD_REQUEST` | 400 | Malformed request or JSON bind failure |
| `UNAUTHORIZED` | 401 | Missing or invalid identity |
| `FORBIDDEN` | 403 | Authenticated, not permitted |
| `NOT_ASSIGNEE` | 403 | Caller is not the task's active assignee/lead (complete/defer) |
| `NOT_AUTHORIZED_FOR_RESOURCE` | 403 | `GET /tasks/:id`/`GET /instances/:id` — caller is not an assignee, not in the current department, and not an admin (§9.2) |
| `TENANT_MISMATCH` | 403 | `WorkflowClient` family — `delegation_id` belongs to a different tenant than the `tenant_id` param |
| `INSTANCE_NOT_FOUND` | 404 | No such workflow instance |
| `TASK_NOT_FOUND` | 404 | No such task |
| `WAIT_NOT_FOUND` | 404 | `nodes/:node/complete` or `.../boundaries/:boundary/fire` — the key is not an outside wait of the instance's plan (§5.11) |
| `NOT_WAITING` | 409 | The instance is not waiting in that outside wait now (§5.11) |
| `TARGET_NODE_NOT_FOUND` | 409 | `force-forward`'s `target_node_key` does not exist in the compiled plan |
| `OVERRIDE_NO_OP` | 400 | `new_user_id` equals the current assignee, on node-override or task reassign |
| `OVERRIDE_MAP_INVALID` | 422 | `POST /instances`'s `override_map` references a node key not present in the compiled plan |
| `DUPLICATE_BUSINESS_KEY` | 409 | `(tenant_id, subject_id, business_key)` collision on instantiation with a live instance that is not the same start sent again (§5.2) |
| `TENANT_NOT_ACTIVE` | 409 | The tenant's recorded status is one `START_REFUSED_STATUSES` names (§5.5); none by default |
| `VERSION_NOT_PUBLISHED` | 409 | `POST /instances`'s `workflow_version_id` refers to a `DRAFT` or `ARCHIVED` version |
| `VERSION_INVALID` | 409 | `POST /instances`'s `workflow_version_id` has `is_valid=false` |
| `RECORD_VERSION_CONFLICT` | 409 | Stale `record_version` on claim/complete/defer/reassign/override |
| `INSTANCE_ALREADY_TERMINAL` | 409 | Lifecycle signal sent to a `COMPLETED`/`TERMINATED`/`FAILED` instance |
| `INVALID_INSTANCE_STATE` | 409 | Lifecycle signal not valid for the instance's current status, or the instance's workflow execution no longer exists to receive it |
| `TASK_ALREADY_CLAIMED` | 409 | Claim race — another assignee already established as lead |
| `CLAIM_NOT_APPLICABLE` | 409 | Claim attempted on a single-assignee task (claim exists only for `assignee_mode='all'`) |
| `INVALID_TASK_STATE` | 409 | Action not valid for the task's current status |
| `FORCE_BACK_NO_SAVED_BRANCH` | 409 | `force-back` with no saved sibling branch to restore |
| `NODE_ALREADY_RESOLVED` | 409 | Node-override race — the node has already progressed |
| `IDEMPOTENCY_KEY_REPLAY` | 409 | `Idempotency-Key` reused with a different request body |
| `ASSIGNEE_INELIGIBLE` | 422 | The replacement assignee fails `(department, level)` eligibility — at instantiation (§5.5), on a node-override target (§5.4), on plain `/reassign` (§5.10's pre-check pattern), or via `port.WorkflowClient.ReassignDelegate` (§5.8). Aligned with the upstream domain service's own equivalent code — one code covers every eligibility failure across the whole contract |
| `PAYLOAD_TOO_LARGE` | 413 | Body exceeds the 10 MB request-body cap, enforced against actual bytes read, not the client-supplied `Content-Length` |
| `UNSUPPORTED_MEDIA_TYPE` | 415 | Non-JSON body |
| `UPSTREAM_UNAVAILABLE` | 503 | Temporal Cluster or an internal-service call failed after retries |
| `EVENT_DECODE_FAILED` | 502 | Inbound event carries a schema-registry `SchemaID` but decoding failed (missing/misconfigured decoder, or a registry error) — retryable infra problem, not a malformed payload (§6.1, §6.8) |
| `INTERNAL_ERROR` | 500 | Unhandled error |

Every operation documents `400`/`401`/`404`/`500` as a floor, plus `403` wherever an authorization check exists, `409` wherever a conflict is possible, `413` wherever a request body exists, and `422` wherever an eligibility check exists.

**Synchronous delivery of signal-detected `409`s.** Two codes in the table above — `RECORD_VERSION_CONFLICT` and `INVALID_INSTANCE_STATE` — assert a conflict that the version/state-checking Activity detects only *after* a signal-forwarded request is accepted; under the Signal-Only pattern, a signal-forwarded endpoint returns `202` the instant the signal is durably accepted, before that Activity ever executes. Every signal-forwarded mutating endpoint (claim, complete, defer, reassign, the six admin lifecycle signals) adopts node-override's own pattern (§5.4): a synchronous HTTP-layer pre-check against the current `record_version`/instance-or-task-state immediately before forwarding the signal, returning `409` directly if it fails; the in-workflow Activity's own check stays in place as a defence-in-depth backstop for the (now narrow) race between the pre-check and the signal actually landing. Both layers use the same version-check logic, so they never disagree except on a genuine last-instant race, which resolves in the Activity's favor (the source of truth). A completion on an instance that is not `RUNNING` or `DEGRADED` is refused with `409 INVALID_INSTANCE_STATE`: the workflow drops a completion signal in any other status, so accepting it would acknowledge a completion that never happens, for example one made with a token issued before the tenant's instances were paused.

**Reassignment eligibility and liveness, checked uniformly.** `/reassign` (plain admin reassignment) and node-override's paired `instance-reassign` signal share one pre-check helper, run synchronously in the API/service layer immediately before the signal is forwarded (the same layer, and the same moment, as the `record_version`/state pre-check above): the replacement assignee's `(department, level)` eligibility is re-validated fail-closed (`422 ASSIGNEE_INELIGIBLE` on failure, nothing signaled), and their liveness is checked via the upstream IAM contract fail-open (matching every self-service task action's own liveness semantics, §5.6) — never the other way around, since fail-closed on a currently-unconfirmed upstream would block reassignment entirely. This closes what was, for a period, a real gap unique to plain reassignment: node-override's own HTTP-handler eligibility check (step 2 above) already gated its path before this shared pre-check existed, and now serves as an earlier-firing instance of the same rule rather than the only gate. `port.WorkflowClient.ReassignDelegate` (§5.8) applies the identical two checks to its own bulk delegate-to-delegate reassignment, so every path that changes a task's assignee — permanent (admin-driven) or temporary (delegation-driven) — re-validates the replacement the same way, through one of these two call sites (`ReassignAssignmentActivity`'s own DB write, by contrast, stays a pure vacate-and-insert with no eligibility logic of its own — Temporal Activities in this package deliberately have no access to the `EligibilityChecker`/compiled-plan machinery, §1.2 — so both checks run in the service layer, before the signal is ever sent, not inside the Activity).

`POST /api/v1/instances/:id/force-forward` — an admin-invoked jump beyond the compiled graph's explicit edges:

```json
{
  "target_node_key": "settlement",
  "record_version": 5
}
```

### 5.11 Outside Answers

An outside party, such as a vendor or authority drawn as an ignored pool, answers through the service that deals with it, which delivers the answer here. The published version names what can be answered: the nodes read lists the version's outside waits (definition LLD §3.3.12.2).

- `POST /api/v1/instances/:id/nodes/:node/complete` completes the receive task whose node key is `:node` (the key percent-encoded, as on node-override, §5.4).
- `POST /api/v1/instances/:id/nodes/:node/boundaries/:boundary/fire` fires the message boundary `:boundary` of the host `:node`: the host stage's node key, or the id of the subprocess or call it is attached to.
- **Body** (both): `{"variables": {...}}`, optional. The variables merge into the instance's variables (§2.1) before the step proceeds.
- **Caller**: a tenant admin, or a user with an active assignment on a task of the instance (`NOT_AUTHORIZED_FOR_RESOURCE` otherwise). Department membership alone does not allow it.
- **Checks**, in order: the instance exists in the tenant (`INSTANCE_NOT_FOUND`); the caller is an admin or an assignee (`NOT_AUTHORIZED_FOR_RESOURCE`); the key is an outside wait of the instance's plan, which is the `dsl.ExternalWaits` of its expanded main plan (`WAIT_NOT_FOUND`); the instance is RUNNING or DEGRADED (`INVALID_INSTANCE_STATE`); the `waiting-external` query lists the key (`NOT_WAITING`). Then the `instance-external-answer` signal is sent and the response is `202`.
- **Not waiting is refused, not kept.** An answer for a step the instance has not reached, or has left, is `NOT_WAITING`: the caller reads the instance and tries again once it waits. The interpreter drops a signal that finds nothing waiting, which covers the moment between the query and the signal.
- **Idempotency**: both accept `Idempotency-Key` (§5.9).

---

## 6. Events & Integration Topology

### 6.1 Inbound Ingestion — HTTP Push via the Shared Workflow-Events Consumer

Execution Service runs **no SQS consumer of its own**. The engine-wide shared Workflow-Events Consumer — the same thin service that already fronts Definition Service's inbound queues — subscribes Execution's inbound SNS topics and forwards each event envelope over HTTP under `/api/v1/internal/events`, exactly as it does for Definition Service.

- **Endpoint group.** `/api/v1/internal/events`, under the `/api/v1/internal` route group, guarded by `RequireInternalToken` (`x-internal-token` vs. `INTERNAL_API_TOKEN`, required outside dev). Not exposed via the gateway — no gateway identity headers reach it. Tenant identity comes from the envelope's `tenant_id`, injected into the RLS GUC by the handler.
- **Type-scoped subpaths, no catch-all.** Every event type this service consumes has its own dedicated subpath, same auth/envelope/retry contract: `/api/v1/internal/events/delegation` (`DelegationStarted`/`.ended`), `/api/v1/internal/events/user-profile` (`UserDeleted`/`UserAvailabilityChanged`), `/api/v1/internal/events/tenant` (`TenantStateChanged`), `/api/v1/internal/events/workflow-task` (`WorkflowTaskCreated`). A type with no subpath of its own cannot reach this service — adding one means wiring both this service's route and the shared consumer's routing table together.
- **Request body.** One `events.Envelope[json.RawMessage]` — the full envelope the shared consumer received off SQS, forwarded verbatim.
- **Retry contract.** `2xx` = handled (including an idempotent dedup no-op); `400`, `413` and `422` = malformed and non-retryable, sent by the shared consumer to that queue's DLQ after one delivery; every other status, including `401`, `403`, `404` and `5xx`, and a timeout is transient, retried with backoff until the consumer's receive threshold sends the message to the DLQ.
- **Event types outside the table below never arrive.** The shared consumer forwards an event to this service only when its type has a subpath here, so an event type added upstream cannot reach a route that does not exist.
- **Schema-registry-encoded envelopes.** An envelope carrying a non-empty `dataschema`/`SchemaID` is decoded before dispatch (see §6.8) — the IAM Delegation Service encodes its events through its schema registry, so `DelegationStarted` and `DelegationEnded` arrive this way and the decode runs on live traffic. A decode failure (missing decoder, malformed wire bytes, registry lookup error) responds `502`, matching this endpoint's own retry contract above (`5xx` = transient, retried) — it is an infra problem, never a malformed client payload, so it must not be routed to the DLQ the way a `400` is.
- **Metrics.** `internal_events_ingest_total` (CounterVec, `event_type, result` with `result ∈ {ok, bad_payload, decode_failed, error}`) — an unknown/ignored type still records `ok`. Auth rejections (`401`) never reach handler dispatch and are covered by standard HTTP-layer request metrics instead, not this metric.
- **Infrastructure ownership.** Per-queue plumbing (queue↔topic subscriptions, SNS filter policies, DLQs, 30s visibility timeout, `maxReceiveCount` 5, `RawMessageDelivery=true`) belongs to the shared consumer's infra config, not to Execution's application code.

**Alternative weighed.** `platform-events` ships a complete SQS consumer (receive loop, DLQ routing, visibility auto-extension, RLS-GUC + trace injection) that would cost Execution almost no code to adopt directly. The shared-consumer/HTTP-push architecture is kept anyway: one engine-wide ingestion pattern, one place to operate queue infrastructure, and the API process remains the only inbound surface. The library consumer is the recorded fallback if the shared consumer's own build stalls.

**Queue set:**

| SNS topic | Events Execution receives |
| --- | --- |
| `iam.membership.events` | `TenantStateChanged`, `MembershipRevoked`, `TenantMembershipsPurged` |
| `iam.user.events` | `UserDeleted`, `UserAvailabilityChanged` |
| `iam.delegation.events` | `DelegationStarted`, `DelegationEnded` |

Tenant lifecycle reaches Execution as a single `TenantStateChanged` relay on `iam.membership.events` — Execution takes no subscription on `iam.tenant.events` or `billing.events`; those raw lifecycle events stay with their producers, and Org & Membership relays its own resolved tenant projection instead. Confirmed directly against IAM's own Org & Membership AsyncAPI spec: `TenantStateChanged` publishes on `iam-membership-events`, delivered to `membership-workflow-q`, documented there as the §7.1 EVT-16 relay of `TenantSuspended`/`Cancelled`/`Offboarded` (sourced from Billing) and `TrialExpired` — not a placeholder attribution, a real, named event on a confirmed topic/queue.

**Two distinct delegation mechanisms, both legitimate.** `DelegationStarted`/`DelegationEnded` (event-driven, this queue) and the synchronous `port.WorkflowClient.ReassignDelegate`/`CancelByDelegate` calls (§5.8) are not redundant with each other. The synchronous calls fire when a caller (IAM's Org & Membership, or any other caller with the right internal-service credential) has already checked delegate-impact and decided to act — a request-driven flow with a definite caller and a definite answer. The event-driven path reacts to a delegation being created or ended wherever that decision actually originated inside IAM, independent of whether Execution's own synchronous endpoint was ever called — the reactive, always-on complement to the request-driven path. Both stay wired; neither supersedes the other.

**Self-consumption.** `WorkflowTaskCreated` is emitted by Execution itself and, for a connector/automation-typed task (`connector_type` set), consumed back by Execution too, on its own `/workflow-task` subpath above: `handleWorkflowTaskCreated` pushes the dispatch onto the Stream `cmd/connector-worker` reads. The shared Workflow-Events Consumer's routing table sends this event to both `workflow-stream-gateway` and Execution.

The shared Workflow-Events Consumer has no LLD of its own anywhere in the design corpus — its contract is defined only from Execution's and Definition's receiving side. Execution's half of the contract is complete here; the consumer service's own spec is future work, tracked as an open item.

### 6.2 Inbound Event Catalogue

`DelegationStarted`, as forwarded by the shared Workflow-Events Consumer to `POST /api/v1/internal/events`:

```json
{
  "id": "e9a71c40-8b3d-4f11-9c2e-5a6b7d8e9f01",
  "type": "DelegationStarted",
  "source": "org-membership-svc",
  "specversion": "1",
  "tenant_id": "7ca648b2-b432-4744-884c-35fd556a310c",
  "trace_id": "8f226j7911f78he0e7g363h424c8170",
  "time": "2026-07-20T08:45:00.500Z",
  "data": {
    "delegation_id": "e9a71c40-8b3d-4f11-9c2e-5a6b7d8e9f01",
    "delegator_id": "4da18bde-7244-47ac-986c-665cd42caaaa",
    "delegate_id": "c1a9e6b0-5f34-4e2a-9d11-8a2c4e7b9911",
    "scope": "department",
    "scope_id": "9b1cf3a0-2d4e-4b6a-8c11-3e9b7d4f6a02",
    "starts_at": "2026-07-20T08:45:00.000Z",
    "ends_at": "2026-07-27T08:45:00.000Z"
  }
}
```

IAM's `org_membership_lld_v12.md` documents `ip_address`/`user_agent` fields on every published event's envelope, including `DelegationStarted`/`DelegationEnded` (CronJob-sourced events get sentinel values, e.g. `"system"`/`"iam-org-membership/<job>-cron"`) — not shown in the example above, since this document's own handling doesn't read them; additive to an already-open envelope schema, no handler change implied. IAM's own envelope field is named `actor` (their internal summary table calls the same value `actor_id`) — this document has no `actor`/`actor_id` field in its own payload tables above because nothing here currently reads it; noted here only so a future reader reconciling field names against IAM's schema isn't left guessing.

| Event (wire) | Topic | Payload (required fields) | Handler behavior | Dedup consumer | Disposition |
| --- | --- | --- | --- | --- | --- |
| `DelegationStarted` | `iam.delegation.events` | `delegation_id, delegator_id, delegate_id, scope, starts_at` (+`scope_id?`, `ends_at?`) | Scope-filtered (`all`/`department`/business-key) reroute of the delegator's active assignments to the delegate, batched eligibility re-check, tagged `reason="delegation:<delegation_id>"` (§5) | `membership-execution` | Acted on; ≤4s p99 SLO (§6.7) |
| `DelegationEnded` | `iam.delegation.events` | `delegation_id, delegator_id, delegate_id, ended_reason` | Reverses every assignment tagged `delegation:<delegation_id>`, batched eligibility re-check on the restored assignee before reinstating (mirrors the forward direction; ineligible rows stay held and flagged), restoring the original assignee. Unknown `ended_reason` values are logged and treated as a generic end | `membership-execution` | Acted on |
| `UserDeleted` | `iam.user.events` | `user_id, deleted_at` | Vacates every active assignment for the user, tenant-wide, no scope filter — the safety net for lifecycle changes that bypass synchronous reconciliation. Scoped per-assignment, not instance-wide: a multi-assignee task's instance keeps running if a co-assignee remains active on it. | `user-execution` | Acted on (safety net; `UserLifecycleBypass` pages SEV-2) |
| `UserAvailabilityChanged` | `iam.user.events` | `user_id, status` (+`ooo_from?`, `ooo_until?`, `delegate_user_id?`) | `status=ooo` with no active delegation → pauses the user's instances (`initiator=ooo`); `status=available` → resumes with `initiator=ooo`, lifting only OOO pauses; a later `DelegationStarted` reroutes and resumes them. Never a reroute driver itself — `delegate_user_id` is informational only. `changed_at`-recency-guarded (`<=` skip), the same protection `TenantStateChanged` already has | `user-execution` | Acted on |
| `TenantStateChanged` | `iam.membership.events` | `tenant_id, status, previous_status, plan, previous_plan, changed_at, cause` | Dispatch on the resolved transition: `offboarded` → terminate every non-terminal instance; `trial`, `active` and `past_due` run instances and `cancelled`, `suspended` and `trial_expired` pause them: every move into a running status resumes with `initiator=tenant_state`, which lifts only the pauses this handler itself applied and changes nothing on an instance that is running, so a skipped or reordered intermediate event cannot strand a paused instance; a move to a pausing status pauses every `RUNNING` instance (`initiator=tenant_state`); `plan != previous_plan` → `active_task_queues` upsert on an upgrade and stop-routing on a downgrade. `changed_at`-recency-guarded (`<=` skip, offboard exempt) | `membership-execution` | Acted on |
| `MembershipRevoked` | `iam.membership.events` | `user_id` | A user removed from a tenant can no longer act in it, so every active assignment they hold is vacated — the same effect `UserDeleted` has, through the same safety-net reconciler | `membership-execution` | Acted on |
| `TenantMembershipsPurged` | `iam.membership.events` | `tenant_id` | IAM Core's tenant-offboarding cascade signal. Drives `TenantStateChanged`'s own `offboarded` branch — terminate every non-terminal instance — rather than a second cascade of its own | `membership-execution` | Acted on |
| `TenderAssigneeOverridden` | `iam.membership.events` | — | **Not consumed.** Per IAM's authoritative topology, this event's only consumers are Audit and Notification — the Temporal reassignment is signalled synchronously by the node-override endpoint (§3), never via the bus | — | Routed to no target |
| `DepartmentMembershipRevoked` | `iam.membership.events` | — | **Not consumed.** Definition Service's own internal-events handler already calls Execution's pause-tasks endpoint synchronously as part of template invalidation; a second handler here would double-pause the same user for no added safety | — | Routed to no target |
| `WorkflowTaskCreated` (self) | `POST /api/v1/internal/events/workflow-task` (§6.1) | `workflow_instance_id, task_id, node_key, department_id, connector_type?, resolved_inputs, output_mapping` | Connector/automation-typed tasks only (`connector_type` set): pushes the connector dispatch onto the Stream `cmd/connector-worker` reads. Non-connector tasks: recognized, no-op | — | Acted on |
| anything else on a subpath | any | — | Accepted and ignored, logged and counted, `200` | (recorded) | Forward-compatible no-op |

Each queue's subscription carries the filter policy that the producing service's AsyncAPI defines: `membership-workflow-q` delivers `TenderAssigneeOverridden`, `DepartmentMembershipGranted`, `DepartmentMembershipRevoked`, `DepartmentMembershipLevelChanged` and `TenantStateChanged`; `user-wf-q` delivers `UserAvailabilityChanged` and `UserDeleted`; `delegation-workflow-q` delivers the four delegation types. "Not consumed" above therefore means the shared Workflow-Events Consumer routes a delivered type to no target. `MembershipRevoked` and `TenantMembershipsPurged` are consumed here and must be added to the membership queue's filter (Workflow Events Consumer LLD Appendix B, item 1).

**A pause is held, not applied, while `DEGRADED`.** The stored status stays `DEGRADED` — there is no status for a degraded and paused instance, and the park keeps showing its failed branches. An `instance-pause` accepted in `DEGRADED` (a tenant suspension, an out-of-office pause or an administrator's) is recorded in the workflow's own state with its initiator and administrator; the first pause owns it, as in `PAUSED`, and a second is dropped and logged. A pause already in force when a branch fails is carried into the park. An `instance-resume` clears a recorded pause by the initiator rule below and is otherwise dropped and logged. A pause recorded during the park writes nothing and emits no event until the park ends; a pause carried into the park still writes the `DEGRADED` status on entry. The park then ends in `PAUSED`, with the `paused` event naming the held initiator (emitted again for a carried pause), when a pause is held, and in `RUNNING` otherwise. Administrator force-forward, force-back and cancel act on the park as before, and task completions of a respawned branch are accepted during it, since the stored status is `DEGRADED`, and the stage that follows the park creates its task but its completion is refused until the instance is resumed, as for any paused instance; an instance with no step after the park completes (§3).

**Resume is initiator-scoped, enforced at signal validation.** The workflow function holds the initiator that paused it in its own interpreter state, set when `instance-pause` is accepted and cleared when a resume is. An `instance-resume` whose `initiator` does not match is rejected there and logged, so a tenant reactivation cannot lift an OOO pause or a deliberate admin pause, and an OOO return cannot lift a tenant-state pause. `admin` is the one initiator that lifts any pause. The state lives on the workflow's own goroutine rather than in a column because a column is written asynchronously and would be read stale by exactly the sweeps that need it — the same staleness item 4.2 removes.

**Handler algorithms**, all reusing the bulk direct-DB-write-then-signal-loop transaction shape (the documented Signal-Only exception, §5):

1. **Delegation reroute (`DelegationStarted`).**
   1. **Establish** the starting set via `ListActiveByUser(tenant_id, delegator_id)`.
   2. **Scope-filter** to `all` / `department` (`workflow_task.department_id`) / business-key (`workflow_instance.business_key`).
   3. **Re-check eligibility** per surviving row, batched by distinct `(department_id, required_level)` pair — one IAM call per distinct pair, not per row, so a delegate with hundreds of assignments across a handful of pairs costs a handful of round-trips. Ineligible rows stay held and are flagged.
   4. **Commit** one bulk transaction that vacates the delegator's assignments and inserts delegate rows tagged `reason="delegation:<delegation_id>"`, writing `TASK_REASSIGNED` events + outbox pairs. The ≤4s SLO clock stops here.
   5. **Signal** each affected instance in a loop.
   6. **Record** dedup (`RecordIfNew`).
2. **Delegation reversal (`DelegationEnded`).**
   1. **Find** every active assignment tagged `reason="delegation:<delegation_id>"` — the tag alone identifies the full starting set, no scope re-filter needed.
   2. **Re-check eligibility** on the original assignee to be restored, batched by distinct `(department_id, required_level)` pair — the same mechanism the forward reroute uses (step 1.3). An original assignee who was deleted or lost department membership during the delegation window stays ineligible; those rows stay with the delegate and are flagged, mirroring the forward direction's partial-success shape rather than blindly restoring.
   3. **Commit** one bulk transaction restoring the eligible original assignees, writing `TASK_REASSIGNED` events + outbox pairs.
   4. **Signal** each affected instance in a loop.
   5. **Record** dedup.
3. **User-deleted pause.**
   1. **Establish** the starting set via `ListActiveByUser(tenant_id, user_id)`, tenant-wide, no scope filter.
   2. **Vacate** the deleted user's assignment row on each affected task — not a whole-instance pause. Where an affected task has no other active assignee left, the instance's own state naturally reflects that (the task has no one to progress it); where a co-assignee remains active, the instance keeps running untouched.
   3. **Record** dedup.
4. **Tenant lifecycle (`TenantStateChanged`).**
   1. **Recency-guard**: skip if `changed_at` is `<=` the tenant's last-applied value (tie resolves to skip); a `status=offboarded` transition is terminal and never skipped.
   2. **Dispatch** on the resolved transition — terminate / resume / pause (`initiator=tenant_state`) / queue registry upsert — a `plan` change and a `status` change on the same event apply together, each its own bulk transaction + signal loop. Every sub-transaction selects **every non-terminal instance in one query** (`RUNNING`, `PAUSED`, `DEGRADED`) and signals unconditionally. Selecting by the status the transition expects would make the row's own status a precondition, and that row is written asynchronously by the workflow's own activity: an instance whose row has not caught up is not skipped, it is never selected, so no signal is sent and none ever will be. Splitting the sweep by status has the same failure in a different shape — an instance moving between two of the queried statuses mid-sweep falls through both. `signalPreconditions` (§3), which runs on the workflow's own goroutine and is never stale, is the only gate. On `offboarded`, each non-terminal instance has its workflow execution closed, then is marked terminated in its own transaction with every open task failed, however many tasks it has. Once every instance is terminated the tenant is recorded for the history purge (§4.13). In every sweep, pause and resume as well as terminate, a failure on one instance does not stop the rest; after the sweep any failure fails the event, so the event is redelivered and re-drives the instances still not in the target state. A signal to an execution that no longer exists is not a failure. Every internal event handler runs to completion within its own ten-minute bound even when the sender has already given up on the request, so the Event Consumer's forward timeout never cuts a sweep short; a redelivery that finds the work done is acknowledged.
   3. **Record** dedup, then update the tenant's last-applied `changed_at` recency value once, after every sub-transaction the event carries (both `plan` and `status`, when both present) has committed successfully — not per-transaction. A retry after a partial failure (one half committed, the other didn't) must still see the recency check pass and re-drive the failed half; updating eagerly after only one half would risk the retry wrongly skipping the other.
5. **OOO pause/resume (`UserAvailabilityChanged`).** **Recency-guard**: skip if the envelope `time` is `<=` the user's last-applied `UserAvailabilityChanged` time (tie resolves to skip) — the same `<=`-skip protection `TenantStateChanged` already has, guarding against an out-of-order `ooo`/`available` pair for the same user. On `status=ooo` with no active delegation: **establish** the affected set (no scope filter), **pause** each (`initiator=ooo`), **record** dedup. On `status=available`: **resume** with `initiator=ooo`, which lifts an OOO pause and leaves any other pause standing, **record** dedup. This handler never reassigns — reassignment happens only when the paired `DelegationStarted` arrives, whose reroute also resumes the OOO-paused instances.

### 6.3 Consumer Idempotency and Dedup

- **Table.** `processed_event (event_id uuid, consumer text, event_type text, processed_at)`, PK `(event_id, consumer)` — service-authored migration, no RLS.
- **Consumer strings**, one per logical subscription: `membership-execution` (delegation pair + `TenantStateChanged`), `user-execution`. One event delivered to two subscriptions dedupes independently per string.
- **Record-after-side-effects.** Dedup is recorded as the handler's *last* step, not its first, matching Definition Service's own shipped convention. A transient side-effect failure returns `5xx` without recording, so redelivery re-drives the whole handler; every side effect is individually idempotent (`ON CONFLICT DO NOTHING`, pre-generated IDs, state-checked signal loops), so a partial redelivery converges. At-least-once plus idempotent handlers — not atomicity — is the correctness guarantee.
- **No separate on-entry check for correctness.** The dedup check *is* the final `RecordIfNew` call. Handlers with expensive side effects (the tenant-wide bulk loops) may additionally do a cheap up-front read as a pure optimization to skip obvious replays early, but this never substitutes for the final record step.
- **Idempotent-across-retries is not the same guarantee as safe-under-concurrency.** Sequential re-runs after a prior delivery's committed `RecordIfNew` are safe by the above. Two deliveries of the same event dispatched *concurrently* — both past any optional early-exit check, neither yet recorded — are a real possibility under at-least-once delivery with no ordering guarantee, and dedup alone does not resolve that window. The actual backstop is each handler's own DB-level uniqueness constraint on the row(s) it writes (`workflow_task_assignment`'s `UNIQUE(task_id,user_id) WHERE is_active`; `active_task_queues`' `UNIQUE(queue_name)`) — the losing concurrent writer hits a constraint violation, surfaces as a transient `5xx`, and its own redelivery then converges normally against dedup.
- **TTL prune.** The retention sweep (once a day and at start, in the API process) deletes `processed_event` rows older than 7 days, and calls the outbox's own `PrunePublished` at the 7-year audit floor (§4.11).

### 6.4 Outbound Event Catalogue

Envelope shape: `events.NewEnvelope(type, "workflow-execution-svc", payload, WithTenantID, WithSchemaVersion("1"), WithSubject("instances/{instanceID}[/tasks/{taskID}]"), WithActor(actorUserID when user-initiated), WithTraceID when a valid span exists)`. `WithCorrelationID` is available but unused — nothing in this design chains an outbound event back to a specific inbound one (a `TASK_REASSIGNED` triggered by `DelegationStarted` already carries `delegation_id` in its own payload).

**Dashboard Stream Gateway payload rule.** The gateway fans out by user — every task-scoped event payload carries the affected `user_id`(s), and instance-scoped events carry `started_by_user_id` plus active assignees where cheap to include. Raw UUIDs only; the Gateway owns any enrichment. Common payload core, all events: `{workflow_instance_id, subject_id, business_key, workflow_version_id}`; task-scoped events add `{task_id, node_key, department_id, assignee_user_ids[]}`.

| Wire type | DB `event_type` | Trigger | Payload beyond the common core | Consumers (illustrative) |
| --- | --- | --- | --- | --- |
| `WorkflowInstanceStarted` | `WORKFLOW_STARTED` | Instance start | `started_by_user_id` | Audit |
| `WorkflowInstancePaused` | `INSTANCE_PAUSED` | admin pause / `TenantStateChanged` loop / OOO loop | `started_by_user_id`, `initiator` (`admin`\|`tenant_state`\|`safety_net`\|`ooo`), `actor_user_id?` | Audit, Notif, Dashboard |
| `WorkflowInstanceResumed` | `INSTANCE_RESUMED` | admin resume / `TenantStateChanged`(→active) loop / OOO-return loop / DEGRADED park-loop exit | `started_by_user_id`, `initiator` (`admin`\|`tenant_state`\|`safety_net`\|`ooo`\|`degraded_recovery`), `actor_user_id?` | Audit, Notif, Dashboard |
| `WorkflowInstanceCancelled` | `INSTANCE_CANCELLED` | admin cancel | `started_by_user_id`, `actor_user_id`, `reason?` | Audit, Notif, Dashboard |
| `WorkflowInstanceTerminated` | `INSTANCE_TERMINATED` | admin terminate / tenant-offboard loop | `started_by_user_id`, `initiator` (`admin`\|`tenant_state`), `actor_user_id?` | Audit, Notif, Dashboard |
| `WorkflowInstanceDegraded` | `INSTANCE_DEGRADED` | parallel aggregation, ≥1 failed branch | `failed_branches[]{department_id, last_node_key}` | Audit, Ops, Notif, Dashboard |
| `WorkflowInstanceFailed` | `INSTANCE_FAILED` | main-path non-retryable failure | `error_class` | Audit, Ops, Notif, Dashboard |
| `WorkflowInstanceFinished` | `WORKFLOW_FINISHED` | clean completion | `started_by_user_id`, `completed_at` | Tender post-hoc finalization, Audit, Notif, Dashboard |
| `WorkflowTaskCreated` | `TASK_CREATED` | task creation | `due_at?`, `follow_up_at?`, `stage_type`, `connector_type?` | Notif, LLM pre-warm, Dashboard, `cmd/connector-worker` (connector-typed tasks only, `workflow_connectors.md` §5.4) |
| `WorkflowTaskClaimed` | `TASK_CLAIMED` | `task-claim`, multi-assignee only | `claimed_by_user_id` | Audit, Dashboard |
| `WorkflowTaskCompleted` | `TASK_COMPLETED` | assignment completion, `allDone` | `completed_by_user_id` | Dashboard, Audit |
| `WorkflowTaskDeferred` | `TASK_DEFERRED` | task deferral | `deferred_to_node_key`, `reason?`, `due_at?` | Notif, Dashboard |
| `WorkflowTaskReassigned` | `TASK_REASSIGNED` | reassign / node-override / delegation reroute | `old_user_id`, `new_user_id`, `initiator` (`admin`\|`override`\|`delegation`), `delegation_id?` | Audit, Notif, Dashboard |
| `WorkflowTaskSuperseded` | `TASK_SUPERSEDED` | force-forward bypass | `actor_user_id` | Audit, Notif, Dashboard |
| `WorkflowTaskFailed` | `TASK_FAILED` | cancel/terminate/instance-fail cascades; a stage that fails (a connector task failed by its connector) | `cascade_source`; `error_class` on a stage failure | Audit, Notif, Dashboard |
| `WorkflowInstanceForceRouted` | `FORCE_ROUTED` | force-route recorded | `actor_user_id`, `from_node_keys[]`, `to_node_key`, `direction` (`forward`\|`back`) | Audit |
| `WorkflowTaskSlaWarning` | `TASK_SLA_WARNING` | SLA follow-up fires | `follow_up_at` | Notif, Dashboard |
| `WorkflowTaskSlaBreached` | `TASK_SLA_BREACHED` | SLA due fires | `due_at` | Notif, Audit, Dashboard |

**No `WorkflowTaskMessageSent`/`TASK_MESSAGE_SENT` event.** `send_task` dispatch fires no dedicated audit event of its own — not meaningful enough on its own to justify a dedicated event/schema entry; `send_task` dispatch is still fully traceable via `workflow_task`'s own row and the surrounding instance-timeline events.

Every row is a single `outbox.Enqueue` call — the audit record and the published envelope are the same row, built from the same in-memory struct (§4.5, §4.10). `UserAvailabilityChanged`'s OOO handling emits no dedicated event of its own; it rides the existing `paused`/`resumed` events through the pause/resume it triggers.

`WorkflowTaskCompleted` **is** emitted for every task completion, matching the unconditional `outbox.Enqueue` rule and feeding the Dashboard Stream Gateway's optimistic-UI reconciliation. `WorkflowInstanceFinished` counts Notification among its consumers, closing the loop for the platform's "workflow completed" notification. On the Tender queue specifically, `WorkflowInstanceFinished` is a post-hoc finalization notification, never the signing trigger — signing happens synchronously before Complete.

**Why Dashboard also consumes `WorkflowInstanceDegraded`:** an admin-interventions-required state is real, live-facing information for the affected tenant, not purely an ops concern — so the Dashboard Stream Gateway carries it alongside Audit/Ops/Notification rather than treating `DEGRADED` as an internal-only signal.

Naming: wire `type` strings, the `EventType` SNS message attribute, and `outbox_events.event_type` are all **PascalCase** (`WorkflowTaskCreated`). The DB column stores the wire type verbatim — `platform-events`' own `Enqueue` applies no case transform — so there is no separate audit representation; the column and the wire value are the same string.

Four representative envelopes, following the shape above:

`WorkflowInstanceStarted`:

```json
{
  "id": "3f9a1c20-6b4d-4e8a-9c11-7d2e5b8a4f01",
  "type": "WorkflowInstanceStarted",
  "source": "workflow-execution-svc",
  "specversion": "1",
  "tenant_id": "7ca648b2-b432-4744-884c-35fd556a310c",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "time": "2026-07-20T09:14:02.331Z",
  "data": {
    "workflow_instance_id": "b6e2b6f0-1c3e-4a2a-9c6d-2f0a7e9b5d11",
    "business_key": "TND-2026-04471",
    "workflow_version_id": "8fa88cde-824c-47bc-836b-665cd42c2222",
    "started_by_user_id": "9e21c8a0-6b45-4a10-8e3f-2d6c1a9b4400"
  }
}
```

`WorkflowInstanceDegraded` — common core plus the failed-branch set that triggered the parallel-aggregation degrade:

```json
{
  "id": "8c2e4f10-9a3b-4d6e-8f01-1c5a7b9d2e33",
  "type": "WorkflowInstanceDegraded",
  "source": "workflow-execution-svc",
  "specversion": "1",
  "tenant_id": "7ca648b2-b432-4744-884c-35fd556a310c",
  "trace_id": "5cf03g4688c45eb7b4d030e1f1f5847",
  "time": "2026-07-20T09:20:15.774Z",
  "data": {
    "workflow_instance_id": "a3c7d1e2-9f45-4b6a-8c12-3e9b7d4f6a01",
    "business_key": "TND-2026-04512",
    "workflow_version_id": "8fa88cde-824c-47bc-836b-665cd42c2222",
    "failed_branches": [
      {
        "department_id": "9b1cf3a0-2d4e-4b6a-8c11-3e9b7d4f6a02",
        "last_node_key": "review_legal"
      }
    ]
  }
}
```

`WorkflowTaskCompleted` — common core plus the task-scoped additions plus `completed_by_user_id`:

```json
{
  "id": "1a5b7c90-3e2d-4f8a-9c01-6d4e8f0a2b15",
  "type": "WorkflowTaskCompleted",
  "source": "workflow-execution-svc",
  "specversion": "1",
  "tenant_id": "7ca648b2-b432-4744-884c-35fd556a310c",
  "trace_id": "6d004h5799d56fc8c5e141f202a6958",
  "time": "2026-07-20T09:31:47.902Z",
  "data": {
    "workflow_instance_id": "b6e2b6f0-1c3e-4a2a-9c6d-2f0a7e9b5d11",
    "business_key": "TND-2026-04471",
    "workflow_version_id": "8fa88cde-824c-47bc-836b-665cd42c2222",
    "task_id": "d4f2a911-7c3b-4e8a-9b21-6f5a8e0c3d77",
    "node_key": "review_finance",
    "department_id": "9b1cf3a0-2d4e-4b6a-8c11-3e9b7d4f6a02",
    "assignee_user_ids": ["c1a9e6b0-5f34-4e2a-9d11-8a2c4e7b9911"],
    "completed_by_user_id": "c1a9e6b0-5f34-4e2a-9d11-8a2c4e7b9911"
  }
}
```

`WorkflowInstanceFinished` — common core plus `started_by_user_id` and `completed_at`. This is the event Tender Service consumes purely as a post-hoc finalization notification, never the signing trigger:

```json
{
  "id": "7e9f1a30-4c6d-4b8e-9f01-2a5c7d9e4b16",
  "type": "WorkflowInstanceFinished",
  "source": "workflow-execution-svc",
  "specversion": "1",
  "tenant_id": "7ca648b2-b432-4744-884c-35fd556a310c",
  "trace_id": "7e115i6800e67gd9d6f252g313b7069",
  "time": "2026-07-20T10:02:33.118Z",
  "data": {
    "workflow_instance_id": "b6e2b6f0-1c3e-4a2a-9c6d-2f0a7e9b5d11",
    "business_key": "TND-2026-04471",
    "workflow_version_id": "8fa88cde-824c-47bc-836b-665cd42c2222",
    "started_by_user_id": "0d4d4e8e-6b1a-4f2a-9c6d-2f0a7e9b5d99",
    "completed_at": "2026-07-20T10:02:33.100Z"
  }
}
```

### 6.5 Decoupled Service Integrations

Downstream consumers subscribe to the shared SNS topic (`wf-workflow-events`) using per-consumer SQS fanout queues:

1. **Audit Log** — queue: `wf-workflow-audit-q`
   - Subscribes to the full lifecycle and task surface (`WorkflowInstanceStarted`, `.paused`, `.resumed`, `.cancelled`, `.terminated`, `.degraded`, `.failed`, `.finished`, `WorkflowTaskCreated`, `.claimed`, `.completed`, `.deferred`, `.reassigned`, `.superseded`, `.failed`, `WorkflowInstanceForceRouted`, `.sla-warning`, `.sla-breached` — every one of the 18 outbound events). On receipt, the Audit indexer extracts event metadata into its own audit store, treating each event as an immutable record of what happened and who acted (`actor_user_id`/`initiator` fields), independent of whatever business content lives in `result_json` on the originating task row.
2. **Notification Service** — queue: `wf-workflow-notif-q`
   - Subscribes to `WorkflowInstancePaused`, `.resumed`, `.cancelled`, `.terminated`, `.degraded`, `.failed`, `.finished`, `WorkflowTaskCreated`, `.deferred`, `.reassigned`, `.superseded`, `.failed`, `.sla-warning`, `.sla-breached`. On receipt, dispatches in-app/email notifications to the affected `user_id`(s) or the tenant's admin team, depending on whether the event is task-scoped or instance-scoped.
3. **Tender Service** — queue: not provisioned; subscription owned by the Tender HLD
   - Subscribes only to `WorkflowInstanceFinished`, and only as a post-hoc finalization notification — the actual approval signing already happened synchronously before Complete was called (§1.3), so this queue never gates or triggers signing.
4. **LLM Service** — queue: `wf-llm-context-q`
   - Subscribes to `WorkflowTaskCreated`, using it purely as a pre-warm signal for `prep`/`review`/`approve` stages. LLM assistance during `prep` drafting itself runs entirely over the Frontend ↔ LLM Service ↔ User Profile path, outside Execution's own call chain.
5. **Dashboard Stream Gateway** — queue: `wf-workflow-dash-q`
   - Subscribes to all 9 task-scoped events (`WorkflowTaskCreated`, `.claimed`, `.completed`, `.deferred`, `.reassigned`, `.superseded`, `.failed`, `.sla-warning`, `.sla-breached`) plus 7 of the 9 instance-lifecycle events (`.paused`, `.resumed`, `.cancelled`, `.terminated`, `.degraded`, `.failed`, `.finished` — excluding `.started`, whose creating client already knows synchronously via the instantiation response, and the purely ops-facing `.force-routed`). On receipt, fans the event out per user over Server-Sent Events to the browser — every task-scoped payload already carries `assignee_user_ids[]` for exactly this reason, and `WorkflowTaskCompleted`'s presence in this set specifically drives the Gateway's optimistic-UI reconciliation. The Gateway owns all UUID-to-display-name enrichment itself; Execution's payloads carry raw IDs only. The Gateway's own subscription mechanism, SSE connection auth, wire format, and backfill design are owned by that service.

### 6.6 Outbox Runner and Publisher Bootstrap

Mirrors Definition Service's shipped configuration, adopted unchanged unless noted:

- `outbox.NewRunner(outbox.Config{Pool, Publisher, Logger, PollInterval, BatchSize (default 50)})` — `PollInterval` is Execution's own app-configurable setting (`OUTBOX_POLL_INTERVAL`, 500ms by default — not a platform-library default; see Appendix A.5 #15). `MaxAttempts` is the platform library's own hardcoded default (5), not app-configurable; exhaustion moves the record to `outbox_dead_letters`, operable via the runner's reprocess/list/discard operations.
- `events.NewSNSPublisher(events.SNSConfig{TopicARN, Region, EndpointURL})` — topic `wf-workflow-events`; `AWS_USE_STUB=true` default for dev. SNS message attributes (`EventType`/`TenantID`/`Source`/`EventID`/`Subject`) are what downstream SQS filter policies key on.
- **Envelope size guard.** `outbox.Enqueue` rejects payloads over 240KB — events reference rows by ID, they never embed result bodies.
- **Lifecycle.** The runner starts/stops in the API process's bootstrap exactly as Definition's does (start after pools, stop after HTTP drain, before tracing flush). The Worker process runs **no** outbox relay — Activities only insert; the API process's relay publishes (the library's lease-claiming makes adding redundant relays later safe if ops ever wants that).
- **Metrics.** `events.Init(serviceName, buildVersion)` registers the library's `events_*`/`outbox_*` families on the existing `/metrics` endpoint.

### 6.7 The `DelegationStarted` ≤4s p99 SLO

Org & Membership's SLO-2 commits a 5s p99 end-to-end budget for `DelegationStarted` → reroute, split: publish half ≤1s (owned upstream), **consume+reroute half ≤4s p99 — owned by Execution Service**.

- **Clock.** Envelope `time` → the reroute transaction's commit (`workflow_task_assignment` rows swapped). This includes shared-consumer forwarding latency — the budget covers everything downstream of the upstream publish.
- **Mechanism.** No new machinery: the handler is one bulk transaction plus a signal loop; the budget bounds the reroute *commit*, not the trailing signal fan-out. A delegate with hundreds of active assignments still fits inside the budget because the eligibility re-check batches by distinct `(department_id, required_level)` pair rather than checking per row — this is the one variable inside the timed window most likely to blow it under real load, which is exactly why the batching decision is called out explicitly rather than left implicit.
- **Measured.** Histogram `delegation_reroute_duration_seconds` (envelope-time → commit), alerting on p99 > 4s. This is a timeliness SLO — a breach pages, it never fails the event.

### 6.8 Event-Schema Governance

Adopts Definition Service's shipped pattern wholesale:

- The AsyncAPI 3.0 event contract: one channel (`wf/workflow/events`, SNS `wf-workflow-events`), one message per §6.4's wire type, `x-lifecycle`/`x-owner` annotations, the envelope documented as a documentation-only schema. Inbound events are documented as `receive` operations for completeness only — Execution does not register their schemas; the producing services own those contracts.
- Governed payload schemas: one flat, self-contained `<Name>Payload` schema per event — only `Payload`-suffixed schemas are extracted, since `platform-schemagov` does not dereference `$ref` and would otherwise pull in envelope/`allOf` composition. Extracted to `internal/eventschema/*.json` (Draft-07, snake_case filename = Glue `SchemaName`). Payloads stay flat and **never** `additionalProperties: false` — schemagov's open-schema guard rejects a closed schema outright.
- **CI.** The `platform-schemagov` container workflow, copied from Definition Service: PR = validate+diff (read-only, breaking-change gate); main = validate+diff+register to the staging Glue registry; release = production. **The diff's baseline is the live, currently-registered Glue schema version, not the file's prior git-committed state** — this catches drift even if a registry change happened out-of-band or a CI run was skipped, which a git-history-only diff would miss. Breaking changes (field removed, type changed, enum narrowed, bound tightened, newly-required) fail the diff. `GlueCodec` (real, runtime) resolves each event type's latest registered Glue schema version, failing closed if the schema is missing from the registry, and prepends the AWS Glue Schema Registry's binary wire-format header (magic byte + compression byte + 16-byte schema-version UUID) to the payload — the same framing Definition Service's and IAM's own `GlueCodec` produce, no repo-specific deviation remains.
  - **Wire-format framing.** `events.WithCodec`, a `Publisher`-level hook, applies `Codec.Encode` immediately before SNS publish — after the plain-JSON envelope is already durably written to the outbox — and base64-wraps the result back into `data` as a JSON string, with the resolved schema-version UUID recorded on the envelope's `dataschema`/`SchemaID` field. `GlueCodec` implements `events.Codec` directly; `buildEnvelope` itself touches no codec, only a `port.EventValidator` that checks the payload against its JSON Schema before it ever reaches the outbox.
  - **Inbound side.** `HandleInternalEvent` (§6.1) reverses this via `port.EventDecoder`, for any received envelope whose `dataschema`/`SchemaID` is non-empty. No producer sets `SchemaID` on events reaching Execution today, so this path is exercised only by tests, not live traffic.
- **Schema naming is casing-agnostic, and unaffected by §6.4's PascalCase wire types.** `platform-schemagov` derives a schema's Glue name from the JSON filename stem verbatim (`scan_event_types` returns `p.stem`), and its AsyncAPI↔schema coverage check normalizes to the same snake_case key from either input: an SNS `event_type` attribute value with `.`→`_`, or — when no such attribute value is declared, which is this service's case — a CamelCase→snake_case fold of the message `name`. Both paths yield `workflow_task_created` for `WorkflowTaskCreated`. The 18 `internal/eventschema/*.json` filenames, the registered Glue schema names, and the CI diff baseline therefore all stay exactly as they are across the rename; no registry migration is involved. The one place that must change is `GlueCodec`'s own `registrySchemaName`, which today derives the stem by replacing `.` with `_` — under PascalCase it must instead apply the same CamelCase→snake_case fold `platform-schemagov` uses, rather than an explicit per-event mapping table (Appendix B).
  - `events.WithCodec` and a real `EventDecoder` are not yet wired into any `cmd/server` composition root: no `cmd/server` code yet constructs an `events.NewSNSPublisher`/`outbox.Runner`, so there is no call site to attach either hook to. `NoopCodec` (dev/test) passes payloads through unchanged in the meantime.
- **Versioning.** Additive change → new Glue version UUID, same `type`. Breaking change → bump `type` to `<name>.v2`, add a new message, mark the old one deprecated, dual-publish for **30 days**, then retire — a provisional number picked to cover a typical consumer-team deploy cadence with margin, not measured against a real breaking change yet; revisit at the first one.
- A `.v2` cutover is not a correctness risk for rows already enqueued under `.v1`: each `outbox_events` row stores its own fully-serialized envelope, with whichever schema version was current baked in at build time, not at publish time — a `.v1` row publishes as `.v1` regardless of what's since been registered for `.v2`.

### 6.9 Synchronous Contract-Drift Safety Net

The CI gate above protects only the *outbound* event schema. Two synchronous contracts this service depends on just as heavily have their own safety net:

- **The compiled-plan read (Definition Service internal API, §1.3/§3.1, §5.3).** Load-bearing on every instantiation and every in-workflow re-fetch. Its OpenAPI document is the authority: the Definition Service's CI refuses a breaking change to it with `make contract-breaking` (oasdiff against `main`) — the same class of gate `schema-gov diff` provides for event schemas — and this service's CI refuses a stale copy with `make contract-check`. `test/contract` holds every request the client sends to the copy.
- **The IAM eligibility-check contract** (`(department_id, required_level) → eligible?`, called from §5.5's bulk re-validation and node-override's own check, §5.4). Not specified in a document either side holds, so not mechanically checkable the way the compiled-plan read is — it is Org & Membership's own HTTP contract, owned and versioned by that team. This is documented as a **named-owner stability commitment** instead of new cross-team tooling: Org & Membership owns this contract's backward compatibility; a breaking change on their side is a cross-team coordination event, not something Execution's own CI can catch. Tracked as an explicit dependency in §9 (Security) alongside the other cross-service trust assumptions already documented there.

---

## 7. Testing & Infrastructure Strategy

### 7.1 Test-Tier Architecture

Execution Service has one deployable unit split into **two processes** — the API process and the Temporal Worker process, which never call each other directly — plus the shared `workflow_execution` Postgres schema. The test layout extends Definition Service's proven four-tier structure with two new tiers for the workflow-function/activity layer that has no analogue there:

| Directory | Content | What it proves | What it explicitly does not | Command |
| --- | --- | --- | --- | --- |
| `internal/**/*_test.go` | White-box unit tests | Unexported logic correctness | Cross-package integration | `make test` |
| `test/unit/<pkg>/` | Black-box unit tests (exported API only), incl. hand-rolled fakes at the handler layer, no mockgen | Exported API contracts, handler routing/DTO mapping | Real DB/Temporal/queue behavior | `make test` |
| `test/workflow/` | Temporal workflow-function and activity unit tests via `testsuite.WorkflowTestSuite` | Workflow-function dispatch, boundary/timer/signal handling, DEGRADED/force-back state machine — all via simulated clock, mocked activities | Real multi-worker timing, real signal delivery ordering (the SDK's own job) | `make test` (fast, no real Temporal server) |
| `test/integration/postgres/` | DB integration tests (testcontainers, real Postgres) | Real transactional/RLS/cascade/idempotent-retry behavior | Temporal semantics | `make test-integration` |
| `test/integration/temporal/` | Worker registered against a real namespace | Real Worker registration, real DEGRADED→respawn→RUNNING cycle, real claim races | Cross-service (Definition/IAM) behavior | `make test-integration` |
| `test/e2e/` | Full API-process + Worker-process, real Temporal dev server | Full HTTP→Temporal→Postgres→outbox-row pipeline, inbound-event dedup, the delegation SLO with a real number | The broker hop itself (§7.2), exhaustive edge cases (pushed down to lower tiers) | `make test-e2e` |
| `test/compat/` | The Definition Service's real binary, this repository's connector worker and the Workflow-Events Consumer's binary, built from checkouts, against this service's real API, worker activities, Postgres, Valkey and Temporal, with LocalStack SNS and SQS; both services driven over their public APIs | That the builds work together: every standalone Definition fixture publishes and runs, a library-module call runs through the module, a connector stage runs through the relayed `WorkflowTaskCreated`, the worker and Definition's alias, the archive guard and the revocation pause cross over, IAM events published through platform-events reach both services through the consumer (department and tenant revocation, tenant suspension), each side accepts only the other's configured token, and a version that cannot start is refused with its documented code | IAM eligibility (faked until the route exists) and a schema-registry framed payload (no Glue registry in LocalStack) | `make test-compat`; CI runs it in both repositories against the other's branch |
| `test/fixtures/` | Shared testcontainers helpers: `NewTestPool`, `NewTestValkey`, `NewTestTemporal` | — | — | — |

`go.temporal.io/sdk/testsuite` is the standard Temporal Go SDK testing framework: `NewTestWorkflowEnvironment()` for workflow-function tests, `NewTestActivityEnvironment()` for activity tests, both running in-process with a simulated clock — no real Temporal server, no network. **This is new engineering practice for the organization** — Definition Service has no Temporal dependency, and no sibling repo tests a Temporal workflow, so there is no internal precedent to mirror; the tier is designed directly against the SDK's own testing framework instead.

**Why the org-first tier exists, and what it stops short of.** The DSL/Temporal design (`runSteps`, boundary events, force-back, `DEGRADED`, SLA timers, §2/§3) is the highest-value, least-precedented logic in the whole service, and it needs exercising at unit-test speed — a real Temporal server per test run would make the suite too slow to run on every change. `env.SetStartTime` plus the environment's simulated clock let SLA timers fire deterministically without real wall-clock waits (`env.RegisterDelayedCallback` schedules a signal at a simulated future point and the environment's time-skipping jumps straight to it). `env.SignalWorkflow`/`env.QueryWorkflow` drive every admin/task signal and the `get-workflow-status` query, including the `DEGRADED` park state's saved-branch fields, against a running test environment. Every Activity is mocked at this tier — activity bodies (record-version bumps, cascade fixes, idempotent retries) get real assertions separately, against a real testcontainers Postgres pool.

**`workflow.GetVersion` patch-safety is a distinct mechanism from the environment tests above.** An ordinary `WorkflowTestSuite` run exercises only the *current* code against a fresh history; it does not by itself prove a patched workflow still replays a *pre-patch* execution's recorded history correctly. That guarantee comes from `worker.ReplayWorkflowHistory`/`ReplayWorkflowHistoryFromJSONFile`: capture a real (or hand-built) history from before the patch, replay it against the patched code, and the SDK's replayer fails loudly on any non-deterministic divergence. A small library of recorded pre-patch histories, one per `GetVersion` call site, exists specifically for this check. Regenerating that library after an unrelated structural code change is a maintenance-process gap, not a technical one — no CI trigger enforces it; ownership defaults to whoever adds a new `GetVersion` call site also recording its corresponding fixture.

**What this tier deliberately does not test:** real multi-worker task-queue registration/routing (needs a real namespace, §7.2), real signal-delivery-ordering/buffering (inherent SDK mechanics, not this service's job to re-test), cross-process API-signals/Worker-executes timing (`test/e2e`'s job).

**Race detector**: `-race -count=1` on both `make test` and `make test-integration`, unchanged from Definition Service.

### 7.2 Integration and End-to-End Testing

**`NewTestTemporal` fixture.** A `testcontainers`-started Temporal dev-server image, bundling the frontend/history/matching services with an in-memory or SQLite-backed persistence layer — no separate Cassandra/Postgres/Elasticsearch stack needed. Mirrors the shape of the existing Postgres and Valkey fixtures: image pinned, health-waited on the frontend gRPC port, torn down per test package. Advanced Visibility (Elasticsearch-backed Search Attributes) is **not** stood up here — Search-Attribute-dependent assertions belong in `test/e2e` against a fuller stack, or are asserted via the Postgres projection instead.

**`test/integration/temporal/` — a real namespace.** Registers a real `worker.Worker` and asserts the mechanics unit tests structurally cannot reach:

1. **Multi-queue dynamic registration** — seed `active_task_queues`, start the registration loop, assert the Worker actually polls the seeded queue in addition to the default queue; remove the row, assert polling stops after the next cycle.
2. **Per-queue concurrency isolation** — assert a burst of default-queue tasks doesn't starve a concurrently-running tenant-isolated-queue task, and vice versa.
3. **`task-claim` lead/non-lead rejection** — a real multi-assignee task, two claim signals, assert the second is rejected against a live workflow execution.
4. **`DEGRADED`/`FAILED` transitions end-to-end** — a dedicated test: a real `Parallel` step with one branch's activity forced to fail non-retryably, asserting the instance actually reaches `DEGRADED` in a live execution, siblings keep running, and a subsequent real force-back signal respawns the failed branch and returns the instance to `RUNNING`. This earns its own dedicated integration test — not just a workflow-environment unit test — specifically because the respawn-a-new-goroutine mechanism interacts with real Temporal history/determinism in ways a mocked environment can gloss over.
5. **A combined `DEGRADED` × `TenantStateChanged` test, alongside the test above, not a separate mechanism.** Park an instance in `DEGRADED`, then deliver a `TenantStateChanged{status=suspended}` envelope for that tenant via `POST /api/v1/internal/events` against the same real-namespace fixture. The mechanism under test: the tenant-suspend handler's `instance-pause` signal is recorded, not rejected — the stored status stays `DEGRADED` and nothing is written for the pause — and when a force-back respawn then completes the failed branch the instance leaves the park `PAUSED` with initiator `tenant_state`, not `RUNNING` and not completed. A `TenantStateChanged{status=active}` delivered before the park ends instead leaves the instance `RUNNING`. This closes the one combination the design and the standalone `DEGRADED` test each individually cover but never jointly exercise — the two riskiest new mechanisms in the whole service, tested together.

**`test/e2e/` — full stack.** Real Postgres + Valkey + `NewTestTemporal`, both the API process and a real Worker process started as goroutines, driving the system through its actual HTTP surface:

- **Instance lifecycle**: instantiate → real `StartWorkflow` → poll status until `RUNNING` → complete a task → assert progression, and that each transition's outbox row is written in the same transaction as the state change it describes.
- **Delegation-reroute SLO**: deliver a `DelegationStarted` envelope and assert the reroute lands within the 4s budget — the one test that gives the SLO a real number instead of an aspirational one. The eligibility-check dependency must be **real or latency-representative, not mocked-instant** — a test double returning instantly wouldn't exercise the one variable most likely to actually blow the SLO, so the fixture stubs the Org & Membership eligibility endpoint with a configurable, non-zero per-distinct-pair latency.
- **Duplicate inbound delivery**: redeliver one envelope and assert the side effect happens once, against the real `processed_event` table. SNS delivers at-least-once, so every inbound handler can be invoked twice with the same envelope; this is where the real repository and the real handler meet, which the unit tier (fake repository) and the repository integration tier (no handler above it) each miss on their own.

**The broker hop is out of scope here, deliberately.** No test in this service asserts `outbox row → relay → SNS → subscribed queue`. Subtract the parts this service does not own and nothing of its own is left under assertion: the claim/lease/publish loop belongs to `platform-events` and is tested there, SNS-to-SQS delivery is covered against real LocalStack by the shared Workflow-Events Consumer that owns the consume side, and the outbox row itself is asserted above. The three risks that *are* this service's — the relay pool must bypass RLS (§4: `app_tenant_id()` returning NULL means the relay sees zero rows forever, with no error), the Glue codec's encoding, and the composition root's own wiring — are each invisible to such a test, since the test pool connects as a superuser, the codec is stubbed against a registry LocalStack does not run, and a test harness builds its own publisher rather than `cmd/server`'s. Coverage for the first of those is a relay-pool RLS test connecting as a non-superuser role, tracked in Appendix B.

DLQ poison-message handling and per-queue fan-out belong to the shared Workflow-Events Consumer's own suite for the same reason.

### 7.3 Coverage Gate

Adopts Definition Service's mechanism and global floor unchanged — **95% global**, merged via the max-count-per-block strategy across unit and integration coverage profiles, enforced by the same awk-based gate script.

**Per-package floors, re-derived for this service's actual packages** (Definition Service's own floors name its own specific packages and don't transfer verbatim — copying the numbers without re-deriving which packages they apply to would silently exempt the wrong code):

| Package | Floor | Rationale |
| --- | --- | --- |
| `internal/workflow` (workflow-function + activities) | 90% | High-value logic, but Temporal SDK internals and real multi-worker timing are integration-tier, not this floor's job. |
| `internal/adapter/outbound/postgres` (excl. sqlc-gen) | 90% | Matches the DB-adapter-layer convention. |
| `internal/core/service` | 95%+ (global floor applies) | Pure business logic, no excuse for a lower bar. |
| `internal/adapter/inbound/http` (excl. asyncapi/swagger renderers) | 90% | Handler logic; static renderers excluded. |

Excluded from the coverage denominator entirely: sqlc-gen `postgres/db`, mocks, any Glue-dependent adapter needing live AWS, the no-op logger, asyncapi/swagger static renderers.

### 7.4 CI Pipeline Shape

Mirrors Definition Service's CI DAG shape — `generate` fans out to parallel branches (`validate-quality`, `validate-test`, `lint-dockerfile`, `build-image-cache`); `trivy-cve-scan` and `smoke-tests` each depend on a subset of those and run in parallel with each other, not chained; `push-image-ghcr` (push-only) depends on all of them — extended for this service's two-process, Temporal-dependent nature:

1. **`generate`** — sqlc (queries) + mockgen, generated artifacts uploaded once and downloaded by every dependent job.
2. **`validate-test`** — `make test` (unit, including the workflow-environment tier, fast, no containers) → `make test-integration` (testcontainers Postgres/Valkey **and** the Temporal dev-server fixture) → merge-coverage → per-package + global gate. A Temporal dev-server image pull is added and cached the same way the other container images already are.
3. **`smoke-tests`** — extends the real-services-as-CI-containers pattern (healthchecked Postgres + Valkey) with a Temporal dev-server container alongside; a wait script backgrounds both `cmd/server` (API, port 8080) and `cmd/worker` (Worker, port 8081), polls both readiness endpoints up to the same budget, then runs an HTTP-driven smoke script: instantiate, claim, complete, verify the instance reaches `COMPLETED` and its transitions' outbox rows are written. Each built image is additionally run with its `version` subcommand (§7.5), which needs no environment at all.
4. **`build-image-cache` / `push-image-ghcr`** — builds **three images** from the same multi-stage Dockerfile (one binary target each, §7.5) — `workflow-execution-api`, `workflow-execution-worker` and `workflow-execution-connector-worker` — each scanned and signed independently.
5. **Deploy gate** — extends to a two-Helm-release (or one chart, two Deployments) upgrade, `--wait --atomic` on both, with the same digest-verification + Prometheus-rollback-gate pattern applied **per process**, since the API and Worker can each independently regress. Each build/push pair is an independent job; a partial push (one image published, one not) blocks the deploy gate entirely, since either Deployment's atomic upgrade would otherwise reference a nonexistent tag.
6. **Schema-governance and changelog workflows** — copied wholesale from Definition Service, pointed at this service's own AsyncAPI/schema paths, triggered independently by their own path filters.

### 7.5 Container / Deploy Topology

**Three deployable binaries from one Dockerfile** (multi-stage, sharing one builder stage; a separate final `FROM` block per binary so each image's layer history is independently cacheable and CI can build/scan/push them as separate artifacts, not repeated runs of the same target differentiated by an entrypoint override):

- `cmd/server` → the API process, HTTP `:8080`.
- `cmd/worker` → the Temporal Worker process, no business HTTP surface, but a minimal `:8081` for `/healthz`/`/readyz`/`/metrics` — k8s probes and Prometheus scraping need something to hit even though the Worker serves no business traffic.
- `cmd/connector-worker` → the connector dispatch process (`workflow_connectors.md` §6), a Valkey Stream consumer with no inbound business surface and the same minimal health/metrics port. It reaches the workflow only through this service's own HTTP completion endpoints, never the Temporal SDK. It runs the shared connector library at `workflow-connectors` `v2.0.0` and wires all of it. Every call carries its own tenant and department. Document references live in the platform's S3 document bucket with their metadata in Valkey, the Drive document registry and send-email duplicate protection live in PostgreSQL under the tenant policy, and startup stops when the document bucket, the Valkey persistence settings or the database is unusable. A failed call is retried only when the library's retry decision allows it (a transient error the connector type's policy permits), with a bounded number of attempts, exponential backoff and jitter; every decision is logged and counted by connector type, provider, error class and whether it was retried. Provider clients are dropped on the alias refresh period so a rotated tenant credential is picked up, and the Drive audit rows are pruned daily past their retention.

The three processes share one module and one repository because they share a domain. Promoting one to a repository and release train of its own stays available, and load testing decides it. The connector runtime is the likeliest candidate: it is the only one with no Temporal dependency, and it carries its own external-call profile.

Every binary answers a `version` subcommand, handled before any configuration is read, which prints its build stamp and exits. That is what CI's per-image check runs: it proves the entrypoint resolves and the binary loads in its runtime stage without standing up a database, a Temporal cluster, or a cache, which every real entrypoint requires.

**Dev stack.** The existing Postgres/Valkey/LocalStack/PgBouncer compose services, plus a Temporal dev-server service (with its own bundled Web UI for local debugging) and one app service per binary rather than one overall. `make dev-api`/`make dev-worker` targets let a developer run only the process they need against the shared stack; `make dev` runs the set.

**Helm chart.** Three `Deployment` templates sharing one chart, plus the `HTTPRoute` and Envoy Gateway `SecurityPolicy` the API process is fronted by:

- The **API Deployment** mirrors Definition Service's exactly: rolling update `maxSurge:0` (given hard pod anti-affinity), distroless nonroot security context, liveness/readiness probes, and its own `PodDisruptionBudget` with `minAvailable:1`.
- The **Worker Deployment** gets the identical PDB treatment for the same reason; its readiness check additionally verifies Temporal frontend connectivity, and its `HorizontalPodAutoscaler` scales on a **Temporal-specific metric** (queue depth / task backlog) rather than CPU, since Worker load is bursty per-task-queue rather than steady HTTP QPS.
- The **connector-worker Deployment** takes the same PDB and security context. It exposes no inbound Service port beyond health and metrics — it polls a Valkey Stream and calls out over HTTP — so it carries no `HTTPRoute` of its own.
- **The API and Worker Deployments default to `replicas: 2` — a stated precondition, not an arbitrary choice.** `minAvailable:1` at `replicas:1` would let the PDB block every voluntary disruption including the rollout itself (`maxSurge:0` requires the old pod gone before the new one starts, but terminating it would drop availability to zero, which the PDB refuses). Environments intentionally running at `replicas:1` accept that rollouts there need the PDB deleted manually first — a documented values override, not a silent trap.
- One shared migration Helm hook runs domain + outbox migrations before either Deployment rolls; a failed hook blocks the upgrade from reaching the rollout at all — it does not auto-rollback a partially-applied migration, since Helm's hook-failure semantics and a Deployment rollout failure are distinct classes of failure.
- **Egress**: the Worker needs egress to the Temporal Cluster frontend (plus Postgres/Valkey); the API needs egress to the Temporal frontend too (signal/start/query calls) plus HTTP egress to the Definition Service's internal API, which the Worker needs too (`GetCompiledPlanActivity`).
- **Ingress restriction (§9.2).** A required ingress `NetworkPolicy` restricts `/api/v1/internal/*` at the network layer to the specific pod identities allowed to call it — the shared Workflow-Events Consumer and Definition Service. Defense-in-depth alongside `x-internal-token`, not a replacement for it; infra implements and maintains the policy, the Helm chart's own responsibility is otherwise unchanged.
- **mTLS/namespace bootstrap.** Registering the Temporal namespace, per-environment isolation, and mTLS for the Temporal connection are cluster-operator actions the Helm chart doesn't perform — it consumes an already-provisioned namespace name via `values.yaml`, the same way it consumes an already-provisioned Postgres DSN. mTLS certs are consumed via a Kubernetes `Secret` volume mount, the same shape already used for other credentials.
- **Version skew is accepted, not accidental.** Independent per-process health gates mean a rollback triggered by one process's gate can leave the other on the newer version — API vN+1 running briefly against Worker vN or vice versa. `workflow.GetVersion` patching exists specifically so the Worker's workflow-function code tolerates old-vs-new mid-flight, and the API/Worker contract is Temporal signals/queries plus the shared Postgres schema — the same "no direct RPC, version-tolerant by construction" surface already established elsewhere, not a new coupling.
- **Shared PgBouncer budget during overlapping rollbacks.** Up to four pod generations (API vN+1/vN, Worker vN+1/vN) could theoretically hold connections simultaneously during an overlap window, briefly exceeding a naive sizing calculation. Not re-designed: PgBouncer's own connection queueing absorbs a short-lived over-budget window, and the overlap is bounded by each rollback's own health-gate observation window — worth accounting for when sizing `MAX_CLIENT_CONN` in practice.

### 7.6 Operational Conventions

**Metrics.** Every collector is built through `platform-gincommon`, which injects the constant labels `domain` (`workflow`), `service` (`workflow-execution-service`) and `environment` (`APP_ENV`) and rejects a name or label outside the platform standard. The three binaries share one identity. Metrics fall in three tiers: platform-shared `platform_*` series that the libraries emit (`platform_http_*`, `platform_db_*`, `platform_outbox_*`, `platform_messages_*`, `platform_build_info`, `platform_library_info`), workflow domain-shared `workflow_*` series (none defined yet), and the service-specific `workflow_execution_service_*` series below. No metric carries `tenant_id` or any other identifier as a label.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `workflow_execution_service_record_version_conflict_total` | CounterVec | `table` (`workflow_instance`\|`workflow_task`) | An optimistic-lock conflict (§4) |
| `workflow_execution_service_internal_events_ingest_total` | CounterVec | `event_type, result` | One ingest call by outcome (§6.1) |
| `workflow_execution_service_delegation_scope_unhandled_total` | CounterVec | `scope` | A delegation arrived with a scope this service has no case for. It matches no assignment; the counter makes an addition to Org & Membership's scope enum visible |
| `workflow_execution_service_delegation_reroute_duration_seconds` | Histogram | — | §6.7's SLO |
| `workflow_execution_service_connector_task_failures_total` | CounterVec | `connector_type`, `provider`, `class`, `retried` | A failed connector call, by the library's error class (`transient`, `permanent`, `unknown`) and whether the retry decision allowed another attempt; a rising `unknown` rate is the signal to alert on |
| `workflow_execution_service_connector_dispatch_dead_lettered_total` | CounterVec | `connector_type` | A connector stream entry passed its delivery cap and moved to the dead-letter stream; its task stays open (`workflow_connectors.md` §6.5 step 0) |
| `workflow_execution_service_task_signal_duration_seconds` | Histogram | `operation` (`claim`\|`complete`\|`defer`) | Request received to `202` returned, successful requests only; §10.3's task-signal SLO |
| `workflow_execution_service_instance_start_duration_seconds` | Histogram | — | Request received to `201` returned; §10.3's instantiation SLO |
| `workflow_execution_service_workflow_activity_duration_seconds` | Histogram | `activity_name, outcome` (`success`\|`error`) | Per-Activity latency, recorded by a worker interceptor |
| `workflow_execution_service_instance_degraded_total` | Counter | — | An instance entered `DEGRADED`; counted once per transition, never for a retried attempt |
| `workflow_execution_service_instance_degraded_current` | Gauge | — | Instances currently `DEGRADED`, sampled every `OPERATIONAL_STATS_INTERVAL` |
| `workflow_execution_service_instance_degraded_oldest_age_seconds` | Gauge | — | Age of the longest-parked `DEGRADED` instance |
| `workflow_execution_service_oldest_ready_task_age_seconds` | Gauge | — | Age of the oldest `READY` task, independent of whether an SLA timer is armed on it |
| `workflow_execution_service_worker_active_queues` | Gauge | — | Worker process: the live count of dynamically registered workers |
| `workflow_execution_service_worker_queue_last_poll_timestamp` | Gauge | `queue` | Per-queue polling health, refreshed on every topology poll for every queue the process serves; the aggregate count cannot see one stalled queue |
| `workflow_execution_service_instances_by_queue` | Gauge | `queue` | Non-terminal instances (`RUNNING`, `PAUSED`, `DEGRADED`) per task queue, sampled through the relay role by the API process; a queue with none is absent |
| `workflow_execution_service_internal_events_last_received_timestamp` | Gauge | `event_type` | Set on every successful dispatch of an internal event; the signal for a shared consumer outage (§10.2) |
| `workflow_execution_service_rls_violations_total` | CounterVec | `violation_type` (`missing_or_invalid_guc`\|`cross_tenant_access`) | Sampled violations read from `rls_violation_log` (§9.2) |
| `workflow_execution_service_workflow_replay_failures_total` | CounterVec | `workflow_type` | A workflow function panicked, which includes non-determinism raised while replaying workflow code |
| `workflow_execution_service_sla_breaches_total` | Counter | — | A `WorkflowTaskSlaBreached` event recorded |
| `workflow_execution_service_upstream_dependency_errors_total` | CounterVec | `dependency` (`definition_service`\|`iam`) | A request answered `503` because an upstream failed, separating a compiled-plan or eligibility outage from the service's own `5xx` rate |
| `workflow_execution_service_wf_cache_hits_total` / `..._wf_cache_misses_total` | Counter | — | The compiled-plan cache's hits and misses |

The API server reads `rls_violation_log` and the three gauges through the outbox relay role: a background loop counts rows added since start every `RLS_VIOLATION_EXPORT_INTERVAL`, deletes rows older than `RLS_VIOLATION_RETENTION`, and samples the gauges every `OPERATIONAL_STATS_INTERVAL`. Each replica reports the same values, so alerts aggregate gauges with `max` and counters with `increase`.

The outbox runner reports `platform_outbox_pending_events` and `platform_outbox_oldest_pending_age`, and moves exhausted events to dead-letter storage counted in `platform_dlq_messages_total{operation="outbox_publish"}`. `platform-pgcommon` reports `platform_db_queries_total`, `platform_db_query_duration_seconds`, the pool series, `platform_db_slow_queries_total` and the transaction retry counters for both pools. `docs/observability/metric-registry.md` lists the series from a real scrape.

**Histogram buckets.** The three SLO-backed histograms use custom boundaries tuned to their own p99 target from §10.3, rather than Prometheus's stock exponential defaults, for finer `histogram_quantile` resolution near the threshold each one's alert rule actually checks: `workflow_execution_service_task_signal_duration_seconds` — `.01, .025, .05, .1, .15, .2, .25, .3, .4, .5, .75, 1, 2.5` (target ≤ 0.3s); `workflow_execution_service_instance_start_duration_seconds` — `.05, .1, .25, .5, .75, 1, 1.5, 2, 3, 5, 10` (target ≤ 1s); `workflow_execution_service_delegation_reroute_duration_seconds` — `.25, .5, 1, 2, 3, 4, 5, 6, 8, 10, 15, 20` (target ≤ 4s). `workflow_execution_service_workflow_activity_duration_seconds` has no stated SLO and uses Prometheus's stock `DefBuckets`.

**Alerts & SLOs** — one consolidated "which signal pages whom" catalogue:

| Signal | Threshold / condition | Response |
| --- | --- | --- |
| Instance entered `DEGRADED` | any increase in `workflow_execution_service_instance_degraded_total` | Page — a parallel-branch failure parked an instance needing admin force-forward/force-back resolution |
| `UserDeleted` safety-net bypass | any `UserLifecycleBypass` log occurrence | SEV-2 — a deleted user still held an active assignment the safety-net handler had to vacate |
| Outbound event dead-lettered | any increase in `platform_dlq_messages_total{operation="outbox_publish"}` | Page — an event failed all publish retries; operable via the runner's DLQ replay |
| Delegation reroute SLO | p99 > 4s over the rolling window | Alert — investigate the eligibility-check batch cost |
| Deploy health gate (API) | 5xx rate over the post-deploy window | Auto-rollback the API Deployment; the Worker's equivalent gate is Activity-failure-rate, not HTTP 5xx |
| Inbound-queue DLQ depth | `workflow_execution_service_internal_events_ingest_total{result="error"}` sustained non-zero over the rolling window | Alert — mirrors the outbound dead-letter pattern; the underlying per-queue DLQ is infra-owned by the shared consumer, but Execution's own ingest-result metric (§6.1) is what actually pages, without waiting on the consumer's own infra to add a symmetric alert |
| Shared Workflow-Events Consumer outage | `time() - workflow_execution_service_internal_events_last_received_timestamp{event_type=~"DelegationStarted\|TenantStateChanged"} > 900` | Page — a live event type going quiet for 15 minutes is a consumer-side stall, not a legitimately idle tenant |
| RLS violation | any increase in `workflow_execution_service_rls_violations_total{violation_type=~"missing_or_invalid_guc\|cross_tenant_access"}` | Page — a tenant-isolation boundary failure, same severity Definition Service's own `RLSMissingGUC`/`RLSCrossTenantAccess` alerts carry |
| Task-signal SLO breach | `histogram_quantile(0.99, sum by (le) (rate(workflow_execution_service_task_signal_duration_seconds_bucket[5m]))) > 0.3` sustained 5m | Alert on a sustained breach of the task-signal SLO |
| Instantiation SLO breach | `histogram_quantile(0.99, sum by (le) (rate(workflow_execution_service_instance_start_duration_seconds_bucket[5m]))) > 1.0` sustained 5m | Alert on a sustained breach of the instantiation SLO |
| Workflow replay failure | any increase in `workflow_execution_service_workflow_replay_failures_total{workflow_type}` | Page — Temporal non-determinism is fatal to the affected workflow execution, the highest-severity Worker failure mode |
| `DEGRADED` backlog stale | `workflow_execution_service_instance_degraded_oldest_age_seconds > 86400` | Alert — `workflow_execution_service_instance_degraded_total` only fires once on entry; this catches an instance nobody has actioned in over a day |
| Worker queue polling stalled | `time() - workflow_execution_service_worker_queue_last_poll_timestamp{queue} > 300` | Page — a Worker silently failing to poll one tenant-isolated queue is invisible to the aggregate `worker_active_queues` gauge alone |
| Instances on an unpolled queue | `max by (queue) (instances_by_queue) > 0` unless the queue's `worker_queue_last_poll_timestamp` is within the last 300 seconds, for 5 minutes | Page — a Worker that could not start a queue's worker only logs, so its instances wait with no task and no other signal |
| Per-activity-type failure rate | `rate(workflow_execution_service_workflow_activity_duration_seconds_count{outcome="error"}[5m])` grouped `by (activity_name)` sustained above a per-activity baseline | Alert — the existing deploy-gate only looks at the aggregate Activity-failure rate, which can mask one consistently-failing activity type inside an otherwise-healthy blend |
| SLA-breach volume spike | `rate(workflow_execution_service_sla_breaches_total[1h])` materially above trailing baseline | Alert — a trend/volume view the per-instance `WorkflowTaskSlaBreached` domain event alone can't provide |
| Stuck/abandoned task | `workflow_execution_service_oldest_ready_task_age_seconds` above a stated threshold | Alert — catches a `READY` task nobody has claimed, independent of whether that task even has an SLA timer armed |
| Upstream dependency errors | sustained non-zero `rate(workflow_execution_service_upstream_dependency_errors_total{dependency}[5m])` | Alert — separates a compiled-plan-read/IAM-eligibility outage from Execution's own generic `5xx` rate, so on-call isn't left guessing which upstream is failing |
| Compiled-plan cache miss ratio | `workflow_execution_service_wf_cache_misses_total / (workflow_execution_service_wf_cache_hits_total + workflow_execution_service_wf_cache_misses_total) > 0.10` sustained | Alert — Execution's own equivalent of a cache-health alert Definition Service already ships |
| Outbox relay wedged | `platform_outbox_oldest_pending_age` above a stated threshold (e.g. 10 minutes, well above the `5s` poll cadence) | Page — a relay that stops running entirely produces zero attempts and zero dead-letters, so nothing else in this table would catch it |

No burn-rate/error-budget alert is defined for the 99.9% Availability SLO — a real burn-rate policy (fast-burn vs. slow-burn windows) is a genuine design decision; tracked as an Appendix B open item.

Distributed tracing (via the existing OTel initializer, run in both processes' `main()`) instruments each hop of the signal-only chain — HTTP handler → Temporal signal → Activity → outbox enqueue → SNS publish — but no consolidated end-to-end trace map exists; each hop's propagation is documented separately. **The HTTP-handler → Temporal-signal → Activity hop specifically requires the Temporal Go SDK's own OTel interceptor** (`go.temporal.io/sdk/contrib/opentelemetry`) — Temporal does not automatically propagate W3C trace context across `SignalWorkflow`/`StartWorkflow` → Activity boundaries without it. Registered via `client.Options.Interceptors` on the API process's Temporal client and `worker.Options.Interceptors` on the Worker; both processes must register it for the trace to actually continue across this boundary, since either side omitting it breaks the chain. The remaining hops (outbox `trace_id` column → SNS `MessageAttributes` → downstream consumer) are a manually-propagated correlation ID, not automatic span continuation — a downstream consumer must deliberately parse and re-attach `trace_id` from the envelope to keep the trace connected past that point.

**Graceful shutdown.** API process: unchanged order (HTTP shutdown → outbox relay stop → pool drain → cache close → tracer shutdown), 30s SIGTERM budget. Worker process: `worker.Worker.Stop()` for every dynamically-registered queue (draining in-flight Activity executions) → pool drain → tracer shutdown; no outbox relay to drain, since the single relay lives in the API process. **`WorkerStopTimeout: 25s`** — 5s of margin under the 30s SIGTERM budget for the drain sequence's own remaining steps (pool close, tracer shutdown) to complete after the Temporal SDK's own drain window elapses, without Kubernetes SIGKILLing the process mid-cleanup. A provisional number, not yet tuned against real Activity-duration data. Connector worker process: stops reading its stream, then waits for every started run to record and report its outcome (`workflow_connectors.md` §6.5 step 0), for up to the longest connector timeout plus the callback timeout plus 10s; its termination grace period is 90s.

**Health/readiness.** API: unchanged base semantics (`/healthz` always `200`; `/readyz` checks DB pool + Valkey) plus a Temporal-frontend reachability check folded into readiness. Worker: `/healthz` always `200`; `/readyz` checks DB pool + Temporal-frontend connectivity (no Valkey dependency, since nothing in this design requires the Worker to read the shared cache).

**Env-var / config inventory.** One `internal/config` package, same fail-fast-at-boot shape as Definition Service. `Load` parses the environment and checks value ranges; each binary then checks the settings it uses — the API its database, relay DSN, SNS, Glue, eligibility, Token Service and token settings, the Worker its database, Temporal and Definition Service settings, the connector-worker its Definition Service, token, stream and OpenBao settings, and `migrate` its database DSN alone — so no binary's environment carries another's settings.

| Var | Process | Purpose | Default / required |
| --- | --- | --- | --- |
| `BUILD_VERSION` | both | Stamped into `/healthz` payload + metrics, the deploy/rollback gate | injected at build |
| `OTEL_SERVICE_NAME` | all three | Service name on every span and the `service` metric label, identical for the three binaries | `workflow-execution-service` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | all three | OTLP/gRPC collector address; `none` or unset disables export while trace and request ids keep working | unset = export disabled |
| `DATABASE_URL` | API, Worker, connector-worker, `migrate` | Postgres via the pooling library (RLS GUC-injected); the connector-worker keeps the Drive document registry and the send-email intents there | required |
| `CONNECTOR_DOCS_BUCKET` | connector-worker | The platform S3 bucket holding document-reference content | required outside dev; empty in dev disables document references; the worker's grant on the prefix covers get, put and delete (startup writes and deletes a probe object) |
| `CONNECTOR_RETRY_MAX_ATTEMPTS` / `CONNECTOR_RETRY_INITIAL_BACKOFF` / `CONNECTOR_RETRY_MAX_BACKOFF` | connector-worker | The attempt limit and backoff bounds of an allowed retry | `5` / `500ms` / `10s` |
| `CONNECTOR_AUDIT_RETENTION` | connector-worker | How long Drive upload audit rows are kept before the daily prune | `2160h` |
| `MAX_CLIENT_CONN` | both (PgBouncer) | Shared PgBouncer client-conn budget split across both Deployments | `10` per process per replica (provisional, §9.7) |
| `TEMPORAL_HOST_PORT` | both | Temporal frontend address | required |
| `TEMPORAL_NAMESPACE` | both | Per-environment namespace, provisioned externally | required |
| `TEMPORAL_TLS_CERT_PATH` / `TEMPORAL_TLS_KEY_PATH` | both | mTLS to the Temporal frontend if the cluster requires it | required iff mTLS |
| `VALKEY_URL` | API | Compiled-plan/template cache + idempotency store | required (API) |
| `DEFINITION_SERVICE_URL` / `DEFINITION_SERVICE_TOKEN` | all three | The Definition Service's internal API base URL, and its `INTERNAL_API_TOKEN` | URL required; token required outside dev |
| `INTERNAL_API_TOKEN` | API, connector-worker | The API validates `x-internal-token` on `/api/v1/internal/*` against it; the connector-worker presents it on its completion callbacks. The Worker does not use it | required outside dev |
| `SNS_TOPIC_ARN` | API | Outbound `wf-workflow-events` publish target | required (API) unless `AWS_USE_STUB=true` |
| `GLUE_REGISTRY_NAME` | API | Glue schema registry the SNS codec encodes against | required unless `AWS_USE_STUB=true` |
| `OUTBOX_RELAY_DATABASE_URL` | API | The outbox relay's own DSN, a role with `BYPASSRLS` (§7.2); the Worker's Activities only enqueue and never use it | required outside dev |
| `ELIGIBILITY_BASE_URL` | API | The Org & Membership eligibility endpoint | required |
| `TOKEN_SERVICE_BASE_URL` | API | The identity subsystem's Token Service, read for each tenant's automation principal (§5.7); `TOKEN_SERVICE_CLIENT_TIMEOUT` bounds a call | required outside dev; `5s` timeout |
| `OUTBOX_POLL_INTERVAL` | API | Outbox relay poll cadence (§6.6) | `500ms` (Execution's own app-configurable default, matching Definition Service — not a platform-library default; Appendix A.5 #15) |
| `OUTBOX_BATCH_SIZE` | API | Outbox relay batch size | platform default |
| `AWS_USE_STUB` | API | In-process stub publisher; refused outside `dev` and `local` | `true` in dev, required `false` elsewhere |
| `APP_ENV` | all three | One of `local`, `dev`, `test`, `staging`, `prod`; any other value fails startup. Only `dev` and `local` relax the required settings | `dev` |
| `OPENBAO_ADDR` / `OPENBAO_TOKEN` | connector-worker | OpenBao KV-v2 read side for connector credentials | required; the token outside dev |
| `RLS_VIOLATION_EXPORT_INTERVAL` / `RLS_VIOLATION_RETENTION` | API | Cadence of the violation counter and the retention of `rls_violation_log` rows (§7.6) | `30s` / `168h` |
| `OUTBOX_RETENTION` / `PROCESSED_EVENT_RETENTION` | API | How long published outbox rows (the audit trail, §4.11) and dedup rows (§6.8) are kept before the sweep deletes them | `61320h` (7 years) / `168h` |
| `RETENTION_SWEEP_INTERVAL` / `RETENTION_SWEEP_BATCH` | API | Cadence of the retention sweeps and rows per delete batch | `24h` / `1000` |
| `WORKFLOW_CACHE_SIZE` | Worker | Temporal sticky workflow cache entries per Worker process; `0` keeps the SDK default (10,000), which does not fit a small memory limit | `0` |
| `LOCK_DATABASE_URL` | API | A direct, unpooled DSN for the advisory lock that serializes tenant and user event handling (§9.7) | falls back to `DATABASE_URL`; required outside dev when `PG_BOUNCER_MODE` is true |
| `START_REFUSED_STATUSES` | API | Comma-separated tenant statuses for which `POST /instances` answers `409 TENANT_NOT_ACTIVE`; known statuses only (`trial`, `active`, `past_due`, `cancelled`, `suspended`, `trial_expired`, `offboarded`) | empty (refuse nothing) |
| `TENANT_PURGE_ENABLED` / `TENANT_HISTORY_RETENTION` | API | Whether the tenant data purge (§4.13) runs, and how long an offboarded tenant's history is kept before it | `true` / `61320h` (7 years) |
| `OPERATIONAL_STATS_INTERVAL` | API | Cadence of the DEGRADED and READY-task gauges (§7.6) | `30s` |
| HTTP `8080`, metrics `9091` | API | Listen ports (the Worker exposes only health/metrics ports) | fixed defaults |

### 7.7 Disaster Recovery

Consistent with the deferred treatment already stated in §4.11, disaster recovery for both of this service's durable stores is managed-infrastructure territory, not an app-repo design decision:

- **Temporal Cluster DR** — namespace replication/failover, cross-region cluster topology, and RTO/RPO targets for the Temporal Cluster are the platform infra team's responsibility. This service consumes an already-provisioned namespace via `TEMPORAL_NAMESPACE` (§7.6) the same way it consumes an already-provisioned Postgres DSN; it does not provision, replicate, or fail over the cluster itself.
- **Postgres DR** — backup/restore, point-in-time recovery, and cross-region replication for the `workflow_execution` schema follow whatever DR posture the platform's managed Postgres already provides (the same posture Definition Service's own schema relies on). No service-specific DR mechanism is designed here.
- **Recovery ordering, if both must be restored together**: Postgres holds the dashboard-facing projection and the audit trail; Temporal holds the authoritative in-flight execution state. A restore that brings back Postgres to an earlier point than Temporal's history (or vice versa) would leave the two out of sync — this scenario is not designed against here, since it depends entirely on the infra team's own backup cadence and coordination for both stores, not on anything this service's application code controls.

### 7.8 Load & Performance Testing

No load/performance-testing process existed anywhere in this document prior to this section — several capacity numbers adopted elsewhere (`MAX_CLIENT_CONN` §9.7, per-queue `WorkerOptions` concurrency §3.2, the dynamically-registered-queue count) are explicitly provisional, with "revisit once real load data exists" as their only stated remediation. This section states the process that produces that data, rather than leaving it implicit.

- **Gate, not a one-time event.** A load-test run against a staging environment (real Postgres/Temporal/Valkey/LocalStack topology, not `test/e2e`'s per-package fixture) is a required, repeatable pre-go-live gate — re-run whenever a capacity-relevant number changes (a `WorkerOptions` limit, `MAX_CLIENT_CONN`, a new tenant tier's isolated-queue expectations), not a single pre-launch checkbox.
- **Tooling: k6**, scripted against the same HTTP surface `test/e2e` already exercises (instantiate → claim → complete, plus a delegation-reroute burst) — chosen for having no new infrastructure dependency beyond what CI already provisions, not because of any feature k6-specific over an equivalent load-test tool.
- **What it validates**: the three explicitly-provisional numbers named above, plus every SLO in §10.3 — a load test that can't reproduce the delegation-reroute SLO's ≤4s p99 or the task-signal SLO's ≤300ms p99 under realistic concurrency is the actual signal that a provisional number was wrong, not just an assumption to revisit later.
- **Rollback-during-load scenario.** §9.7's `MAX_CLIENT_CONN` sizing reasons that PgBouncer's own connection queueing absorbs the brief overlap window where up to four pod generations (API vN+1/vN, Worker vN+1/vN) hold connections simultaneously during a rollout/rollback. The gate's scenario list includes triggering a rollback mid-run (while the load generator keeps issuing instantiate/claim/complete/delegation traffic alongside steady-state throughput) and asserting no `503`s or connection-pool exhaustion occur beyond what a normal deploy's own health-gate window already tolerates — the one condition most likely to invalidate the `MAX_CLIENT_CONN` provisional number.
- **Scope explicitly deferred**: the actual pass/fail thresholds, target concurrency profile (how many simultaneous tenants/instances/tasks a "realistic" run models), and staging-environment sizing are themselves provisional until a first real run exists — this section states the process, not numbers a first run hasn't produced yet.

---

## 8. Worked Example: Tender Lifecycle Walkthrough

This section ties together §2 (DSL/Temporal semantics), §3 (Temporal operations), §4 (data model), §5 (API design), and §6 (events) by walking one workflow instance end-to-end: instantiation, node-override, branch/exception handling, and completion. It introduces no new design — each step cross-references the section that owns it — and it is narrated with tender vocabulary purely as a worked example: the engine itself is domain-agnostic. `business_key` is an opaque domain-scoped identifier (a tender ID here, a project or case ID in another domain); nothing tender-specific lives in Execution-owned schema, API, or events.

### 8.1 Happy Path

The uneventful case: a tender assigned to a template, every node's default assignee eligible, each task completing in order, to a clean finish.

1. **Instantiation request.** The tender admin assigns the tender to the template; the request reaches Execution as an instantiation call carrying `{workflow_version_id, business_key, override_map?}` (§5). Role authorization is applied upstream at the gateway.
2. **Instantiation validation, synchronous, in-request.** The version must be published and valid; `(tenant_id, subject_id, business_key)` must not collide with a live instance; any `override_map` node keys must exist in the compiled plan; every node's default assignee (plus any override entry) is re-validated for existence and current department/level, batched by distinct `(department_id, required_level)` pair. Any failure returns a `4xx`/`409`/`422` naming the offending node(s); nothing is persisted on a rejected request.
3. **Instance record and workflow start.** On validation pass, the service layer writes the instance row (status `RUNNING`) synchronously, then starts the Temporal workflow keyed by `{tenantID}:{subjectID}:{businessKey}`. `WorkflowInstanceStarted` is emitted (§6.4).
4. **Workflow function runs.** The workflow function's first activity fetches the compiled plan — the compiled-plan cache is written through by each instance start (§1.3, §5.1), with no dedicated eager-populator event driving it — then walks the plan step by step under Temporal's determinism constraints (§2).
5. **Node → task creation.** For each user-task node reached, a task row is written (status `READY`) plus one assignment row per default assignee, and `WorkflowTaskCreated` is emitted with the task's identity, department, assignees, and any due/follow-up timestamps. SLA timers, if present, are armed.
6. **Assignee acts.** The assignment surfaces on the assignee's dashboard. The assignee completes the task; the assignment is marked done, and when every assignment on the task is done, the task transitions to `COMPLETED`, emitting `WorkflowTaskCompleted`.
7. **Stage progression.** The workflow function advances to the next step: **sequential** moves to the next node; an **exclusive gateway** evaluates the completed task's decision field and takes the matching branch (the dominant tender case is a yes/no approve-forward vs. reject-revert); a **parallel gateway** spawns one goroutine per branch and the join waits for all of them.
8. **Terminal node reached.** The instance transitions `RUNNING → COMPLETED`, `completed_at` is stamped, and `WorkflowInstanceFinished` is emitted. Completion is implicit — reaching the terminal node completes the instance directly, no admin sign-off gate. A domain's own business-completion step (e.g. tender signing) is a separate write on its own data and does not gate this transition.

Events emitted along the happy path, in order: `instance.started` → (`task.created` → `task.completed`) × N → `instance.finished`.

### 8.2 Branches and Exceptions

Each of the following is a numbered sub-flow off the happy path, stated as its trigger and mechanism.

1. **Node-override (admin reassigns a node's assignee).** An admin submits a reassignment request with a version stamp. **Validate-then-persist**: if the task already resolved, reject as a conflict; else version-check the stamp; then call the eligibility check for the new assignee and reject if it fails, with nothing persisted; only on success does the request persist the override and the reassigned assignment, signal Temporal, and emit `WorkflowTaskReassigned` (`initiator=override`) (§5, §6.4).
2. **Delegation reroute (out-of-office coverage).** Triggered by `DelegationStarted`. Scope-filters the delegator's active assignments, re-checks each surviving assignee's eligibility (batched), vacates and reassigns to the delegate under the assignment-uniqueness backstop, and emits `WorkflowTaskReassigned` (`initiator=delegation`) inside the ≤4s p99 SLO. `DelegationEnded` reverses by the delegation tag. Ineligible-delegate rows stay with the delegator, flagged for admin attention (§6.2, §6.7).
3. **OOO without a delegation.** Triggered by `UserAvailabilityChanged{status=ooo}` with no active delegation. Pauses every instance where the user holds an active assignment (`initiator=ooo`) rather than leaving the work unattended. A later `DelegationStarted` reroutes and resumes; `status=available` resumes directly. This handler never reassigns (§6.2).
4. **Force-back (revert to an earlier node).** An admin-triggered revert pops the completed-nodes history to the target node, which re-opens with a fresh task and assignments; the node left behind is marked `SUPERSEDED` and its assignments vacated, the same closure force-forward performs. Non-targeted sibling branches of an active parallel gateway have their progress saved and resumed, not restarted, to avoid re-running side-effecting steps; only the regressing branch's own unconsumed message buffer is cleared. Emits `WorkflowTaskSuperseded` plus `WorkflowInstanceForceRouted` (`direction=back`) (§2, §3).
5. **Force-forward (skip a stuck node).** An admin-triggered skip marks the bypassed task `SUPERSEDED` (a resolved state distinct from `FAILED`/`DEFERRED`) and jumps the workflow to the target node. Emits `WorkflowTaskSuperseded` plus `WorkflowInstanceForceRouted` (`direction=forward`) (§3).
6. **`DEGRADED` (a parallel branch fails unrecoverably).** When an activity inside a parallel branch exhausts its retry policy, sibling branches keep running; at the join the instance parks in `DEGRADED`, emitting `WorkflowInstanceDegraded {failed_branches[]}`. An admin resolves by force-forward (supersede and proceed) or force-back (respawn the failed branch as a fresh goroutine with fresh timers). A tenant-suspend arriving while `DEGRADED` is recorded and applied when the park ends: the instance leaves the park `PAUSED` unless a tenant reactivation arrives first. A non-parallel unrecoverable failure instead transitions the instance to `FAILED` (§3, §6.2, §7.2).
7. **SLA timers.** A follow-up date fires `WorkflowTaskSlaWarning`; a due date fires `WorkflowTaskSlaBreached`, both raced in the task's own timer selector and cancelled on task exit. Message-send/-receive stages never create a task row, so a due date on a message stage can't be tracked the same way — Definition Service's validator rejects a `send_task`/`receive_task` element carrying a `dueDate`/`followUpDate` attribute at compile time (tracked as an implementation TODO in Definition Service's own repo), so this case never reaches Execution.
8. **Intra-pool message send/receive.** A send stage fires a message, correlated by a receive stage — possibly in a sibling parallel branch — via the instance-wide, node-keyed message buffer; no dedicated event is emitted for the send itself — not meaningful enough on its own to justify its own schema entry, §6.4. Cross-sibling correlation has no exercising fixture today, and one force-back residual is accepted, not resolved.
9. ~~**Ignored-pool admin stub.**~~ Removed (rev 1.67): an ignored pool produces nothing, so no stub task exists. An outside party's message reaches the instance as a message its receive task or boundary waits for.
10. **Admin lifecycle signals: pause/resume/cancel.** Each version-checked, each emitting the matching event (`initiator=admin`). Cancel writes a terminal state and cascades every active task to `FAILED`, vacating assignments.
11. **Tenant lifecycle.** `TenantStateChanged` drives, on the settled transition: offboard → terminate every non-terminal instance; `cancelled`, `suspended` and `trial_expired` → pause (`initiator=tenant_state`); back to `trial`, `active` or `past_due` → resume (initiator-filtered); `trial`, `active` and `past_due` run instances; plan delta → task-queue registry upsert/cleanup. Offboard data-purge scope is a compliance open item (§6.2, Appendix B: Open Items).
12. **`UserDeleted` safety net.** Vacates the deleted user's assignment row on every task where they hold an active assignment — a backstop for a user removed without prior delegation or reassignment — without touching the instance's status when a co-assignee remains active on the affected task; the corresponding bypass log pages SEV-2 (§6.2, §7.6).

### 8.3 Instance Status Machine

```mermaid
stateDiagram-v2
    [*] --> RUNNING: Instantiation validated + workflow started
    RUNNING --> PAUSED: admin pause / TenantStateChanged(non-active) / OOO
    PAUSED --> RUNNING: admin resume / TenantStateChanged(active) / availability restored
    RUNNING --> DEGRADED: parallel-branch activity failure
    DEGRADED --> RUNNING: force-forward (supersede) / force-back (respawn)
    RUNNING --> COMPLETED: terminal node reached (implicit, no admin sign-off gate)
    RUNNING --> FAILED: main-path unrecoverable failure
    PAUSED --> FAILED: main-path unrecoverable failure on an already in-flight activity
    RUNNING --> TERMINATED: admin cancel / admin terminate / TenantStateChanged(offboarded)
    PAUSED --> TERMINATED: admin cancel / offboarded
    DEGRADED --> TERMINATED: admin cancel / offboarded
    COMPLETED --> [*]
    FAILED --> [*]
    TERMINATED --> [*]
```

`UserDeleted` deliberately has no transition of its own in this diagram: it vacates the deleted user's assignment row per-task (§6.2, §8.2.12), it never drives an instance-level status change the way admin pause/`TenantStateChanged`/OOO do. An instance with a co-assignee still active on the affected task stays `RUNNING`, untouched.

### 8.4 Full Lifecycle Flow

```mermaid
flowchart TD
    INST[Instantiation request] --> VAL{Validation:<br/>version published+valid,<br/>business_key free,<br/>override_map valid,<br/>assignees eligible? §8.1}
    VAL -->|no| REJ[[4xx / 422:<br/>version conflict,<br/>duplicate business_key,<br/>invalid override,<br/>ineligible assignee]]
    VAL -->|yes| REC[Write instance RUNNING<br/>+ start workflow<br/>emit instance.started]
    REC --> PLAN[Fetch compiled plan<br/>walk execution steps §2]
    PLAN --> NODE[Create task + assignments<br/>emit task.created<br/>arm SLA timers]
    NODE --> WAIT{Await task resolution}

    WAIT -->|assignee completes| CT[task.completed]
    WAIT -->|SLA follow-up / due| SLA[sla-warning / sla-breached] --> WAIT
    WAIT -->|admin override| OV[validate-then-persist<br/>task.reassigned override §8.2.1] --> WAIT
    WAIT -->|DelegationStarted| DEL[scope-filter reroute<br/>task.reassigned delegation<br/>≤4s p99 §6.7] --> WAIT
    WAIT -->|UserAvailabilityChanged ooo| OOO[pause initiator=ooo §8.2.3] --> PAUSED
    WAIT -->|UserDeleted| UD[vacate deleted user's assignment<br/>no instance-level pause §8.2.12] --> WAIT
    WAIT -->|admin pause / TenantStateChanged non-active| PAUSED[[PAUSED]]
    PAUSED -->|resume / active / delegation| WAIT

    CT --> GW{Next execution step}
    GW -->|sequential| NODE
    GW -->|exclusive: forward| NODE
    GW -->|exclusive: reject / force-back| FB[force-back:<br/>pop completed nodes,<br/>save+resume siblings,<br/>reset branch buffer<br/>force-routed back §8.2.4] --> NODE
    GW -->|force-forward| FF[supersede task<br/>force-routed forward §8.2.5] --> NODE
    GW -->|parallel| PAR[goroutine per branch] --> JOIN{Parallel join}
    JOIN -->|all branches ok| GW
    JOIN -->|a branch failed| DEG[[DEGRADED: park<br/>emit instance.degraded §8.2.6]]
    DEG -->|admin force-forward| FF
    DEG -->|admin force-back| FB

    GW -->|terminal node| DONE[[COMPLETED<br/>completed_at<br/>emit instance.finished]]
    PLAN -->|main-path unrecoverable failure| FAIL[[FAILED<br/>emit instance.failed]]
    PAUSED -->|already in-flight activity fails| FAIL
    WAIT -->|cancel / terminate / offboarded| TERM[[TERMINATED<br/>cascade tasks FAILED<br/>emit instance.terminated §8.2.10/11]]
```

The two items this walkthrough touches that are still open — the cross-sibling message-correlation residual and `TenantOffboarded` purge scope — are tracked in full in **Appendix B: Open Items**. Instance-completion semantics, synchronous `409` delivery, the `send_task`/`receive_task` SLA gap, and the business-key partial-unique-index predicate are covered in §3.4, §5.10, §2.8, and §4.2 respectively.

### 8.5 Compound Worst Case: `DEGRADED` × Tenant Suspension × Force-Back

§8.2's numbered sub-flows and §7.2's test #5 each establish the individual mechanisms this scenario combines; this section is the one place they are narrated together end-to-end, since a reader assembling them independently from a happy-path-only walkthrough risks missing an ordering subtlety at step 4 below.

1. **A `Parallel` gateway is mid-flight.** A tender's compiled plan reaches a two-branch parallel gateway — say, "Legal Review" and "Finance Review" running concurrently as sibling goroutines (§2.5.2, §8.1.8). Finance Review's task is claimed and in progress; Legal Review's task has just been completed and its branch has moved on to a follow-up activity.
2. **Legal Review's follow-up activity fails unrecoverably.** The activity exhausts its retry policy on a non-retryable error (e.g. a `ValidationError` from a downstream call, §3.3). Per §8.2.6, the branch's goroutine returns an error; **Finance Review is not cancelled** — it keeps running, still mid-claim, exactly as designed. Nothing externally observable happens yet: the instance is still `RUNNING`.
3. **Finance Review completes normally, then the join fires.** The surviving sibling's task is completed, its branch finishes, and control reaches the parallel join. Because one branch (Legal Review) failed and the other (Finance Review) succeeded, the join transitions the instance `RUNNING → DEGRADED` (§8.3's state diagram), stamping which branch(es) failed and emitting `WorkflowInstanceDegraded {failed_branches: ["legal_review"]}` (§8.2.6). The instance now sits parked, waiting on an admin.
4. **Before an admin acts, the tenant is suspended.** Ops suspends the tenant for an unrelated billing reason; `TenantStateChanged{status=suspended}` arrives via `POST /api/v1/internal/events` (§8.2.11). The handler's ordinary path for a non-active transition is to signal every affected instance to pause (`initiator=tenant_state`). For this instance, that signal is **recorded, not rejected**: the stored status stays `DEGRADED` and the park still shows its failed branch, and the pause is held until the park ends (§8.2.6, §7.2 test #5). The instance does **not** become some third `DEGRADED_AND_PAUSED` hybrid state; the design deliberately has no such status, and the pause is remembered intent.
5. **The tenant is restored, then an admin resolves the `DEGRADED` park with a force-back.** `TenantStateChanged{status=active}` arrives; the resume's initiator (`tenant_state`) owns the pause step 4 recorded, so it clears it and the instance will leave the park running; had the tenant stayed suspended, the park would have ended `PAUSED` (§8.2.6, §8.2.11). An admin then reviews the `DEGRADED` park and decides Legal Review's failure needs a redo rather than a skip, issuing a force-back to the Legal Review node (§8.2.4). This respawns Legal Review as a **fresh goroutine with fresh timers** — Finance Review's already-completed work is not re-run, consistent with the "save, don't discard" sibling rule that made Finance Review's completion durable across the whole detour.
6. **The instance returns to `RUNNING`** with Legal Review re-executing and Finance Review's branch already past the join point for its own side, and the happy path resumes from there.

The scenario demonstrates three independently-designed rules composing correctly without a fourth mechanism to glue them together: sibling-branch survival on `DEGRADED` (§8.2.6), a pause held across the park and applied only when it ends (§8.2.6, §7.2 test #5), and force-back's save-not-discard semantics for already-completed sibling work (§8.2.4). The tenant-suspend-while-`DEGRADED` interleaving needs no new status or signal: the pause is held in the interpreter's state and the park ends in the state the latest pause or resume leaves.

---

## 9. Security

This section consolidates all security controls enforced by Execution Service. As with Definition Service, controls are split into transport, application, and data layers, extended for Execution's two-process, Temporal-backed topology.

### 9.1 Transport Security

- **mTLS between all pods**: enforced by the same Envoy service mesh Definition Service runs behind. No service can call Execution's API process without a valid client certificate; the service trusts that gateway headers were injected by Envoy and cannot be spoofed by arbitrary in-mesh callers.
- **TLS termination**: all external (browser-originated) traffic terminates at the Envoy API gateway. Execution never directly handles raw TLS on its inbound HTTP surface.
- **mTLS to the Temporal frontend**: conditional, not universal — required only if the shared Temporal cluster is configured to require it. `TEMPORAL_TLS_CERT_PATH`/`TEMPORAL_TLS_KEY_PATH` are `required iff mTLS` in the env-var inventory (§7.6); certs are consumed via a Kubernetes `Secret` volume mount, the same shape already used for other credentials. Registering the namespace and provisioning the mTLS material are cluster-operator actions outside this service's own deploy topology (§7.5) — the Helm chart consumes an already-provisioned namespace and cert pair, the same way it consumes an already-provisioned Postgres DSN.
- **Header validation**: the handler middleware chain validates the presence of Envoy-injected gateway headers (`x-tenant-id`, `x-user-id`, `x-tenant-roles`, `x-departments`, `x-plan`, `x-feature-flags`) on every browser-originated route, identical to Definition Service's convention (§1.2).

### 9.2 Authentication & Authorization

- **Identity-agnostic by design (§1.2).** Execution Service performs no local authentication and stores no user/department records — every browser-originated request carries gateway-injected headers, trusted for the same reason Definition Service trusts them: mTLS in the service mesh guarantees they originate from Envoy after upstream JWT validation, and Execution performs no JWT validation of its own.
- **Tenant boundary.** Every database query sets the `app.tenant_id` GUC via `pgcommon.WithGUCSet` before any DB access, sourced only from the validated `x-tenant-id` gateway header, never the request body — the same triple-layer isolation model as Definition Service, extended with Temporal task-queue partitioning as a third layer (`wf-queue-<tenant_uuid>`) for enterprise/noisy-neighbor isolation (§1.2). The Temporal Web UI/cluster boundary is internal-ops-only and never tenant-facing (§1.2, §3.6), so it doesn't need a fourth isolation layer of its own.
- **Intra-tenant read scope.** RLS scopes every query to the caller's own tenant, but that alone would let any authenticated tenant member read any other member's task/instance detail — approval comments, assignment history — regardless of department or involvement. `GET /tasks/:id`, `GET /instances/:id`, and their list counterparts (`GET /tasks`, `GET /instances`) are visible to a caller only if at least one holds: the caller holds an active assignment on the task, or on any task under the instance (`workflow_task_assignment.user_id` with `is_active`; a vacated assignment grants nothing, on a task or on an instance); the caller's `x-departments` header — comma-separated `<department-uuid>:<role>` pairs (`definition_service.md` §3.1) — includes the task's `department_id` as the UUID half of one of those pairs (task-level), or any task's `department_id` under the instance, open or not (instance-level); or the caller holds `tenant_admin`/`tenant_owner` (`AdminRole`). List endpoints apply this as an additional `WHERE` filter — non-visible rows are simply absent from the page, not an error. Detail endpoints return `403 NOT_AUTHORIZED_FOR_RESOURCE` when none of the three hold — not `404`, since the resource is already confirmed to exist within the caller's own tenant; `404`'s anti-enumeration purpose (§5.9) is specifically for the cross-tenant case, a stronger guarantee than this intra-tenant one. The instance-level check considers every task of the instance, however many it has, and `GET /instances/:id` returns every task of the instance. `AdminRole` callers bypass the check entirely, matching every other admin-gated operation in this design.
- **Internal-service authentication.** The `/api/v1/internal` route group carries no gateway identity headers and is service-to-service: event ingestion (§6), the Definition Service's active-instances and pause-tasks calls (§5.3), Org & Membership's workflow-client calls and connector callbacks. `RequireInternalToken` compares the `x-internal-token` header with `INTERNAL_API_TOKEN` in constant time (required outside dev) and answers a mismatch with `401` and a problem body. The gateway routes only the public path prefixes, never `/api/v1/internal`. Calling the Definition Service, Execution sends that service's token, `DEFINITION_SERVICE_TOKEN`.
  - **Trust boundary, stated explicitly.** A valid `x-internal-token` authenticates that the *caller* is a legitimate internal service — it does not scope *which* tenant that caller may act on. Any request bearing a valid token can name any `tenant_id` in its body; the only tenant-consistency check performed is a mismatch check between a referenced resource's own tenant and the request's stated `tenant_id`, not a check that the calling service is "allowed" to touch that particular tenant. Per-tenant authorization for a given caller's requests is the upstream service's own responsibility, upstream of ever calling this endpoint.
  - **Outbound eligibility calls** (Execution → Org & Membership) authenticate however the upstream contract names — Execution supplies whatever credential Org & Membership's contract requires; this is not an Execution-side design decision. **Contract stability** for this same call is a named-owner commitment, not CI-enforced (§6.9): unlike the compiled-plan read (an OpenAPI document both services gate in CI, §5.3), this is Org & Membership's own HTTP contract — they own its backward compatibility, and a breaking change on their side is a cross-team coordination event Execution's own CI cannot catch.
  - **Ingress restriction.** The static token alone is not sufficient network-layer access control. A required ingress `NetworkPolicy` restricts `/api/v1/internal/*` to the specific pod identities allowed to call it — the shared Workflow-Events Consumer and Definition Service — as defense-in-depth alongside the token, not a replacement for it. Infra implements and maintains the policy; the Helm chart's own responsibility is unchanged (§7.5).
  - **Token rotation.** `INTERNAL_API_TOKEN` stays a single static secret per environment, rotated only as an ordinary deployment/secrets-management operation — no time-limited or automated refresh flow is adopted. Accepted as sufficient given the ingress restriction above narrows who can even present the token.
- **Connection role restrictions.** The same two-role pattern Definition Service uses: the API process (and the Worker's Activities) connect via a role with `BYPASSRLS=false`; the outbox relay role follows the same `BYPASSRLS=true`/table-ownership convention as Definition Service's relay role, since it is the one process here too that needs a cross-tenant scan.

### 9.3 Input Validation

- **JSON body binding.** All Execution HTTP endpoints bind JSON request bodies only — there is no XML attack surface anywhere in this service. BPMN/XML parsing, XXE prevention, and the XML token-bomb guard are entirely Definition Service's concern; Execution never touches raw BPMN XML at any point in its own request path, since it consumes only the already-compiled, already-validated `CompiledCollaboration` JSON via the Definition Service's compiled-plan read.
- **Size cap.** A 10 MB hard body-size limit is enforced by the Gin body-limit middleware before any handler reads a request, the same cap Definition Service applies.
- **Envelope size guard.** `outbox.Enqueue` rejects any outbox payload over 240KB — events reference rows by ID and never embed result bodies (§6.6), which is itself a defense against an oversized `result_json` value ever reaching the outbox.

### 9.4 Data Security

- **No plaintext secrets.** Database credentials, the Temporal mTLS cert/key pair, SNS ARNs, and AWS keys are injected via OpenBao, synced into a Kubernetes `Secret` (via the External Secrets Operator or OpenBao's own Kubernetes auth/injector) and consumed the same way Definition Service consumes its own — never plaintext environment variables.
- **Encryption-at-rest for sensitive `jsonb` columns.** Four columns carry business content of varying sensitivity (`workflow_instance.context_json`, `workflow_instance.override_map`, `workflow_task_assignment.result_json`, `workflow_task.extras_json`), plus `outbox_events.payload` — the merged audit trail's own JSONB column (§4.5, §4.10). Ordinary cluster/volume-level encryption-at-rest (the same posture Definition Service relies on) is sufficient for all of these as a confidentiality control — no custom Temporal `DataConverter` is adopted. No per-user erasure mechanism is needed on top of this: per §9.6, this schema stores no PII, so there is no user content requiring targeted, revocable encryption.
- **Outbox payloads.** Same convention as Definition Service: events reference rows by ID, never embed result bodies, so `outbox_events.payload` itself carries no richer content than an ID pointer plus metadata.

### 9.5 Audit Trail

`outbox_events` **is** Execution's full internal audit trail — no separate table exists (§4.5, §4.10). Every Activity writes its audit event directly as one `outbox.Enqueue` call, so no event class is held back from the audit trail's external, published counterpart. This is the schema's highest-write-volume table; retention/partitioning for it is deferred pending real production volume data (§4.11), with `PrunePublished` explicitly held to the 7-year audit floor rather than the library's short-lived-relay default (§4.11).

**RLS-violation audit logging** (`db/migrations/000006_rls_audit_logging`, additive to the T1.2 persistence-layer ticket, ported from `iam-user-profile`'s `rls_violation_log`/`log_rls_violation`/`rls_check_tenant` pattern): every tenant-scoped table's `tenant_isolation_policy` swaps its bare `tenant_id = app_tenant_id()` comparison for `rls_check_tenant(tenant_id, '<table>')`, a `STABLE STRICT SECURITY DEFINER` function that returns the same boolean either way but additionally calls a 1%-sampled `log_rls_violation` on denial — `missing_or_invalid_guc` when the GUC doesn't resolve, `cross_tenant_access` when it resolves to a different tenant. Rows land in `rls_violation_log`, a table with RLS deliberately **disabled** (the logging function fires from inside another table's own RLS check; enabling RLS on the log table would recurse into itself). `app_tenant_id()`'s existing fail-closed behavior (§4.8) is unchanged — this only adds a signal on top of an already-enforced denial, never alters what's allowed.

### 9.6 PII and Data Classification

**No PII stored.** Every user reference anywhere in the `workflow_execution` schema is a bare IAM UUID (`workflow_task_assignment.user_id`, `assigned_by`, `workflow_instance.started_by_user_id`, `actor_user_id`/`assignee_user_ids` inside `outbox_events.payload`, and `assignee_overrides.actor_user_id`/`previous_user_id`/`new_user_id`) — no display names, email addresses, phone numbers, or any other personal data are stored in a dedicated column, a free-text field, or a jsonb blob anywhere in this schema (§4.9). Free-text fields this service does hold (`workflow_task_assignment.result_json`'s completion comments, `assignee_overrides.reason`, the pause/resume/cancel/defer signals' own `reason`) are business content about the workflow action itself, not identity data — this service is never the source of record for who a user is. UUID-to-display-name enrichment, and any other IAM-owned personal data a caller needs, is exclusively a read-API-layer concern: the browser itself, composing with IAM's `/users:batch` directly, fetches it from IAM at read time, never from this schema (§1.3, §4.9). Because no PII is ever persisted here, this service needs no per-user erasure mechanism — there is nothing for one to erase.

### 9.7 PgBouncer & Connection Pools

Execution connects to PostgreSQL through PgBouncer, in Transaction Mode, the same pooling shape as Definition Service — with the added wrinkle that two independently-scaled Deployments (API and Worker) share one budget:

- **`MAX_CLIENT_CONN`** is a shared PgBouncer client-connection budget split across both Deployments (§7.6's env-var inventory). **Provisional number:** each process's own pool is capped at **10 connections per replica**, mirroring Definition Service's own precedent (§8.7 there) — at `replicas: 2` (§7.5's stated floor) that's 20 per process, 40 total at steady state, sized with headroom above that for the rollback-overlap window below. This is an estimate to unblock deployment, not a measured number — no production traffic exists yet (Appendix B); revisit once real load data exists.
- **Overlap during rollbacks.** Up to four pod generations (API vN+1/vN, Worker vN+1/vN) could theoretically hold connections simultaneously during a rollout/rollback overlap window, briefly exceeding a naive sizing calculation. PgBouncer's own connection queueing absorbs a short-lived over-budget window, and the overlap is bounded by each rollback's own health-gate observation window (§7.5) — worth accounting for when sizing `MAX_CLIENT_CONN` in practice.
- **Role isolation** mirrors Definition Service's: the API process's (and Activities') connections use a role with `BYPASSRLS=false`; the outbox relay's role follows the same `BYPASSRLS=true`/table-ownership convention as Definition Service's relay role.
- **Session locks do not go through the pooler.** A session-level advisory lock taken through a transaction-pooling proxy is not tied to one server connection: its release can run on a different one, leaving the lock held on a connection other clients reuse, so a later event for the same scope waits on it until that server connection is recycled. The lock that serializes event handling for a tenant or a user (§6.2) therefore uses its own small pool on a direct connection, `LOCK_DATABASE_URL` (the migration step already uses `MIGRATION_DATABASE_URL` for the same reason). Behind a pooler outside dev the setting is required; without a pooler it falls back to `DATABASE_URL`. The lock is taken with a non-blocking attempt on a pooled connection and retried every 50 milliseconds, so a waiter holds no connection and queued events for one busy tenant cannot starve the others; a connection whose release fails is closed instead of being returned to the pool. The migration job takes the direct address from the chart too.

---

## 10. Non-Functional Requirements

### 10.1 Performance Targets

- **Delegation reroute (`DelegationStarted` consume+reroute half of Org & Membership's SLO-2)**: p99 ≤ 4 s, measured from the envelope's `time` field to the reroute transaction's commit (`workflow_task_assignment` rows swapped) — this includes shared-consumer forwarding latency, since the budget covers everything downstream of the upstream publish. Org & Membership's SLO-2 commits a 5 s p99 end-to-end budget split: publish half ≤ 1 s (owned upstream, not Execution), consume+reroute half ≤ 4 s p99 (owned here). No new machinery meets this budget — it is a bulk transaction plus a signal loop, with the eligibility re-check batched by distinct `(department_id, required_level)` pair rather than per assignment row, since per-row calls are the one variable inside the timed window most likely to blow it under real load (§6.7). Measured by the `delegation_reroute_duration_seconds` histogram; a breach pages, it never fails the event — this is a timeliness SLO, not a correctness one.
- **Keyset, not offset, pagination** on `workflow_task` and `outbox_events` — the schema's two highest-cardinality, highest-write-volume tables. Offset pagination degrades linearly with page depth on tables expected to grow large fast; keyset pagination stays constant-cost provided an index leads with the filter column(s) and continues with the keyset ordering columns, which is exactly why the `outbox_events` instance-timeline expression index (`(tenant_id, (payload -> 'data' ->> 'workflow_instance_id'), created_at DESC, id DESC)`) and the `workflow_task` tenant-listing index (`(tenant_id, created_at DESC, id DESC)`) are shaped the way they are (§4.9). The HTTP-level pagination contract this enables is carried in §5.9.

### 10.2 Dependency Failure Matrix

| Dependency | Failure mode | Execution's behavior |
| --- | --- | --- |
| **Temporal Cluster** | Frontend unreachable | The API process's `/readyz` folds in a Temporal-frontend reachability check and fails, removing the pod from load-balancer rotation; `SignalWorkflow`/`StartWorkflow`/`QueryWorkflow` calls fail and the handler returns `503`. No instance state is corrupted — Temporal is the system of record for in-flight execution state, and Execution never writes the DB directly from the signal-only API path (§1.2). The Worker process's own `/readyz` checks the same connectivity and similarly fails, halting new task-queue polling until the frontend recovers; in-flight Activity executions already dispatched continue under Temporal's own retry semantics once the frontend returns. |
| **Definition Service unreachable** (the compiled-plan read) | Down / timeout | At instantiation (`POST /instances`), the handler returns `503 UPSTREAM_UNAVAILABLE` after the configured retry budget; no instance is created. Its detail is fixed, since the cause names the Definition Service's internal URL and the tenant; the service that read the plan logs the cause. Inside `GetCompiledPlanActivity` (the authoritative in-workflow re-fetch, §3.1), a failed call surfaces as a retryable Activity failure under Temporal's own retry policy — the workflow does not proceed past that step until the call succeeds, however long the outage lasts, retrying with backoff capped at 30 seconds, so an outage of the Definition Service delays an instance and never fails it, and each failed read counts as an upstream dependency error, which is what raises the alert; a rejection (the version is unknown, unpublished or invalid, or its plan cannot be read) is not retried and fails the instance (§3.3). A long outage shows in the upstream dependency error alert (§7.6). Instance start does not consult the Valkey compiled-plan cache; running-instance reads do, fail-open on a miss or an unreachable cache, and the in-workflow re-validation stays authoritative. |
| **IAM / Org & Membership unreachable** (eligibility checks) | Down / timeout | Delegation-reroute and bulk-reassignment eligibility re-checks fail; the affected bulk handler's transaction does not commit for the rows depending on that check, and the delegation SLO's clock keeps running — an IAM outage is the direct, expected cause of a `delegation_reroute_duration_seconds` p99 breach, alerted on via §6.7's SLO threshold rather than treated as a separate failure signal. Node-override's synchronous eligibility call at instantiation similarly returns `503 UPSTREAM_UNAVAILABLE` to the caller; no override is applied. |
| **PostgreSQL** | Down / unreachable | Both processes' `/readyz` fail on DB-pool health check, removing both from rotation. API-process handlers touching the DB return `503`. Worker-process Activities retry under Temporal's own Activity retry policy — an Activity failing on a DB error is exactly the kind of transient failure Temporal's retry semantics exist to absorb; the workflow function itself does not advance past a failed Activity until it succeeds or exhausts retries. The outbox relay (API process only) pauses and retries on reconnect, identical to Definition Service's own behavior. |
| **Valkey** | Down / unreachable | Fail-open, matching Definition Service's own convention: the compiled-plan cache falls through transparently to the compiled-plan read on miss/unreachable-cache — no caller-visible impact beyond added latency. The API process's `/readyz` reflects cache unavailability but the process does not remove itself from rotation solely for this reason, since Valkey is a pure operational accelerator here, not a correctness dependency. The Worker process has no Valkey dependency at all (§7.6), so a Valkey outage has zero effect on the Worker. |
| **SNS / outbox publish failure** | Down / throttled | Outbox rows accumulate with `published_at IS NULL`; the relay retries with exponential backoff. After `MaxAttempts` (platform default 5), rows move to `outbox_dead_letters` and `platform_dlq_messages_total{operation="outbox_publish"}` fires a page (§7.6's alert catalogue) — operable via the runner's reprocess/list/discard operations. No HTTP request or Activity fails due to SNS unavailability, since the write completes and commits before the relay ever runs; events are durable in `outbox_events` until delivered. |
| **Shared Workflow-Events Consumer unreachable** | Down / degraded | All inbound events (delegation reroute/end, tenant-lifecycle, user lifecycle, template pre-warm) stop reaching `POST /api/v1/internal/events` entirely — Execution has no own SQS consumer to fall back to (§6.1). Each SNS topic's own SQS queue retains messages per its retention window (the consumer's own infra, unowned by this document, §6.1's "gap acknowledged" note); once the consumer recovers, delivery resumes and `processed_event` dedup makes replay safe. No Execution-side alert exists for this outage today — it surfaces only indirectly, as a stall in delegation reroutes/tenant-lifecycle actions, which the delegation SLO alert (§6.7) would eventually catch for that one case. |
| **Temporal Advanced Visibility (Elasticsearch) unavailable** | Down / degraded | Custom Search Attribute queries (`InstanceStatus`, `TenantId`, etc., §3.6) fail or return stale results in the Temporal Web UI — this affects only ops/admin querying, never the Postgres-backed dashboard or any correctness-critical path, since Search Attributes are a query-side convenience layered on top of Temporal's own execution state, not a store anything else reads from. An admin loses the ability to query "every `DEGRADED` instance for tenant X" via the Web UI until the store recovers; the same instances remain fully resolvable via the ordinary Postgres dashboard query path. |

### 10.3 Service-Level Objectives

- **Delegation reroute SLO**: p99 ≤ 4 s (envelope-time → reroute-commit), measured by `delegation_reroute_duration_seconds`; alert on p99 > 4 s over the rolling window, response is to investigate the eligibility-check batch cost (§6.7, §7.6's alert catalogue).
- **Task signal SLO** (claim/complete/defer): p99 ≤ 300 ms, HTTP-request-received → `202` returned (the Signal-Only path's own latency, not workflow-side processing after the signal lands) — measured by `task_signal_duration_seconds{operation}` (histogram, `operation ∈ {claim, complete, defer}`), labeled by operation since `complete`'s `allDone` fan-out check makes it the more expensive of the three. A provisional target, not yet measured against real production load (§9.7's own provisional-numbers caveat applies equally here).
- **Instantiation SLO**: p99 ≤ 1 s, `POST /instances` received → `202` returned — measured by `instance_start_duration_seconds`, covering the synchronous DB write + compiled-plan pre-check + `StartWorkflow` call (§3.7). Provisional, same caveat.
- **Dashboard-push SLO**: p99 ≤ 2 s, outbox commit → SSE frame delivered to the browser — owned and measured by the Dashboard Stream Gateway, not this service; Execution's own contribution ends at the outbox commit, which the outbox-relay's existing publish-latency metrics already cover indirectly.
- **Availability SLO**: 99.9%/month, measured by an external uptime checker polling `/readyz` on both the API and Worker Deployments at a fixed interval — an outage is any window where `/readyz` returns non-`200` on the API Deployment (the Worker's own availability is covered indirectly via the Activity-failure-rate deploy gate, §10.3 below, rather than a separate uptime measurement, since the Worker has no direct external caller to observe availability from).
- **Coverage gate**: 95% global floor, adopted from Definition Service's mechanism unchanged — merged via the max-count-per-block strategy across unit and integration coverage profiles, enforced by the same awk-based gate script (§7.3). Per-package floors are re-derived for Execution's own packages: `internal/workflow` at 90% (Temporal SDK internals and real multi-worker timing are integration-tier, not this floor's job), `internal/adapter/outbound/postgres` (excl. sqlc-gen) at 90%, `internal/core/service` at the 95%+ global floor, `internal/adapter/inbound/http` (excl. static renderers) at 90% (§7.3).
- **Deploy-gate rollback thresholds**: extends Definition Service's digest-verification + Prometheus-rollback-gate pattern to **per-process** gates, since the API and Worker Deployments can each independently regress (§7.5, §7.4). The API Deployment's gate is a post-deploy-window 5xx rate; a breach triggers an automatic rollback of the API Deployment only. The Worker Deployment has no HTTP business traffic to measure 5xx against, so its equivalent gate is Activity-failure-rate over the same post-deploy window — a breach triggers an automatic rollback of the Worker Deployment only (§7.6's alert catalogue). A partial image push (one process's image published, the other not) blocks the deploy gate entirely, since either Deployment's atomic (`--wait --atomic`) upgrade would otherwise reference a nonexistent tag (§7.4).

### 10.4 Cost & Resource-Efficiency Acknowledgment

This design accepts three real, ongoing cost multipliers in exchange for correctness/operability guarantees discussed elsewhere in this document. Each is a deliberate trade-off:

- **Unlimited Activity retries on DB-writing activities** (`MaximumAttempts: 0`, §3.3). Trading bounded compute cost for correctness: a `ScheduleToCloseTimeout` would silently cap retry duration and could abandon a DB write mid-outage, leaving an instance stuck with no automatic path to recovery. The cost is unbounded — a sustained Postgres outage means Worker CPU spent retrying, not idle — but Temporal's own exponential backoff (`BackoffCoefficient: 2.0`, `MaximumInterval: 60s`) keeps the steady-state retry rate low, and the outage itself is already the dominant incident, not the retry loop.
- **Mandatory Temporal Advanced Visibility (Elasticsearch-backed store)** for the five custom Search Attributes (§3.6, §10.2). This is a standing infra cost — a full Elasticsearch cluster provisioned and operated indefinitely — purely to support ops/admin querying in the Temporal Web UI; no correctness-critical path reads from it (§10.2's own dependency-failure entry). Accepted because the alternative (no Search Attributes) removes the only "every RUNNING/DEGRADED instance for tenant X" operational query surface, and the same querying need cannot be met by the Postgres dashboard's per-tenant, application-level query path in an ops-incident context where a tenant's own RLS scoping isn't the relevant lens.
- **Per-tenant dedicated Temporal task queues** (`active_task_queues`, §3.7, §4.9) for tenants on an isolated plan tier. Each isolated queue requires its own Worker registration and adds one row of steady-state polling overhead per active tenant on that tier, rather than amortizing polling cost across a shared queue. Accepted because it is the mechanism that makes per-tenant noisy-neighbor isolation possible at the Temporal task-dispatch layer — the alternative (a single shared queue for every tenant) would let one tenant's workflow volume starve another's task-queue throughput, which the isolated-tier's own value proposition depends on avoiding.

No action is proposed here — this section exists so a future capacity-planning or cost-review pass starts from an explicit list instead of rediscovering these three from scratch.

---

### Appendix A: Design Decisions

This appendix consolidates every design decision made across the document, grouped by the section that made it.

#### A.1 DSL & Temporal Semantics (§2)

| # | Decision | Rationale |
| --- | --- | --- |
| 1 | `subProcess` → inline/recursive interpretation, not a child workflow | Keeps the whole BPMN inside one deterministic workflow for state/history simplicity. |
| 2 | ~~Crossing BPMN pools branches on the target's `Ignored` field — ignored → admin-completed task, no child workflow; non-ignored → inline/flat by default, child workflow available when a concrete trigger justifies it~~ Superseded (rev 1.67): one pool runs, and the call-pool step is removed | Every ignored pool compiles to `steps: null` regardless of source richness — nothing to spawn a workflow for. |
| 3 | `callActivity` → a `call_plan` step, expanded at decode by the shared `dsl.ExpandCalls` into an inline `sub_workflow` with call-scoped departments | The module is compiled once and each call runs its own tasks, addressable by their own node keys, in the caller's departments. The interpreter still needs no `callActivity` code, and expanding inside the `GetCompiledPlan` activity keeps replay on the recorded, already-expanded plan. It replaces compile-time inlining, which duplicated a module called twice into one department and gave its tasks colliding keys. |
| 4 | ~~`callActivity` timer/error-boundary gap documented as a current limitation, no fix filed~~ Superseded (rev 1.61) | A call's boundaries compile like a `subProcess`'s and run on the `sub_workflow` its call expands to (§2.2, §2.3). |
| 5 | Force-back permitted during an active parallel gateway; sibling branches saved, not cancelled | Saving (vs. discarding) sibling progress avoids redoing completed work. |
| 6 | Exclusive-gateway evaluation scoped to binary yes/no + implicit-else, no third-party expression engine | Matches actual domain usage (non-technical authors, binary approve/reject pattern); avoids committing to a general evaluator before it's needed. |
| 7 | Unrecognized `StageDef.Type` does not fail — dispatched as an ordinary claim/complete task | Definition Service documents unknown `type` as intentional forward-compatibility for a task type it does not define; hard-failing would break that path. |
| 8 | ~~`CompiledCollaboration.Messages` treated as diagram-only, not a correlation mechanism~~ Superseded (rev 1.67): the field is removed | Zero consumers anywhere in the compiler; real correlation already exists via `Extras["message"]`. |
| 9 | `prep`/`review` LLM assistance is out of scope for the Temporal workflow function entirely | Frontend calls LLM Service directly; Execution's only touchpoint is a one-way async cache pre-warm event, not a functional dependency. |
| 10 | ~~`call_pool` into an `Ignored` target → ordinary admin-completed task, not a new signal/status mechanism~~ Superseded (rev 1.67): one pool runs, and the call-pool step is removed | The target has no compiled steps to run; a full task/signal apparatus would be overkill for what is really a manual checkpoint. |
| 11 | ~~Cross-pool reverts get the same admin-completed-task treatment, gated to `tenant_admin`-equivalent~~ Superseded (rev 1.67): one pool runs, and the call-pool step is removed | Avoids building automated cross-workflow rollback machinery for a case with zero real examples. |
| 12 | Intra-pool `send_task`→`receive_task` delivery via a workflow-local, instance-wide, node-keyed message buffer, not a Temporal signal | No signal channel exists between two tasks in the same execution; single-threaded workflow scheduling makes the shared map lock-free and replay-safe; instance-wide scope is required for cross-sibling correlation, and node-keyed reset prevents double-counting on regression without breaking that correlation. |
| 13 | `DueDate`/`FollowUpDate` are real and flow through to task creation; the SLA-timer runtime mechanism is designed in §3, not here | Matches the DSL-semantics/Temporal-operations split used throughout this document; parsing a FEEL-expression date reuses whichever evaluator the condition-expression decision (§2.6) eventually picks. |

#### A.2 Temporal Operations (§3)

| # | Decision | Rationale |
| --- | --- | --- |
| 1 | `admin-route` retired as a name; its cases split into `instance-force-forward` (goto) and the direct `TerminateWorkflow` client call (terminate) | One name per operation removes the ambiguity an overloaded signal created. |
| 2 | Claim is scoped to multi-assignee tasks only, establishing a lead assignee | Matches the actual motivating use case; avoids over-building a mechanism with no current enforcement need beyond reserving the concept. |
| 3 | Temporal Workers run in a separate deployment/repo from the HTTP API | Confirms the originally intended architecture. |
| 4 | `active_task_queues` registry + periodic dynamic worker registration, not a static queue list | A static list would require a Workers redeploy every time a tenant changes plan tier. |
| 5 | Instance record write moves to the API service layer, before workflow start; `CreateInstanceRecordActivity` removed | Avoids an async-activity-vs-synchronous-write contradiction — the instance record must exist before `StartWorkflow` is called, not as a step inside the workflow. |
| 6 | Non-retryable business errors fail an activity immediately; infra-transient errors retry unlimited-attempts with backoff | Matches Temporal's intended retry-policy design; prevents a permanent validation error from retrying forever while tolerating real infra blips. |
| 7 | Parallel-branch unrecoverable activity failure → `DEGRADED` instance status, siblings keep running, no auto-recovery | Avoids discarding completed sibling work and avoids hiding a real failure; makes it visible and requires deliberate admin resolution. |
| 8 | `DEGRADED` is set at the parallel aggregation point (all branches terminated), then the workflow function parks in an admin-signal-only Selector loop; force-forward supersedes and resumes past the parallel step, force-back respawns the failed branch as a fresh goroutine from the saved context | A failed branch's goroutine has already returned — only a respawn is possible; parking in a Selector is the deterministic, replay-safe way to wait indefinitely for admin intervention. |
| 9 | A respawned branch gets fresh SLA timers and a node-keyed message-buffer reset, not resumed/carried-forward state | A respawn is, from both mechanisms' perspective, an ordinary new task/branch creation — resuming stale state would either misapply timer semantics or double-count a buffer entry. |
| 10 | Instance-level `FAILED` is scoped to non-`Parallel`-branch failures; `DEGRADED` is scoped to `Parallel`-branch failures | Keeps the two statuses non-overlapping and each independently meaningful: `FAILED` = no sibling work to preserve, terminal; `DEGRADED` = sibling work may still be in flight, not terminal. |
| 11 | Force-back saved sibling branches resume from their last checkpoint, not restart from the rejoin point | Avoids re-running side-effecting steps already completed in that branch. |
| 12 | Each `worker.Worker` instance gets its own `WorkerOptions` concurrency limits, per queue class, not one process-wide default | Task-queue partitioning alone doesn't achieve the priority-isolation goal if every worker instance shares one undifferentiated concurrency budget. |
| 13 | `WorkflowIDReusePolicy: AllowDuplicate` | Required because `workflowID = {tenantID}:{subjectID}:{businessKey}` and a subject/key pair can be reused after a prior instance completes. |
| 14 | `workflow.GetVersion` patching mandated as a coding convention for future workflow-function changes | Instances are unbounded-duration; a workflow-function deploy will always eventually happen mid-flight for some instance. |
| 15 | The custom Search Attributes require provisioning Temporal's Advanced Visibility (Elasticsearch) store | Minimum set for Temporal Web UI operational querying, independent of the Postgres dashboard's own query path. |
| 16 | New `SUPERSEDED` task status for `instance-force-forward`-bypassed tasks, rather than reusing `FAILED`/`DEFERRED` | Neither existing terminal status fits: not a failure, not the assignee's own choice to send it back. |
| 17 | `DueDate`/`FollowUpDate` SLA tracking via `workflow.NewTimer`, raced against the task's own resolution in the same Selector — not a periodic DB poll/cron job | Idiomatic for this architecture; Temporal already owns "wait for the first of several things to happen" as a first-class primitive. |
| 18 | Approval/signing adopts Tender-first-synchronous ordering | Advancing workflow state ahead of a signed artifact defeats the non-repudiation guarantee the signing step exists to provide. |
| 19 | Tender Service reaches Execution via direct synchronous HTTP, not gRPC or an event, immediately after its own signing step succeeds | The synchronous `409`-on-race behavior required cannot be delivered asynchronously; Execution's existing Complete endpoint already does the job a new contract would duplicate. |
| 20 | The browser calls Execution's business-action endpoints directly; a domain service with its own precondition (Tender's signing step, §3.5) calls Execution's Complete endpoint directly itself, immediately after that precondition succeeds. No intermediary sits in this path | `Complete`'s contract is caller-agnostic either way — identity-forwarding plus version-check only (Appendix A.4 #5) — so no invariant requires a third-party intermediary between either caller and Execution; collapsing signer-and-caller into one service removes a crash window between "signed" and "completed" rather than adding one. `IAM/tender-service-approval-sequencing-sync.md` is a Tender-Service-facing artifact and is left unchanged; confirming this design with Tender Service is a cross-team action outside this document's scope. |
| 21 | The API handler returns `202` only after its own `SignalWorkflow`/`StartWorkflow` call succeeds | Narrows the signal-vs-DB-write race to the smallest possible window. |
| 22 | Instantiation is idempotent end-to-end: a start matching a live instance of the same version, caller, context and override map issues `StartWorkflow` again when no run id is recorded and returns that instance, keyed by `(tenant_id, subject_id, business_key)`, and an execution already running under the identifier counts as success | Without this, a client retry on a `StartWorkflow` timeout could duplicate the instance if the first attempt actually landed. |
| 23 | `StartWorkflow`'s input argument is `{instanceID, tenantID, workflowVersionID, businessKey, overrideMap, contextJSON}` — the compiled plan itself never crosses this wire | Matches exactly what `workflow_instance` already carries at write time; the workflow function fetches the plan itself via `GetCompiledPlanActivity`, its first activity (§3.7). |
| 24 | `StartWorkflow`'s `TaskQueue` is `workflow_instance.task_queue`, snapshotted once at instantiation: the tenant's routable registered queue, else `CompiledPlan.TaskQueue` | Connects the dynamic queue registration mechanism (§3.2) to which queue a given instance actually runs on (§3.7). |
| 25 | `ScheduleToCloseTimeout` deliberately unset on every activity class | It bounds the sum of all retry attempts; setting one would silently cap the unlimited-retry-with-backoff intent `MaximumAttempts: 0` already commits to for infra-transient failures (§3.7). |
| 26 | `SignalWithStartWorkflow` not adopted; `StartWorkflow` and every signal stay separate calls | Instantiation happens exactly once via an already-idempotent DB-write-then-`StartWorkflow` sequence (Decision 22); every signal always targets a known-existing execution by deterministic `workflowID` — the atomic start-or-signal case this SDK call solves never occurs in this design (§3.7). |
| 27 | No activity uses `RecordHeartbeat` today; a concrete revisit criterion governs when one should | Every activity is a fast, bounded DB write or HTTP call; a future activity whose duration can legitimately exceed its `StartToCloseTimeout` needs `RecordHeartbeat` and a matching `HeartbeatTimeout` (§3.7). |
| 28 | Continue-As-New not adopted | Instance histories stay bounded given the already-mandated max-loop-iteration bound on cyclic `Exclusive` revert cycles (Definition Service's own compile-time guard against an unbounded revert loop) — with that bound in place, even a template with heavy revert cycling stays well within Temporal's default history limits (~50MB / 50K events). Revisit only if a real template is observed exceeding this in practice. |
| 29 | Shared `wf-queue-default` fairness is achieved by generalizing the existing isolation trigger — a standard-tier tenant whose task-creation volume exceeds a rolling threshold gets auto-promoted to its own `active_task_queues` entry, same as an enterprise-plan upgrade | Temporal has no native per-tenant fairness inside one task queue; reusing the already-built isolation mechanism avoids inventing a second one. Threshold is provisional, pending real traffic data (§3.2). |
| 30 | Isolated queues are not capped or sharded: every replica polls every registry row | A per-process cap with consistent-hash sharding was designed and never built; it is designed when the isolated-tenant count makes one process's pollers a problem (§3.2). |
| 31 | There is no BE-for-UI service. Business-action intermediation, response enrichment, and the module/starter-workflow library each resolve without a dedicated intermediary: the browser calls Execution's business-action endpoints directly (Decision 20); the browser composes IAM's `/users:batch`/`/departments` directly for UUID-to-display-name enrichment, matching `definition_service.md` and `dashboard_stream_gateway.md`'s own approach; the module/starter-workflow library is absorbed into Definition Service's own schema (`definition_service.md` §10.16). Connector-authoring templates and credential custody belong to Definition Service | None of these four responsibilities requires a dedicated intermediary service; only real-time dashboard push was ever independently justified, and that's `dashboard_stream_gateway.md`'s own responsibility. |

#### A.3 Data Model (§4)

| # | Decision | Rationale |
| --- | --- | --- |
| 1 | No dedicated `workflow_event` table — every audit/dashboard event is written as a single `outbox.Enqueue` call, with identifying fields (`workflow_instance_id`, `task_id`, `node_key`) inside `payload` and two service-authored JSONB expression indexes for query access | One table, no independently-maintained audit copy to keep in sync; `payload` is genuinely arbitrary JSON so no `platform-events` schema change is needed, and a service can add its own indexes to a library-owned table exactly as it already does for that table's RLS policy (§4.5, §4.10) |
| 2 | `department_id` is `uuid NOT NULL` from day one, no nullable/interim state, no backfill migration | Avoids a later backfill entirely; requires Definition Service's compiler to emit a real IAM department UUID (tracked as an implementation TODO in Definition Service's repo, §4.3) |
| 3 | `active_task_queues` carries no RLS policy | Workers need a cross-tenant read to compute their own registration set; this is operational/infra data, not tenant business data (§4.6, §4.8) |
| 4 | `record_version` (renamed from `version`) is added to `workflow_instance` as well as `workflow_task` | Every instance-scoped admin signal needed the same stale-client-view guard task-level signals already had; `instance-reassign` is task-addressed and checks the task's own version instead (§4.2) |
| 5 | Arrays (`current_node_keys`, `saved_node_keys`) over a separate table | Both are read/written as one unit per instance; no relational benefit from normalizing what has no independent access pattern |
| 6 | Dashboard aggregates are computed live over existing indexes, not stored in a counter table | No measured performance need justifies the write-path complexity and staleness risk of a stored counter |
| 7 | Pagination is keyset, not offset, on `workflow_task`/`outbox_events` | These are the schema's highest-cardinality, highest-write-volume tables; offset pagination degrades with page depth precisely where it matters most |
| 8 | The `record_version`/`created_at`/`updated_at`/`deleted_at` convention is applied need-based, not blanket-copied | A table gets each column only where a genuine concurrent-write risk or a genuine "removed as a business concept" need exists (§4.11) |
| 9 | Retention/partitioning for all five audit/history-bearing tables is deferred, not designed | No production volume data exists yet for a design-only pass (§4.11) |
| 10 | `processed_event` is a service-authored migration, not a `platform-events`-provided table | Matches the real, existing convention already used by Definition Service and IAM's `iam-user-profile` (§4.7) |

#### A.4 API Design (§5)

| # | Decision | Rationale |
| --- | --- | --- |
| 1 | Path prefix is `/api/v1` | Matches Definition Service's shipped convention (§5.2) |
| 2 | `override_map` added to instance-detail response; a single shared `ActiveUserTask` type serves both its external and internal call sites; comments/resource-links removed from scope | Closes two real gaps in the endpoint set without inventing new ones; comments/resource-links tracked out-of-scope per §4.1 |
| 3 | Node-override validates eligibility before persisting, after a local not-resolved + `record_version` check, and persists only on eligibility success | Matches the upstream domain contract's validate-then-persist requirement: no override row may exist for a rejected override; local checks precede the external call so an already-resolved or stale-version request is rejected without a wasted call (§5.4) |
| 4 | Bulk node-eligibility re-validation hard-blocks with `422 ASSIGNEE_INELIGIBLE` rather than inline-replacing an invalid default assignee | A compliance-sensitive workflow should not silently substitute an assignee the caller never confirmed (§5.5) |
| 5 | Complete-task's contract is caller-agnostic — identity-forwarding plus version-check only | Which service calls it directly is unresolved upstream; the endpoint does not hard-code either assumption (§5.6) |
| 6 | `get-workflow-status` is not exposed via any HTTP endpoint | Avoids a second, Temporal-latency-bound read path duplicating the Postgres projection; reserved for internal reconciliation only (§5.6) |
| 7 | The delegation-scope filter is two special cases (`all`, `department`) plus a `tender` case compared with `subject_id`; every other scope value matches nothing and is counted | A scope added to the identity subsystem's enum cannot reroute the wrong tasks; a new subject-scoped value is one added case (§5.8) |
| 8 | `reassign-delegate`/`cancel-by-delegate` use a bulk-write-then-signal shape, not one signal per HTTP request | A single delegate can hold dozens of active tasks; mirrors the existing bulk SQS-handler transaction shape (§5.8) |
| 9 | Inbound internal-service authentication adopts Definition Service's `RequireInternalToken`/`x-internal-token`/`INTERNAL_API_TOKEN` convention | An identical, already-shipped convention guards Definition Service's own `POST /api/v1/internal/events` — precedent-following, not a new mechanism (§5.7) |
| 10 | The pause-tasks endpoint is the department-membership-revocation/user-deletion safety-net pause vehicle | The Definition Service calls it when a revocation invalidates a published version (§5.3) |
| 11 | Execution's error envelope is RFC 9457 `problemDetails` with its own per-rule error-code table | Matches Definition Service's convention and per-rule granularity (§5.10) |
| 12 | Keyset pagination (`cursor`/`limit`, default 25/max 100, `next_cursor`, no `total`) on every list endpoint | A deliberate deviation from Definition Service's offset contract, driven by `workflow_task`/`outbox_events`' write volume; the page-size cap is sized to keep downstream enrichment composable one batch call per page (§5.9) |
| 13 | `Idempotency-Key` is adopted on every mutating endpoint; HTTP caching and `Retry-After` are not adopted | Matches Definition Service's own precedent on all three (§5.9) |
| 14 | Doc surface (`/swagger/` dev-only, `/api/openapi.yaml`, `/asyncapi`) and the 10 MB request-body cap adopted unchanged | Same shipped conventions as Definition Service, no reason to diverge (§5.2, §5.10) |
| 15 | Task/instance read visibility is scoped to assignee-or-department-or-admin, not flat tenant-wide; list endpoints filter, detail endpoints `403 NOT_AUTHORIZED_FOR_RESOURCE` | RLS alone permits any tenant member to read any other member's approval comments and assignment history — a real gap for a workflow whose whole domain is need-to-know approval data (§9.2) |
| 16 | HTTP API versioning is path-based `/api/v2`, whole-API not per-route, adopted for the first breaking change | Matches Org & Membership's own already-established platform convention exactly, rather than inventing a different scheme; mirrors the coexistence shape the event side's `.v2` mechanism already uses (§5.9) |
| ~~17~~ | ~~`POST /instances` synchronously checks tenant status (`active`/`trial` only) before writing `workflow_instance`, returning `409 TENANT_NOT_ACTIVE` otherwise~~ | Replaced by the configurable start gate (§5.5): the statuses to refuse are a setting, empty by default, instead of a fixed allow-list of `active` and `trial` |
| 18 | The internal HTTP contracts with the Definition Service are OpenAPI documents each server owns; the server's CI refuses a breaking change (oasdiff), the caller keeps a byte-identical copy its CI checks against the source and its tests validate requests against; the IAM eligibility-check contract gets a documented named-owner stability commitment instead of new tooling | The hand-copied protos had already drifted once; one document per endpoint, held by both sides' tests and gated on both sides' CI, makes the drift a failing check. The IAM eligibility contract is documentation-only since it's owned by another team's service (§5.5, §9) |
| 19 | The Definition Service's calls are guarded by `RequireInternalToken` on the internal route group, not by `RequirePermission` | `RequirePermission`'s per-user action/resource model needs gateway-injected end-user identity a machine-to-machine call never carries; §5.7/§9.2 establish `x-internal-token` as the internal route group's trust model (§5.3) |

#### A.5 Events & Integration Topology (§6)

| # | Decision | Rationale |
| --- | --- | --- |
| 1 | Inbound events arrive via the shared Workflow-Events Consumer forwarding to `POST /api/v1/internal/events`; Execution runs no SQS consumer | One engine-wide ingestion pattern, already shipped by Definition Service. The library's own SQS consumer is the recorded fallback. |
| 2 | Ingestion endpoint mirrors Definition's exactly: `RequireInternalToken`, envelope bind, unknown-type `200`, `2xx`/`4xx`/`5xx` retry contract, `internal_events_ingest_total` | Precedent-following; the shared consumer already speaks this contract. |
| 3 | Dedup recorded after side effects, `(event_id, consumer)` PK, per-subscription consumer strings, 7-day TTL prune | At-least-once + idempotent-handler correctness; a transient failure must re-drive the whole handler on redelivery. Idempotent-across-retries and safe-under-concurrency are treated as distinct properties — the actual backstop for concurrent-in-flight duplicates is each handler's own DB uniqueness constraint, not the dedup table. |
| 4 | `DepartmentMembershipRevoked`: no Execution subscription | Definition Service's consumer already calls Execution's pause-tasks endpoint; a second subscription double-pauses for no added safety. |
| 5 | `TenderAssigneeOverridden`: not consumed; the synchronous node-override path is the only reassignment mechanism | IAM's authoritative topology states Execution is not a bus consumer of this event. |
| 6 | There is no template-sync consumption of any kind in this catalogue. | — |
| 7 | Tenant lifecycle arrives as one `TenantStateChanged` relay on `iam.membership.events`, not by subscribing to raw `iam.tenant.events`/`billing.events` | Execution has no consumer grant on those two topics per Org & Membership's authoritative topology; O&M relays its own settled effective state on the topic Execution already consumes. |
| 8 | Tenant-lifecycle handlers are recency-guarded by envelope `time` (last-writer-wins, offboard exempt) | SNS→SQS delivery is unordered; a suspend/reactivate pair applied out of order would leave a paying tenant paused. |
| 9 | A `TenantStateChanged`→`active` transition resumes only `initiator=tenant_state` pauses | An admin's individual pause must survive a tenant-level resume. |
| 10 | `UserAvailabilityChanged` drives a pause, never a reroute | Reassignment stays the sole province of the delegation events, which carry the committed scope/delegate contract. |
| 11 | ~~Wire event types are dotted-lowercase under `workflow.`~~ **Reversed (rev 1.45): wire event types and the `EventType` SNS attribute are PascalCase**, matching IAM platform-wide. `outbox_events.event_type` is the same PascalCase value verbatim — `platform-events`' `Enqueue` applies no case transform, so there is no separate SCREAMING_SNAKE DB representation (corrected rev 1.46, Appendix B) | The original rationale — mirroring Definition Service's shipped dotted-lowercase convention — no longer holds: Definition Service publishes no events at all since `workflow.template.published` was retired (rev 1.38), leaving Execution as the platform's only dotted producer against IAM's every-service-PascalCase standard. This service's own `api/asyncapi.yaml` already names every message in PascalCase; only the attribute value diverged. `platform-schemagov` is casing-agnostic — it normalizes either form to the same snake_case schema key (§6.8) — so no schema file, Glue registry name, or registry migration is affected |
| 12 | `WorkflowTaskCompleted` is emitted | The unconditional `outbox.Enqueue` rule requires it, and the Dashboard Stream Gateway's optimistic-UI reconciliation needs the completion push. |
| 13 | Every task-scoped payload carries the affected `user_id`(s); instance events carry `started_by_user_id` | The Dashboard Stream Gateway fans out per user; an event it can't route by user is invisible to the UI. |
| 14 | Events reference rows by ID; result bodies are never embedded | The 240KB `Enqueue` guard and the sensitivity rule that the audit row holds the body while the event holds the pointer. |
| 15 | Outbox runner/publisher config mirrors Definition Service on batch size (50) and single-relay-in-API-process topology. `PollInterval` is configured at **500ms** (`OUTBOX_POLL_INTERVAL`, matching Definition Service's own default exactly). No dedicated outbox `MaxAttempts` config field exists in either service; `platform-events`' own `outbox.Runner` uses a library-internal default, not an app-configurable one. None of this is live yet — no `outbox.NewRunner`/publisher construction exists anywhere in `cmd/` (§6.6's own gap). | No load data justifies divergence; the library's claim-leasing makes adding relays later safe if ever needed. |
| 16 | Schema governance copies Definition Service's pipeline wholesale | Shipped, enforced, and the registry/CI conventions are already engine-wide. |
| 17 | The ≤4s p99 delegation consume+reroute SLO is formally owned here, measured envelope-time→reroute-commit | O&M's SLO-2 explicitly assigns this half of the 5s budget to Execution Service. |
| 18 | The reroute-time eligibility re-check batches by distinct `(department_id, required_level)` pair, not one IAM call per row | This is the one variable inside the timed window most likely to blow the budget under real load. |
| 19 | The compiled-plan cache is written through by each start and read only for running instances (§1.3); start reads the Definition Service live, so it sees an invalidation or an archive at once. No dedicated event keeps the cache warm. | — |
| 20 | No `WorkflowTaskMessageSent`/`TASK_MESSAGE_SENT` event exists in the catalogue | Not meaningful enough on its own to justify a dedicated event/schema entry — `send_task` dispatch stays fully traceable via `workflow_task`'s own row and the surrounding instance-timeline events (§6.4) |
| 21 | Dashboard Stream Gateway subscribes to `.paused`/`.resumed`/`.claimed`/`.superseded`/`.task.failed`; Notification subscribes to `.degraded`/`.failed`/`.superseded`/`.task.failed` | A pause/resume, a claim, or a task getting bumped/cascaded-failed all change what a specific user sees on their task list in real time (§6.4, §6.5) |
| 22 | `WorkflowInstanceStarted` and `.force-routed` stay Audit-only, not added to Dashboard | `.started`'s creating client already knows synchronously via the instantiation response; `.force-routed` is a purely ops/admin action with no end-user-facing state change (§6.4, §6.5) |
| 23 | `DelegationEnded`'s reversal handler gains the same batched `(department_id, required_level)` eligibility re-check the forward `DelegationStarted` reroute already does, before restoring the original assignee | The forward direction never restores an assignment blind; reversal shouldn't either — an original assignee who became ineligible or was deleted during the delegation window shouldn't get the task handed back with no gate (§6.2, §3 item 2) |
| 24 | `UserDeleted`'s safety-net pause is scoped per-assignment, not instance-wide | The prior instance-wide pause loop would freeze a multi-assignee task's whole instance even when co-assignees remain active and could still progress it; vacating just the deleted user's assignment row is the narrower, correct blast radius (§6.2, §3 item 3) |
| 25 | `UserAvailabilityChanged` gains the same `<=`-skip envelope-time recency guard `TenantStateChanged` already has | Unlike that handler, this one had no ordering protection — an out-of-order `ooo`/`available` pair for the same user could leave them wrongly stuck paused or resumed under unordered SNS→SQS delivery (§6.2, §3 item 5) |
| 26 | `TenantStateChanged`'s last-applied `changed_at` recency-guard value updates once, after both the `plan` and `status` sub-transactions (when both present on one event) commit successfully — not per-transaction | Guarantees a retry re-drives whichever half failed, rather than risking a partial-failure redelivery being skipped as "already applied" (§6.2, §3 item 4) |

#### A.6 Testing & Infrastructure Strategy (§7)

| # | Decision | Rationale |
| --- | --- | --- |
| 1 | `test/workflow` (via `testsuite.WorkflowTestSuite`) is a new, org-first test tier — not adapted from any existing repo | No sibling repo tests a Temporal workflow; this is new engineering practice, designed directly against the SDK's own testing framework. |
| 2 | `test/integration/temporal` gets its own real-namespace tier, separate from workflow-environment unit tests | `DEGRADED`'s respawn-a-new-goroutine mechanism and multi-worker queue registration interact with real Temporal history/timing in ways a simulated environment can gloss over. |
| 3 | Three deployable binaries (`cmd/server`, `cmd/worker`, `cmd/connector-worker`) from one Dockerfile, three Helm Deployments, one shared migration hook | Matches the already-decided separate-deployment topology; one Dockerfile avoids duplicating the builder stage. |
| 4 | Worker Deployment's HPA scales on Temporal queue depth/backlog, not CPU | Worker load is bursty per-task-queue, not steady HTTP QPS — CPU-based scaling would lag the real signal. |
| 5 | Worker process exposes a minimal health/metrics HTTP surface despite having no business API | k8s probes and Prometheus scraping need an HTTP endpoint to hit. |
| 6 | Per-package coverage floors are re-derived for this service's own packages, not copied from Definition Service's table | Copying the numbers without re-deriving which packages they apply to would silently exempt the wrong code. |
| 7 | The broker hop is left to its owners — `platform-events` for the relay, the shared Workflow-Events Consumer for SNS→SQS, DLQ and per-queue fan-out; this service's e2e tier asserts the outbox row, inbound dedup, the delegation SLO, and the full lifecycle | A test that asserts only a dependency's behavior reports coverage this service does not have (§7.2). |
| 8 | Tenant purge scope, the shared consumer's own spec, and Temporal/Postgres cluster DR stay explicitly out of scope | Each has a real owner outside this service's testing/infra design — compliance/legal, the shared consumer's future owner, and the platform infra team, respectively. |
| 9 | The API and Worker Deployments default to `replicas: 2`, stated as a precondition for the `minAvailable:1` PDB | `minAvailable:1` at `replicas:1` blocks every voluntary disruption including the rollout itself. |
| 10 | `test/integration/temporal` gets a combined `DEGRADED` + `TenantStateChanged{suspended}` test alongside the standalone `DEGRADED`-respawn test | The two riskiest new mechanisms in the service were each tested in isolation; the designed no-op behavior deserves a real assertion against a live parked execution, not just a design claim. |
| 11 | An ingress `NetworkPolicy` restricting `/api/v1/internal/*` to the shared Workflow-Events Consumer and Definition Service's pod identities | The internal token alone is not sufficient network-layer access control (§7.5, §9.2). |
| 12 | A k6-based load-test gate against a real staging topology, re-run whenever a capacity-relevant number changes, not a one-time pre-launch event | Every provisional capacity number in this document defers to "revisit once real load data exists" — this is the process that actually produces that data, rather than leaving the remediation implicit (§7.8) |
| 13 | Every binary answers a `version` subcommand handled before configuration is read, and CI's per-image check runs it | It is the only entrypoint that proves the image's binary loads without provisioning a database, a Temporal cluster and a cache first (§7.5). |
| 14 | The revert-cycle bound is a compile-time constant, not configuration | A workflow reading mutable configuration mid-execution replays non-deterministically if that value changes under it (§2.6). |

---

### Appendix B: Open Items

What remains here needs an answer from outside the Execution team, needs real production data that doesn't exist yet, or is explicitly non-blocking.

| Theme | Item | Owner |
| --- | --- | --- |
| ~~Cross-team~~ | ~~**A connector callback's actor is asserted, not proven (§5.7).** The worker states the automation principal's identifier in the request body and the mesh plus `x-internal-token` vouches for the caller. The identity subsystem serves a per-tenant JWKS and the principal authenticates to it by signed assertion, so verifying that assertion at this hop would replace trust with proof. Not a gap that lets an outside caller in — the token already gates the route — but the difference between authenticating a service and attributing an action.~~ **Resolved (rev 1.70)**: no request names an actor; Execution resolves the principal from the Token Service itself, so a caller cannot assert one. | — |
| ~~Cross-team~~ | ~~**The worker reads its tenant's automation subject from configuration (§5.7).** The identity subsystem's registry holds it, and the lookup it exposes today answers "is this subject the automation principal" rather than "which subject is this tenant's". Two questions are with that team: whether its Token Service can answer the second directly, and whether the subject survives key rotation and a re-mint — an actor retained on an old record points at an identity nobody can resolve if it does not. Execution resolves nothing itself, so the answer changes the worker and not this service's contract. Until then the worker holds a `<tenant>=<subject>` map and fails a callback for any tenant absent from it.~~ **Resolved (rev 1.70)**: the Token Service answers the first question (its per-tenant read) and the subject does not survive a re-mint, so Execution records `principal_id`, which does. The map is gone. | — |
| Accepted residual | **A task's `COMPLETED` write can abort its instance where the `SUPERSEDED` write beside it deliberately cannot (§4.3).** The supersede path in the interpreter swallows its status-write error on the stated ground that failing an instance over a bookkeeping write is the worse outcome. The completion path propagates its error instead, because the status is what the connector callback's re-completion guard reads, so swallowing it reopens a hole. Both positions are defensible and the difference is currently unexplained; settling it means deciding whether a task status is bookkeeping or contract, and applying that answer to both. | Execution team |
| Deferred | **`task-completed-status`, `iso-timer-duration`, `boundary-no-target-ends-path`, `exclusive-end-completes-path` and `parallel-join-by-branch` gate real behaviour changes and have no replay fixture.** `test/workflow/replay/` holds only `initial_interpreter.json`. Unit tests force each `DefaultVersion` branch through the test environment's `OnGetVersion`, which proves the branch exists but not that a history recorded before the change replays clean against the built worker. | Execution team, when the replay harness next gains a fixture |
| ~~Open~~ | ~~**A task open inside a call or `subProcess` that an interrupting boundary cancels is never marked `SUPERSEDED`.**~~ **Resolved (rev 1.72)**: every task open in a cancelled scope is superseded (§4.3). A receive task and `runParallel` still do not react to cancellation themselves; a cancelled body holding one is simply left, since its caller no longer waits for it. | — |
| Deferred | **`workflow_task.deferred_from_task_id` is no longer written** (§4.3). A defer re-runs the earlier node instead of creating a regression task; the column, its self-reference and the response fields remain until the schema is next revised. | Execution team |
| Accepted residual | **A connector callback whose signal the interpreter drops is lost outright (§3.1, §5.2).** The callback returns `204` as soon as the signal is accepted, and the worker acknowledges its stream entry. A `stage-transition` arriving while the instance is `PAUSED` fails its precondition and is dropped; one whose visit count no longer matches the node's current visit is buffered where nothing reads it. The connector has already run, so nothing retries, and the task stays open with its status guard never engaged. Sizing a fix means deciding whether the callback should reject a signal the interpreter will not act on, which needs the instance's live status at callback time. | Execution team |
| Accepted residual | ~~**A failed assignment write re-runs the connector's external call (§5.7).** The actor's row is written before the signal, so a database failure there returns `500`, the worker leaves its stream entry unacknowledged, and redelivery re-dispatches the connector — a second outbound call for a `rest-call` that is not idempotent.~~ **RESOLVED (rev 1.60 — the worker records each outcome before calling back, and a redelivery re-sends it; `workflow_connectors.md` §6.5 step 0).** A failed callback no longer re-runs the connector while its outcome is recorded. The connector still runs again if the outcome was never recorded, which that section states as the one remaining window. | Execution team, RESOLVED (rev 1.60) |
| Deferred | **`DepartmentMembershipGranted`/`DepartmentMembershipRevoked`/`DepartmentMembershipLevelChanged` are a confirmed, real, but deliberately unconsumed subscription opportunity.** IAM's Org & Membership AsyncAPI spec documents all three arriving on the same `membership-workflow-q` queue this service already subscribes to for `TenantStateChanged`, intended to feed assignee-selection pick-list surfaces — Execution doesn't own the department-membership model and today's eligibility decisions all go through a live `EligibilityClient.CheckEligibility` call, not a local cache, so nothing currently reads this data. Deliberately not consumed until a real pick-list/candidate-search feature is scoped; decide then whether it's backed by a local cache or another live call, together with whatever concretely reads it — don't build the write side alone. | Execution team, once a pick-list feature is actually scoped |
| Accepted residual | Cross-sibling message correlation via the instance-wide message buffer is designed but has zero real compiled-plan fixtures to test against today; one accepted residual behavior on force-back-past-a-consumed-send is resolved by the existing "completed sibling work survives a force-back" rule | Execution team, re-examine if real fixtures land |
| Deferred | No test covers the outbox relay's own RLS requirement (§7.2). The relay connects through `OUTBOX_RELAY_DATABASE_URL`, a BYPASSRLS role; connected as an ordinary role with no tenant GUC set, `app_tenant_id()` returns NULL and the relay claims zero rows forever, with no error — a silently wedged event pipeline. The test tier's pool connects as a superuser, so no existing test can reproduce it. The coverage that would is an integration test connecting as a non-superuser role and asserting the claim returns nothing without BYPASSRLS | Execution team, when the relay's deployment configuration is first exercised |
| Infra | Shared Workflow-Events Consumer has no LLD. Execution's ingestion side is fully specified, but the consumer's deployment/DLQ/scaling is unowned | Whoever owns engine-wide infra services |
| Deferred | Row-count, read/write-ratio, and latency/throughput/growth targets are not stated anywhere in this document — no production data exists yet; the provisional numbers adopted elsewhere in this doc (§4.11, §9.7) are estimates, not measurements | Execution team, revisit once real usage data exists |
| Infra | `StartWorkflow` against a task queue with zero polling Worker replicas silently strands the instance — Temporal persists the scheduled task indefinitely, no cluster-level timeout fires, and the caller's `202` succeeds regardless (§3.7). An alert on "workflow task scheduled but not started within N minutes" needs Temporal's own visibility/metrics tooling, not application code | Infra/cluster-operator |
| Cross-team | `TenantOffboarded` data-purge scope: the engine terminates a tenant's instances on offboard and purges its instance, task and connector rows after a configurable retention period (§4.13); the period, whether the 7-year audit trail is kept, exported or wiped, and the retention of Temporal event histories are decisions needing legal/compliance input | Compliance/legal + infra |
| Deferred | Multi-assignee claim-release mechanics: `is_lead` (§4.4) reserves the concept but release (who releases, a new signal vs. re-claim, in-progress-work handling) isn't designed — not blocking since claim itself isn't built out yet | Execution team, design when claim is actually built |
| Deferred | No burn-rate/error-budget alert exists for the 99.9% Availability SLO (§10.3) — a real fast-burn/slow-burn policy is a genuine design decision | Execution team, design once a real incident or capacity-planning need justifies the policy work |
| ~~Accepted residual~~ | ~~`instance-terminate` writes the DB then calls `TerminateWorkflow` outside the Activity-is-sole-writer pattern (§3). If the DB commit succeeds but the `TerminateWorkflow` call fails, the DB correctly reflects `TERMINATED` but the Temporal execution may still be running — a lightweight reconciliation sweep (confirm every DB-`TERMINATED` instance's Temporal execution is actually closed) is undesigned~~ | Closed: the execution is closed before the DB write, so a failed close is retried instead of being left behind a TERMINATED row (§3, §6.2) |
| Accepted residual | Node-override's own validate-then-persist-then-signal contract (§5.4) has the same shape of gap: if the persist step (the `assignee_overrides` row + `workflow_task_assignment` update) commits but the subsequent Temporal signal fails, the DB reflects the override but the running workflow hasn't yet been told — reconciliation is undesigned | Execution team, low priority — same reasoning as the Terminate-path item above |
| Cross-team | Delegation chains (A delegates to B, B has independently delegated to C) and mid-chain deletion are both undesigned. IAM's `delegations` table (delegator_id/delegate_id) and this document's `DelegationStarted`/`DelegationEnded` handling (§6.2, §6.4) are strictly single-hop — no rule resolves what happens when a delegate's own outbound delegation is active. Compounding this, `UserDeleted`'s safety net (§6.2, §8.2.12) vacates every active assignment for the deleted user with no scope filter, including rows tagged `reason="delegation:<id>"` inherited from an upstream delegator — the task is left with zero owner instead of falling back to the original delegator or the next chain link. IAM's own user-deletion reconciliation flow doesn't help either: it checks only Workflow Service's active task assignments, never the `delegations` table itself, so a pending delegation where the deleted user is the delegate goes unreconciled on IAM's side too | IAM (Org & Membership) + Execution team, joint design |
| Cross-team | ~~Task actions never re-verify the assignee's live status at the moment of the action — a synchronous pre-check is undesigned.~~ **The pre-check is designed and wired** at all four call sites (claim, the complete/defer precheck, reassign, and `WorkflowClient.ReassignDelegate`), and **fails open by design**: any error from the status call is logged and the action proceeds, so an IAM outage never blocks task work. What is open is the upstream contract — `IAMClient.GetUserStatus` returns `ErrIAMContractNotConfirmed` until IAM publishes the endpoint, so the check blocks nothing today and the eventual-consistency window (§6.2) remains the only enforcement. The client's struct and constructor already match the intended shape, so the swap needs no caller changes | IAM — publish the user-status endpoint contract |
| Deferred | ~~`GET /api/v1/internal/workflows/delegate-impact`'s delegate-centric scoping (§5.8) is an implicit side effect of its parameter choice, not a stated rule, and has never been verified against a chained delegate.~~ **RESOLVED.** The asymmetry is stated as an explicit rule on `DelegateImpact` itself — the preview covers the delegate's own resulting workload, never the delegator's, and never follows a chain onward — and a chained A→B→C fixture asserts both directions | Execution team, RESOLVED |
| Infra | ~~Execution needs its first `deploy/helm` chart, now that the browser calls it directly (rev 1.35, Appendix A.2 #20), mirroring Definition Service's `HTTPRoute` + `SecurityPolicy` + `NetworkPolicy` triple.~~ **RESOLVED.** The chart carries all three, alongside a Deployment, PDB and NetworkPolicy per binary | Execution team / infra, RESOLVED |
| Cross-team | ~~**The `department_id` display-slug gap (§4.3) has a concrete blast radius, not previously spelled out.** Discovered implementing `CreateTaskActivity` (T1.3, 2026-08-13): the column has no real value to write until Definition Service's compiler reads an IAM department UUID out of a BPMN lane's `extensionElements` (§4.3's own tracked TODO), so `CreateTaskActivity` populates it with a deterministic placeholder UUID (`uuid.NewSHA1` over the compiled plan's display-slug `DepartmentDef.ID`) purely so task creation works end-to-end today. Every already-built consumer that hands this column to IAM's real `(department_id, required_level) → eligible?` contract inherits a functionally meaningless input as a result: node-override's `CheckEligibility` call (§5.4, already shipped in `node_override_handler.go`), the bulk delegation-reroute eligibility re-check (§5.5, §6.7), and instantiation-time default-assignee re-validation (§5.5's node-eligibility batch). None of these can pass a real eligibility check against a fabricated department until the compiler fix lands — this is the existing gap's actual functional impact, not a new design decision~~ **RESOLVED (rev 1.36) — Definition Service's compiler fix landed.** `ValidateDeptID` now rejects any `UserTask`/`SendTask`/`ReceiveTask` lane lacking a valid IAM department UUID in its `dept_id` Zeebe property, and the compiler surfaces it as `DepartmentDef.IAMDepartmentID`. Execution now consumes it directly wherever this column is written or compared (`CreateTaskActivity`, node-override, delegation-reroute, instantiation-time re-validation) — per this row's own original §4.3 note, this needed no nullable/interim state or backfill, so the placeholder scheme was deleted outright rather than bridged. A related compiler bug found in the same pass — a `CallActivity`-flattened department's real ID being dropped during the merge — was fixed alongside it (`definition_service`, `mergeDepts`) | Definition Service + Execution team, RESOLVED |
| Infra | ~~`cmd/connector-worker` has no CI build/scan/push and no Dockerfile stage, so this third process (§1.6) can't be deployed as-is.~~ **RESOLVED.** It has its own Dockerfile stage, an entry in every CI and release image matrix, and its own Helm Deployment, PDB and NetworkPolicy. The image smoke step was rebuilt at the same time: it ran each image with no arguments, which no real entrypoint survives, and now runs the `version` subcommand (§7.5) | Execution team, RESOLVED |
| Cross-team | ~~**A called sub-process with no `laneSet` at all has no IAM department UUID to give any of its tasks** — `ValidateLaneMembership`'s and `ValidateDeptID`'s own documented exceptions both skip validation entirely when a process defines no lanes, so a `UserTask` inside a fully laneless module can legitimately reach Execution with an empty `IAMDepartmentID`. Distinct from the row above (that one was about lane-based tasks losing an ID that already existed; this is a lane-based validation never applying in the first place) and not previously documented anywhere. Execution treats this as a genuine "no identity" case (`uuid.Nil`), not a synthetic placeholder — an eligibility check against such a task fails closed rather than passing against a fabricated department~~ **RESOLVED (rev 1.61) — definition LLD rev 1.22.** A laneless module is one open slot bound to the calling lane's department, so its tasks carry the caller's IAM department; a workflow lane a slot binds to must itself have a `dept_id` (definition LLD rev 1.25). | Definition Service, RESOLVED (rev 1.61) |
| Build gap | ~~**The §6.4 PascalCase rename is a doc-only change so far; the code still emits dotted types.**~~ **RESOLVED (rev 1.46).** `domain/events.go`'s 18 wire-type constant values, both inbound-dispatch handler files, `temporal/audit.go`/`instance.go`, `core/port/connector_events.go`, `cmd/connector-worker/main.go`, `api/asyncapi.yaml`'s `EventType` description and every message title, `GlueCodec.registrySchemaName` (CamelCase→snake_case), and every affected test all landed together | Execution team, RESOLVED |
| Cross-repo | ~~**The rename does not land in this repo alone.**~~ **RESOLVED (rev 1.46).** `workflow-stream-gateway`'s `internal/usecase/fanout.go` and `event_consumer`'s `config/topology.{example,local}.yaml`/`deploy/helm/values.yaml`/`internal/forwarder/category.go` all landed the same day, coordinated | Execution team + `workflow-stream-gateway` + `event_consumer`, RESOLVED |
| Live bug | ~~**`event_consumer`'s routing table is already broken against what IAM ships today.**~~ **RESOLVED (rev 1.46).** All six IAM route keys (plus the `WorkflowTaskCreated` self-dispatch key) corrected to PascalCase in `category.go`, both `topology.*.yaml`, and `deploy/helm/values.yaml` | Execution team, RESOLVED |
| Cross-team | **IAM's event catalogue names this service's events differently from this service's own spec — and the mismatch survives the PascalCase rename.** `IAM/iam_hld.md` §9.4/§9.5 lists the events Audit Log consumes off `wf-workflow-events` as `TaskCreated`, `TaskSLAWarning`, `TaskSLABreached`, `TaskDeferred`, `TaskReassigned`, `WorkflowFinished`. This service emits `WorkflowTaskCreated`, `WorkflowTaskSlaWarning`, `WorkflowTaskSlaBreached`, `WorkflowTaskDeferred`, `WorkflowTaskReassigned`, `WorkflowInstanceFinished`. Both sides are PascalCase — IAM has always documented Workflow's events that way, which is itself corroboration for §6.4's rename — but the strings differ, so an SQS filter policy written from IAM's catalogue would match nothing. Ours is the authoritative producer contract (IAM's own table defers to "WF HLD §9.3" for it); IAM's catalogue needs correcting. Raise in the next Workflow→IAM sync. | IAM, next Workflow→IAM sync — ours is the authoritative producer contract |
| Cross-team | **`wf-template-events` and `wf-template-audit-q` are dead and IAM does not know.** `IAM/iam_hld.md` §9.1/§9.4/§9.5 still carries the topic, the queue, and a `TemplatePublished` event for Audit Log's template-version history. Definition Service publishes nothing at all — its `api/asyncapi.yaml` declares a single `action: receive` channel, and `workflow.template.published` was retired platform-wide in rev 1.38. Audit Log is subscribed to a topic with no producer. Either Definition re-introduces a template event or IAM drops the subscription; raise in the next Workflow→IAM sync. | IAM + Definition Service, next Workflow→IAM sync |
| Cross-team | **SNS topic names are dotted in IAM's docs and hyphenated in the shipped specs.** `IAM/iam_hld.md` §9.1 uses `wf.workflow.events`/`wf.template.events`; the real topic in this service's `api/asyncapi.yaml` is `wf-workflow-events`, and IAM's own `OAM_asyncapi.yaml`/`UserProfile_asyncapi.yaml` use hyphenated addresses (`iam-membership-events`, `iam-user-events`) for their own topics. IAM's HLD prose is the outlier against every shipped spec including IAM's. Cosmetic in this document (already corrected here and in `workflow_hld.md` rev 0.7), but worth confirming so nobody provisions against the dotted form. | IAM, next Workflow→IAM sync — confirmation only, already corrected on our side |

#### Appendix B.1 Go-Live Checklist

A consolidated ship/no-ship gate. Every row is either a **Blocker** (must be true before the first production tenant is cut over) or **Accepted residual** (the table above already dispositions it as non-blocking, restated here only so this checklist is self-contained). This list does not introduce new requirements — it rolls up commitments already made elsewhere in this document.

| # | Item | Type | Verified by |
| --- | --- | --- | --- |
| 1 | Both migrations (outbox schema + domain schema) applied and `go-arch-lint`-clean build deployed to the target environment | Blocker | `make migrate`, `make arch-lint`, §Common Commands |
| 2 | Coverage gate passing at the stated per-package floors, unit + integration merged | Blocker | `make test-ci`, §7.3 |
| 3 | Both API and Worker `/readyz` checks wired into the deploy pipeline's health gate, including the Temporal-frontend and DB-pool sub-checks | Blocker | §7.5, §10.2 |
| 4 | Per-process deploy-gate rollback thresholds (API 5xx rate, Worker Activity-failure rate) configured and tested against a synthetic breach | Blocker | §7.4, §10.3 |
| 5 | The custom Search Attributes registered against a provisioned Advanced Visibility (Elasticsearch) store in the target environment — not just the dev/test fixture, which deliberately skips this (§7.2) | Blocker | §3.6, §7.2 |
| 6 | `active_task_queues` seeded for every tenant's current plan tier before first instantiation on that tenant; Worker replicas confirmed polling every queue the registry lists (closes the zero-worker-on-queue gap operationally, even though the alerting for it remains an open item below) | Blocker | §3.7, Appendix B row 4 |
| 7 | Outbox relay running with `platform_dlq_messages_total` alerting wired to the on-call rotation | Blocker | §7.6, §10.2 |
| 8 | k6 load-test gate (§7.8) run at least once against a staging environment sized to the first real tenant cohort, with the three SLOs (task-signal, instantiation, delegation-reroute) measured, not just asserted provisional | Blocker | §7.8, §10.3 |
| 9 | Shared Workflow-Events Consumer's own deployment reachable from this service's `/api/v1/internal/events` endpoint in the target environment, with `InternalServiceAuth` configured on both sides | Blocker | §5.9, §6.1 |
| 10 | Row-count/growth/throughput targets not yet measured against real production data | Accepted residual | Appendix B row 3 |
| 11 | `TenantOffboarded` purge-scope policy (retain vs. export vs. wipe) not yet decided by legal/compliance | Accepted residual | Appendix B row 5 |
| 12 | Multi-assignee claim-release mechanics undesigned — acceptable since claim itself has no production traffic yet | Accepted residual | Appendix B row 6 |
| 13 | Shared Workflow-Events Consumer's own infra (scaling/DLQ) has no LLD outside this document's ingestion-side contract | Accepted residual | Appendix B row 2 |

---

### Appendix C: Database Migration Scripts

The database schema and policies are defined in SQL migration files using golang-migrate format (e.g., `db/migrations/0001_create_schema.up.sql`), following the same convention as Definition Service.

> **Ownership split:** `outbox_events` and `outbox_dead_letters` are created entirely by `platform-events`' own embedded migrations — Execution Service authors no `CREATE TABLE` for either and they are **not reproduced here**. The service migration files only add the RLS policy for those two tables, matching Definition Service's own outbox RLS migration pattern exactly.

```sql
-- Enums use the CREATE TYPE convention already established by Definition Service
-- rather than a CHECK constraint, for consistency across the two services' migration styles.

CREATE TYPE workflow_instance_status AS ENUM (
    'RUNNING', 'PAUSED', 'COMPLETED', 'TERMINATED', 'FAILED', 'DEGRADED'
);

CREATE TYPE workflow_task_status AS ENUM (
    'READY', 'IN_PROGRESS', 'COMPLETED', 'DEFERRED', 'FAILED', 'SUPERSEDED'
);

-- One row per running/completed workflow instance — the dashboard-facing
-- projection of Temporal's authoritative execution state (§4.2).
CREATE TABLE workflow_instance (
    id                    UUID                     PRIMARY KEY,
    tenant_id             UUID                     NOT NULL,
    workflow_id           UUID                     NOT NULL,  -- Definition Service's workflow.id; lineage/reporting join only, no FK (cross-service, cross-schema)
    workflow_version_id   UUID                     NOT NULL,  -- Definition Service's workflow_version.id; same cross-service caveat
    subject_id            TEXT                     NOT NULL,  -- the entity this instance is about; opaque to the engine
    business_key          TEXT                     NOT NULL,  -- identifies one instance within that subject
    temporal_workflow_id  TEXT                     NOT NULL,
    temporal_run_id       TEXT,
    status                workflow_instance_status NOT NULL,
    current_node_keys     TEXT[]                   NOT NULL,
    saved_node_keys       TEXT[]                   NOT NULL DEFAULT '{}',
    context_json          JSONB,
    override_map          JSONB,
    task_queue            TEXT                     NOT NULL,
    started_by_user_id    UUID                     NOT NULL,
    started_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    record_version        BIGINT                   NOT NULL DEFAULT 1 CHECK (record_version > 0),
    created_at            TIMESTAMPTZ              NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ              NOT NULL DEFAULT now()
);

-- Idempotent-instantiation key. Partial to allow key reuse once an instance
-- reaches a terminal state. DEGRADED does NOT count as terminal here — it still
-- needs admin resolution, so its key isn't reusable yet (§4.2). One subject
-- holds as many concurrent instances as it has distinct business_keys.
CREATE UNIQUE INDEX uq_workflow_instance_business_key
    ON workflow_instance (tenant_id, subject_id, business_key)
    WHERE status NOT IN ('COMPLETED', 'TERMINATED', 'FAILED');

CREATE INDEX idx_workflow_instance_tenant_status
    ON workflow_instance (tenant_id, status);

-- The archive-guard query behind the Definition Service's active-instances call.
CREATE INDEX idx_workflow_instance_version_active
    ON workflow_instance (workflow_version_id)
    WHERE status IN ('RUNNING', 'PAUSED', 'DEGRADED');

-- One row per dispatched stage/task (prep/review/approve, unrecognized-Type
-- passthrough, connector-typed automatic tasks) (§4.3).
CREATE TABLE workflow_task (
    id                    UUID                 PRIMARY KEY,
    tenant_id             UUID                 NOT NULL,
    workflow_instance_id  UUID                 NOT NULL REFERENCES workflow_instance(id) ON DELETE RESTRICT,
    node_key              TEXT                 NOT NULL,
    -- Snapshotted from the compiled plan at task creation. Definition Service's
    -- compiler reads a real IAM department UUID out of a BPMN lane's
    -- extensionElements (tracked as an implementation TODO in that repo, §4.3) —
    -- no interim nullable state, no backfill migration needed once that lands.
    department_id         UUID                 NOT NULL,
    status                workflow_task_status NOT NULL,
    record_version        BIGINT               NOT NULL DEFAULT 1 CHECK (record_version > 0),
    assignee_mode         TEXT                 NOT NULL,  -- 'single' | 'all'
    -- Set only for a connector-typed automatic task (workflow_connectors.md
    -- §5.2); NULL for every other task type.
    connector_type        TEXT,
    extras_json           JSONB,
    deferred_from_task_id UUID                 REFERENCES workflow_task(id) ON DELETE RESTRICT,
    due_at                TIMESTAMPTZ,
    follow_up_at          TIMESTAMPTZ,
    created_at            TIMESTAMPTZ          NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ          NOT NULL DEFAULT now(),
    completed_at          TIMESTAMPTZ
);

CREATE INDEX idx_workflow_task_tenant_dept_status
    ON workflow_task (tenant_id, department_id, status);

CREATE INDEX idx_workflow_task_instance_status
    ON workflow_task (workflow_instance_id, status);

CREATE INDEX idx_workflow_task_tenant_keyset
    ON workflow_task (tenant_id, created_at DESC, id DESC);

-- cmd/connector-worker's own connector-task lookup (workflow_connectors.md §5.2).
CREATE INDEX idx_workflow_task_tenant_connector_status
    ON workflow_task (tenant_id, connector_type, status)
    WHERE connector_type IS NOT NULL;

-- One row per assignee on a task; carries claim/completion/reassignment state (§4.4).
CREATE TABLE workflow_task_assignment (
    id            UUID        PRIMARY KEY,
    tenant_id     UUID        NOT NULL,
    task_id       UUID        NOT NULL REFERENCES workflow_task(id) ON DELETE RESTRICT,
    user_id       UUID        NOT NULL,
    assigned_by   UUID,
    reason        TEXT,
    is_lead       BOOL        NOT NULL DEFAULT false,
    is_active     BOOL        NOT NULL DEFAULT true,
    assigned_at   TIMESTAMPTZ,
    claimed_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    result_json   JSONB,
    vacated_at    TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Correctness constraint (also serves as a performance index): prevents two
-- active rows for the same (task_id, user_id) pair — also the backstop against
-- duplicate assignment under redelivered delegation events (§6).
CREATE UNIQUE INDEX uq_workflow_task_assignment_active
    ON workflow_task_assignment (task_id, user_id) WHERE is_active;

CREATE INDEX idx_workflow_task_assignment_task_active
    ON workflow_task_assignment (task_id) WHERE is_active;

-- "Show me my active tasks" — the single most common dashboard query.
CREATE INDEX idx_workflow_task_assignment_user_active
    ON workflow_task_assignment (tenant_id, user_id) WHERE is_active;

-- No local CREATE TABLE for the audit trail — it lives in outbox_events, which
-- is entirely platform-events-owned (see the ownership-split note further down,
-- where this service's own two audit/dashboard query indexes on that table are
-- also defined, §4.5, §4.9, §4.10).

-- Registry of tenant-isolated Temporal task queues (§4.6). A row is never
-- deleted; routable says whether new instances start on the queue.
CREATE TABLE active_task_queues (
    id             UUID         PRIMARY KEY,
    tenant_id      UUID         NOT NULL,
    queue_name     TEXT         NOT NULL UNIQUE,
    routable       BOOLEAN      NOT NULL DEFAULT true,
    registered_at  TIMESTAMPTZ  NOT NULL,
    updated_at     TIMESTAMPTZ  NOT NULL
);

-- Consumer-idempotency dedup table for inbound events (§6), following the same
-- platform-wide convention already used by Definition Service and IAM's
-- iam-user-profile. Service-authored migration, not platform-events-provided.
CREATE TABLE processed_event (
    event_id     UUID        NOT NULL,
    consumer     TEXT        NOT NULL,
    event_type   TEXT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer)
);

CREATE INDEX idx_processed_event_processed_at
    ON processed_event (processed_at);

-- Node-override's own audit record (§4.12, §5.4). Insert-only, immutable: no
-- record_version/updated_at/deleted_at, the same reasoning applied to every
-- audit-bearing row in this schema (§4.5, §4.11).
CREATE TABLE assignee_overrides (
    id                    UUID        PRIMARY KEY,
    tenant_id             UUID        NOT NULL,
    workflow_instance_id  UUID        NOT NULL REFERENCES workflow_instance(id) ON DELETE RESTRICT,
    node_key              TEXT        NOT NULL,
    previous_user_id      UUID        NOT NULL,
    new_user_id           UUID        NOT NULL,
    reason                TEXT,
    actor_user_id         UUID        NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_assignee_overrides_instance_node
    ON assignee_overrides (workflow_instance_id, node_key);

-- PostgreSQL Row-Level Security (RLS) Policies
-- Notes (mirrors Definition Service's own Appendix A pattern exactly):
--   FORCE ROW LEVEL SECURITY: prevents table owner from bypassing RLS
--   REVOKE ALL FROM PUBLIC:   strips default public access
--   current_setting(..., true): missing_ok=true — unset GUC returns NULL instead of raising ERROR (fail-closed)
--   WITH CHECK:               constrains INSERTs and UPDATEs, not only SELECTs and DELETEs
--
-- Every tenant-scoped table below gets the identical policy; two named
-- exceptions are called out afterward instead of silently omitted.

REVOKE ALL ON workflow_instance FROM PUBLIC;
ALTER TABLE workflow_instance ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_instance FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON workflow_instance
    FOR ALL
    USING      (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

REVOKE ALL ON workflow_task FROM PUBLIC;
ALTER TABLE workflow_task ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_task FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON workflow_task
    FOR ALL
    USING      (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

REVOKE ALL ON workflow_task_assignment FROM PUBLIC;
ALTER TABLE workflow_task_assignment ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_task_assignment FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON workflow_task_assignment
    FOR ALL
    USING      (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

REVOKE ALL ON assignee_overrides FROM PUBLIC;
ALTER TABLE assignee_overrides ENABLE ROW LEVEL SECURITY;
ALTER TABLE assignee_overrides FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON assignee_overrides
    FOR ALL
    USING      (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

-- NO RLS on active_task_queues, despite the tenant_id column (§4.6, §4.8).
-- Workers need to read every queue across every tenant in one
-- query to compute their own registration set — forcing that through a
-- per-tenant GUC context switch would be backwards for what is fundamentally an
-- operational/infra table, not tenant business data.
-- (Intentionally: no ALTER TABLE ... ENABLE ROW LEVEL SECURITY statement here.)

-- NO RLS on processed_event, and no tenant_id column at all (§4.7, §4.8):
-- infrastructure dedup state keyed by a globally-unique envelope ID, not tenant
-- business data. Matches Definition Service's own processed_event table exactly.
-- (Intentionally: no ALTER TABLE ... ENABLE ROW LEVEL SECURITY statement here.)

-- outbox_events / outbox_dead_letters DO get RLS, but per the ownership-split
-- note above, neither the tables nor their RLS policies are authored in this
-- file — see Definition Service's own outbox RLS migration for the exact
-- library table shape and policy pattern this service's own migration mirrors.

-- outbox_events IS authored here, unlike its RLS policy above: these two
-- indexes are this service's own addition on top of the library-owned table,
-- giving the merged audit trail its query access (§4.5, §4.9, §4.10).
CREATE INDEX idx_outbox_events_instance_created
    ON outbox_events (tenant_id, (payload -> 'data' ->> 'workflow_instance_id'), created_at DESC, id DESC);

CREATE INDEX idx_outbox_events_task
    ON outbox_events ((payload -> 'data' ->> 'task_id')) WHERE payload -> 'data' ->> 'task_id' IS NOT NULL;

-- SLA events (WorkflowTaskSlaWarning/sla-breached) are audit-only (§3.1) —
-- nothing gates a retry on status the way most other Activities can, so
-- RecordSLAWarningActivity/RecordSLABreachActivity check outbox_events.ExistsForTask
-- before enqueueing. That check runs under RunInTx's plain READ COMMITTED with no
-- serializable retry, so two overlapping activity attempts (a real at-least-once
-- scenario, not just sequential retry) can both pass it before either commits. This
-- partial unique index is the DB-level backstop: a second concurrent insert for the
-- same (event_type, task_id) hits a unique violation instead of succeeding,
-- classified as already-done the same way workflow_task_pkey already is (§4.5).
CREATE UNIQUE INDEX idx_outbox_events_sla_task_unique
    ON outbox_events (event_type, (payload -> 'data' ->> 'task_id'))
    WHERE event_type IN ('WorkflowTaskSlaWarning', 'WorkflowTaskSlaBreached');
```

---

### Appendix D: Glossary

#### Core Concepts

| Term | Definition |
| --- | --- |
| **`business_key`** | The opaque, domain-scoped natural key of a workflow instance (a `tender_id` in the tender domain, a `project_id` under a future PMS deployment), used as the idempotent-instantiation key and as a component of `temporal_workflow_id`. |
| **`node_key`** | The `{deptID}/{stageType}` addressing scheme identifying a single task's position in the compiled plan. |
| **`CompiledPlan`/`CompiledCollaboration`** | The hierarchical JSON DSL produced by Definition Service's compiler and consumed by the Execution Service and Temporal Workers to drive orchestration. |
| **`ExecutionStep`** | A single unit of forward progress the workflow function advances through — the runtime counterpart to a compiled plan's node/stage. |
| **`StageDef`** | The compiled definition of a stage (`Extras`, `DueDate`, `FollowUpDate`, etc.) snapshotted into a task at creation time. |
| **`assignee_mode`** | A task's assignment cardinality: `single` (one assignee completes it) or `all` (every assignee must act, with claim semantics). |
| **`IOMapping`** | The mechanism by which a workflow instance's variable/context store (`context_json`) is read and written at runtime; applied on entry to the step that carries it, a call's (§2.1). |
| **`DEGRADED`** | The one new `workflow_instance.status` enum value — an instance in a failure-adjacent state distinct from `FAILED`/`TERMINATED`, still counted as "active" for task-queue and archive-guard purposes. |
| **`SUPERSEDED`** | The newest `workflow_task.status` enum value — marks a task bypassed by an `instance-force-forward`, distinct from both `FAILED` and `DEFERRED`. |

#### Temporal & Orchestration

| Term | Definition |
| --- | --- |
| **Signal-Only API** | The pattern by which all instance/task mutating admin actions are delivered to a running workflow exclusively via Temporal `SignalWorkflow` calls, never by direct DB mutation of live orchestration state. |
| **`force-forward`/`force-back`** | Admin signals that respectively bypass a task (marking it `SUPERSEDED`) to advance execution past it, or roll execution back to a prior node, parking the bypassed branch's nodes in `saved_node_keys` rather than restarting them. |
| **`completedNodes`** | The workflow function's internal record of nodes it has already executed past, used to compute correct forward/back-edge behavior on force-routing signals. |
| **`task_queue`/`active_task_queues`** | A Temporal task queue name (`wf-queue-default` or `wf-queue-<tenant_uuid>`) that Workers listen on; `active_task_queues` is the registry of which tenant-isolated queues are currently active. |
| **`DelegationStarted`/`DelegationEnded`** | Inbound event pair marking the start and end of an out-of-office delegation window, driving `workflow_task_assignment` reroutes while the delegation is active. |
| **`TenantStateChanged`** | The tenant-lifecycle relay event off `iam.membership.events` that can trigger tenant-scoped orchestration effects such as stopping routing to a tenant's task queue. |

#### Database & Persistence

| Term | Definition |
| --- | --- |
| **`record_version`** | The optimistic-lock counter present on `workflow_instance` and `workflow_task`, bumped on every row update and separately checked against a client-supplied value for any write with an external caller. |
| **outbox / transactional outbox** | The reliability pattern that persists domain events to `outbox_events` in the same transaction as the triggering domain write, later published to SNS by a background relay. There is no separate `workflow_event` audit table — `outbox_events` is Execution's full internal audit trail as well, with identifying fields living inside its `payload` column (§4.5, §4.10). |
| **`processed_event`** | The service-authored, composite-PK (`event_id, consumer`) dedup table enforcing idempotent consumption of inbound events. |
| **`assignee_overrides`** | Node-override's own insert-only audit table — one row per admin override action, written by the API process itself before the Temporal signal, distinct from the `outbox_events` row the eventual reassignment signal separately produces (§4.12, §5.4). |

#### Cross-Service & UI Integration

| Term | Definition |
| --- | --- |
| **BE-for-UI** *(retired, rev 1.35)* | A previously-planned separate service that would have been the sole intermediary between the browser (and any domain service like Tender) and Execution's user-facing endpoints. **Retired** (Appendix A.2 #19/#20/#31): the browser calls Execution's business-action endpoints directly; Tender Service calls Execution's `Complete` endpoint directly itself after its own signing step (§3.5); UUID-to-display-name enrichment is frontend-only against IAM directly; the "custom BPMN module"/"reusable authoring component" library this entry used to describe is absorbed into Definition Service instead of a new service's database. Kept here for historical continuity — earlier revisions of this document (and `workflow_management_service.md`, now retired) refer to it by this name. |
| **`port.WorkflowClient`** | The inbound HTTP contract family Org & Membership calls to drive bulk delegation/cancellation flows against Execution — `reassign-delegate`, `cancel-by-delegate`, and the delegate-impact preview (§5.8). |
| **`ActiveUserTask`** | The shared domain type returned by `GET /workflows/active-by-user`, serving both the admin/IAM-reconciliation external call site and an internal event-handler call site with one consistent shape rather than two divergent ones (§5.2). |

#### Eventing & Integration

| Term | Definition |
| --- | --- |
| **Shared Workflow-Events Consumer** | The engine-wide service that consumes SQS queues and HTTP-routes each envelope to the relevant workflow service, keeping Execution Service API-only on the inbound path. |

---

### Appendix E: Coding Style

Coding style, formatting rules, naming conventions, comment/documentation standards, error-handling and structured-logging conventions, unit-testing/mocking conventions, and commit/PR guidelines are shared org-wide and apply unchanged. This document does not duplicate them.

---

### Revision history

| Rev | Date | Change |
| --- | --- | --- |
| 1.0 | 2026-07-20 | Initial LLD: service overview, DSL/Temporal semantics, Temporal operations, data model, API design, events and integration topology, testing and infrastructure strategy, security, non-functional requirements, and a worked tender-lifecycle example. Design Decisions and Open Items consolidated into end-of-document appendices (Appendix A, Appendix B); a database migration-script appendix and glossary added (Appendix C, Appendix D). |
| 1.1 | 2026-07-20 | Open-items closure pass. Resolved 21 of 27 Appendix B items: `department_id`, the message-boundary silent-drop, `send_task`/`receive_task` SLA gap, and the implicit-else ambiguity are now Definition Service compiler fixes (tracked as TODOs in that repo, reject-at-compile-time where applicable); the general condition-expression evaluator, FEEL-expression date parsing, and `IOMapping` runtime timing are accepted scope limits (binary-only, entry-only) rather than open designs; synchronous `409` delivery under the Signal-Only API is resolved (every signal-forwarded endpoint gets its own synchronous pre-check, mirroring node-override's existing pattern, §5.10); instance completion is confirmed implicit (§3.4, §8.1); the business-key partial-unique-index predicate is settled (`DEGRADED` blocks reuse, §4.2, Appendix C); Temporal Web UI multi-tenant visibility is resolved (internal-ops-only, never tenant-facing, `TenantId` Search Attribute is the scoping mechanism, §1.2, §3.6, §9.2); encryption-at-rest, ingress `NetworkPolicy` on `/api/v1/internal/*`, and `INTERNAL_API_TOKEN` rotation are each resolved (§9.2, §9.4); the four working assumptions (no `department.membership.revoked` subscription, dotted-lowercase wire types, single outbox relay, `OUTBOX_POLL_INTERVAL`) are confirmed final; `workflow_event` gets a provisional monthly-partition/7-year-retention plan and `MAX_CLIENT_CONN` a provisional 10-per-replica number, both following Definition Service's own precedent and flagged as estimates pending real production data; read-replica routing is resolved (none, for now); the schema `.v2` dual-publish window is set to 30 days; inbound-DLQ alerting now has a concrete metric. Six items remain genuinely open (Compliance/legal purge scope, the cross-sibling message-correlation residual, the shared Workflow-Events Consumer's missing LLD, who calls Complete-task, multi-assignee claim-release mechanics, and row-count/growth targets) — see Appendix B. |
| 1.2 | 2026-07-20 | Closed the "who calls Complete-task" item (rev 1.1's list of six remaining open items, now five): BE-for-UI is the sole, permanent intermediary for every user-facing call into Execution — no domain service (Tender or any other domain) and no raw browser call ever reaches Execution directly. Generalizes what was previously Tender-specific, hedged language in §1.3, §3.5, and §5.6 into a settled, domain-agnostic decision (Appendix A decision 20); clarifies in §5.7 that BE-for-UI authenticates like a direct browser call (gateway headers), never via the internal-service token reserved for non-user-driven automation. The matching open sub-item on the cross-team coordination side is closed as well. |
| 1.3 | 2026-07-23 | New §3.7 (Worker Runtime: Instantiation-to-Execution Flow): the instantiation input contract, `StartWorkflow`'s `TaskQueue` selection, one-owner/replay semantics, and `SignalWithStartWorkflow`'s non-use. §3.3 states `ScheduleToCloseTimeout`'s non-use and a concrete heartbeat-revisit criterion. Appendix A.2 gained five decisions (23-27); Appendix B gained a zero-worker-on-a-task-queue alerting gap. §6.5's Dashboard Stream Gateway subscriber row states the real 12-event subset (6 task-scoped, 6 instance-lifecycle), replacing a narrower illustrative list. Every Dashboard Stream Gateway reference in this document now points to that service's own design, which specifies the SSE fan-out mechanism, connection authentication, wire format, and backfill in full. |
| 1.4 | 2026-07-23 | Production-readiness audit pass (internal consistency, security/compliance, operational readiness, API/event-contract hardening, completeness). §9.6 rewritten: narrowed the prior "no PII" claim to what's actually true (no structural PII; a real, unaddressed content risk existed in `result_json`/`payload_json` free-text fields). Designed per-user crypto-shredding to close it — new `workflow_data_keys` table (§4.12), a sixth domain table; `UserDeleted`'s existing handler (§6.2) now also revokes the erased user's key. §9.4's encryption-at-rest note updated to distinguish confidentiality (cluster-level, unchanged) from erasure (the new targeted mechanism). Appendix A.3 gained decision 11; Glossary gained an entry. Fixed a stale self-contradiction: §2.1 resolved `IOMapping`'s application timing, but §4.2 and the Glossary both still said it was open, citing the very section that resolved it — both corrected to match §2.1. |
| 1.5 | 2026-07-23 | Fixed a self-contradiction on the `/api/v1/internal/*` ingress `NetworkPolicy`: §9.2/§5.7 already stated it as resolved, but §7.5 and Appendix A.6 #11 still called it "flagged, not yet designed" — both corrected to match the resolved state (a NetworkPolicy scoped to the shared Workflow-Events Consumer and Definition Service's pod identities). The OpenAPI spec updated to match every §9.2/§5.7 claim about this route group: added the missing `InternalServiceAuth` security scheme (`x-internal-token`, mirroring Definition Service's own shipped scheme name/shape exactly) and wired it to the three `WorkflowClient` endpoints (previously `security: []`, contradicting the prose); added the `POST /api/v1/internal/events` operation itself, absent from the spec entirely despite being fully designed in §6.1/§6.2, with a new `EventEnvelope` schema. Also fixed two enum gaps in the same spec: `InstanceStatus`/`TaskStatus` were missing `DEGRADED`/`SUPERSEDED`, real states this design introduces; `AssigneeMode`'s `any` value corrected to `single`, matching the prose's own terminology everywhere else. |
| 1.6 | 2026-07-23 | Added the missing `assignee_overrides` table (§4.13), a seventh domain table: §5.4's node-override endpoint has referenced this table since it was written, but it existed in no entity inventory, no migration SQL, and no Glossary entry anywhere — code written against the prior text would not have migrated. New Appendix C `CREATE TABLE`/index/RLS policy; new §4.9 indexing-strategy row; new Entity-Relationship Summary row; new Glossary entry. Its free-text `reason` field is folded into §9.6's crypto-shredding mechanism (rev 1.4) as a third content-risk field, keyed by the acting admin's `actor_user_id`. |
| 1.7 | 2026-07-23 | The OpenAPI spec brought into line with `record_version`'s actual role (§3.1, §5.10): `pause`/`resume`/`cancel`/`force-forward`/`force-back` had no version field in their request bodies at all despite the synchronous-409 optimistic-lock guarantee depending on it; `node-override`/`reassignTask` were missing it too. All six now require `record_version` and document `RECORD_VERSION_CONFLICT` in their 409s. Task-level `claim`/`complete`/`defer` used a bare `version` field name, inconsistent with the real column name used everywhere else (prose, DB schema, every instance-level endpoint) — renamed to `record_version` throughout, including the `TaskSummary`/`TaskDetail` response schemas. Fixed `force-forward`'s field name (`target_node_id` in this document's own signal table and a worked example → `target_node_key`, matching the OpenAPI spec and every other citation). Fixed `OVERRIDE_NO_OP`'s status code split — `node-override` correctly returned `400` (matching §5.10's error table) but `reassignTask` returned `409` for the identical condition; `reassignTask` corrected to `400`. Removed a fabricated citation in `reassignTask`'s description (`TemplateEligibilityInvalidated`, an event name that doesn't exist anywhere else in this document or the real Definition Service design). |
| 1.8 | 2026-07-23 | Closed a real implementability gap in the Dashboard Stream Gateway's routing model: the event-contract documentation's own Design Decision 13 and the Gateway's own design both assert instance-scoped events "always" carry `started_by_user_id`, but the governed schemas only had it on `workflow.instance.started` — 5 of the Gateway's 6 subscribed instance events had nothing to route on. Added `started_by_user_id` to `.paused`/`.resumed`/`.cancelled`/`.terminated`/`.finished` in the AsyncAPI spec and the event-contract documentation. Also reconciled §6.4's own per-event Consumers column with §6.5 — 8 of 18 rows (the same 5 instance events plus `.started`/`.reassigned`/`.sla-breached`) were missing "Dashboard" despite §6.5 and the Gateway's own subscription list already including them; both revised the same day (rev 1.3) but never cross-checked against each other until now. |
| 1.9 | 2026-07-23 | Closed an intra-tenant BOLA gap (§9.2, Appendix A.4 #15): RLS scoped every query to the caller's tenant, but nothing beyond that scoped `GET /tasks/:id`/`GET /instances/:id` — any authenticated tenant member could read any other member's approval comments and assignment history. New rule: visible only to an assignee (current or past), a same-department caller (`x-departments`), or an admin; list endpoints filter silently, detail endpoints return the new `403 NOT_AUTHORIZED_FOR_RESOURCE` (§5.10) otherwise — not `404`, since the resource already exists within the caller's own tenant. The OpenAPI spec updated to match: both detail endpoints gained the `DepartmentsHeader` parameter (previously defined but never referenced by any operation) and the new `403` response; both list endpoints' descriptions note the same filter. |
| 1.10 | 2026-07-23 | The OpenAPI spec brought into line with §5.9's own keyset-pagination decision, which no list endpoint actually implemented: `GET /instances`/`GET /tasks` used offset `page`/`limit` + `{page, limit, total, has_more}` — exactly the contract §5.9 says was deliberately abandoned; `GET /instances/:id/events` used a third, ad-hoc `limit`/`before`-timestamp/`has_more` shape; `GET /workflows/active-by-user` and `GET /api/v1/internal/workflows/delegate-impact` had no pagination at all despite returning unbounded arrays. All five now take `cursor`/`limit` (default 25, max 100) and return `items[]`/`next_cursor`, matching §5.9's worked example exactly; the offset-only `PageQuery` parameter and `Pagination` schema are removed, replaced by `CursorQuery`. `delegate-impact`'s `reassigned_count` stays an unpaginated true total (a cheap `COUNT`) alongside the now-paginated `workflow_ids` preview list. |
| 1.11 | 2026-07-23 | Continued production-readiness audit fixes. Fixed the OpenAPI spec's payload-size mismatch (`PayloadTooLarge` said 5 MB; §5.10/§9.3 say 10 MB, matching Definition Service — the OpenAPI number was stale). Restored two items to Appendix B that had silently disappeared with no revision-history explanation despite still being genuinely open (`TenantOffboarded` purge scope; multi-assignee claim-release mechanics, confirmed still-open per `is_lead`'s own "not yet fully enforced" text, §4.4) — this document's own rev 1.1 arithmetic (6 items named as remaining) never matched the 4 rows the table actually carried since. Added the two missing rows to §10.2's Dependency Failure Matrix (the shared Workflow-Events Consumer; Temporal Advanced Visibility/Elasticsearch) that this document itself establishes as real dependencies but never gave a failure-mode entry. Resolved the `.v2` dual-publish window's cross-doc disagreement — the event-contract documentation said "not yet defined," this document already said 30 days (rev 1.1); both now agree. |
| 1.12 | 2026-07-23 | Four more production-readiness gaps closed. **API versioning** (§5.9, Appendix A.4 #16): adopted path-based `/api/v2`, matching Org & Membership's own already-established platform convention exactly, closing the asymmetry with the event side's fully-worked `.v2` mechanism. **Worker `StopTimeout`** (§7.6): set to 25s, giving 5s of margin under the 30s SIGTERM budget the doc already named as a constraint with no number to check it against. **Trace-context propagation** (§7.6): named the actual mechanism for the one hop that was silently unconfirmed — the Temporal Go SDK's own OTel interceptor (`go.temporal.io/sdk/contrib/opentelemetry`), registered on both the API process's Temporal client and the Worker, since Temporal does not auto-propagate W3C trace context across `SignalWorkflow`/`StartWorkflow` → Activity without it. **`task-claim` audit trail** (§3.1, §6.4): added `ClaimAssignmentActivity` and the new `workflow.task.claimed`/`TASK_CLAIMED` event — confirmed via the stage docs' own history that Execution's `is_lead` (multi-assignee claim mechanism, reserved since Stage 3) is a real, standing decision, unrelated to IAM's own, different, confirmed-obsolete `dept_memberships.is_lead` (same word, two systems, already disambiguated in §4.4) — `task-claim` was the one signal in the whole catalogue with no corresponding Activity, DB write, or audit event. |
| 1.13 | 2026-07-23 | Three more production-readiness gaps closed. **SLO instrumentation** (§10.3, §7.6): only the delegation-reroute SLO had a named metric; added `task_signal_duration_seconds` (claim/complete/defer, p99 ≤ 300ms), `instance_start_duration_seconds` (p99 ≤ 1s), a stated availability-SLO measurement mechanism (external `/readyz` uptime checks), and cross-referenced the dashboard-push SLO to its real owner, the Dashboard Stream Gateway, rather than leaving it disconnected from any metric. **Load/performance testing** (new §7.8, Appendix A.6 #12): no process existed anywhere despite several capacity numbers explicitly deferring to "revisit once real load data exists" — added a k6-based staging load-test gate, re-run whenever a capacity-relevant number changes, that validates those numbers and every §10.3 SLO together; the specific pass/fail thresholds and traffic profile are left for a first real run to produce, not guessed here. **Schema-governance CI diff baseline** (§6.8): states that `schema-gov diff` compares against the live, currently-registered Glue schema version, not the file's prior git state — closes a real gap where an out-of-band registry change or a skipped CI run could otherwise drift undetected. |
| 1.14 | 2026-07-23 | Four writing-quality gaps closed (no new design decisions — all connect or narrate facts already established elsewhere). **DSL backward compatibility** (new §2.5 paragraph): explicitly states the argument that was only ever implicit — "compiled plan fetched once, held for the workflow's lifetime" (step 1) is exactly what makes an in-flight instance immune to a later re-publish, with the DSL major-schema-mismatch fail-closed check as the one deliberate, non-retryable exception. **Go-live checklist** (new Appendix B.1): a 14-row ship/no-ship rollup distinguishing hard blockers from already-accepted residuals, consolidating commitments already made across §3, §4, §7, §9, and Appendix B rather than introducing new requirements. **Cost acknowledgment** (new §10.4): named the three real, accepted cost multipliers this design commits to — unlimited-retry DB-writing activities, mandatory Elasticsearch for Advanced Visibility, and per-tenant dedicated task queues — each already a design decision elsewhere, never previously gathered into one place for a capacity-planning reader. **Compound worst-case walkthrough** (new §8.5): narrated the `DEGRADED` × tenant-suspension × force-back interleaving end-to-end (the same combination §7.2 test #5 already exercises at the test level), showing three independently-designed rules — sibling-branch survival, signal-validation-level pause rejection, and force-back's save-not-discard semantics — compose correctly with no fourth mechanism needed. |
| 1.15 | 2026-07-23 | Mechanical audit cleanup, final pass. **§1.1/§1.3**: fixed two citations of the database-per-service exception pointing at §7.6 (Operational Conventions, unrelated) — corrected to §4.1/§5.1, where the exception is actually explained. **§4.9**: added the missing `processed_event` index row (the 7-day TTL prune's own access pattern); removed a redundant, functionally-identical `CREATE UNIQUE INDEX` on `active_task_queues.queue_name` that duplicated the column's own `UNIQUE` constraint (Appendix C). **§5.9**: stated `Idempotency-Key`'s Valkey key shape and 24-hour TTL, and its fail-open behavior on a Valkey outage (logged `WARN`, request proceeds without dedup) — both previously unstated, both now matching Definition Service's own shipped convention exactly rather than being re-derived. **Appendix D**: added three missing Glossary entries (`BE-for-UI`, `port.WorkflowClient`, `ActiveUserTask`) used throughout the document with no formal definition. **The OpenAPI spec**: added the missing `instance` field to 12 example error responses (a copy-paste gap); wired `INSTANCE_ALREADY_TERMINAL` to `cancelInstance`/`terminateInstance`'s 409s (both had been using the more generic `INVALID_INSTANCE_STATE` for a condition the error-code table already gives its own dedicated code) and `CLAIM_NOT_APPLICABLE` to `claimTask`'s 409 (previously listed in the code enum but never wired to any real response); fixed `override_map`'s response schema (an untyped, inline `additionalProperties: true` object) to `$ref` the same typed `OverrideMap` schema its own request body already uses; added the missing `FeatureFlagsHeader` (`x-feature-flags`) parameter component, matching Definition Service's own shipped `FeatureFlags` component for documentation parity with the standard gateway header set. Three residual items reviewed and accepted as-is, no change needed: `membership-wf-q`/`membership-workflow-q` are two distinct, correctly-named real queues; `GET /readyz`'s bespoke (non-`ProblemDetails`) 503 body is a standard, justified exception for a health-probe endpoint; `reassignTask`'s lack of an eligibility check on the new assignee is already explicitly documented as a deliberate admin-override behavior, distinct from node-override's own eligibility-checked path. This closes every item raised across the three-pass production-readiness audit (Tier 1, Tier 2, Tier 3) run against this document. |
| 1.16 | 2026-07-23 | Second integrity/gap sweep (3 fresh read-only audit passes: post-fix consistency re-check, new-category production-readiness gap hunt, cross-doc/stage-doc sync check). Fixed 2 dangling citations introduced in rev 1.14 (§2.5's `§2.5.3`→`§8.2.6`; the `BE-for-UI` Glossary entry's missing `§1.3` citation). Closed 6 fresh production-readiness gaps, all confirmed with the user, several researched against IAM's own PII/soft-delete precedent first (finding: IAM has no crypto-shredding/KMS-DEK mechanism and no Temporal-equivalent durable-history store, so two of these were genuinely new work, not something to copy): **KMS** added to the Dependency Failure Matrix (§10.2), a new `kms_operation_errors_total` alert (§7.6), a KMS-throttle e2e test (§7.2), and an explicit note that crypto-shredding needs its own dedicated CMK (`KMS_KEY_ID`), never IAM's S3 signature-image key (§4.12, Appendix B.1 rows 15-16). **Alerting/metrics expanded substantially** per explicit direction — 13 new metrics and 14 new alert rules added to §7.6 covering the Shared Workflow-Events Consumer outage gap §10.2 itself had flagged, RLS violations (matching Definition Service's own convention), the two SLO metrics that existed but were never wired to an alert, Temporal replay failures, `DEGRADED` backlog age, per-queue polling health, per-activity-type failure rate, SLA-breach volume, stuck/abandoned tasks, upstream-dependency-labeled errors, compiled-plan cache hit ratio, and the original wedged-outbox-relay gap; an Availability-SLO burn-rate alert is explicitly deferred to Appendix B as its own open item rather than guessed at. **k6 load-test gate** (§7.8) gained a rollback-during-load scenario, testing the exact PgBouncer-absorption assumption §9.7 states but never previously exercised. **Tenant-status pre-check** added to instantiation (§5.5, §8.1 step 3): `POST /instances` now rejects with new code `409 TENANT_NOT_ACTIVE` unless the tenant is `active`/`trial`, closing the gap where `TenantStateChanged`'s reactive pause/terminate handlers never covered a *new* instance being created for an already-suspended/offboarded tenant (the OpenAPI spec updated to match: new example + enum entry). **Crypto-shredding's Temporal-history residual** closed in two parts (§9.6, §4.12, Appendix A.3 #12): the pause/cancel/defer signals' own free-text `reason` field, previously missed, is now in scope; a new custom Temporal `DataConverter`, registered on both the API client and the Worker, encrypts content-risk payloads before Temporal's own history persists them, closing the gap where DEK revocation reached Postgres but not Temporal's own durable signal history. **Contract-drift safety net** added as new §6.9: a `buf` breaking-change lint CI job for the shared `GetCompiledWorkflow` proto, plus a documented named-owner stability commitment for the IAM eligibility-check contract, since only the outbound SNS event schema previously had any CI diff gate (Appendix A.4 #17-18). Companion fixes back-propagated into the underlying stage docs and the platform HLD. |
| 1.17 | 2026-07-31 | T1.2 (persistence layer + outbox relay) implementation pass — three documentation fixes surfaced by building real code against this text for the first time. **RLS-violation audit logging** added as new §9.5 content (additive to T1.2's own ticket, ported from `iam-user-profile`'s `rls_violation_log`/`log_rls_violation`/`rls_check_tenant` pattern, `db/migrations/000006_rls_audit_logging`): every tenant-scoped policy swaps its bare `tenant_id = app_tenant_id()` comparison for `rls_check_tenant(...)`, which returns the identical boolean but additionally 1%-samples a `missing_or_invalid_guc`/`cross_tenant_access` row into a new, RLS-disabled `rls_violation_log` table. **§4.5/§4.9's `outbox_events` JSONB paths corrected**: both expression indexes and every prose reference said `payload->>'workflow_instance_id'`/`payload->>'task_id'`, assuming `payload` holds the flat event data — but `platform-events`' `outbox.Enqueue` `json.Marshal`s the *whole envelope* into that column, so these fields actually live under the envelope's own `data` key (`payload -> 'data' ->> 'workflow_instance_id'`). As shipped, neither index had ever matched a real query; this is the first task to author one against them. **§4.8's self-contradiction with §9.2/§9.7 resolved**: §4.8 stated RLS uses "no `BYPASSRLS` role anywhere," while §9.2 and §9.7 both explicitly describe the outbox relay's own `BYPASSRLS` connection role — §4.8 reworded to state the RLS policy expression itself doesn't special-case any role, without contradicting the relay role's existence. |
| 1.18 | 2026-07-31 | **Crypto-shredding removed** — the business requirement it addressed no longer applies. §9.6 (PII and Data Classification) reverted to a "no PII stored" position: this schema's free-text fields (`result_json`, `assignee_overrides.reason`, signal `reason` fields) carry workflow business content, never identity data — any personal data a caller needs is fetched from IAM at read time, never persisted here, so no per-user erasure mechanism is required. §9.4's encryption-at-rest note simplified to match (cluster-level encryption only, no custom `DataConverter`). Removed entirely: §4.12 (`workflow_data_keys`), its entity-inventory/lifecycle-table rows, its Appendix C `CREATE TABLE`/RLS policy, its Glossary entry, the `kms_operation_errors_total` metric and its alert (§7.6), the `KMS_KEY_ID` env var (§7.6), the AWS KMS row in the Dependency Failure Matrix (§10.2), the KMS-throttle e2e test (§7.2), `UserDeleted`'s DEK-revocation clause (§6.2), and Appendix A.3 decisions 11–12 (both were the list's last two entries, so no further renumbering was needed there). `assignee_overrides` renumbered from §4.13 to §4.12 to close the gap; every citation to it elsewhere in this document updated accordingly. Appendix B.1's Go-Live Checklist rows 9, 15, and 16 (all crypto-shredding-specific) removed, with rows 10–14 renumbered to 9–13. **Not addressed here**: the `workflow_data_keys` table and its RLS policy already merged into `execution_service`'s `main` branch as part of Tier-0 remain in place as now-unused schema — authoring a migration to drop them is a separate decision left to whoever owns the next schema change, not assumed in this revision. |
| 1.19 | 2026-08-01 | T1.10 (outbox event catalog + schema governance) implementation pass. **§6.8 corrected**: the prior text stated the wire format carries an 18-byte Glue header ahead of the CloudEvents envelope JSON, implying `GlueCodec` prepends that binary header to the published envelope — as shipped in the reference services this section was modeled on (`definition_service`, `iam-user-profile`), doing so makes the envelope's `data` field invalid JSON, breaking `outbox.Enqueue`'s own marshal step (confirmed by direct reproduction). Corrected to state `GlueCodec`'s actual, load-bearing role: resolving each event type's latest registered Glue schema version at encode time (failing closed if missing from the registry) for schema-governance purposes only, never applying wire-level binary framing to the envelope itself. `NoopCodec` (dev/test) is unaffected. **SNS topic name corrected** to `wf-workflow-events` (hyphenated) throughout §1.5, §6.5, §6.6, §6.8, and §10.2's dependency table — the prior dotted `wf.workflow.events` disagreed with the authoritative `api/asyncapi.yaml` draft (`design/LLD/execution_asyncapi.yaml`) and with the platform's own established naming convention (`iam-user-profile`'s `iam-user-events`, hyphenated, used identically for both the SNS topic and the Glue registry name). **§6.4's `workflow.instance.terminated` row gains an explicit `initiator` enum** (`admin`\|`tenant_state`), matching the parenthetical style `.paused`/`.resumed` already had — `iam-user-profile` (the only other service with an `initiator`-shaped field) has no equivalent event or enum to cross-check against, so this is a fresh decision, not a ported one. |
| 1.20 | 2026-08-04 | T1.4 (gRPC adapters) implementation pass. **§5.3 corrected**: the prior text said both directions of the gRPC surface reuse `grpccommon.FullUnaryInterceptors`/`FullStreamInterceptors` + `grpccommon.RequirePermission`, a per-user action/resource authorization model — but this directly contradicted §5.7/§9.2, which already state the same two inbound RPCs are service-to-service traffic authenticated via the `x-internal-token`/`INTERNAL_API_TOKEN` convention. Building against the text surfaced why: `RequirePermission` needs gateway-injected `x-user-id`/`x-tenant-id`/`x-tenant-roles` metadata from an authenticated end user, which Definition Service's real, already-shipped outbound client never sends on this machine-to-machine path — following §5.3 literally would have rejected every real call before the handler ever ran. §5.3 now describes what's actually implemented: an observability-only interceptor chain (matching, not exceeding, Definition Service's own shipped setup) plus a dedicated `x-internal-token` gRPC-metadata interceptor adapting §5.7/§9.2's existing HTTP convention, exempting `grpc_health_v1.Health/Check` and gRPC reflection. New Appendix A.4 decision #19 records the rationale. |
| 1.21 | 2026-08-04 | T1.5 (API Instances) implementation pass — two self-contradictions in §5.10's error-code catalogue, surfaced by building the six lifecycle-signal handlers and `POST /instances` against this text and the OpenAPI spec together. **`TARGET_NODE_NOT_FOUND` corrected from 404 to 409**: this table said 404, but the OpenAPI spec's `forceForward` operation already grouped it with the other pre-check-driven 409s (`INVALID_INSTANCE_STATE`, `RECORD_VERSION_CONFLICT`) — 409 is also consistent with §5.9's own rule that `*_NOT_FOUND` status semantics are reserved for path-addressed `:id` lookups, not body fields like `target_node_key`. **`OVERRIDE_MAP_INVALID` corrected from 400 to 422**: this table said 400, but the OpenAPI spec's `startInstance` operation already documented it as 422 — 422 is also consistent with §5.5's own `ASSIGNEE_INELIGIBLE` (422), the sibling instantiation-time validation failure. The OpenAPI spec's `startInstance` 422 block gained a second example (`ASSIGNEE_INELIGIBLE`, §5.5) alongside the existing `OVERRIDE_MAP_INVALID` one — previously undocumented at the operation level despite being required by §5.5's own prose. `GET /instances/:id/events` gained a documented `403 NOT_AUTHORIZED_FOR_RESOURCE` response, applying the same intra-tenant visibility rule §9.2 already gives its sibling `GET /instances/:id` — this endpoint is a sub-resource of the same instance and was previously silent on the check entirely. `InstanceSummary`'s schema gained `record_version` and `saved_node_keys` (both real, already-populated columns, shown in this document's own §5.2/§5.9 worked examples, but absent from the schema itself) — every lifecycle-signal endpoint requires the caller to submit `record_version`, and the schema previously gave no documented way for a client to learn it. Fixed the OpenAPI spec's three inline `INVALID_INSTANCE_STATE` examples, which used the word-order-swapped slug `instance-state-invalid`, to the codebase's own established convention (`invalid-instance-state`, matching `CodeInvalidTaskState`'s `invalid-task-state`). |
| 1.22 | 2026-08-04 | Full-branch review pass across `feat/api-tasks` (T1.4–T1.8) — one residual doc-text fix, surfaced alongside several code-only bugs (missing admin gates on `reassignTask`/`getActiveTasksByUser`, an unscoped-per-tenant recency-guard key, and four routes never wrapped in `WithIdempotency`) that didn't need a doc change since this document already specified the correct behavior. **Fixed the OpenAPI spec's three `INVALID_INSTANCE_STATE` example `title` fields**, which still read `"Instance State Invalid"` (rev 1.21 fixed the `type` slug's word order but missed the parallel `title` string) — now `"Invalid Instance State"`, matching the codebase's own established `title` convention for every other multi-word code (`"Invalid Task State"`, etc.). |
| 1.23 | 2026-08-04 | Resolved a real three-way disagreement (this document's own §5.6 prose, the OpenAPI spec, and the shipped code) over what `claim`/`complete`/`defer`/`reassign`/`overrideNodeAssignee` actually return — decided in the shipped code's favor, since it already returns strictly more information than either doc claimed, and nothing has been built against either doc's version yet. New OpenAPI schemas `TaskActionResult` (the mutated task — a narrower `TaskSummary` that omits `tenant_id`/`due_at`/`deferred_from_task_id`, none of which the handler currently populates) and `OverrideResult` (the persisted override record), used in place of the previous `SignalAccepted`-derived bodies for all five operations. Two real, accepted information-loss gaps surfaced in the process, left unfixed for now since fixing them means changing the handler, which the code side of this decision deliberately did not do: `completeTask`'s response has no `all_done` flag (caller must read `status` instead); `deferTask`'s response has no field carrying the newly-created regression task's ID at all. `overrideNodeAssignee`'s status corrected from the documented `202` to its actual `200` — it already does all its work (validate → eligibility → persist → signal) synchronously before responding, unlike the true signal-forwarded endpoints. §5.6's own worked `CompleteTask` response example (a third, `completed_by_user_id`-bearing shape, matching neither the OpenAPI spec nor the code) replaced with the actual returned shape. Also fixed, while in the same schema: `TaskSummary`'s `required` list still said `version` (a stale artifact rev 1.7 was supposed to have renamed throughout but missed here) — now `record_version`. `getActiveTasksByUser`'s response field renamed from `assignments` to `items`, matching this API's own established list-wrapper convention on every other paginated endpoint. |
| 1.24 | 2026-08-07 | Full doc-vs-code sync pass following the `platform-events` v1.4.0 upgrade and a broad audit against the shipped `execution_service` codebase. **GlueCodec** (§6.1, §6.8): now correctly describes real Glue wire-format framing applied at SNS-publish time via `events.WithCodec` (v1.4.0), superseding the old "never prepends the header" deviation. **§5.10 RFC 9457**: rewritten to describe Execution's own actual convention (`application/problem+json`, `https://errors.bcbp.io/execution/<slug>`) instead of a false claim of identity with Definition Service; `EVENT_DECODE_FAILED`/502 added to the code table; `invalid_params` corrected to say the field exists on the struct (added this pass) but isn't populated by any handler yet, rather than claiming it doesn't exist at all. **§4.2/§4.4/§4.11**: `created_at`/`updated_at`/assignment `updated_at` corrected from "trigger-maintained" to the actual mechanism — bumped per-statement in application SQL, no database trigger exists. **§6.6/§7.6/Appendix A.5 #15**: `OUTBOX_POLL_INTERVAL` corrected from a claimed "5s platform-library default" to the actual 500ms, app-configurable default (matching Definition Service); no dedicated outbox `MaxAttempts` config field exists in either service. **§5.6**: the `complete` worked example (shared by claim/complete/defer/reassign) updated to include `tenant_id`/`due_at`, matching `toTaskResp`'s now-complete field population; its `TaskActionResult` schema reference updated to `TaskSummary` (the two schemas were merged in the OpenAPI spec — `TaskActionResult` had fallen stale, claiming to omit fields the handler now populates). **§5.2**: `GET /tasks`'s endpoint-table row now names its actual filters (`status`, `department_id`, `instance_id`, `assignee_user_id`, `due_before` — the last two newly added this pass). Also reconciled `execution_asyncapi.yaml`/`execution_openapi.yaml` against the shipped `execution_service` copies (region corrected to `ap-south-1`; SQS binding structure, `x-lifecycle` annotations, and a real `WorkflowInstanceResumedPayload.initiator` enum bug fixed to match the shipped, code-verified files) and checked every nullable column against a real described null-scenario rather than just mirroring current DB/Go state — `AssignmentRecord.assigned_at`/`TaskSummary.department_id` corrected to required+non-nullable (no null scenario exists for either; the DB/Go side lagging this is tracked as a follow-up, not yet fixed). Confirmed and left unchanged: `workflow_task_assignment.is_lead` is unrelated to IAM's now-fully-reverted `dept_memberships.is_lead` (§4.4's own disambiguation note, rev 1.12, already covers this — re-confirmed, no new action needed). |
| 1.25 | 2026-08-10 | T1.11 (Observability & Metrics) implementation pass — filled in three details this document named but never specified, surfaced by building `internal/observability` (centralized Prometheus metrics, OTel tracing-init helpers, Temporal Search-Attribute helpers) against this text for the first time. **§7.6 histogram buckets**: none of the four latency histograms had stated bucket boundaries; added custom boundaries for the three SLO-backed ones, tuned to each one's own §10.3 p99 target for finer `histogram_quantile` resolution near its alert threshold (`task_signal_duration_seconds`, `instance_start_duration_seconds`, `delegation_reroute_duration_seconds`); `workflow_activity_duration_seconds` (no stated SLO) uses Prometheus's stock `DefBuckets`. **§7.6 env-var table**: added `OTEL_SERVICE_NAME`/`OTEL_EXPORTER_OTLP_ENDPOINT`, previously undocumented despite §7.6's own prose already referencing "the existing OTel initializer" — tracing setup is `platform-gincommon`'s `InitTracingFromEnv`, gated by a caller-side no-op guard (mirroring `iam-user-profile`'s `cmd/server/main.go`) that skips exporter construction entirely when the endpoint is unset, since `platform-gincommon`'s own default (`localhost:4317`) is never empty and would otherwise always attempt a connection. **§3.6**: named the concrete API choice for the four Search Attribute helpers — Temporal's newer typed `SearchAttributeKeyKeyword`/`UpsertTypedSearchAttributes`, not the older untyped map form, for compile-time key/type safety on a fixed four-attribute set. This task defines the instrumentation surface only (metric vars + one `Register()`, tracing-init helpers, Search-Attribute constants + a typed upsert helper); it does not add `.Observe()`/`.Inc()`/`UpsertSearchAttributes(...)` call sites into any other package, and does not wire a composition root — both remain later tasks' jobs, consistent with §3.6's own rule that Search Attribute upserts must originate from workflow-context code. |
| 1.26 | 2026-08-07 | Automatic connector tasks (new `automatic_connector_tasks.md`, a standalone cross-service design) — this document's own footprint of that change. **§3.1**: `CreateTaskActivity`'s "Used by" column now includes connector-typed automatic tasks; a new "Explicitly out of scope here" note clarifies a connector's actual execution is never this service's own code. **§4.1/§4.3**: `workflow_task`'s Entity Inventory description and its own column table gain a nullable `connector_type` column (a real column, not `extras_json`, so the owning domain service can filter on it efficiently) plus a supporting partial index. **§6.4**: `workflow.task.created`'s payload gains an optional `connector_type`, and the owning domain service is added as a named consumer for connector-typed tasks; `exec_eventschema/workflow_task_created.json` updated to match. No Activity, retry-policy class, or interpreter dispatch branch is added by this change — see the new document for why. |
| 1.27 | 2026-08-11 | Connector worker/catalogue design deepened into new `workflow_connectors.md` (worker runtime, credential-fetch callback API, full v1 catalogue of 7 connector types) — this document's footprint reworded only, no structural change. **§2.4/§3.1**: "the owning domain service" now reads as "`workflow-connectors`, calling back into the owning domain service for tenant-specific credentials/content" — reflecting the reopened placement decision (`workflow_connectors.md` Decision #8); this service's own dispatch table and Activity catalogue remain unchanged and agnostic to where that runtime ends up (still an open question, `workflow_connectors.md` §6.1). **§4.3**: `connector_type`'s description cross-references `workflow-models`' new `StageDef.ConnectorType` field (`workflow_models_lib.md` §2.3) as the field it mirrors. |
| 1.28 | 2026-08-11 | `automatic_connector_tasks.md` consolidated into `workflow_connectors.md`; the former deleted. Every citation into it throughout §2.4/§3.1/§4.3/§6.4 and Appendix C's DDL comments repointed at `workflow_connectors.md`'s own (renumbered) sections — no runtime behavior changed, citation-only pass. |
| 1.29 | 2026-08-12 | Two changes, both citation/wording-only, no runtime behavior change. **§9.2**: `x-departments`'s format stated explicitly for the first time in this document — `<department-uuid>:<role>` pairs, per `definition_service.md` §3.1 — the intra-tenant read-scope check now names which half of the pair it compares against `department_id`. **§2.4/§3.1/§4.3/§6.4/Appendix C**: "the owning domain service" reworded to `cmd/connector-worker` throughout, reflecting `workflow_connectors.md` rev 4.0's resolved worker placement (a binary inside this service's own repo) and removed credential-callback API — this service's own dispatch table and schema were already placement-agnostic and needed no structural change, only wording that no longer implies an external domain-service caller. |
| 1.30 | 2026-08-12 | Synced §6.2's delegation-event documentation against IAM's current schema (`org_membership_lld_v12.md`, rev 1.73, formerly `iam-lld-org-membership-v1_6.md`) — confirmed `DelegationStarted`/`DelegationEnded`'s own payload fields already match exactly, no discrepancy. Added a note that IAM's envelope has since gained `ip_address`/`user_agent` (rev 1.68, additive, no handler change) and uses `actor`/`actor_id` naming this document doesn't currently have an equivalent field for. IAM's own new 90-day delegation-review-window feature (`DelegationReviewRequested`, new endpoints) is correctly absent here — IAM's own routing table doesn't subscribe this service's queue to it. |
| 1.31 | 2026-08-12 | `workflow_connectors.md` rev 6.0's biggest change — connector tasks are now fully automation-only, no human fallback — lands its one real footprint here: a **new `stage-fail:{instanceID}` signal** (§3.1), sent only by `cmd/connector-worker`, feeding the same `FAILED`/`DEGRADED` transition logic a non-retryable Activity error already drives. §2.4's dispatch-table row and §3.1's "Explicitly out of scope here" note updated to describe both outcomes (`stage-transition` on success, `stage-fail` on failure/unregistered type) instead of a human fallback. §1.6 notes `cmd/connector-worker` as a third process with its own new Valkey dependency (a Stream consumer, `workflow_connectors.md` §6.5) — distinct from `cmd/server`'s existing cache/idempotency usage and `cmd/worker`'s continued lack of any Valkey dependency, both unchanged. |
| 1.32 | 2026-08-13 | BE-for-UI scope review: confirmed its full responsibility set for the first time in this live document (§1.3, Glossary, Appendix A.2 decision 31) — business-action intermediation + response enrichment (already documented) plus ownership of a "custom BPMN module"/"reusable authoring component" library in its own database (previously stated only in an archived orientation doc, never carried into this LLD). Explicitly excluded connector-authoring templates/credential custody, reassigned to Definition Service (`workflow_connectors.md` §4.3/§6.2, `definition_service.md` §10.14/§10.15) — fixed, code-generated catalogue metadata, not a new stored-entity type, so it doesn't need BE-for-UI's own database the way the module library does. §9.4's secrets mechanism renamed from generic "Secrets Manager" to OpenBao, the platform's now-settled choice, replacing AWS Secrets Manager everywhere in this document and in `workflow_connectors.md`. |
| 1.33 | 2026-08-13 | IAM sync check against the newly-updated `iam_1.41.md` (a genuine HLD v1.41 this time, confirming the `x-departments` `dept_uuid:role` format independently documented in `definition_service.md` §3.1 — no change needed here beyond the citation still resolving correctly). Three new Appendix B rows added, none of them resolved by this pass: delegation chains (A→B→C) and mid-chain delegate deletion are undesigned on both the IAM and Execution sides; task actions (claim/complete/defer) never re-verify live OOO/delegation/deleted status synchronously, only reactively via the existing event handlers; `delegate-impact`'s delegate-centric (not delegator-centric) scoping is real but was never stated as a deliberate rule. |
| 1.34 | 2026-08-13 | T1.3 (Worker Runtime) implemented — all 15 Activities plus `cmd/worker` wiring, `internal/adapter/outbound/temporal` (no code-level footprint on this document itself; §3.1's activity catalogue and §3.7's flow were already accurate). One new Appendix B row from a fresh IAM cross-check done alongside the implementation: the already-tracked `department_id` display-slug gap (§4.3) turns out to functionally break every already-shipped consumer of IAM's `(department_id, required_level)` eligibility contract — node-override, delegation reroute, instantiation-time re-validation — not just a future cosmetic/dashboard concern as previously implied. No other IAM-facing conflict found; the `x-departments` UUID format (HLD v1.41) and the eligibility-endpoint contract (§5.4) are both already correctly reflected here. |
| 1.35 | 2026-08-21 | **BE-for-UI retired** (§1.3, §1.5, §3.5, §5.6, §5.7, §5.9, §4.9, Appendix A.2 decisions 19/20/31, Glossary). Direct code/doc verification (platform-libs' `RequireAuth()` only checks gateway-header presence, not caller identity; this document's own §1.5 diagram already drew the browser calling Execution directly, contradicting decision 20's prose; `IAM/approver-approval-signature-workflow.md` already specifies Tender Service calling Workflow/Execution directly; `IAM/tender-service-approval-sequencing-sync.md`'s own revision history shows Tender's confirmation of the BE-for-UI redirect was never actually obtained) found none of BE-for-UI's four responsibilities required a dedicated intermediary. Resolution: the browser calls Execution's business-action and read endpoints directly through the API gateway (new Appendix B item: Execution needs its first `deploy/helm` chart, mirroring Definition Service's/`iam-user-profile`'s proven `HTTPRoute`+`SecurityPolicy`+`NetworkPolicy` template); Tender Service calls Execution's `Complete` endpoint directly itself, immediately after its own signing step (§3.5) — confirming this with Tender Service is a cross-team action outside this document's scope; `IAM/tender-service-approval-sequencing-sync.md` (a Tender-Service-facing artifact, not this team's to unilaterally revise) is left unchanged; UUID-to-display-name enrichment is frontend-only against IAM directly, no backend layer; the module/starter-workflow library is absorbed into Definition Service instead of a new service's database. `workflow_management_service.md` (the placement doc proposing a consolidated "Management Service" for these responsibilities) is retired/marked historical; `dashboard_stream_gateway.md` (real-time dashboard push, the one responsibility that was always independently justified) is unaffected. A separate, unrelated observation about `NetworkPolicy`/`ext_authz` scope made while researching this — outside this document's own authority to characterize — is raised as an open cross-team question in `IAM/networkpolicy-header-trust-gap.md`, not asserted here. |
| 1.36 | 2026-08-25 | **Appendix B's `department_id` display-slug gap (row from rev 1.34) RESOLVED** — Definition Service's compiler fix landed (`ValidateDeptID` mandates a real IAM UUID on every `UserTask`/`SendTask`/`ReceiveTask` lane, surfaced as `DepartmentDef.IAMDepartmentID`); Execution's placeholder-UUID scheme (`CreateTaskActivity`, node-override, delegation-reroute, instantiation-time re-validation) is deleted, not bridged — this column was always specified as `uuid NOT NULL` with no interim state (§4.3), so there was nothing to migrate. A related `definition_service` bug found in the same pass — `mergeDepts` dropping a `CallActivity`-flattened department's real ID during the merge — was fixed alongside it. One new, narrower Appendix B row added for a case the original gap didn't cover: a fully laneless called sub-process's `UserTask` has no `dept_id` validation at all, so it can legitimately reach Execution with no real IAM identity; Execution treats that as `uuid.Nil`, not a synthetic hash. |
| 1.37 | 2026-08-28 | **`POST /api/v1/internal/events` gains 4 type-scoped subpaths** (§6.1) — `/delegation`, `/user-profile`, `/tenant`, `/workflow-template` — the shared Workflow-Events Consumer now posts execution-bound events directly to whichever subpath matches; the flat `/api/v1/internal/events` path stays registered as the catch-all for `workflow.task.created` and any future/unrecognized type, so nothing is a breaking change. Bundled with removing `TemplateCachePrewarmer` (§6.2's `workflow.template.published` row and algorithm step 5, §8.1, §8.4, Appendix A.5 decision 19 marked superseded): the eager cache-warm's `workflow_key → version_id` map turned out to have no reader anywhere in the codebase, and `InstanceService.Start`'s own compiled-plan cache-aside read (§5.1) was found to be read-only — never writing back on a miss — which would have left it permanently cold once the eager populator was gone; fixed to write through on a miss as part of this same change, so the cache still self-warms with the prewarmer removed. Both `execution_openapi.yaml` and the in-repo `docs/swagger/openapi.yaml` copy gain the 4 new `POST /events/{category}` operations, mirroring how `POST /api/v1/internal/events` itself was originally documented (rev 1.5). |
| 1.38 | 2026-08-29 | **`workflow.template.published` removed entirely, platform-wide** — not just the dead cache-prewarm consumer behavior rev 1.37 already closed, the event itself: producer (`definition_service` `feat/service`, commit 60c7db5 — `VersionService.Publish()`/`Promote()` no longer build or enqueue its payload), consumer (this service's internal-events handler, its `/events/workflow-template` route, `internal/eventschema/workflow_template_published.json`), and both services' AsyncAPI/OpenAPI entries. §1.3's cache-aside citation, §6.1's type-scoped-subpath count (now 3, not 4: `/delegation`, `/user-profile`, `/tenant`), §6.1's queue set (the `wf.template.events`/`workflow.template.published` row dropped), §6.2's event catalogue (row removed) and handler-algorithm list (step removed, `UserAvailabilityChanged`'s recency-guard step renumbered 6→5, its own cross-reference in Appendix A.5 decision 25 updated to match), §6.3's dedup-consumer-string list (`template-sync-execution` dropped), §8.1's happy-path walkthrough (its own step 1 removed, the rest renumbered 2-9→1-8), §8.4's lifecycle flowchart (the `TP` node and its dotted edge into instantiation removed), and the §1.5 topology diagram (the `Def -- "workflow.template.published" --> Consumer` edge removed) are all updated to match. Appendix A.5 decision 19 — rev 1.37 had marked it "superseded" for the cache-prewarm removal alone — rewritten to record the event's full retirement; decision 6 (template-sync consumption) and decision 11 (wire-naming-convention citation) updated to stop citing a now-nonexistent event. Also bundled, from the same cross-repo change: execution_service's own HTTP router consolidated to one `router.go` per service (the prior `internal/adapter/inbound/http/router.go` + `internal/adapter/inbound/http/handler/router.go` split is gone), and event_consumer's forwarding to this service moved from a single flat `/api/v1/internal/events` catch-all to the 3 type-scoped subpaths named above for the event types this service still actually consumes — the flat path remains registered as the catch-all for anything unmapped. |
| 1.39 | 2026-08-30 | **Observability audited against `gincommon`/`pgcommon`'s actual capabilities and brought into line** (§7.6): custom metrics now register via `gincommon.MetricsRegisterer()` instead of `prometheus.MustRegister` on the default registry directly. `db_tx_retry_total` retired — it duplicated `platform-pgcommon`'s own `pgcommon_retry_total`/`pgcommon_retry_exhausted_total` in intent and was never incremented in production code. A new `observability.RegisterPGMetrics` activates `pgcommon`'s full built-in collector set (`pgcommon_query_total`, `query_duration_seconds`, `pool_acquire_*`, `slow_query_total`, `retry_total`, `retry_exhausted_total`) via `pgmetrics.InitWithRegisterer` — previously never called anywhere, leaving these permanently no-op for a capability already paid for by the dependency. A new `internal/adapter/outbound/pglogger` adapter wires a real logger into every `pgcommon.Config`/`migrate.Runner` construction site (`cmd/server`'s app/relay pools, `cmd/worker`'s pool, `cmd/server/migrate.go`'s two runners) — previously unset, silently dropping slow-query warnings and GUC-injection failures. **Version bumps** (§1.1): `platform-gincommon` v1.2.1 → v1.3.0 — a hard prerequisite, `MetricsRegisterer()`/`MetricsConstLabels()` don't exist before v1.3.0; `platform-pgcommon` v1.1.2 → v1.2.1, the version that made `Config.Logger`/`migrate.Runner.Logger` externally implementable (v1.2.0's public `domain.Logger`, replacing an unexported internal type). |
| 1.40 | 2026-09-04 | **3 documentation gaps closed, surfaced by the `feat/workflow-engine-core` branch review's bug fixes (12 real bugs found+fixed, remerged `f82e891`) — the code was already right or already fixed; this brings the LLD's prose in line with it.** §4.3's `workflow_task` status-transition table gains a second `SUPERSEDED` row: an interrupting boundary event (§2.2) cancelling an in-flight host task en route to `TargetDept` also supersedes it, not only an `instance-force-forward` bypass — a real gap, since the workflow function's `SUPERSEDED` write on this path was previously unwritten entirely (fixed in the same review) and so had no documented trigger either. §2.3's `CallPool` `Ignored: true` admin-stub task gains its own `NodeKey` format note — `{pool}#{visitCount}`, not the general `{deptID}/{stageType}` scheme (§4.3), the visit counter existing specifically to stop two concurrent `Parallel` branches reaching the same pool from colliding on one key (the review's fix for exactly that collision). §3.3's DEGRADED force-back respawn description gains the multi-department-branch case: a branch whose `steps[]` runs more than one department in sequence resumes from its last-completed node and continues the remaining steps, rather than restarting the whole branch from scratch (the review's fix for a branch that had been silently mis-resumed, or hung, previously) — the structurally-nested (`CallPool`/`SubWorkflow`) shape still falls back to a full re-run, unchanged. |
| 1.41 | 2026-09-05 | **Full-service contract review (AsyncAPI/OpenAPI vs. IAM, 5 real bugs found+fixed) lands its LLD footprint.** §5.10/§5.8/§5.4/§5.10's `ASSIGNEE_INELIGIBLE` row: plain `/reassign` and `port.WorkflowClient.ReassignDelegate` now re-validate the replacement assignee's eligibility (fail-closed) and liveness (fail-open) via a shared pre-check run synchronously before the signal is forwarded — closing a gap unique to these two paths relative to node-override's and the delegation reconciler's own already-checked paths; rev 1.15's changelog entry calling plain reassignment's lack of an eligibility check "a deliberate admin-override behavior" is superseded by this change, not narrated as a before/after in the body itself. §1.5/§5.2/§6.1/§9.2: every `/internal/*` citation corrected to `/api/v1/internal/*`, matching the router and `openapi.yaml`, both already consistent — the LLD prose was the one place still describing the pre-`/api/v1` path. §4.9/Appendix C: added `idx_outbox_events_sla_task_unique`, a DB-level backstop under the existing `ExistsForTask` app-level SLA-event dedup check (closes a real duplicate-write race between two overlapping activity attempts under `RunInTx`'s READ COMMITTED). §6.1/§6.2: added `workflow.task.created`'s self-consumption path for connector dispatch — previously undocumented in the LLD prose despite already being in `api/asyncapi.yaml` — with an honest note (and new Appendix B row) that its actual subscription topology is unconfirmed, not a settled mechanism. §3.1/§5.2: corrected the connector-task-completion description — `cmd/connector-worker` never imports the Temporal SDK and never signals the workflow directly; it calls back over HTTP (`POST /api/v1/internal/connector-tasks/:id/complete`\|`/fail`, now added to §5.2's endpoint table), and `ConnectorTaskService` is what raises the `stage-transition`/`stage-fail` signal in response. `EligibilityClient.CheckEligibilityBatch`'s partial-success behavior needed no doc change — §6.2's existing prose already read consistent with the corrected `[]port.EligibilityResult` per-item contract. Also, unrelated to any of the above: 4 more instances of the nil-slice-marshals-to-null bug class found alongside the original `workflow.task.created` one (`workflow.task.failed`/`.superseded`/`.sla-warning`/`.sla-breached`), all fixed the same way — no LLD footprint, since the payload shape itself was always correctly documented, only the Go implementation nil-checked incorrectly. |
| 1.42 | 2026-09-05 | **IAM directly confirmed the platform-wide event-casing question this document has carried as unresolved since the delegation-casing conflict was first found**: every IAM event type is PascalCase, no exceptions — verified directly against updated `IAM/userprofile_asyncapi.yaml` (User Profile's events migrated from dot-notation to PascalCase while still undeployed) and `IAM/OAM_asyncapi.yaml` (Org & Membership's events, including `TenantStateChanged`, always PascalCase). This service's own inbound handler matched on dotted-lowercase strings for all 5 IAM-sourced event types — a real bug, not a doc gap — now corrected everywhere: the handler's dispatch constants, `api/asyncapi.yaml`'s channel keys and message `name:` fields, `docs/swagger/openapi.yaml`. §6.1's `TenantStateChanged` attribution replaced with a precise citation (`OAM_asyncapi.yaml`, §7.1 EVT-16, `membership-workflow-q`) in place of the previous vague `x-schema-owner` placeholder. §6.1 also now states plainly that the event-driven `DelegationStarted`/`DelegationEnded` path and the synchronous `ReassignDelegate`/`CancelByDelegate` calls are two distinct, both-legitimate mechanisms — not a lingering open question. New Appendix B row: `DepartmentMembershipGranted`/`Revoked`/`LevelChanged` are a confirmed-real subscription opportunity on the same queue, deliberately not consumed yet — no pick-list feature exists to read that state, and building the write side alone would be half a feature; revisit once one is scoped. |
| 1.43 | 2026-09-09 | **`workflow.task.created`'s self-consumption path confirmed reachable, and the flat `/api/v1/internal/events` catch-all retired.** `/workflow-task` joins the existing 3 type-scoped subpaths (§6.1) as the fourth and last event type this service consumes — every consumed type now has its own subpath, so the generic catch-all serves no one and is gone; a future event type needs its own subpath wired here and in the shared consumer's routing table together, not a default fallback. §3.1/§5.2's connector-task-completion endpoints (`POST /api/v1/internal/connector-tasks/:id/complete`\|`/fail`) needed no contract change but had shipped with a path-prefix bug in `cmd/connector-worker`'s own caller — fixed, and now covered by a real end-to-end test exercising the full dispatch-through-completion round trip against live Temporal/Postgres/Valkey. New Appendix B row: `cmd/connector-worker` still has no Dockerfile stage or CI build/scan/push entry, so it can't be deployed as-is despite being fully implemented and tested.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 1.44 | 2026-09-10 | Cross-doc naming sweep landed alongside the Tender HLD's source-verification pass. **`wf-workflow-sign-q` dropped, not renamed** (§6.7): verified against the real repos, the queue exists in no code, config, or infrastructure — `event_consumer/config/topology.{example,local}.yaml` declares only `membership`, `user`, and `dashboard`, and this service's own `api/asyncapi.yaml` declares the outbound topic without any consumer queue. Under §3.5's Tender-first ordering there is no signing trigger for it to carry either. Whether Tender subscribes to `wf-workflow-events` at all, and to which types, is the Tender HLD's decision to record, not this document's to name a queue for. **Four residual dotted-lowercase IAM event names** (`department.membership.revoked`, `user.deleted`, `user.availability.changed`) corrected to PascalCase in §1.4, §5.8, §6.2 and Appendix A.5 — rev 1.42 fixed the handler and the specs but missed these prose citations. **Paste debris removed from §1.6**, where an inbound note about IAM event casing had been pasted mid-sentence, splitting "independently built, containerized, deployed…". No behavior, schema, or contract change. |
| 1.45 | 2026-09-10 | **Wire event types switched from dotted-lowercase to PascalCase, reversing Appendix A.4 decision 11** (§6.4, §6.8, Appendix A.4 #11, Appendix B). Verified against the real repos rather than the docs: decision 11's sole stated rationale was mirroring Definition Service's shipped dotted-lowercase convention, and that convention no longer exists — `definition_service/api/asyncapi.yaml` declares one channel, `action: receive`, and publishes nothing at all since `workflow.template.published` was retired in rev 1.38. That left this service as the platform's only producer emitting dotted types against IAM's confirmed every-service-PascalCase standard, while its own `api/asyncapi.yaml` already named every message in PascalCase — only the attribute value diverged, the same split `iam-user-profile` explicitly migrated away from (its LLD rev 0.37/0.38). All 69 dotted citations in this document renamed. **No schema-registry impact** (new §6.8 note): `platform-schemagov` derives the Glue name from the JSON filename stem and normalizes either casing to the same snake_case key — its coverage check already folds this service's PascalCase message *names* to `workflow_task_created`, since no SNS `event_type` attribute value is declared — so the 18 `internal/eventschema/*.json` files and their registered names are untouched; only `GlueCodec.registrySchemaName`'s `.`→`_` derivation changes. **`wf-workflow-sign-q` dropped, not renamed** (§6.7): the queue exists in no code, config, or infrastructure — `event_consumer/config/topology.{example,local}.yaml` declares only `membership`, `user`, `dashboard` — and under §3.5's Tender-first ordering there is no signing trigger for it to carry; whether Tender subscribes at all is the Tender HLD's decision. Three Appendix B rows added: the code-side rename scope, its cross-repo reach into `workflow-stream-gateway`/`event_consumer`, and a live `event_consumer` routing bug found on the way (all six IAM route keys are still dotted and therefore dead against what IAM publishes today, falling through to the broadcast-to-all-targets fallback). The rename is documentation-only in this pass; the code still emits dotted types. |
| 1.47 | 2026-09-10 | **Corrects a factual error introduced by rev 1.1/1.45: `outbox_events.event_type` is not SCREAMING_SNAKE.** (§6.4, Appendix A.4 #11). `platform-events`' `Enqueue` writes the wire `type` value into that column verbatim, with no case transform anywhere in the library — so the column holds the same PascalCase string as the wire type and the `EventType` SNS attribute, not a distinct DB-audit casing. Both citations corrected to state this plainly. |
| 1.46 | 2026-09-10 | **Rev 1.45's rename lands in code, closing all three Appendix B rows it added.** `domain/events.go`'s 18 wire-type constant values, `internal_events.go`/`internal_events_connector.go`'s inbound dispatch, `temporal/audit.go`/`instance.go`, `core/port/connector_events.go`, `cmd/connector-worker/main.go`, `api/asyncapi.yaml`'s `EventType` description and every message title, `GlueCodec.registrySchemaName` (now a CamelCase→snake_case fold, pinned by a new test against all 18 real `internal/eventschema/*.json` filenames — unchanged, confirming §6.8's "no schema-registry impact" note), and every affected test all landed the same day. `MessageName` (§6.4) collapsed to an identity function, since the wire type and the AsyncAPI message name are now the same string. `workflow-stream-gateway`'s `internal/usecase/fanout.go` and its own LLD, and `event_consumer`'s `category.go`/both `topology.*.yaml`/`deploy/helm/values.yaml` (which also still keyed the six IAM routes on dotted-lowercase, the rev 1.45 "Live bug" row) landed together, coordinated. Verified with `schema-gov validate`'s real 8-pass governance gate (`platform-schemagov`, run locally via its own venv) against `api/asyncapi.yaml` + `internal/eventschema/`, all passes clean, plus each repo's own full test suite. |
| 1.48 | 2026-09-15 | **Three interpreter defects found against shipped code and corrected here** (§2.6, §2.7, §3.1, §3.4, §6.4, Appendix A.6 #14). Force-routing named the bypassed node from `completedNodes`, which holds only finished nodes, so the activity's status filter matched nothing and the task it was meant to close stayed open — §2.7 now fixes both directions on the live pending set, read before the cancelled run unwinds it, and force-back supersedes and audits the same way force-forward does. §3.1's `RecordForceRouteActivity` gains `direction`, and its trigger column both signals. §2.6 gains steps 5–7: a revert re-enters the gateway, so the Nth rejection sends work back again rather than the second falling through to the forward branch; the loop is bounded at 50 cycles and fails closed there; and a re-evaluation matching nothing resolves the step rather than erroring. The bound is a constant, not configuration (Appendix A.6 #14). §2.6 step 4 adds the empty-`Target` branch: the single-forward-with-reverts shape compiles its forward path as the following steps, so naming a `Target` there as well dispatched that department twice. |
| 1.49 | 2026-09-15 | **Tenant-lifecycle sweeps and resume scoping corrected** (§3.1, §6.2). The sweep selected instances by the status each transition expected, making the asynchronously-written status row a precondition: an instance whose row had not caught up was not skipped, it was never selected, so no resume was sent and none ever would be — §6.2 item 4.2 now selects every non-terminal instance in one query and signals unconditionally, leaving `signalPreconditions` as the only gate. Initiator-filtered resume (§6.2 items 4–5) is specified as enforced at signal validation against interpreter-held state, since no `initiator` column exists and adding one would reintroduce exactly the staleness item 4.2 removes; `instance-resume`'s signal row carries the precondition. §6.2 also gains `MembershipRevoked` and `TenantMembershipsPurged` as consumed types, and its "Not subscribed" column is corrected to "Routed to no target" — no SNS filter policy exists on this platform, so an unconsumed type arrives and is dropped by the shared consumer's routing table, not by a subscription filter. |
| 1.50 | 2026-09-15 | **Testing and deploy topology brought in line** (§7.1, §7.2, §7.5, Appendix A.6 #3/#7/#13, Appendix B). §7.2 described a LocalStack SNS/SQS/Glue e2e, plus DLQ and SNS-filter-policy routing tests, that no repo builds and that would assert only dependencies' behavior — it now states the broker hop as out of scope with the reasoning, and specifies the duplicate-inbound-delivery test that covers this service's own dedup guarantee instead. The relay's own RLS requirement is filed as a new Appendix B item, being the one risk in that hop that is genuinely this service's. §7.5 covers three binaries rather than two, adds the connector-worker Deployment and the chart's `HTTPRoute`/`SecurityPolicy`, and specifies the `version` subcommand every binary answers before reading configuration — CI's per-image check runs it, having previously run each image bare, which no real entrypoint survives. Appendix B's helm-chart, connector-worker-deploy and delegate-impact rows close; the live-status row is re-scoped to IAM, the pre-check being wired and fail-open by design rather than undesigned. |
| 1.51 | 2026-09-15 | **Binary count stated consistently** (§1.6, §7.4, §7.5). Rev 1.50 added the connector runtime to the deploy topology but left three statements on the two-binary shape: §1.6's technical stack, §7.4's image-build job, and §7.5's dev stack. All three now describe three processes from one module. §7.5 also records that the packaging is not settled — the three share a repository because they share a domain, and promoting one to its own release train, the connector runtime being the likeliest candidate, is a load-testing decision. |
| 1.52 | 2026-09-22 | **A node resolving to no assignee is refused at instantiation** (§5.5). The eligibility re-check built one request per resolvable assignee and returned success when it built none, so a plan whose node named nobody — or whose only named assignee did not parse — instantiated cleanly. The resulting task carries no assignment rows and `assignee_mode = "single"`, and the claim path requires `assignee_mode = "all"` and an existing row for the caller, so the task could be neither claimed nor completed and the instance never advanced. Such a node is now named in the same `ASSIGNEE_INELIGIBLE` payload as a stale assignee, both being a named node no one can act on. §5.5's algorithm gains the resolution and skip steps ahead of the batching step, the skip list mirroring §2.6's own dispatch: connector stages, `send_task`/`receive_task`, and `ignore` departments create no task and need no assignee. Definition rejects the same shape at authoring time (`definition_service.md` rev 1.17); this check covers versions published before that rule. |
| 1.53 | 2026-09-22 | **Instance identity is a subject and a key within it** (§1.1, §4.2, §5.2, §5.9, Appendix C). An instance carried one opaque `business_key`, unique per tenant across every non-terminal instance, and that same value composed the Temporal workflow identifier. A calling domain whose entity runs several workflows at once could not express that: the second collided with the first, and an instance left paused or degraded held the entity's key indefinitely. `workflow_instance` gains `subject_id TEXT NOT NULL` beside `business_key`; uniqueness for unfinished instances moves to `(tenant_id, subject_id, business_key)`; the same index serves "every live instance for this subject", leading on `(tenant_id, subject_id)`; the Temporal identifier composes as `{tenantID}:{subjectID}:{businessKey}`. `POST /instances` requires both, with no default for either — a domain running one instance per subject sets the key to a constant of its own choosing. `GET /instances` gains `?subject_id=`, composing with the department/assignee scoping already applied. `DUPLICATE_BUSINESS_KEY` keeps its name and now covers the triple. Both identifiers stay opaque to the engine, which is what keeps a second registered domain a code-free addition (§1.1). **The event common core carries the subject** (§6.4, `api/asyncapi.yaml`, `internal/eventschema/`). An earlier draft of this change left it out on the grounds that no consumer reads it, which missed the real risk: `business_key`'s *meaning* changed under consumers, from the entity's own identifier to a value identifying one run within that entity. A consumer correlating on the key alone would group every subject's instance of the same stage. The payload carries the identity, and the identity is the pair. |
| 1.54 | 2026-09-22 | **A delegated task keeps its lead, and a subject-scoped delegation matches** (§3.1, §4.4, §5, §11). Also corrects rev 1.53 where review found it overstated: the `409` on a duplicate carries a fixed sentence and names neither identifier, and the separate `(tenant_id, subject_id)` index it described is dropped — the uniqueness index leads on those two columns over the identical predicate and already serves that read. `SubjectId` joins `BusinessKey` as a Temporal Search Attribute (§3.6), since searching on the key alone now spans every subject. Two defects on the delegation reroute path. The reroute vacated the delegator's assignment and inserted the delegate's without carrying `is_lead`, so a multi-assignee task the delegator had claimed came out of the reroute with no lead row at all — and the action path requires an active lead assignment belonging to the caller, so the task lost the claim silently: `Complete` and `Defer` refuse it for everyone, and any co-assignee can take a task the delegate was meant to inherit. `Claim` recovers it, which is what keeps this short of a dead end. The new row now inherits the flag. Separately, the scope fall-through compared the delegation's `scope_id` against `business_key`, where the identity subsystem mints a subject-scoped delegation's value as the bare subject identifier; with §4.2's `subject_id` holding exactly that, the comparison moves there and starts matching. §4.4's `is_lead` note is corrected: the flag gates `Complete` and `Defer` through `currentAssignee`, rather than being "not yet fully enforced". Admin reassign is gated on admin authority instead and does not read it. A scope with no case is logged and counted (`delegation_scope_unhandled_total`) rather than rejected — the delegation already exists upstream, and dropping it would strand the delegator's work. The backlog's request to reject a misspelled scope needs nothing here: the identity subsystem's Delegation Service constrains the value to its own enum at creation and returns `invalid_delegation_scope` otherwise, so no such value reaches this service, which has no delegation-creation endpoint of its own. |
| 1.55 | 2026-09-22 | **A connector-driven completion carries an actor** (§5.2, §5.7, Appendix B). The callback took tenant, task and output, so automation was authenticated and unattributed — the platform could establish that a call came from inside the cluster and not on whose behalf. Both callbacks now require `actor_user_id`, the tenant's `platform-automation` principal. The principal holds a real assignment row on the task, created before the signal, so the completion resolves through the same path a human's does rather than through a branch with its own rules: the actor is retained where a human actor is, and the completion event names it. The row's identifier derives from task and user, so a retried callback re-creates the same row. No tier check is added here — the principal cannot hold a department membership at all, barred structurally on the identity side, so it satisfies no eligibility requirement this service checks; a second enforcement path would only drift from the structural one. Verifying the principal's own signed assertion at this hop is filed in Appendix B. |
| 1.56 | 2026-09-23 | **A task now reaches `COMPLETED`** (§4.3, §5.2). §4.3's table has always specified `READY/IN_PROGRESS → COMPLETED` once the `allDone` gate is satisfied. The gate was computed by `CompleteAssignmentActivity` and discarded by its caller, so no task ever left `READY`/`IN_PROGRESS`, `completed_at` never populated, and the connector callback's re-completion guard read a status that never appeared. The interpreter now writes the status when the last assignment on a stage completes, gated by a workflow change id so a history recorded beforehand replays unchanged. Connector idempotency gains a durable backstop wherever the signal resolves its node: the Valkey dedup claim covers the window before the status write commits, and the status covers everything after it. A signal the interpreter drops or buffers writes no status, so the claim's expiry is still the only guard there — filed in Appendix B. |
| 1.57 | 2026-09-23 | **The connector worker names the actor it sends** (§5.7, Appendix B). The callback requires `actor_user_id`, and the worker had no source for one. It reads the tenant's automation subject from configuration (`CONNECTOR_ACTOR_USER_ID`) until the identity subsystem exposes a read answering which subject a tenant's automation principal is; that question and the subject's stability across a re-mint are filed in Appendix B and asked in a standalone note to that team. Execution resolves nothing itself, so the answer changes the worker alone. |
| 1.58 | 2026-09-23 | **Review corrections to revs 1.55 and 1.56** (§4.4, §5.7, Appendix B). Four defects found auditing them. The worker held one automation subject for every tenant it serves, which would have recorded each tenant's connector work under whichever principal the process started with; it now holds a per-tenant map and fails a callback for a tenant absent from it. `Fail` took the actor and dropped it, so a connector failure named nobody — it creates the same assignment row a completion does. The `COMPLETED` gate read its change id only when the completion happened to finish the task, which would have split running instances by their assignee counts rather than by whether they predate the change; the version is now read first. A completion carrying no acting user no longer satisfies the gate, since it enqueues no event and a terminal status no consumer was told about is worse than an open task. Rev 1.56's idempotency claim is narrowed to the path where the signal resolves its node, with the dropped-signal case and the redispatch window filed as residuals. |
| 1.59 | 2026-09-23 | **Two residuals from rev 1.56's review are recorded** (§4.3, Appendix B). The task `COMPLETED` write propagates its error where the `SUPERSEDED` write beside it is deliberately best-effort, and the change id gating the completion status has no replay fixture. Neither changes behaviour; both were found in review and had been carried in a planning document rather than here. |
| 1.60 | 2026-09-24 | **A failed connector callback no longer re-runs the connector** (§7 graceful shutdown, Appendix B). The worker records each outcome and re-sends only the callback on redelivery, which resolves the residual rev 1.58 accepted. §7 gains the connector worker's shutdown: a started run finishes and reports, and the pod's grace period covers it. §7.6 lists `connector_dispatch_dead_lettered_total`. The design is the worker's own (`workflow_connectors.md` §6.5 step 0, rev 8.11). This service's callback contract is unchanged. |
| 1.61 | 2026-09-25 | **Calls to reusable modules are expanded at decode** (§2.3, Appendix A #3). A `callActivity` now reaches Execution as a `call_plan` step (workflow-models `CallPlanStep`), not as steps inlined at compile time. The `GetCompiledPlan` activity, instance start and the compiled-plan cache run `dsl.ExpandCalls` on every stored plan, so each call runs its own tasks under call-scoped departments bound to the caller's IAM departments, and `override_map`, eligibility, reassign and delegation all see module tasks by their node keys `<callNodeID>::<department>/<task>`. Expansion inside the activity keeps replay on the recorded plan. Tested against workflow-models' golden `dsltest.LibraryCalls`, which Definition's publish produces, including an e2e run through real Temporal. A plan that cannot be expanded refuses the start as `VERSION_INVALID` (409); a plan of another schema major is not expanded. **Boundaries** (§2.2): a call's boundaries run like a `subProcess`'s; a timer's ISO 8601 period (`P5D`) is now read, where it was silently dropped; an interrupting boundary with no target department, whose path leads to an end event, ends that path instead of failing the instance; each under its own change id. Tested against a second golden, `dsltest.LibraryCallBoundaries`. Two residuals are recorded in Appendix B: the new change ids have no replay fixtures, and a task open inside a call that a boundary cancels is not marked `SUPERSEDED`. §2.1, §2.5, §4.3 `node_key`, Appendix A #4 and the laneless-module row of Appendix B now describe calls as they run. |
| 1.62 | 2026-09-26 | **Only a boundary that terminates ends its path** (§2.2 rule 6, §5.5, Appendix B). workflow-models `v1.3.0-rc.2` gives every boundary `Terminates`, and `dsl.ExpandCalls` refuses one that sets both or neither of it and `TargetDept`. An empty target had meant both "leads to an end event" and "leads somewhere Definition's compiler could not name" — a laneless module's boundary to a task, or one leading to a call or a send task — so such an instance completed silently; that plan now fails at decode and never starts. The interpreter still reads an empty target, which for every plan that expands is exactly a terminating boundary, so no new change id is needed and an instance whose plan was recorded before `Terminates` runs as it did. §5.5: Definition now refuses to publish a workflow with a task that has no default user, so the start-time check is the backstop, and a module task's default comes from its call's `Assignees` (which may name it by a path of calls) before its own. Module node keys name a laneless module by its process id, without its version. §2.1, §2.2 rule 3, §2.4 and Appendix A #7 corrected; Appendix B's laneless-module row moves to the RESOLVED form. |
| 1.63 | 2026-10-01 | **Node keys and the human-task rule are shared** (§2.5 table). A stage's node key and whether it gets an assignee come from workflow-models' `dsl.NodeKey` and `StageDef.CreatesHumanTask`, which Definition's node read (definition LLD §3.3.12.2) uses too, so every key that read returns is one `override_map` accepts. The key format is unchanged. |
| 1.64 | 2026-10-01 | **The Definition Service link is internal HTTP; gRPC is gone** (§1.2, §1.3, §1.4, §1.5, rewritten §5.3, §5.7, §6.2, §7.2–§7.6, §9.2, §9.3, §10, App. A.2 decisions 10, 18, 19). Execution reads compiled plans from the Definition Service's internal API (`DEFINITION_SERVICE_URL`, `DEFINITION_SERVICE_TOKEN`, used by the API, the Worker and the connector-worker) and serves the active-instances and pause-tasks endpoints on its own internal route group. Each server's `api/internal-openapi.yaml` is the authority, with a design-repo copy; the caller keeps a byte-identical copy checked by `make contract-check`, and `make contract-breaking` (oasdiff) replaces `buf breaking`. The plan's own `schema_version` is the one schema-version source. The Worker gets the Definition Service URL, token and egress it lacked; the gateway routes only the public routes. §5.3 named the internal IAM event consumer as `PauseUserTasks`' caller; it is the Definition Service. |
| 1.65 | 2026-10-01 | **A failed compiled-plan read hides its cause from the caller** (§3.1, §5.7, §6.1, §7.6, §10.2). `POST /instances` answers `503 UPSTREAM_UNAVAILABLE` with a fixed detail, and the service that read the plan logs the cause, which names the Definition Service's internal URL and the tenant. §3.1's `GetCompiledPlanActivity` row places the schema-version check in `Execute`, where §2.5 has it. §5.7, §6.1 and §7.6 state `INTERNAL_API_TOKEN` as required outside dev, as `Config.validate()` has it, and §7.6 scopes it to all three binaries. |
| 1.66 | 2026-10-02 | **An exclusive branch runs its own steps; parallel branches are told apart by place** (§2.1, §2.6, §3.3). A forward branch with `Steps` runs them (workflow-models LLD rev 2.14): a branch could only name one department and stage, so a call or a subprocess on a branch never ran, and a branch ran every later task of its department. A branch without `Steps` is dispatched as before, so a plan recorded in an existing history replays unchanged. A branch that leads to an end event ends its path rather than the instance, which completes and writes its status (change id `exclusive-end-completes-path`): it had ended as `TERMINATED` with no status written, so the instance row stayed `RUNNING`. The parallel join counts branches by their place in the step: two branches with one `DeptID`, which a fork whose branches begin with a call or a pool compiles to, hung the join (change id `parallel-join-by-branch`). An admin force-route resumes inside an exclusive or parallel branch's `Steps` at the department it names, and runs the rest of that branch before the steps after the gateway; it had resumed at the gateway's join. Departments are lane segments in the plans Definition compiles (definition LLD rev 1.29), so the stages of a department run in the order the flows give. |
| 1.67 | 2026-10-02 | **One pool runs; the call-pool stub is removed** (§2.1, §2.3, §2.4, §2.5, §2.7, §3.3, §4.3, §8.2 #9, Appendix A.1 #2, #8, #10, #11). A BPMN document runs one pool, and outside parties' pools and lanes produce nothing (definition LLD rev 1.30), so no `call_pool` step reaches the interpreter: the call-pool step, its admin stub task and its `{pool}#{visitCount}` node key are gone, with the plan's `messages`, `ignored` and department `ignore` fields (workflow-models LLD rev 2.15). The eligibility check at start no longer skips an ignored department; none exists. |
| 1.68 | 2026-10-07 | **Outside answers, instance variables and one input rule** (§2.1, §2.4, §3.1, §5.2, §5.10, new §5.11). A receive task or message boundary that no send task of the main plan feeds is completed only by an outside party's answer, addressed by its node key: `POST /instances/:id/nodes/:node/complete` and `.../boundaries/:boundary/fire`, open to an admin and to an assignee of a task of the instance, new codes `WAIT_NOT_FOUND` and `NOT_WAITING`. The `instance-external-answer` signal and the `waiting-external` query carry it (change id `external-wait-by-key`). `context_json` is the instance's variables: every completed task's result and every answer merges into it, last write winning (change id `task-result-variables`). An `IOMapping` input is `=name` for a variable or a literal, an output names a key of the connector's result (change id `io-input-rule`, workflow-models LLD rev 2.16); before, `=name` read a variable called `=name` and a call input `=x` to `x` set `x` to nothing. The connector worker reads each secret field of its connector type from the tenant's stored credential, `connectors/<tenant>/<type>/<field>`, and discards a value a plan carries for one; it no longer fails a task for a path the task names (`workflow_connectors.md` §6.2, rev 8.13). |
| 1.69 | 2026-10-08 | **Each binary checks only the settings it uses** (§1.1, §7.6). `WORKER_MODE`, which the design named but no code read, is dropped from the inventory. `Load` parses the environment and checks value ranges; the API, the Worker and the connector-worker each check their own required settings, and `migrate` only its DSN. Before this, every binary demanded `DATABASE_URL`, the relay DSN and the SNS and Glue settings, so the connector-worker and the migrate Job could not start from the environment the chart gave them, and the Deployments omitted settings the API requires (`GLUE_REGISTRY_NAME`, `OUTBOX_RELAY_DATABASE_URL`, `ELIGIBILITY_BASE_URL`). The chart now sets each binary's own set. |
| 1.70 | 2026-10-08 | **Execution names the connector actor** (§5.2, §5.7, §7.6, Appendix B). The callbacks took an `actor_user_id` the worker stated from a static `<tenant>=<subject>` map, so any caller holding the internal token could record any actor, and a new tenant needed a configuration change and a restart. The callbacks now carry no actor. Execution resolves the tenant's `platform-automation` principal from the Token Service's per-tenant read, caches it briefly, and records its `principal_id` (stable across the re-mint at conversion) rather than its `sub`. It resolves when a connector task is put on the stream, so a tenant with no principal is refused before any side effect, and again on each callback (`409` when there is none). `CONNECTOR_ACTOR_USER_IDS` is gone; the API requires `TOKEN_SERVICE_BASE_URL` outside dev. Both Appendix B rows about the actor are resolved. |
| 1.71 | 2026-10-08 | **Who reads the automation principal, and how it is admitted** (§5.7). The read is made by the API service; the identity subsystem admits it by namespace and mesh identity, as an onboarding step. A principal that is not active counts as none, and neither that nor a missing principal is cached. Execution does not subscribe to the registration event, since it keys on the stable `principal_id`. |
| 1.72 | 2026-10-08 | **A fired boundary runs its whole path; a failed stage raises its error class; an abandoned task is superseded** (§2.2 rules 3 and 5, §4.3, Appendix B). A boundary ran the one department it named, then the flow went on as if the host had finished, so a path of several tasks, lanes or gateways was cut short; and a failure inside a call failed the instance whatever error boundaries the call had, since nothing pushed to the error channel; and a task open in a call that a boundary cancelled stayed open and claimable. The interpreter now runs a boundary's `Steps` and ends the interrupting path (workflow-models LLD rev 2.17; a plan without `Steps` behaves as before), matches the error class of a failed stage against the call's error paths, and supersedes every task open in a cancelled scope, vacating its assignments and enqueuing the event, whose actor is the nil user identifier. Change ids `subworkflow-error-raised` and `abandoned-task-superseded`. |
| 1.73 | 2026-10-08 | **Defer sends the instance one step back; an admin force names a node key** (§2.7, §3.1, §4.3, Appendix B). Defer closed the deferring task and created a replacement at the same node, so nothing went back, and the public API had promised a regression to the previous step. It is now the assignee's one-step force-back through the same routing as `instance-force-back`: the earlier node's department creates a new task, `workflow.task.deferred` names that node, and a task with no completed step before it is `409 INVALID_TASK_STATE`. An admin force-forward inside a parallel step or a DEGRADED instance took its branch from a `target_dept_id` no HTTP path set, so it was dropped; the branch is now the department of `target_node_key`. An admin force-back of a DEGRADED instance may name the failed branch's node, and a respawned branch that fails again keeps its steps. Change ids `defer-routes-back` and `force-route-by-node-key`. |
| 1.74 | 2026-10-09 | **Platform observability standard** (§1.1, §7.6, §9.2). Metrics are built through `platform-gincommon` and named `workflow_execution_service_*`; no metric carries `tenant_id`. Fourteen metrics the design listed were never recorded and are now recorded: the task-signal and start durations, activity durations, version conflicts, cache hits and misses, upstream errors, SLA breaches, DEGRADED entries, workflow panics, sampled RLS violations, and the DEGRADED and READY-task gauges. The outbox-age gauge is replaced by `platform_outbox_oldest_pending_age`. `APP_ENV` is validated against `local`, `dev`, `test`, `staging`, `prod`, the stub publisher is refused outside dev, the connector-worker requires `OPENBAO_TOKEN` outside dev, and the worker installs the Temporal tracing interceptor. The internal OpenAPI and AsyncAPI list exactly the routes and event types the code serves, including `MembershipRevoked` and `TenantMembershipsPurged`. |
| 1.75 | 2026-10-13 | **Plan expansion has a stage budget** (§2.5). Execution takes `workflow-models` `v2.0.0`, whose `ExpandCalls` refuses a zero budget. Both places that expand a stored plan, the compiled-plan activity and the service's plan cache, pass the 1000-stage limit Definition enforces at publish, so a crafted or oversized stored plan can no longer exhaust a worker's memory: it is an invalid version, non-retryable in the activity. The import path is `…/workflow-models/v2/pkg/…`. |
| 1.76 | 2026-10-13 | **The connector worker runs `workflow-connectors` v2 in full** (§3.1, §5.3, §7.6, §7.7). The worker moves from `v1.0.0` to `v2.0.0` and `platform-pgcommon/v2` `v2.0.1`. Retries follow the library's decision (a transient error the type's policy allows, a bounded attempt count with backoff and jitter) instead of retrying every error without limit; every decision is logged and counted by the new `workflow_execution_service_connector_task_failures_total`. Each call carries its tenant; document references, the Drive registry and the send-email intents are wired, with a tenant policy on their three tables (migration `000009`, after the library's own schema in the migrate step). `/fail` accepts the connector's output map, whose keys the service logs. The audit prune runs once at start and then daily through the outbox relay role, which the worker requires outside dev because the app role cannot see rows across tenants; the alias refresh also refreshes the method the retry decision reads. New settings: `CONNECTOR_DOCS_BUCKET`, `CONNECTOR_RETRY_*`, `CONNECTOR_AUDIT_RETENTION`; the worker needs `DATABASE_URL`. Closes connectors LLD OQ-14 and OQ-17. |
| 1.76 | 2026-10-12 | **Inbound queue set and the consumer's delivery contract follow IAM's AsyncAPI** (§6.1, §6.2). `DelegationStarted` and `DelegationEnded` arrive from `iam.delegation.events` through a Workflow-owned queue, not from `iam.membership.events`, and carry a schema-registry-encoded payload that the decode of §6.8 handles on live traffic. Each IAM queue's subscription carries the filter policy that the producer's AsyncAPI defines, so the sentence that no filter policy exists is removed, and `MembershipRevoked` and `TenantMembershipsPurged` are recorded as requiring a place in the membership queue's filter. The retry contract states the consumer's classification: 400, 413 and 422 go to the DLQ after one delivery, every other failure is retried with backoff. Event types outside the table never reach this service, because the shared consumer forwards only types that have a subpath here. |
| 1.77 | 2026-10-13 | **Branch conditions use the shared parser; the connector callback respects the instance's status; a failed task releases its assignment** (§2.6, §3.1, §3.2). The gateway evaluates a condition with `workflow-models` `dsl.ParseCondition`, so the documented FEEL form (`= decision = "bid"`) works, a literal that contains `==` is no longer mis-split, numbers compare as numbers, and any other form is refused. A connector callback for a paused instance returns `409 INVALID_INSTANCE_STATE` instead of success with the signal dropped; for an instance that is over it is acknowledged with no signal. A task failed by its own stage vacates its active assignments, as a cascade-failed one does, and its `WorkflowTaskFailed` event still names them and now carries the connector's `error_class`, an optional string, so a consumer can tell a timeout from a rejection. The internal spec declares `502 EVENT_DECODE_FAILED` on the four category routes, and contract tests send the category routes and the connector-task callbacks through the real router against it. |
| 1.78 | 2026-10-14 | **`workflow-connectors/v2` v2.1.0** (§3.1, §7.6, §7.7). The worker's Valkey ACL user needs `+asking`, which document references use during a cluster slot migration. The startup document-reference check also puts and deletes an empty probe object under the key prefix, so the worker's S3 grant covers `s3:PutObject` and `s3:DeleteObject` on that prefix as well as reads. Strict alias YAML: an unknown key is a startup error. A send-email task retried after an ambiguous reservation failure no longer fails as a duplicate. The registry no longer lists `chat-notify`; a failed `chat-notify` task keeps its `unsafe` retry policy. |
| 1.79 | 2026-10-14 | **The public spec follows the handlers** (§5, Appendix A). A task's assignment record has no `task_id`; an active-assignment item carries `task_id`, `workflow_instance_id`, `node_key`, `user_id`, `department_id`, `status` and `record_version` only; the two outside-answer routes declare `413` and `415`. Contract tests send the instance and task routes through the router and fail on any undeclared response field. |
| 1.80 | 2026-10-14 | **A retention sweep runs in the API process** (§4.11, §6.8, §7.6). Once a day, and at start, the API prunes published `outbox_events` older than the 7-year audit floor in batches of 1000 through the outbox runner on the relay pool, and `processed_event` rows older than 7 days on the app pool (the table has no row-level security and no tenant column, so the app role needs no tenant). Both windows, the cadence and the batch are environment settings (`OUTBOX_RETENTION`, `PROCESSED_EVENT_RETENTION`, `RETENTION_SWEEP_INTERVAL`, `RETENTION_SWEEP_BATCH`); `workflow_execution_service_retention_rows_deleted_total` counts the rows by table. Dead-letter redrive stays an operator action; the existing dead-letter alert covers detection. |
| 1.81 | 2026-10-14 | **The connector callbacks answer `204`, as built and as the internal spec says** (§9). The residual-risk row said `202`. |
| 1.82 | 2026-10-14 | **A delegation scope this service has no case for matches nothing** (§5.8, §11, Appendix A). The scopes are `all`, `department` and `tender` (matched on `subject_id`); any other value reroutes no assignment and is logged and counted, so a scope added to Org & Membership's enum cannot move the wrong tasks. A new subject-scoped value gets its own case here. Before this, an unknown scope was compared with `subject_id`. |
| 1.83 | 2026-10-14 | **The plan activity refuses a version that cannot start, a receive wait is a current node, and a compatibility tier runs the real Definition Service** (§3.1, §7.1). `GetCompiledPlanActivity` refuses a version that is not `PUBLISHED` or not valid, without retry: the API's cached read could start a version the Definition Service had marked invalid, and the instance ran. A receive task records its node in `current_node_keys` when its wait begins (change ID `receive-records-node`); a plan whose main pool starts at a message wait showed no node. `test/compat` runs the Definition Service's binary against this service. |
| 1.84 | 2026-10-14 | **A stage cancelled while its activities run is abandoned** (§2.5, §2.7). A force-forward that cancels a task stage while it is still creating its task or recording its node returns the stage as abandoned, superseding a task it created, rather than failing it with the cancelled activity's error, which a parallel join counted as a failed branch (change ID `abandoned-mid-activity`). |
| 1.85 | 2026-10-14 | **Instance start reads the compiled plan live** (§1.3, §5.1, §10.2, Appendix A #19). A start read the version's status and validity from this service's one-hour cache, so after the Definition Service invalidated a version (a membership revocation) or archived it, a start within the hour answered `201` and the instance then failed in `GetCompiledPlanActivity`. Start now always reads the Definition Service, which evicts its own copy on both events, and answers `409 VERSION_INVALID` or `409 VERSION_NOT_PUBLISHED` at once. The cache is written through by each start and read only for running instances, whose plan body never changes. |
| 1.86 | 2026-10-14 | **The compatibility tier covers the connector worker and the event consumer** (§7.1). It runs a connector stage through the relayed `WorkflowTaskCreated`, the connector worker binary and the Definition Service's alias, and IAM events published through platform-events on LocalStack SNS through the Workflow-Events Consumer's binary to both services. |
| 1.87 | 2026-10-17 | **Tenant offboarding fails every open task and is redelivered on failure** (§6.2). Offboarding failed only the first page of an instance's tasks, and a failed termination was logged while the event was recorded as processed, so nothing retried it. Every open task is failed, each instance is attempted, and a database failure fails the event for redelivery. |
| 1.88 | 2026-10-17 | **A terminate closes the workflow execution before it writes the database** (§3, §6.2, Appendix B). Both the admin terminate and the tenant-offboarding sweep wrote the database and then called the workflow engine, so a failed call left a TERMINATED row over a running execution that no redelivery could reach. The execution is closed first, a call on an already-closed execution succeeds, and a failed call or write fails the request or the event so a retry completes it. The accepted residual in Appendix B is closed. |
| 1.89 | 2026-10-17 | **Instance reads consider every task** (Appendix A, intra-tenant read scope). The instance detail, its event list and an outside party's answer decided visibility from the first 100 tasks only, so a caller whose only task was the 101st was refused, and the detail returned only 100 tasks although the API says it returns all. Visibility is now decided by the same rule as the instance list, and the detail returns every task. |
| 1.90 | 2026-10-17 | **A terminate during instantiation leaves no live execution** (§3). A terminate that landed between the instance row and a successful workflow start, which is retried up to three times, found no execution to close, marked the row TERMINATED, and the late start then ran a workflow under it. The execution is closed again after the terminate's DB write, and instance creation closes the execution it just started if the row is already terminal. |
| 1.91 | 2026-10-17 | **Task creation refuses an instance that is no longer running** (§3.1). An activity still in flight when its instance was terminated could insert a READY task after the termination had already failed the existing ones, and nothing failed it afterwards. The activity refuses a terminated, failed or completed instance without retry. |
| 1.92 | 2026-10-17 | **`trial` and `past_due` tenants keep running, and the unbuilt start pre-check is withdrawn** (§5.5, §6.2, §5.10, Appendix A). Every status other than `active` paused instances, so a `trial` or `past_due` tenant stopped working although the Commercial design runs both. `trial`, `active` and `past_due` run; `cancelled`, `suspended` and `trial_expired` pause; every move into a running status resumes, which changes nothing on a running instance. The `409 TENANT_NOT_ACTIVE` pre-check at instance start was documented and never built, and Execution holds no tenant status to build it on, so the code is withdrawn; the platform stops a `cancelled` tenant's writes and a `suspended` tenant's tokens. |
| 1.93 | 2026-10-17 | **Instances start on the tenant's registered task queue** (§3.2, §5.1). The registry of tenant queues was filled on an upgrade to enterprise and polled by the Workers, but every instance started on the compiled plan's shared queue, so no instance ever used it. Instance start reads the registry: a routable registered queue is used, otherwise the plan's queue, and a failed read fails the start. A downgrade used to delete the queue once no instance ran on it, which raced a concurrent start and was never retried when instances were still running; a queue is now never deleted and a downgrade only stops routing to it. |
| 1.94 | 2026-10-17 | **The intra-tenant read scope counts active assignments and every task of the instance** (Appendix A). The text counted vacated assignments as involvement and only currently-open tasks for the department rule; every read, in the task and instance queries alike, counts active assignments and every task. A caller whose assignment was vacated (a failed or reassigned task) no longer reads the instance. |
| 1.95 | 2026-10-18 | **A failed pause or resume signal fails the tenant event** (§6.2). The terminate sweep already returned its failures so the event was redelivered, but a pause or resume signal that failed (a Temporal outage for a moment) was logged and the event was acknowledged and recorded as applied, leaving instances running for a suspended tenant or paused for a resumed one with no further event to correct them. Every sweep now fails the event after trying every instance, and only a missing execution is ignored. |
| 1.96 | 2026-10-18 | **A task completion on an instance that is not running is refused, and internal event handlers outlive a sender that gave up** (§5.10, §6.2). Complete forwarded its signal without looking at the instance, and the workflow drops a completion in any status but `RUNNING` or `DEGRADED`, so a user completing a task of a paused instance (a tenant suspended moments before, a token still valid) got success while the task stayed open and nothing redelivered the completion. The synchronous pre-check that claim, defer and the admin signals already have now covers complete. Event handlers took their context from the request, so the Event Consumer's five-second forward timeout cancelled a tenant sweep after a few seconds of progress and a large tenant was dead-lettered half done; handlers now run under their own bound. The chart's API and worker grace periods exceed the processes' own shutdown timeouts, and the migration hook's ServiceAccount and Secret are hooks too, so a first install can run it. |
| 1.97 | 2026-10-18 | **A tenant data purge after a retention period** (§3.2, §4.1, §4.6, §4.11, §4.13, §6.2, §7.6, Appendix B). Offboarding terminated a tenant's instances and deleted nothing, so its work history stayed for ever. An offboarded tenant is now recorded with a purge date (the offboarding time plus `TENANT_HISTORY_RETENTION`, 7 years by default), and a sweep deletes its instance, task, assignment, override and connector rows, its queue registry row and its recency rows in batches once the date has passed; the audit trail keeps its own 7-year floor and Temporal histories are out of scope. A tenant is marked purged only after a check finds no row left, since a batch that deleted nothing can mean another replica took the rows, and the failure alert looks back 26 hours because the sweep runs daily. The compliance rule changes the setting, not the code. |
| 1.98 | 2026-10-18 | **A Definition outage no longer fails an instance that has just started** (§3.3, §8). The first activity read the compiled plan with five attempts over about eight seconds, after which the workflow wrote the instance `FAILED` with no way back, although the client had already been told the start succeeded. The read now retries without limit, with backoff capped at 30 seconds, like the database-writing activities; only a rejection of the version is final. |
| 1.99 | 2026-10-18 | **Queue health is observable and the unbuilt queue designs are removed** (§3.2, §7.6). The poll timestamp of a dynamic queue was set once when its worker started, so `WorkerQueuePollingStalled` fired for every tenant queue five minutes later; it is refreshed on every topology poll for each queue the process serves. A worker that could not be started for a queue left its instances waiting with no signal; a per-queue gauge of non-terminal instances and an alert for instances on a queue nobody polls close that. The volume-triggered isolation and the per-process cap with sharding (§3.2 items 5 and 6) were described and never built; they are replaced by a statement that every replica polls every row. |
| 1.100 | 2026-10-18 | **The tenant status is recorded and a start can be refused on it** (§4.1, §5.5, §5.10, §7.6, Appendix A). Execution kept no tenant status, so a start for a suspended or trial-expired tenant, or inside the minutes a token outlives a suspension, was never refused. `TenantStateChanged` now records the status (the latest change wins), and `START_REFUSED_STATUSES` names the statuses for which a start answers `409 TENANT_NOT_ACTIVE`; the default is none, so behaviour is unchanged until a status is named. The tenant purge removes the row (§4.13). |
| 1.101 | 2026-10-18 | **The event lock no longer goes through PgBouncer** (§6.2, §7.6, §9.7). Tenant and user event handling is serialized by a session advisory lock, and behind a transaction-pooling PgBouncer the lock and its release could land on different server connections: a lock stayed held on one, and the next event for that tenant waited on it for as long as the connection lived. A test through a real PgBouncer showed the third acquisition timing out. The lock now uses its own small pool on a direct connection (`LOCK_DATABASE_URL`), required outside dev when `PG_BOUNCER_MODE` is true; the chart passes it and the migration job's direct address. |
| 1.102 | 2026-10-18 | **Small execution leftovers** (§3.3, §5.10). A signal to a workflow execution that no longer exists, from Complete, Defer and the other callers, surfaced as `500`; it is `409 INVALID_INSTANCE_STATE`, the instance not being in a state to take the signal. A department with several pending nodes named an arbitrary one as the old node of a force-route audit record; it names the smallest node key, so a replay records the same value. |
| 1.103 | 2026-10-18 | **Worker memory and pool settings reach the chart** (§7.5, §7.6). The Worker's 256Mi limit sat beside the SDK's default sticky cache of 10,000 workflows and no Go memory limit, and the chart could not set the pool ceiling, PgBouncer mode or the topology poll interval at all. `WORKFLOW_CACHE_SIZE` bounds the cache; the chart passes it with `PG_MAX_CONNS`, `PG_BOUNCER_MODE`, `QUEUE_TOPOLOGY_POLL_INTERVAL` and a per-process `GOMEMLIMIT`, defaults unchanged. |
| 1.104 | 2026-10-18 | **Review fixes for the start gate, the event lock and queue health** (§3.3, §4.13, §5.5, §7.6, §9.7). The purge removed the recorded status, so the start gate allowed an offboarded tenant again and a late event re-created a misleading row; the status now stays. The status was recorded after the transition, leaving a window in which a tenant whose pause was being retried could still start; it is recorded first. The event lock waited for the lock while holding a pooled connection, so a few events queued behind one long handler exhausted the pool; it now tries without blocking and retries, and discards a connection whose release failed. A persistent Definition failure inside the workflow raised no alert; each failed plan read counts as an upstream dependency error. The topology poll interval is limited to two minutes so the five-minute queue alerts cannot fire between polls, the per-queue instance gauge no longer disappears for a moment during a refresh, and the chart refuses a pooled deployment without direct lock and migration addresses and a memory limit without a unit. |
| 1.105 | 2026-10-18 | **A failed start is completed by its retry, and every close names the run** (§4.2, §5.2, §5.10, Appendix A #22). A start that failed after the instance row was written left a `RUNNING` row with no execution; the client's retry was refused with `409 DUPLICATE_BUSINESS_KEY` and the row blocked archiving the workflow, although decision 22 promised an idempotent retry. A start matching a live instance that is `RUNNING`, of the same version, started by the same caller and carrying the same context and override map now issues `StartWorkflow` again when no run id is recorded and returns that instance, before the plan is read so a later archive or assignee change cannot strand it; every other match stays `409`. The run id is recorded once the start returns, without a version bump, and every post-commit close of an execution names it when recorded, so a close issued after the key was reused cannot end the newer execution. |
| 1.106 | 2026-10-19 | **A pause survives `DEGRADED`** (§3.1, §3.4, §6.2, §7.2 test #5, §8.2.6, §8.5). A branch failure overwrote a pause with `DEGRADED` and leaving the park set `RUNNING`, and `instance-pause` and `instance-resume` were dropped while degraded, so a suspended tenant's instance ran again after an administrator's force-forward and a tenant suspension during the park was never applied. A pause in force when a branch fails is carried into the park, a pause or resume received during it is recorded with its initiator under the existing ownership rule, and the park ends `PAUSED` when a pause is held and `RUNNING` otherwise. The stored status stays `DEGRADED` throughout, so there is still no status for a degraded and paused instance; a pause recorded during the park writes nothing and emits no event until the park ends. The pause and resume API pre-checks accept `DEGRADED`. |
