# Architecture

This document describes how `workflow-execution-service` is built: its processes, layers, data flow and operational surfaces. The behaviour of the engine is specified in the execution LLD in [docs/lld/](docs/lld/).

## Processes

| Binary | Role | Listeners |
| --- | --- | --- |
| `cmd/server` | HTTP API, Temporal client, outbox relay, RLS-violation exporter, operational gauge sampler | API `:8080`, metrics `:9091` |
| `cmd/worker` | Temporal worker: the workflow function and its Activities, one worker per active task queue | health `:8081`, metrics `:8082` |
| `cmd/connector-worker` | Runs connector-typed tasks from a Valkey Stream and reports outcomes over HTTP | health and metrics `:8081` |

All three call the same telemetry initialisation before they create any pool or collector, so each exports the same identity (`domain`, `service`, `environment`).

## Layers

```text
domain ← port ← service ← adapter
                workflow (peer of adapter)
                observability (leaf, importable by any layer)
```

- `internal/core/domain` holds entities, sentinel errors and event type names.
- `internal/core/port` holds the interfaces the service layer needs.
- `internal/core/service` holds the business logic and imports only domain, port and observability.
- `internal/adapter/inbound/http` holds the Gin router, handlers and middleware; `internal/adapter/outbound` holds the Postgres repositories (sqlc over `platform-pgcommon`), the Temporal Activities and client, Valkey cache and stream, the SNS and Glue codec path, OpenBao, and the HTTP clients for the Definition Service, eligibility, IAM and the Token Service.
- `internal/workflow` holds the interpreter. It never imports an adapter; `cmd/worker` registers the Activities at runtime.

`.go-arch-lint.yml` enforces these directions.

## Data flow

1. A client calls `POST /api/v1/instances`. The API server reads the compiled plan from the Definition Service, writes the instance row, and starts a Temporal workflow on the tenant's task queue.
2. The worker interprets the plan. Each Activity writes task, assignment and instance state and enqueues an outbox event in the same transaction.
3. The outbox relay, running as a `BYPASSRLS` role, publishes each event to the `wf-workflow-events` SNS topic with the Glue wire format.
4. Human tasks are claimed and completed through the API, which signals the workflow. Connector tasks are published to a Valkey Stream; the connector worker runs them and reports the outcome to the internal endpoints, which signal the workflow.
5. Events from IAM and Org & Membership reach the internal event endpoints through the shared Workflow-Events Consumer. Each handler checks the processed-event table, applies the change in one transaction and records the event id.
6. When a tenant is offboarded its instances are terminated and the tenant is recorded in `tenant_retention` with a purge date (`TENANT_HISTORY_RETENTION`, 7 years by default). A sweep in the API process deletes the tenant's instance, task, assignment, override and connector rows, its queue registry row and its recency rows in batches through the relay role once the date has passed. The outbox, the audit trail, keeps its own retention.

## Tenant isolation

Every tenant table has row-level security forced, with a policy that calls a function checking the `app.tenant_id` GUC and logging sampled violations to `rls_violation_log`. A repository call that carries a tenant id sets the GUC itself; a caller composing several calls in one transaction sets it on the context before opening the transaction. The relay role bypasses RLS and is used only for the outbox, the violation exporter and the operational gauges.

## Observability

Collectors are built in `internal/observability` with `platform-gincommon` constructors, so the identity labels and naming rules are enforced at construction. The Temporal worker installs a metrics interceptor (activity duration by outcome, workflow panics) and a tracing interceptor; outbound HTTP calls carry the trace context. [docs/observability/](docs/observability/) documents the tiers and the generated inventory.

## Configuration

`internal/config` loads and validates the environment. `APP_ENV` must be one of `local`, `dev`, `test`, `staging`, `prod`. Each binary validates only the settings it uses, and refuses to start outside dev without them. `.env.example` is the reference list.

## Specifications

| File | Describes | Checked by |
| --- | --- | --- |
| `docs/swagger/openapi.yaml` | Public REST API | a parity test against the router |
| `api/internal-openapi.yaml` | Internal HTTP API | a parity test and `make contract-breaking` |
| `api/contracts/` | The Definition Service's internal contract, copied | `make contract-check` |
| `api/asyncapi.yaml` | Produced and consumed events | parity tests against the produced and handled event types, and schema governance |
