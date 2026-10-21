# workflow-execution-service

The runtime control plane of the BCBP Workflow Engine: it starts, tracks and drives workflow instances to completion on Temporal, and dispatches tasks to human assignees and connectors.

**Repository:** `github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service`  
**Module:** Go 1.26.10 · private module · three containerised binaries not yet deployed to any live environment

---

## Mental model

The engine has two halves. The Definition Service is the design half: it owns templates and their compiled plans. This service is the runtime half: it reads a compiled plan once per instance and interprets it on Temporal.

```text
Frontend ──REST──► API server ──Temporal──► Worker ──► PostgreSQL (outbox) ──relay──► SNS
                       │                       ▲
                       └── Valkey Stream ──► Connector worker ──internal HTTP──► API server
Definition Service ◄── compiled plan, connector aliases
```

| Binary | What it owns | What it does NOT own |
| --- | --- | --- |
| **API server** (`cmd/server`) | The public and internal HTTP API, the Temporal client, the outbox relay, the compiled-plan cache, the RLS-violation exporter and the operational gauges | Interpreting a plan: the worker does |
| **Worker** (`cmd/worker`) | The Temporal workflow function and its Activities, one worker per active tenant task queue | Any business HTTP surface; it serves health and metrics only |
| **Connector worker** (`cmd/connector-worker`) | Consuming connector-typed tasks from a Valkey Stream and running them through `workflow-connectors` | The Temporal SDK: it reports outcomes over the API server's internal HTTP endpoints. It keeps the Drive document registry and send-email intents in PostgreSQL and document references in S3 and Valkey |

| Concern | Owner |
| --- | --- |
| Workflow instances, tasks, assignments, overrides, delegation reroute | This service |
| Compiled plans and the connector alias registry | Definition Service |
| Event consumption from SQS, dedup and forwarding | The shared Workflow-Events Consumer |
| Eligibility, membership, tenant lifecycle, automation principal | IAM and Org & Membership |

**Source of truth for two contracts:** the internal API in [api/internal-openapi.yaml](api/internal-openapi.yaml) and the 18 produced events in [api/asyncapi.yaml](api/asyncapi.yaml).

---

## Why this service exists

A published template can run for days across departments and humans. Keeping the interpreter, the task and assignment state, and the outbound event stream in one service gives a single owner of instance truth: Temporal holds the durable execution, PostgreSQL holds the queryable state, and every state change leaves one transactional outbox row that is published to SNS.

---

## Contents

- [Quick start](#quick-start)
- [Project layout](#project-layout) and [Architecture](#architecture)
- [API overview](#api-overview)
- [Configuration](#configuration)
- [Deployment](#deployment), [Observability](#observability), [Security](#security)
- [Testing](#testing), [CI](#ci), [Versioning and releases](#versioning-and-releases)
- [Out of scope](#out-of-scope) and [See also](#see-also)

> **Architecture** — [ARCHITECTURE.md](ARCHITECTURE.md): layer model, process model and data flow.
>
> **API specs** — REST: [docs/swagger/openapi.yaml](docs/swagger/openapi.yaml). Internal: [api/internal-openapi.yaml](api/internal-openapi.yaml). Events: [api/asyncapi.yaml](api/asyncapi.yaml), rendered at `/asyncapi` in dev mode.
>
> **Contributing** — [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Private Module Access

This service consumes private Go modules from the `github.com/BCBP-SOLUTIONS-FZC-LLC/*` organization (`platform-events/v2`, `platform-pgcommon/v2`, `platform-gincommon`, `workflow-models`, `workflow-connectors`).

```bash
go env -w GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC/*
git config --global url."ssh://git@github.com/".insteadOf "https://github.com/"
```

CI authenticates with a read-only token held in the repository secrets.

---

## Quick start

```bash
make tools          # pinned dev tooling
make setup          # .env from .env.example, pre-commit hook
make docker-up      # Postgres 18, Valkey 8, LocalStack, PgBouncer, Temporal dev server
make migrate        # no binary migrates at boot
make generate       # sqlc
go run ./cmd/server
go run ./cmd/worker
go run ./cmd/connector-worker
```

AWS stubs are active by default in `dev` and `local`; any other `APP_ENV` requires the real SNS topic and Glue registry. Temporal's Web UI is at <http://localhost:8233>.

---

## Project layout

```text
cmd/server, cmd/worker, cmd/connector-worker   composition roots
internal/core/{domain,port,service}            entities, interfaces, business logic
internal/workflow                              the Temporal workflow function and Activities
internal/adapter/{inbound,outbound}            HTTP handlers; Postgres, Temporal, Valkey, SNS, OpenBao, HTTP clients
internal/observability                         metrics, telemetry init, Temporal interceptors, search attributes
internal/eventschema                           embedded JSON Schemas of the 18 produced events
internal/config                                environment loading and validation
db/migrations, db/queries                      schema and sqlc queries
api/, docs/swagger/                            internal OpenAPI, AsyncAPI, public OpenAPI
deploy/helm, deploy/monitoring                 chart and Prometheus rules
test/{fixtures,unit,integration,e2e,workflow,contract}
```

## Architecture

Clean Architecture, dependency direction `domain ← port ← service ← adapter`. `internal/workflow` and `internal/observability` are peer leaves: the workflow package never imports an adapter, and the two connect only through registration in `cmd/worker`. `.go-arch-lint.yml` enforces the graph in CI.

Platform libraries and what each owns:

| Library | Owns |
| --- | --- |
| `platform-gincommon` | Logging, tracing, HTTP middleware, every metric constructor, the metrics identity |
| `platform-pgcommon/v2` | The pgx pool, RLS GUC injection, transactions, migrations, `platform_db_*` metrics |
| `platform-events/v2` | The event envelope, the outbox and its relay, the SNS publisher, `platform_library_info` |
| `workflow-models/v2` | The compiled-plan DSL (`pkg/dsl`) and stage enums |
| `workflow-connectors/v2` | The connector implementations the connector worker runs, with the retry decision, document references and send-intent store |

---

## API overview

| Surface | Spec | Callers |
| --- | --- | --- |
| Public REST under `/api/v1` | [docs/swagger/openapi.yaml](docs/swagger/openapi.yaml) | The frontend, through the gateway |
| Internal HTTP under `/api/v1/internal` | [api/internal-openapi.yaml](api/internal-openapi.yaml) | The Definition Service, IAM, the Workflow-Events Consumer, the connector worker; `x-internal-token` required |
| Produced events | [api/asyncapi.yaml](api/asyncapi.yaml) | Subscribers of the `wf-workflow-events` SNS topic |
| Consumed events | the same file | The Workflow-Events Consumer forwards each to `/api/v1/internal/events/{delegation,user-profile,tenant,workflow-task}` |

Tests fail when a router operation is missing from its spec or the reverse, and when the AsyncAPI messages differ from the produced and handled event types. `make design-sync-check` fails when a spec, the LLD or an event schema differs from its copy in the design repository.

---

## Configuration

`.env.example` lists every variable, including the ones with a product decision behind them: `START_REFUSED_STATUSES` (tenant statuses whose instance starts are refused, empty by default), the tenant purge settings (`TENANT_PURGE_ENABLED`, `TENANT_HISTORY_RETENTION`), `WORKFLOW_CACHE_SIZE` (sizing) and `LOCK_DATABASE_URL`; the chart also sets `GOMEMLIMIT` per process. `APP_ENV` is one of `local`, `dev`, `test`, `staging`, `prod`; outside `dev` and `local` each binary refuses to start without the settings it needs:

| Binary | Required outside dev |
| --- | --- |
| API server | `DATABASE_URL`, `OUTBOX_RELAY_DATABASE_URL`, `VALKEY_ADDR`, `TEMPORAL_HOST_PORT`, `DEFINITION_SERVICE_URL` and `_TOKEN`, `INTERNAL_API_TOKEN`, `ELIGIBILITY_BASE_URL`, `TOKEN_SERVICE_BASE_URL`, `SNS_TOPIC_ARN`, `GLUE_REGISTRY_NAME`, `AWS_USE_STUB=false`, `LOCK_DATABASE_URL` when `PG_BOUNCER_MODE` is true |
| Worker | `DATABASE_URL`, `TEMPORAL_HOST_PORT`, `DEFINITION_SERVICE_URL` and `_TOKEN` |
| Connector worker | `DATABASE_URL`, `VALKEY_ADDR`, `DEFINITION_SERVICE_URL` and `_TOKEN`, `INTERNAL_API_TOKEN`, `EXECUTION_SERVICE_INTERNAL_ADDR`, `OPENBAO_TOKEN`, `CONNECTOR_DOCS_BUCKET`; `OPENBAO_ADDR` and `EXECUTION_SERVICE_INTERNAL_ADDR` in every environment, dev included |

---

## Deployment

Three images from one multi-stage `Dockerfile` (`server`, `worker`, `connector-worker`), base images pinned by digest (`make pin-base-images`). The Helm chart in `deploy/helm` has one Deployment per binary, each with its own PDB and NetworkPolicy, HPAs for the API and the worker, a migration job, and an optional `ServiceMonitor` and `PrometheusRule`. `deploy/monitoring` holds the static twins for clusters without the Prometheus Operator. `make helm-lint` renders the chart and validates it with `kubeconform`; a test checks that every Deployment sets the variables its binary requires.

Roll one process back with `kubectl rollout undo deployment/<name>`, not `helm rollback`, which reverts the whole release. mTLS between pods is the service mesh's job; the NetworkPolicies are L3/L4 defence in depth.

---

## Observability

Logs, metrics and traces go through `platform-gincommon`; every binary calls the same telemetry initialisation. Every series carries `domain` (`workflow`), `service` (`workflow-execution-service`) and `environment` (`APP_ENV`).

| Tier | Names | Source |
| --- | --- | --- |
| Platform-shared | `platform_http_*`, `platform_db_*`, `platform_outbox_*`, `platform_messages_*`, `platform_build_info`, `platform_library_info` | The platform libraries |
| Workflow domain-shared | none defined yet | Registry |
| Service-specific | `workflow_execution_service_*` | This service |

[docs/observability/metric-registry.md](docs/observability/metric-registry.md) is generated from the registry and a real scrape. `make metrics-lint` fails on a collector built outside `internal/observability`, a name or label the standard rejects, an alert naming a retired metric, a stale inventory, and a test checks that every metric an alert rule names is exported. Every alert carries a `runbook` annotation naming its entry in [docs/observability/runbooks.md](docs/observability/runbooks.md), and a test keeps the annotations, the entries and the chart in step. Tracing exports over OTLP and propagates through Temporal and outbound HTTP calls. The API server exports sampled `rls_violation_log` rows and samples the DEGRADED and READY-task gauges through the outbox relay role.

---

## Security

- **Tenant isolation:** row-level security is forced on every tenant table, with a policy that checks the tenant GUC through a function that logs sampled violations.
- **Internal API:** `x-internal-token`, compared in constant time; request bodies are capped at 10 MiB.
- **Connector credentials:** read from OpenBao per tenant; a plan-supplied credential field is never trusted.
- **Servers:** read-header, read, write and idle timeouts on every listener; the request timeout sits inside the observability middleware.
- **Container:** distroless, non-root, read-only root filesystem, all capabilities dropped.

---

## Testing

| Directory | Content | Command |
| --- | --- | --- |
| `internal/**/*_test.go`, `test/unit/` | Unit tests | `make test` |
| `test/workflow/` | Temporal workflow tests on the test environment | `make test` |
| `test/contract/` | Internal API and Definition client contract tests, chart environment check | `make test` |
| `test/integration/` | Postgres, Valkey, Temporal via testcontainers | `make test-integration` |
| `test/e2e/` | The API and worker against real Temporal, Postgres and Valkey, driven over HTTP | `make test-e2e` |
| `test/compat/` | The Definition Service (`DEFINITION_SERVICE_DIR`, default `../definition_service`), this repository's connector worker and the Workflow-Events Consumer (`EVENT_CONSUMER_DIR`, default `../event_consumer`) as real binaries against this service's real API, worker and stores, with LocalStack SNS and SQS | `make test-compat` |

`make check` runs formatting, lint (build tags `integration`, `e2e` and `compat` included), vet, arch-lint, metrics-lint, the breaking-change gate, the design-sync script's test and the merged-coverage gate.

| Command | What it does |
| --- | --- |
| `make tools` / `make setup` | Install tooling, `.env` and the pre-commit hook |
| `make docker-up` / `make docker-down` | Local infrastructure |
| `make migrate` / `make generate` / `make mock` | Schema, sqlc, mocks |
| `make test` / `make test-integration` / `make test-ci` | Unit, integration, both with merged coverage |
| `make test-e2e` / `make test-compat` | End to end, and compatibility with the Definition Service |
| `make lint` / `make fix` | golangci-lint read-only or fixing |
| `make arch-lint` / `make metrics-lint` / `make metrics-inventory` | Import graph, observability standard, regenerate the inventory |
| `make contract-check` / `make contract-breaking` | Copied Definition contract, internal spec breaking-change gate |
| `make design-sync-check` / `make design-sync-check-test` | Fail when a document kept here and in the design repository differs (`DESIGN_DIR=<checkout>`, or `GH_TOKEN` reads `Haroon-BCBP/design`); local only, because CI does not read the personal design repository |
| `make docker-build` / `make pin-base-images` / `make lint-workflows` | The three images, base-image digests, `actionlint` on the workflows |
| `make helm-lint` / `make vuln` / `make docker-check` | Chart, dependency vulnerabilities, Dockerfile lint and scan |
| `make check` | Everything CI gates on, locally |

---

## CI

Pull requests and pushes run `ci.yml`: a `ci-gate` job skips draft pull requests and those labelled `skip-ci`, then code generation, `validate-quality`, `validate-test`, image builds, a Trivy scan and a smoke test. `validate-test` also runs the end-to-end tier in an `e2e` job, and in a `compat` job the compatibility suite against the Definition Service's `feat/service` branch, which that repository's CI runs against this one too. Pull requests that change only Markdown or `docs/` skip CI. `changelog-check.yml` requires a CHANGELOG entry on every pull request that touches code or contracts.

## Versioning and releases

Releases are Git tags `vMAJOR.MINOR.PATCH`, cut on the merged commit after the CHANGELOG `[Unreleased]` section moves to a versioned heading. The `Release` workflow refuses a tag with no CHANGELOG entry. See [VERSIONING.md](VERSIONING.md) and [CHANGELOG.md](CHANGELOG.md).

---

## Out of scope

| Concern | Owner |
| --- | --- |
| Authoring, validating and compiling templates | Definition Service |
| Consuming SQS and forwarding events | The shared Workflow-Events Consumer |
| Identity, tokens, tenant resolution, eligibility | IAM and Org & Membership |
| DSL types | `workflow-models` |
| Connector implementations | `workflow-connectors` |

This repository carries no `LICENSE` file: it is private and proprietary to BCBP Solutions.

## See also

- [ARCHITECTURE.md](ARCHITECTURE.md) · [CONTRIBUTING.md](CONTRIBUTING.md) · [SECURITY.md](.github/SECURITY.md) · [VERSIONING.md](VERSIONING.md) · [CHANGELOG.md](CHANGELOG.md)
- The execution-service LLD in [docs/lld/](docs/lld/), content-identical to the design repository copy
