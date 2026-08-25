# execution-service

The Temporal-backed execution control plane for the BPMN Workflow Engine platform. Starts, tracks, and drives workflow instances to completion; dispatches tasks to human assignees and other services. Execution-time counterpart to `definition_service` (design-time authoring/compilation of workflow templates).

Three independently deployed binaries, one Go module:

- `cmd/server` — HTTP API (`:8080`) + gRPC (`:9090`) + the outbox relay + the Temporal client (`StartWorkflow`/`SignalWorkflow`/`QueryWorkflow`).
- `cmd/worker` — the Temporal Worker process: polls task queues, hosts the workflow function and Activities. Minimal `:8081` health/ready surface plus a dedicated `:8082` `/metrics` listener, no business HTTP/gRPC surface of its own.
- `cmd/connector-worker` — executes connector-typed tasks (automatic REST/SQL/storage/email/document-extract/chat-notify dispatch, per the `workflow-connectors` LLD). Consumes a Valkey Stream `cmd/server` publishes onto, dispatches to a real `workflow-connectors.Connector`, and reports the outcome back via `cmd/server`'s own `/internal/connector-tasks` HTTP endpoints — it never touches the Temporal SDK directly. Its own new dependencies (Valkey Streams, OpenBao) are separate from `cmd/server`'s KV cache and `cmd/worker`'s (still zero) Valkey usage.

## Private Module Access

This service consumes private Go modules from the `github.com/BCBP-SOLUTIONS-FZC-LLC/*` organization (`platform-events`, `platform-pgcommon`, `platform-gincommon`, `workflow-models`, `workflow-connectors`).

Before running Go commands or compiling the app, configure Go to bypass the public proxy and checksum database:

```bash
go env -w GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC/*
```

### GitHub Authentication

Configure Git to authenticate against GitHub to fetch the private packages:

**SSH key (recommended for local dev):**

```bash
git config --global url."ssh://git@github.com/".insteadOf "https://github.com/"
```

**Personal Access Token (for CI/CD or HTTPS):**
Add a classic/fine-grained PAT with read access to the credential store:

```bash
git config --global credential.helper store
echo "https://x-access-token:<your-github-token>@github.com" > ~/.git-credentials
chmod 600 ~/.git-credentials
```

---

## Quick start

```bash
# 1. Configure Go private module path
go env -w GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC/*

# 2. Install dev tooling
make tools

# 3. Configure environment and install the local pre-commit hook
make setup

# 4. Start local infra (Postgres 18 + Valkey 8 + LocalStack + PgBouncer + Temporal dev server)
make docker-up

# 5. Apply schema migrations (outbox + domain) — neither binary migrates at boot
make migrate

# 6. Generate proto + sqlc code
make generate

# 7. Start the server, worker, or connector-worker (AWS stubs active by default)
go run ./cmd/server
go run ./cmd/worker
go run ./cmd/connector-worker
```

`cmd/connector-worker` additionally requires `OPENBAO_ADDR`, `DEFINITION_SERVICE_INTERNAL_HTTP_ADDR` (definition_service owns the endpointAlias/queryAlias registry and serves it over `GET /internal/connector-aliases`), and `EXECUTION_SERVICE_INTERNAL_ADDR` — see `.env.example` for the full list.

Temporal's Web UI is available at <http://localhost:8233> once `make docker-up` is running.

## Project layout

Clean Architecture, dependency direction `domain ← port ← service ← adapter`, plus two peer layers that sit alongside `adapter` rather than under it: `internal/workflow` for the Temporal workflow function and Activities (never imported by `adapter/`, never imports it back — the two connect only via runtime registration in `cmd/worker/main.go`), and `internal/observability` for centralized Prometheus metrics, OTel tracing-init helpers, and Temporal Search-Attribute helpers (execution LLD §7.6, §3.6) — a leaf package importable by any layer, including `workflow`, since Search Attributes can only be upserted from inside workflow-context code. See `.go-arch-lint.yml` for the enforced import graph.

```sh
cmd/server/, cmd/worker/, cmd/connector-worker/ — composition roots (three independently deployed binaries)
internal/core/{domain,port,service}/
internal/workflow/             — Temporal workflow function + Activities
internal/observability/        — metrics, tracing init, Search-Attribute helpers
internal/adapter/{inbound,outbound}/
internal/eventschema/          — embedded JSON Schemas for the 18 outbound events (LLD §6.8)
internal/config/
db/migrations/, db/queries/    — workflow_execution schema (RLS via app_tenant_id()/rls_check_tenant())
db/sqlc_schema_ref/            — non-migration DDL so sqlc can type-check queries against library-owned tables (outbox_events)
api/proto/                     — execution_service.proto + definition.proto (buf)
api/asyncapi.yaml              — outbound/inbound event contract (LLD §6)
docs/lld/                      — in-repo copy of the design repo's LLD
deploy/helm/                   — Helm chart: two Deployments (api, worker), PDB/HPA/NetworkPolicy per process
test/{fixtures,unit,integration,e2e,workflow}/
```

## Event schema governance

`make schema-validate` runs `platform-schemagov` (Docker) against `api/asyncapi.yaml` + `internal/eventschema/`. See `make help` for `extract-schemas`/`schema-diff`/`schema-register`/`schema-prune`.

## Common commands

Run `make help` for the full target list.

| Command | What it does |
| --- | --- |
| `make tools` | Install pinned dev tooling (sqlc, buf, mockgen, golangci-lint, go-arch-lint) |
| `make setup` | Copy `.env.example` → `.env`, install the pre-commit hook |
| `make docker-up` / `make docker-down` | Start/stop local infra (Postgres, Valkey, LocalStack, PgBouncer, Temporal dev server) |
| `make migrate` | Apply schema migrations (outbox + domain) |
| `make generate` | Regenerate proto (buf) + sqlc code |
| `make build` | Compile `cmd/server`, `cmd/worker`, and `cmd/connector-worker` |
| `make test` | Unit tests — `internal/...`, `test/unit/...`, `test/workflow/...` — race detector, coverage |
| `make test-integration` | Integration tests (testcontainers, real Postgres) |
| `make test-ci` | Unit + integration, merged coverage — what CI runs |
| `make lint` / `make fix` | golangci-lint (read-only / with `--fix`) |
| `make arch-lint` | Enforce Clean Architecture import direction (`.go-arch-lint.yml`) |
| `make cover-gaps` | List uncovered/partially-covered functions from the last `test-ci` run |
| `make check` | Full local CI: gofmt + lint + vet + arch-lint + test + per-package coverage gate |
| `make helm-lint` | `helm lint` + `kubeconform -strict` against the rendered Helm chart |
| `make pin-base-images` | Re-pin the Dockerfile's base images by digest (`.docker-digests`) |

## Deployment

Two images, one multi-stage `Dockerfile` (`--target server` / `--target worker`), digest-pinned base images (`.docker-digests`, re-pin with `make pin-base-images`). `make docker-build` / `make docker-lint` / `make docker-trivy` / `make docker-check` build and check them locally.

**Helm chart**: `deploy/helm/` — one chart, two Deployments (`-api`, `-worker`), each with its own PDB/HPA/ServiceAccount/NetworkPolicy, plus a `ServiceMonitor`+`PrometheusRule` (`serviceMonitor.enabled`, requires the Prometheus Operator CRDs). `make helm-lint` runs `helm lint` plus a `kubeconform -strict` validation of the rendered manifests.

**`deploy/monitoring/`**: static, non-Helm-templated twins for clusters without the Prometheus Operator — `app-alerts.yml` (must stay in sync with `deploy/helm/templates/prometheusrule.yaml`) and `prometheus-adapter-rule.yaml` (the Custom Metrics API rule the API's optional RPS-based HPA scaling needs).

```bash
helm lint deploy/helm
helm template my-release deploy/helm \
  --set-string secret.secretValues.DATABASE_URL=... \
  --set-string secret.secretValues.TEMPORAL_HOST_PORT=...
helm upgrade my-release deploy/helm --install --namespace <ns>
```

**Transport security.** mTLS between all pods is enforced by the cluster's service mesh (Envoy) — the chart's `NetworkPolicy` templates are L3/L4 defense-in-depth alongside it, not a substitute. `/internal/*` has no dedicated port yet (one HTTP port serves both browser-gateway and machine-to-machine traffic), so its NetworkPolicy rule scopes all of that port to the specific caller pod identities allowed to reach it (the Shared Workflow-Events Consumer, Definition Service) rather than the path itself — real path-level enforcement is the existing `x-internal-token` application-layer check. mTLS to the Temporal frontend is conditional on the cluster requiring it (`temporalMTLS.enabled` in `values.yaml`); the cert/key pair is cluster-operator-provisioned and mounted from a Kubernetes `Secret`, never generated by this chart.

**Note**: `cmd/server` now has a real composition root and listener (T2.1); `cmd/worker` likewise runs a real Temporal worker. The chart's shape (two Deployments, per-process PDB/HPA/NetworkPolicy) matches what's actually deployed today, not just the LLD's target shape.
