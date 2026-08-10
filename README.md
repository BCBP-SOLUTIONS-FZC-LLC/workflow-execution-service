# execution-service

The Temporal-backed execution control plane for the BPMN Workflow Engine platform. Starts, tracks, and drives workflow instances to completion; dispatches tasks to human assignees and other services. Execution-time counterpart to `definition_service` (design-time authoring/compilation of workflow templates).

Two independently deployed binaries, one Go module:
- `cmd/server` — HTTP API (`:8080`) + gRPC (`:9090`) + the outbox relay + the Temporal client (`StartWorkflow`/`SignalWorkflow`/`QueryWorkflow`).
- `cmd/worker` — the Temporal Worker process: polls task queues, hosts the workflow function and Activities. Minimal `:8081` health/metrics surface, no business HTTP/gRPC surface of its own.

Status: Tier-0 (bootstrapped skeleton — build/lint/test tooling, DB schema, proto contracts). No business logic yet.

## Private Module Access

This service consumes private Go modules from the `github.com/BCBP-SOLUTIONS-FZC-LLC/*` organization (`platform-events`, `platform-pgcommon`, `platform-gincommon`, `workflow-models`).

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

# 7. Start the server or worker (AWS stubs active by default)
go run ./cmd/server
go run ./cmd/worker
```

Temporal's Web UI is available at <http://localhost:8233> once `make docker-up` is running.

## Project layout

Clean Architecture, dependency direction `domain ← port ← service ← adapter`, plus a new peer layer `internal/workflow` for the Temporal workflow function and Activities (never imported by `adapter/`, never imports it back — the two connect only via runtime registration in `cmd/worker/main.go`). See `.go-arch-lint.yml` for the enforced import graph.

```
cmd/server/, cmd/worker/       — composition roots (two independently deployed binaries)
internal/core/{domain,port,service}/
internal/workflow/             — Temporal workflow function + Activities
internal/adapter/{inbound,outbound}/
internal/config/
db/migrations/, db/queries/    — workflow_execution schema (RLS via app_tenant_id())
api/proto/                     — execution_service.proto + definition.proto (buf)
deploy/helm/                   — Helm chart: two Deployments (api, worker), PDB/HPA/NetworkPolicy per process
test/{fixtures,unit,integration,e2e,workflow}/
```

## Common commands

Run `make help` for the full target list. Most used: `make tools`, `make setup`, `make docker-up`, `make migrate`, `make generate`, `make build`, `make test`, `make test-integration`, `make check`.

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

**Note**: `cmd/server`/`cmd/worker` are still stub mains (no real listener, Temporal client/worker, or `/healthz`/`/readyz` yet) — the chart documents the LLD's real target deploy shape, ready for once a composition root lands; deployed today, both Deployments' pods exit immediately and crash-loop.
