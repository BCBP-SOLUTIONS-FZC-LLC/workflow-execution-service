# execution-service

The Temporal-backed execution control plane for the BPMN Workflow Engine platform. Starts, tracks, and drives workflow instances to completion; dispatches tasks to human assignees and other services. Execution-time counterpart to `definition_service` (design-time authoring/compilation of workflow templates).

Two independently deployed binaries, one Go module:

- `cmd/server` — HTTP API (`:8080`) + gRPC (`:9090`) + the outbox relay + the Temporal client (`StartWorkflow`/`SignalWorkflow`/`QueryWorkflow`).
- `cmd/worker` — the Temporal Worker process: polls task queues, hosts the workflow function and Activities. Minimal `:8081` health/metrics surface, no business HTTP/gRPC surface of its own.

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

```sh
cmd/server/, cmd/worker/       — composition roots (two independently deployed binaries)
internal/core/{domain,port,service}/
internal/workflow/             — Temporal workflow function + Activities
internal/adapter/{inbound,outbound}/
internal/eventschema/          — embedded JSON Schemas for the 18 outbound events (LLD §6.8)
internal/config/
db/migrations/, db/queries/    — workflow_execution schema (RLS via app_tenant_id()/rls_check_tenant())
db/sqlc_schema_ref/            — non-migration DDL so sqlc can type-check queries against library-owned tables (outbox_events)
api/proto/                     — execution_service.proto + definition.proto (buf)
api/asyncapi.yaml              — outbound/inbound event contract (LLD §6)
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
| `make build` | Compile `cmd/server` and `cmd/worker` |
| `make test` | Unit tests — `internal/...`, `test/unit/...`, `test/workflow/...` — race detector, coverage |
| `make test-integration` | Integration tests (testcontainers, real Postgres) |
| `make test-ci` | Unit + integration, merged coverage — what CI runs |
| `make lint` / `make fix` | golangci-lint (read-only / with `--fix`) |
| `make arch-lint` | Enforce Clean Architecture import direction (`.go-arch-lint.yml`) |
| `make cover-gaps` | List uncovered/partially-covered functions from the last `test-ci` run |
| `make check` | Full local CI: gofmt + lint + vet + arch-lint + test + per-package coverage gate |
