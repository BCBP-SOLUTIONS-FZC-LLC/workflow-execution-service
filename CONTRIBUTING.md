# Contributing to workflow-execution-service

## Prerequisites

| Tool | Install |
| --- | --- |
| Go 1.26+ | [go.dev/dl](https://go.dev/dl) |
| Docker, for integration tests and local infrastructure | [docs.docker.com](https://docs.docker.com/get-docker/) |
| Private module access | `go env -w GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC/*` and SSH access to the organisation |

## Workflow

```bash
make tools && make setup     # tooling, .env, pre-commit hook
make docker-up && make migrate && make generate
make check                   # what CI gates on, locally
```

The pre-commit hook runs tidy, format and lint.

## Conventions

- Dependency direction is `domain ← port ← service ← adapter`; `make arch-lint` enforces it.
- Non-test code carries no comments except `//go:` and `//nolint` directives. Names carry the meaning and the design documents carry the rationale.
- Logs, metrics and traces go through `platform-gincommon`, database access through `platform-pgcommon`, events and the outbox through `platform-events`. New metrics are built in `internal/observability` as `workflow_execution_service_*` collectors, and `make metrics-lint` must pass.
- Code that inserts a row inside a Temporal Activity derives the row id deterministically, and an Activity with no status to gate a retry on checks for the existing record first.
- A repository call with a tenant id sets the tenant GUC; a caller composing several calls in one transaction sets it on the context before the transaction opens.
- A fix starts with a test of the correct behaviour that fails, then the change.
- Never add a `replace` directive to `go.mod` and never import from a local path.

## Specifications and documents

A change to a route updates `docs/swagger/openapi.yaml` or `api/internal-openapi.yaml`; the parity tests fail otherwise. A new produced event adds its schema under `internal/eventschema`, its message in `api/asyncapi.yaml` and its type to the domain list. A change to behaviour updates the LLD, the README, `ARCHITECTURE.md` and the CHANGELOG in the same pull request. The in-repo LLD stays content-identical to the design repository copy.

## Pull requests

Open pull requests as drafts while iterating, fill every section of the template, and add a CHANGELOG entry under `[Unreleased]` for any change to code, contracts or deployment. Tags are cut after merge, never inside a pull request.
