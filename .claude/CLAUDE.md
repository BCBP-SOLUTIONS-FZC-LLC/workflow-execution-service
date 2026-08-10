# CLAUDE.md

## What This Repo Is

`execution-service` is the Temporal-backed runtime for the BPMN Workflow Engine platform: it starts, tracks, and drives workflow instances to completion, dispatching tasks to human assignees and other services along the way. It's the execution-time counterpart to `definition_service`, which owns design-time authoring and compilation of workflow templates.

Currently at Tier-0: a bootstrapped skeleton (build/lint/test tooling, DB schema, proto contracts). No business logic yet.

## Getting Started

Run `make help` for all available targets, or see [README.md](../README.md) for the full setup walkthrough.

## Critical Rules — Never Break These

- **Never add a `replace` directive** pointing at a local path in `go.mod`.
- **Never import from a local path** in source code — always the full `github.com/BCBP-SOLUTIONS-FZC-LLC/...` module path.
- do not bloat this file with code specifics, this should only contain guidance. update this when needed.

## Deploy/CI Conventions

- Digest-pin the Dockerfile with `make pin-base-images`, never by hand — its `sed` pattern preserves each `FROM` line's own `AS <stage>` regardless of how many stages share a base image (a real bug here once silently dropped `AS server`/`AS worker` since both stages share the same distroless image).
- `cmd/server`/`cmd/worker` are stub mains with no real listener yet — CI's `smoke-tests` job only checks what's real today (clean container startup, the `migrate` subcommand) and says so in its own comment. Don't add the full instantiate/claim/complete flow until a composition root exists to serve it.
- Helm chart resources needing independent per-process rollback (this chart's two Deployments) must use `kubectl rollout undo deployment/<name>`, not `helm rollback` — Helm's rollback history is release-scoped, so rolling back the release would revert both Deployments even when only one process regressed.
- New Docker/CI/Helm tooling is installed via `make tools` (see the Makefile's version-pinned block), never invoked ad hoc — `lint-workflows` (actionlint), `helm-lint` (helm + kubeconform) mirror the existing `docker-lint`/`docker-trivy` pattern.

## Local Development Context

See `.claude/CLAUDE.local.md` (gitignored, not committed) for local reference paths — e.g. sibling repos and design docs on this machine.
