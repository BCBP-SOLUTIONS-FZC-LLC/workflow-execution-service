# CLAUDE.md

## What This Repo Is

`execution-service` is the Temporal-backed runtime for the BPMN Workflow Engine platform: it starts, tracks, and drives workflow instances to completion, dispatching tasks to human assignees and other services along the way. It's the execution-time counterpart to `definition_service`, which owns design-time authoring and compilation of workflow templates.

## Getting Started

Run `make help` for all available targets, or see [README.md](../README.md) for the full setup walkthrough.

## Critical Rules — Never Break These

- **Never add a `replace` directive** pointing at a local path in `go.mod`.
- **Never import from a local path** in source code — always the full `github.com/BCBP-SOLUTIONS-FZC-LLC/...` module path.
- do not bloat this file with code specifics, this should only contain guidance. update this when needed.

## Local Development Context

See `.claude/CLAUDE.local.md` (gitignored, not committed) for local reference paths — e.g. sibling repos and design docs on this machine.
