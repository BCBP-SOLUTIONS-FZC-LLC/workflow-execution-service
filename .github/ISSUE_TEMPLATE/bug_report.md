---
name: Bug report
about: Report a defect in execution-service (gRPC API, Temporal workflow/Activities, database adapter, outbox relay, CI)
title: '[BUG] '
labels: bug
assignees: ''
---

## Description
A clear description of the bug.

## Service version
`github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service` — tag / commit SHA:

## Go version
`go version goX.Y.Z ...`

## Environment
- [ ] Local dev (`go run ./cmd/server` / `go run ./cmd/worker`)
- [ ] Docker Compose (`make docker-up`)
- [ ] Staging
- [ ] Production

## Affected area
- [ ] gRPC inbound (`CheckActiveInstances` / `PauseUserTasks`)
- [ ] gRPC outbound (`GetCompiledWorkflow` client to Definition Service)
- [ ] Temporal workflow function / Activities (`internal/workflow`)
- [ ] Database / migrations (`workflow_execution` schema, RLS)
- [ ] Outbox relay
- [ ] cmd/server
- [ ] cmd/worker

## Steps to reproduce
1.
2.
3.

## Expected behaviour
What you expected to happen.

## Actual behaviour
What actually happened. Include error messages, gRPC status codes, log output, or stack traces.

```
paste relevant log output or error here
```

## Minimal reproduction
```go
// paste the smallest snippet or grpcurl command that triggers the bug
```

## Additional context
Any other relevant context (Postgres version, PgBouncer mode, Temporal namespace/task queue, related issues).
