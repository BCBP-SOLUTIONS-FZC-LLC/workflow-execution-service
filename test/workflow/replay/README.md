# Replay fixtures

Every `getVersion` call site in `internal/workflow/version.go` needs a matching
recorded workflow history here, replayed via `worker.ReplayWorkflowHistory` /
`ReplayWorkflowHistoryFromJSONFile`, before the interpreter change that added
that call site merges.

No fixtures exist yet — this package is greenfield, so there's no production
history to replay against. Add the first one alongside the first real
`getVersion` call site.

There is no CI check enforcing this today (a maintenance-process gap, not a
technical one — see `internal/workflow/version.go`).
