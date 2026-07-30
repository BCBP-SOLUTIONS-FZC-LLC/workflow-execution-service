# Replay fixtures

Every `getVersion` call site in `internal/workflow/version.go` needs a matching
recorded workflow history here, replayed via `worker.NewWorkflowReplayerWithOptions` +
`ReplayWorkflowHistoryFromJSONFile`, before the interpreter change that added
that call site merges. See `initial_interpreter_test.go` for the pattern to copy.

## Regenerating a fixture

`gen/main.go` runs `Execute` for real against a local Temporal dev server, then the
`temporal` CLI exports its history to JSON:

```bash
temporal server start-dev --headless --port 17233 --ui-port 0 --db-filename /tmp/replay-gen.db &
go run ./test/workflow/replay/gen
temporal workflow show --address localhost:17233 --workflow-id initial-interpreter-fixture \
  --output json > test/workflow/replay/testdata/initial_interpreter.json
# stop the dev server (fg + Ctrl-C, or: pkill -f "temporal server start-dev.*17233")
```

`gen/main.go` is never run by CI — only when adding or regenerating a fixture. Add a
second scenario function (not a generic flag) only once a second fixture actually
needs one — same "3+ consumers before generalizing" threshold this repo already uses
elsewhere.

`replayer.RegisterWorkflow(wfengine.Execute)` registers the workflow under Go's
default derived type name (`"Execute"`). Whatever name `gen/main.go`'s worker
registers under must match every fixture's recorded `WorkflowType.Name` — if
`cmd/worker`'s eventual real registration (a separate Tier-1 task) ever moves to
`RegisterWorkflowWithOptions` with a custom name, every existing fixture and every
replay test's `RegisterWorkflow` call move together.

There is no CI check enforcing that a semantics-affecting `internal/workflow` change
adds a new changeID + fixture pair — see the PR template's Temporal/Workflow section
and `validate-quality.yml`'s changeID check for the (lightweight, acknowledgment-based)
enforcement that does exist.
