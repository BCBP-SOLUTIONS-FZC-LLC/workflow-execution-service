// Package replay_test proves replay-safety across GetVersion patches
// (execution LLD §7.1) — see this directory's README.
package replay_test

import (
	"testing"

	"go.temporal.io/sdk/worker"

	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
)

// TestReplay_InitialInterpreter replays the pre-existing initial-interpreter
// fixture against the current interpreter code — the greenfield baseline
// this package's replay convention is anchored to (version.go's
// initialInterpreterChangeID).
func TestReplay_InitialInterpreter(t *testing.T) {
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{})
	if err != nil {
		t.Fatalf("NewWorkflowReplayerWithOptions: %v", err)
	}
	replayer.RegisterWorkflow(wfengine.Execute)

	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, "testdata/initial_interpreter.json"); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
}
