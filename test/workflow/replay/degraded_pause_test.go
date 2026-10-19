package replay_test

import (
	"testing"

	"go.temporal.io/sdk/worker"

	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
)

// TestReplay_DegradedPauseDropped replays an instance that parked in
// DEGRADED, was sent instance-pause there (rejected by the interpreter that
// recorded it) and left the park through a force-forward. Every interpreter
// that keeps a pause across DEGRADED must still replay this history.
func TestReplay_DegradedPauseDropped(t *testing.T) {
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{})
	if err != nil {
		t.Fatalf("NewWorkflowReplayerWithOptions: %v", err)
	}
	replayer.RegisterWorkflow(wfengine.Execute)

	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, "testdata/degraded_pause.json"); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
}

// TestReplay_DegradedPauseHeld replays an instance that held a pause across
// DEGRADED and left the park PAUSED, recorded by the interpreter that keeps
// the pause. Any edit to that path must still replay this history.
func TestReplay_DegradedPauseHeld(t *testing.T) {
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{})
	if err != nil {
		t.Fatalf("NewWorkflowReplayerWithOptions: %v", err)
	}
	replayer.RegisterWorkflow(wfengine.Execute)

	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, "testdata/degraded_pause_held.json"); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
}
