package workflow

import (
	"testing"

	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"
)

func TestNewAdminChannelHoldsSixtyFourSignals(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var accepted int
	var overflowAccepted bool
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		ch := newAdminChannel(ctx)
		for ch.SendAsync(adminSignalEnvelope{}) {
			accepted++
			if accepted > 2*adminSignalBuffer {
				break
			}
		}
		overflowAccepted = accepted > adminSignalBuffer
		return nil
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if accepted != 64 || overflowAccepted {
		t.Errorf("channel held %d signals, want exactly 64", accepted)
	}
}
