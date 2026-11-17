package workflow

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"
)

// An ISO 8601 period on a boundary timer is read: PT1H fires before a 2h timer.
func TestAddTimerCase_ISOPeriodFires(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var fired bool
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		sel := wf.NewSelector(ctx)
		cancel := addTimerCase(ctx, sel, "PT1H", true, "", func(boundaryFire) { fired = true })
		sel.AddFuture(wf.NewTimer(ctx, 2*time.Hour), func(wf.Future) {})
		sel.Select(ctx)
		cancel()
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if !fired {
		t.Error("PT1H did not fire before the 2h timer")
	}
}

// A boundary with no target department ends its path; one with a target does not.
func TestEndsPath(t *testing.T) {
	if !endsPath(&boundaryFire{Interrupting: true}) {
		t.Error("a boundary with no target did not end its path")
	}
	if endsPath(&boundaryFire{Interrupting: true, TargetDept: "escalation"}) {
		t.Error("a boundary with a target ended its path")
	}
}
