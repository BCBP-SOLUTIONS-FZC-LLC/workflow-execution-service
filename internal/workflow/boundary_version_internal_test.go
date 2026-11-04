package workflow

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"
)

// timerFires reports whether a PT1H boundary timer fires before a 2h timer.
func timerFires(t *testing.T, older bool) bool {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	if older {
		env.OnGetVersion(isoTimerDurationChangeID, wf.DefaultVersion, 1).Return(wf.DefaultVersion)
	}
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
	return fired
}

// An execution started before ISO periods were read replays with its ISO
// timer dropped, as it ran; a later one fires it.
func TestAddTimerCase_ISOPeriodByVersion(t *testing.T) {
	if timerFires(t, true) {
		t.Error("an older history fired an ISO timer it never scheduled")
	}
	if !timerFires(t, false) {
		t.Error("PT1H did not fire")
	}
}

// An execution started before a boundary with no target ended its path
// replays as it ran, where such a boundary went to runDepartment.
func TestEndsPath_ByVersion(t *testing.T) {
	for _, older := range []bool{true, false} {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		if older {
			env.OnGetVersion(boundaryEndsPathChangeID, wf.DefaultVersion, 1).Return(wf.DefaultVersion)
		}
		var noTarget, withTarget bool
		env.ExecuteWorkflow(func(ctx wf.Context) error {
			noTarget = endsPath(ctx, &boundaryFire{Interrupting: true})
			withTarget = endsPath(ctx, &boundaryFire{Interrupting: true, TargetDept: "escalation"})
			return nil
		})
		if err := env.GetWorkflowError(); err != nil {
			t.Fatalf("workflow returned error: %v", err)
		}
		if noTarget == older || withTarget {
			t.Errorf("older=%v: endsPath = %v with no target, %v with one; want %v and false", older, noTarget, withTarget, !older)
		}
	}
}
