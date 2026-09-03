package workflow

import (
	"testing"

	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"
)

func TestAddSLATimersInvalidDateIsANoOp(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		sel := wf.NewSelector(ctx)
		cancel := addSLATimers(ctx, sel, slaTimerParams{DueDate: "not-a-date", FollowUpDate: "also-not-a-date"})
		cancel()
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}

func TestAddSLATimersEmptyDatesAreANoOp(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		sel := wf.NewSelector(ctx)
		cancel := addSLATimers(ctx, sel, slaTimerParams{})
		cancel()
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}
