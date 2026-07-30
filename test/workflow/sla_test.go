package workflow_test

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// TestExecute_SLATimerFiresWarningThenBreach drives LLD §3.4's timer-racing
// procedure using the test environment's simulated clock: a stage with both
// FollowUpDate and DueDate in the future fires a warning first, then a
// breach, and the task's own resolution (arriving after both) still
// completes the instance normally — audit-only timers never change status.
func TestExecute_SLATimerFiresWarningThenBreach(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	env.SetStartTime(start)

	collab := singleStageCollaboration(dsl.StageDef{
		Type: "approve", Activity: "approve_order", Role: "sales_rep",
		FollowUpDate: start.Add(1 * time.Hour).Format(time.RFC3339),
		DueDate:      start.Add(2 * time.Hour).Format(time.RFC3339),
	})

	var warningFired, breachFired bool
	var warningBeforeBreach bool
	registerFakeActivities(env, collab, &activityHooks{
		recordSLAWarn: func(port.RecordSLAWarningInput) {
			warningFired = true
			warningBeforeBreach = !breachFired
		},
		recordSLABreach: func(port.RecordSLABreachInput) {
			breachFired = true
		},
	})

	// Resolve the task after both timers should have fired.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "sales", ToStage: "approve", ResultJSON: `{"decision":"approved"}`, RecordVersion: 1,
		})
	}, 3*time.Hour)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if !warningFired {
		t.Error("RecordSLAWarningActivity was never called")
	}
	if !breachFired {
		t.Error("RecordSLABreachActivity was never called")
	}
	if !warningBeforeBreach {
		t.Error("FollowUpDate warning should fire before DueDate breach")
	}

	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult error: %v", err)
	}
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED (SLA timers are audit-only, never change status)", out.Status)
	}
}

// TestExecute_SLATimerCancelledOnEarlyResolution verifies that a task
// resolved before its SLA timers elapse never fires either audit call (the
// timer is cancelled, not left to fire spuriously later).
func TestExecute_SLATimerCancelledOnEarlyResolution(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	env.SetStartTime(start)

	collab := singleStageCollaboration(dsl.StageDef{
		Type: "approve", Activity: "approve_order", Role: "sales_rep",
		DueDate: start.Add(24 * time.Hour).Format(time.RFC3339),
	})

	var breachFired bool
	registerFakeActivities(env, collab, &activityHooks{
		recordSLABreach: func(port.RecordSLABreachInput) { breachFired = true },
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "sales", ToStage: "approve", ResultJSON: `{"decision":"approved"}`, RecordVersion: 1,
		})
	}, time.Millisecond)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if breachFired {
		t.Error("RecordSLABreachActivity fired despite the task resolving well before DueDate")
	}
}
