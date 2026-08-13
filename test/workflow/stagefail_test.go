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

// TestExecute_StageFailSignalFailsTaskNotAssignment drives a stage-fail
// signal (LLD §3.1) against the one pending task: completeAssignment must
// never fire, updateTaskStatus must fire with FAILED, and the instance ends
// FAILED rather than COMPLETED.
func TestExecute_StageFailSignalFailsTaskNotAssignment(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := singleStageCollaboration(dsl.StageDef{Type: "connector", Activity: "send_email", Role: "sales_rep"})

	var completeAssignmentCalled bool
	var gotStatus port.UpdateTaskStatusInput
	registerFakeActivities(env, collab, &activityHooks{
		completeAssignment: func(in port.CompleteAssignmentInput) (port.CompleteAssignmentOutput, error) {
			completeAssignmentCalled = true
			return port.CompleteAssignmentOutput{AllDone: true}, nil
		},
		updateTaskStatus: func(in port.UpdateTaskStatusInput) error {
			gotStatus = in
			return nil
		},
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-fail:instance-1", stageTransitionWire{
			DeptID: "sales", ToStage: "connector", Reason: "provider timeout", RecordVersion: 1,
		})
	}, time.Millisecond)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("GetWorkflowError() = nil, want a non-nil error from the failed stage")
	}
	if completeAssignmentCalled {
		t.Error("completeAssignment was called; stage-fail must skip it")
	}
	if gotStatus.Status != domain.TaskStatusFailed {
		t.Errorf("updateTaskStatus Status = %v, want FAILED", gotStatus.Status)
	}
	if gotStatus.RecordVersion != 1 {
		t.Errorf("updateTaskStatus RecordVersion = %d, want 1", gotStatus.RecordVersion)
	}
}
