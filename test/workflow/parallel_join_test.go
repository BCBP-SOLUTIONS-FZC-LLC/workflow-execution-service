package workflow_test

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// TestExecute_ParallelBranchesSharingADepartmentID_Join pins that branches
// are told apart by their place in the step, not by DeptID: two branches that
// start with a call both carry an empty one.
func TestExecute_ParallelBranchesSharingADepartmentID_Join(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := parallelCollaboration()
	for i := range collab.Plans[0].Execution.Steps[0].Parallel {
		collab.Plans[0].Execution.Steps[0].Parallel[i].DeptID = ""
	}
	collab.Plans[0].Execution.Steps = append(collab.Plans[0].Execution.Steps, dsl.ExecutionStep{Sequential: []string{"warehouse"}})
	registerFakeActivities(env, collab, &activityHooks{})

	for i, visit := range []struct {
		dept  string
		count int64
	}{{"warehouse", 1}, {"billing", 1}, {"warehouse", 2}} {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
				DeptID: visit.dept, ToStage: "prep", ResultJSON: "{}", RecordVersion: 1, VisitCount: visit.count,
			})
		}, time.Duration(i+1)*time.Millisecond)
	}

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("both branches completed, yet the join never did: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult: %v", err)
	}
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED", out.Status)
	}
}
