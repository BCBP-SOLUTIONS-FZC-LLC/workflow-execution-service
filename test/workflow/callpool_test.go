package workflow_test

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// TestExecute_CallPoolIgnoredCreatesAdminStubTask drives the only real
// CallPool pattern today (LLD §2.3): the target plan is Ignored:true, so
// the main pool creates an ordinary admin-completed task rather than a
// child workflow; completing it lets the main pool proceed.
func TestExecute_CallPoolIgnoredCreatesAdminStubTask(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{
			{
				Name: "main",
				Execution: dsl.ExecutionPlan{
					Steps: []dsl.ExecutionStep{
						{CallPool: &dsl.CallPoolStep{Pool: "vendor-pool"}},
					},
				},
			},
			{
				Name:    "vendor-pool",
				Ignored: true,
			},
		},
	}
	registerFakeActivities(env, collab, nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "call_pool", ToStage: "approve", NodeID: "vendor-pool", ResultJSON: "{}", RecordVersion: 1,
		})
	}, time.Millisecond)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult error: %v", err)
	}
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED", out.Status)
	}
}
