package workflow_test

import (
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// TestExecute_SupportedSchemaVersionRuns confirms a CompiledCollaboration
// stamped with the current DSL schema version dispatches normally. Other
// versions never reach the workflow: the plan-read activity refuses them.
func TestExecute_SupportedSchemaVersionRuns(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := singleStageCollaboration(dsl.StageDef{Type: "approve", Activity: "approve_order", Role: "sales_rep"})
	collab.SchemaVersion = dsl.CurrentSchemaVersion
	registerFakeActivities(env, collab, nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "sales", ToStage: "approve", ResultJSON: `{"decision":"approved"}`, RecordVersion: 1, VisitCount: 1,
		})
	}, tick)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
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
