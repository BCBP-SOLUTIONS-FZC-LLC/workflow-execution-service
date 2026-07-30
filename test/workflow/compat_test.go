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

// TestExecute_SupportedSchemaVersionRuns confirms a CompiledCollaboration
// stamped with the current DSL schema version resolves a strategy and
// dispatches normally (execution LLD §2.5).
func TestExecute_SupportedSchemaVersionRuns(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := singleStageCollaboration(dsl.StageDef{Type: "approve", Activity: "approve_order", Role: "sales_rep"})
	collab.SchemaVersion = dsl.CurrentSchemaVersion
	registerFakeActivities(env, collab, nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "sales", ToStage: "approve", ResultJSON: `{"decision":"approved"}`, RecordVersion: 1,
		})
	}, time.Millisecond)

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

// TestExecute_UnsupportedSchemaVersionFailsClosed confirms an unrecognized
// DSL schema major fails the instance closed before dispatching any stage —
// no CreateTaskActivity call, no partial progress (execution LLD §2.5).
func TestExecute_UnsupportedSchemaVersionFailsClosed(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := singleStageCollaboration(dsl.StageDef{Type: "approve", Activity: "approve_order", Role: "sales_rep"})
	collab.SchemaVersion = 99

	createTaskCalls := 0
	registerFakeActivities(env, collab, &activityHooks{
		createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			createTaskCalls++
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
	})

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if env.GetWorkflowError() == nil {
		t.Fatal("Execute should fail closed on an unsupported dsl schema version")
	}
	if createTaskCalls != 0 {
		t.Errorf("CreateTaskActivity called %d times, want 0 — should fail before dispatching any stage", createTaskCalls)
	}
}
