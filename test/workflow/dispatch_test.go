package workflow_test

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// TestExecute_SequentialDispatchCompletes drives the smallest real path:
// one department, one approve-shaped stage, resolved by a stage-transition
// signal, ending in COMPLETED (LLD §2.5's execution algorithm).
func TestExecute_SequentialDispatchCompletes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := singleStageCollaboration(dsl.StageDef{Type: "approve", Activity: "approve_order", Role: "sales_rep"})
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

// TestExecute_ExclusiveGatewayRoutesOnCondition drives a two-department plan
// joined by an exclusive gateway: the first department's decision routes to
// one of two possible second departments (LLD §2.6).
func TestExecute_ExclusiveGatewayRoutesOnCondition(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{
			{
				Name: "main",
				Departments: []dsl.DepartmentDef{
					{ID: "review", Stages: []dsl.StageDef{{Type: "review", Activity: "review_order"}}},
					{ID: "shipping", Stages: []dsl.StageDef{{Type: "prep", Activity: "ship_order"}}},
					{ID: "rework", Stages: []dsl.StageDef{{Type: "prep", Activity: "rework_order"}}},
				},
				Execution: dsl.ExecutionPlan{
					Steps: []dsl.ExecutionStep{
						{Sequential: []string{"review"}},
						{Exclusive: []dsl.ExclusiveBranch{
							{ConditionExpression: `decision == "approved"`, Target: "shipping"},
							{ConditionExpression: "", Target: "rework"},
						}},
					},
				},
			},
		},
	}

	registerFakeActivities(env, collab, nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "review", ToStage: "review", ResultJSON: `{"decision":"approved"}`, RecordVersion: 1,
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "shipping", ToStage: "prep", ResultJSON: `{}`, RecordVersion: 1,
		})
	}, 2*time.Millisecond)

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
