package workflow_test

import (
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func nestedParallelCollaboration() *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "a", Stages: []dsl.StageDef{{Type: "prep", Activity: "work"}}},
				{ID: "b1", Stages: []dsl.StageDef{{Type: "prep", Activity: "work"}}},
				{ID: "b2", Stages: []dsl.StageDef{{Type: "prep", Activity: "work"}}},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
				{DeptID: "a", Steps: []dsl.ExecutionStep{{Sequential: []string{"a"}}}},
				{DeptID: "b", Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
					{DeptID: "b1", Steps: []dsl.ExecutionStep{{Sequential: []string{"b1"}}}},
					{DeptID: "b2", Steps: []dsl.ExecutionStep{{Sequential: []string{"b2"}}}},
				}}}},
			}}}},
		}},
	}
}

func TestExecute_ForceForwardInANestedParallelReachesTheInnerFork(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := nestedParallelCollaboration()
	registerFakeActivities(env, collab, &activityHooks{createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		if in.NodeKey == "b2/prep" {
			return port.CreateTaskOutput{}, temporal.NewApplicationError("card declined", "ValidationError")
		}
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	}})

	for _, dept := range []string{"a", "b1"} {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
				DeptID: dept, ToStage: "prep", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1,
			})
		}, tick)
	}
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-forward:instance-1", adminSignalWire{
			AdminUserID: "admin-1", TargetDeptID: "b2", TargetNodeKey: "b2/prep", RecordVersion: 1,
		})
	}, 10*tick)

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

func TestExecute_ForceForwardInANestedParallelReachesTheInnerForkWhileTheOuterForkIsStillOpen(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := nestedParallelCollaboration()
	registerFakeActivities(env, collab, &activityHooks{createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		if in.NodeKey == "b2/prep" {
			return port.CreateTaskOutput{}, temporal.NewApplicationError("card declined", "ValidationError")
		}
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	}})

	for dept, at := range map[string]time.Duration{"b1": tick, "a": 20 * tick} {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
				DeptID: dept, ToStage: "prep", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1,
			})
		}, at)
	}
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-forward:instance-1", adminSignalWire{
			AdminUserID: "admin-1", TargetDeptID: "b2", TargetNodeKey: "b2/prep", RecordVersion: 1,
		})
	}, 10*tick)

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
