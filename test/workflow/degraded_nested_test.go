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

func twoInnerForksCollaboration() *dsl.CompiledCollaboration {
	dept := func(id string) dsl.DepartmentDef {
		return dsl.DepartmentDef{ID: id, Stages: []dsl.StageDef{{Type: "prep", Activity: "work"}}}
	}
	inner := func(id, left, right string) dsl.ParallelBranch {
		return dsl.ParallelBranch{DeptID: id, Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
			{DeptID: left, Steps: []dsl.ExecutionStep{{Sequential: []string{left}}}},
			{DeptID: right, Steps: []dsl.ExecutionStep{{Sequential: []string{right}}}},
		}}}}
	}
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name:        "main",
			Departments: []dsl.DepartmentDef{dept("x1"), dept("x2"), dept("y1"), dept("y2")},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
				inner("x", "x1", "x2"), inner("y", "y1", "y2"),
			}}}},
		}},
	}
}

// Two inner forks fail at once, so two parks are open together. Each admin
// force-forward reaches the park that holds its branch, and the instance
// writes RUNNING once, when the last park closes.
func TestExecute_TwoParksAtOnceEachTakeTheirOwnSignals(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var statuses []domain.InstanceStatus
	registerFakeActivities(env, twoInnerForksCollaboration(), &activityHooks{
		createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			if in.NodeKey == "x2/prep" || in.NodeKey == "y2/prep" {
				return port.CreateTaskOutput{}, temporal.NewApplicationError("card declined", "ValidationError")
			}
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
		updateInstanceStatus: func(in port.UpdateInstanceStatusInput) { statuses = append(statuses, in.Status) },
	})
	for _, d := range []string{"x1", "y1"} {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{DeptID: d, ToStage: "prep", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1})
		}, tick)
	}
	for i, d := range []string{"y2", "x2"} {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("instance-force-forward:instance-1", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: d + "/prep", RecordVersion: 1})
		}, time.Duration(10*(i+1))*tick)
	}

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult: %v", err)
	}
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED: each force-forward must reach its own park", out.Status)
	}
	running := 0
	for i, s := range statuses {
		if s == domain.InstanceStatusRunning {
			running++
			if i != len(statuses)-1 && statuses[len(statuses)-1] != domain.InstanceStatusCompleted {
				t.Errorf("statuses = %v, want RUNNING only once the last park closes", statuses)
			}
		}
	}
	if running != 1 {
		t.Errorf("statuses = %v, want exactly one RUNNING, written when the last park closes", statuses)
	}
}
