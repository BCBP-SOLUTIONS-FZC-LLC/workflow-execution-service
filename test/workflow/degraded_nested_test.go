package workflow_test

import (
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

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

	var mu sync.Mutex
	var statuses []domain.InstanceStatus
	registerFakeActivities(env, twoInnerForksCollaboration(), &activityHooks{
		createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			if in.NodeKey == "x2/prep" || in.NodeKey == "y2/prep" {
				return port.CreateTaskOutput{}, temporal.NewApplicationError("card declined", "ValidationError")
			}
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
		updateInstanceStatus: func(in port.UpdateInstanceStatusInput) {
			mu.Lock()
			defer mu.Unlock()
			statuses = append(statuses, in.Status)
		},
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
	mu.Lock()
	defer mu.Unlock()
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

// A second signal for a park that closes on the first one must not block
// the signal router: the stage after the park still gets its result.
func TestExecute_ASignalForAClosingParkDoesNotBlockLaterSignals(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := parallelCollaboration()
	plan := collab.Plans[0]
	plan.Departments = append(plan.Departments, dsl.DepartmentDef{ID: "tail", Stages: []dsl.StageDef{{Type: "prep", Activity: "ship_order"}}})
	plan.Execution.Steps = append(plan.Execution.Steps, dsl.ExecutionStep{Sequential: []string{"tail"}})
	registerFakeActivities(env, collab, &activityHooks{createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		if in.NodeKey == "billing/prep" {
			return port.CreateTaskOutput{}, temporal.NewApplicationError("card declined", "ValidationError")
		}
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	}})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{DeptID: "warehouse", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1})
	}, tick)
	env.RegisterDelayedCallback(func() {
		for range 2 {
			env.SignalWorkflow("instance-force-forward:instance-1", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 1})
		}
	}, 10*tick)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{DeptID: "tail", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1})
	}, 20*tick)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}

// A force-back aimed at a failed branch of the inner fork runs that branch
// again, and the instance completes once it does.
func TestExecute_ForceBackInANestedParallelRespawnsTheInnerBranch(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var mu sync.Mutex
	b2Calls := 0
	registerFakeActivities(env, nestedParallelCollaboration(), &activityHooks{createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		mu.Lock()
		defer mu.Unlock()
		if in.NodeKey == "b2/prep" {
			b2Calls++
			if b2Calls == 1 {
				return port.CreateTaskOutput{}, temporal.NewApplicationError("card declined", "ValidationError")
			}
		}
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	}})

	at := func(d time.Duration, dept string, visit int64) {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
				DeptID: dept, ToStage: "prep", ResultJSON: "{}", RecordVersion: 1, VisitCount: visit,
			})
		}, d)
	}
	at(tick, "a", 1)
	at(tick, "b1", 1)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-back:instance-1", adminSignalWire{
			AdminUserID: "admin-1", TargetNodeKey: "b2/prep", RecordVersion: 1,
		})
	}, 10*tick)
	at(20*tick, "b2", 2)

	out := finish(t, env, "instance-1")

	mu.Lock()
	defer mu.Unlock()
	if out.Status != domain.InstanceStatusCompleted || b2Calls != 2 {
		t.Errorf("status %v, b2 task attempts %d; want COMPLETED after a second attempt", out.Status, b2Calls)
	}
}

// A force-back while every branch of both forks is open sends the instance
// to the step before the outer fork; once it completes again, the branches
// go on and the instance completes.
func TestExecute_ForceBackWhileANestedParallelIsOpenReturnsBeforeTheFork(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := nestedParallelCollaboration()
	plan := collab.Plans[0]
	plan.Departments = append(plan.Departments, dsl.DepartmentDef{ID: "intake", Stages: []dsl.StageDef{{Type: "prep", Activity: "work"}}})
	plan.Execution.Steps = append([]dsl.ExecutionStep{{Sequential: []string{"intake"}}}, plan.Execution.Steps...)

	var mu sync.Mutex
	created := map[domain.NodeKey]int{}
	registerFakeActivities(env, collab, &activityHooks{createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		mu.Lock()
		defer mu.Unlock()
		created[in.NodeKey]++
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	}})

	at := func(d time.Duration, dept string, visit int64) {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
				DeptID: dept, ToStage: "prep", ResultJSON: "{}", RecordVersion: 1, VisitCount: visit,
			})
		}, d)
	}
	at(tick, "intake", 1)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-back:instance-1", adminSignalWire{
			AdminUserID: "admin-1", TargetNodeKey: "intake/prep", RecordVersion: 1,
		})
	}, 5*tick)
	at(10*tick, "intake", 2)
	at(15*tick, "a", 1)
	at(20*tick, "b1", 1)
	at(25*tick, "b2", 1)

	out := finish(t, env, "instance-1")

	mu.Lock()
	defer mu.Unlock()
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED", out.Status)
	}
	if created["intake/prep"] != 2 {
		t.Errorf("tasks created = %v, want intake twice: before the forks and after the force-back", created)
	}
	for _, k := range []domain.NodeKey{"a/prep", "b1/prep", "b2/prep"} {
		if created[k] != 1 {
			t.Errorf("tasks created = %v, want %s once: an open branch keeps its task through the force-back", created, k)
		}
	}
}

func supersededAfterNestedForceForward(t *testing.T, beforeTheChange bool) map[string]bool {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	if beforeTheChange {
		env.OnGetVersion("nested-force-forward-innermost-gateway", wf.DefaultVersion, 1).Return(wf.DefaultVersion)
	}

	var mu sync.Mutex
	superseded := map[string]bool{}
	registerFakeActivities(env, nestedParallelCollaboration(), &activityHooks{
		createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
		updateTaskStatus: func(in port.UpdateTaskStatusInput) error {
			if in.Status == domain.TaskStatusSuperseded {
				mu.Lock()
				superseded[in.TaskID] = true
				mu.Unlock()
			}
			return nil
		},
	})

	var supersededBeforeCancel map[string]bool
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-forward:instance-1", adminSignalWire{
			AdminUserID: "admin-1", TargetDeptID: "b1", TargetNodeKey: "b1/prep", RecordVersion: 1,
		})
	}, 10*tick)
	env.RegisterDelayedCallback(func() {
		mu.Lock()
		defer mu.Unlock()
		supersededBeforeCancel = map[string]bool{}
		for k := range superseded {
			supersededBeforeCancel[k] = true
		}
	}, 20*tick)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-cancel:instance-1", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 1})
	}, 30*tick)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})
	return supersededBeforeCancel
}

func TestExecute_ForceForwardOnARunningNestedBranchLeavesTheOtherBranchesRunning(t *testing.T) {
	supersededBeforeCancel := supersededAfterNestedForceForward(t, false)

	if supersededBeforeCancel["b2/prep"] {
		t.Errorf("force-forward on b1 superseded the open task of its sibling branch b2: %v", supersededBeforeCancel)
	}
	if supersededBeforeCancel["a/prep"] {
		t.Errorf("force-forward on b1 superseded the open task of the other outer branch a: %v", supersededBeforeCancel)
	}
	if !supersededBeforeCancel["b1/prep"] {
		t.Errorf("force-forward on b1 left its own open task running: %v", supersededBeforeCancel)
	}
}

func TestExecute_NestedForceForward_BeforeTheChangeCancelsTheOuterBranch(t *testing.T) {
	supersededBeforeCancel := supersededAfterNestedForceForward(t, true)

	if !supersededBeforeCancel["b2/prep"] {
		t.Errorf("a history recorded before the change replays as it ran: the outer branch is cancelled with its inner siblings, got %v", supersededBeforeCancel)
	}
}
