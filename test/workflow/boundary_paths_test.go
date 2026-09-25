package workflow_test

import (
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// runBoundaryPlan runs steps over depts, sending each signal at its delay,
// and returns the outcome and the node keys of the tasks created.
func runBoundaryPlan(t *testing.T, depts []dsl.DepartmentDef, steps []dsl.ExecutionStep, signals map[time.Duration]stageTransitionWire) (wfengine.ExecuteOutput, []domain.NodeKey) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans:    []*dsl.CompiledPlan{{Name: "main", Departments: depts, Execution: dsl.ExecutionPlan{Steps: steps}}},
	}
	// Parallel branches run their activities concurrently.
	var mu sync.Mutex
	var created []domain.NodeKey
	registerFakeActivities(env, collab, &activityHooks{
		createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			mu.Lock()
			defer mu.Unlock()
			created = append(created, in.NodeKey)
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
	})
	for delay, sig := range signals {
		env.RegisterDelayedCallback(func() { env.SignalWorkflow("stage-transition:instance-1", sig) }, delay)
	}
	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult: %v", err)
	}
	return out, created
}

func dept(id string) dsl.DepartmentDef {
	return dsl.DepartmentDef{ID: id, Stages: []dsl.StageDef{{Type: "review", NodeID: id + "_task"}}}
}

func done(deptID string) stageTransitionWire {
	return stageTransitionWire{DeptID: deptID, NodeID: deptID + "_task", ToStage: "review", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1}
}

func contains(keys []domain.NodeKey, key domain.NodeKey) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}

// A timer boundary authored in ISO 8601, which Definition accepts and passes
// through, fires like a Go duration does.
func TestExecute_ISOTimerBoundaryFires(t *testing.T) {
	out, created := runBoundaryPlan(t,
		[]dsl.DepartmentDef{dept("review"), dept("escalation")},
		[]dsl.ExecutionStep{{SubWorkflow: &dsl.SubWorkflowStep{
			NodeID: "sub1", Name: "sub",
			Plan:       dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"review"}}}},
			TimerPaths: []dsl.TimerPath{{Duration: "PT1H", Interrupting: true, TargetDept: "escalation"}},
		}}},
		map[time.Duration]stageTransitionWire{2 * time.Hour: done("escalation")},
	)
	if out.Status != domain.InstanceStatusCompleted || !contains(created, "escalation/escalation_task") {
		t.Errorf("status %v, tasks %v; want COMPLETED through the PT1H boundary's escalation", out.Status, created)
	}
}

// An interrupting boundary whose path leads straight to an end event has no
// target department: its path ends there, and nothing after the call runs.
func TestExecute_InterruptingBoundaryWithNoTargetEndsThePath(t *testing.T) {
	t.Run("on a call", func(t *testing.T) {
		out, created := runBoundaryPlan(t,
			[]dsl.DepartmentDef{dept("review"), dept("after")},
			[]dsl.ExecutionStep{
				{SubWorkflow: &dsl.SubWorkflowStep{
					NodeID: "sub1", Name: "sub",
					Plan:       dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"review"}}}},
					TimerPaths: []dsl.TimerPath{{Duration: "1h", Interrupting: true}},
				}},
				{Sequential: []string{"after"}},
			},
			nil,
		)
		if out.Status != domain.InstanceStatusCompleted || contains(created, "after/after_task") {
			t.Errorf("status %v, tasks %v; want COMPLETED with nothing after the call", out.Status, created)
		}
	})

	t.Run("on a task", func(t *testing.T) {
		review := dept("review")
		review.Stages[0].BoundaryTimer = &dsl.BoundaryTimer{Duration: "1h", Interrupting: true}
		out, created := runBoundaryPlan(t,
			[]dsl.DepartmentDef{review, dept("after")},
			[]dsl.ExecutionStep{{Sequential: []string{"review", "after"}}},
			nil,
		)
		if out.Status != domain.InstanceStatusCompleted || contains(created, "after/after_task") {
			t.Errorf("status %v, tasks %v; want COMPLETED with nothing after the task", out.Status, created)
		}
	})

	t.Run("inside a module, only the module's own flow ends", func(t *testing.T) {
		out, created := runBoundaryPlan(t,
			[]dsl.DepartmentDef{dept("review"), dept("inner_after"), dept("outer_after")},
			[]dsl.ExecutionStep{
				{SubWorkflow: &dsl.SubWorkflowStep{
					NodeID: "outer", Name: "module",
					Plan: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
						{SubWorkflow: &dsl.SubWorkflowStep{
							NodeID: "inner", Name: "call",
							Plan:       dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"review"}}}},
							TimerPaths: []dsl.TimerPath{{Duration: "1h", Interrupting: true}},
						}},
						{Sequential: []string{"inner_after"}},
					}},
				}},
				{Sequential: []string{"outer_after"}},
			},
			map[time.Duration]stageTransitionWire{2 * time.Hour: done("outer_after")},
		)
		if out.Status != domain.InstanceStatusCompleted || contains(created, "inner_after/inner_after_task") || !contains(created, "outer_after/outer_after_task") {
			t.Errorf("status %v, tasks %v; want the module's flow to end and the caller to go on to outer_after", out.Status, created)
		}
	})
}

// A parallel branch whose path ends at a boundary counts as done at the join.
func TestExecute_BoundaryEndingABranchLetsTheJoinProceed(t *testing.T) {
	out, created := runBoundaryPlan(t,
		[]dsl.DepartmentDef{dept("a"), dept("b"), dept("after")},
		[]dsl.ExecutionStep{
			{Parallel: []dsl.ParallelBranch{
				{DeptID: "a", Steps: []dsl.ExecutionStep{{SubWorkflow: &dsl.SubWorkflowStep{
					NodeID: "sub_a", Name: "sub",
					Plan:       dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"a"}}}},
					TimerPaths: []dsl.TimerPath{{Duration: "1h", Interrupting: true}},
				}}}},
				{DeptID: "b", Steps: []dsl.ExecutionStep{{Sequential: []string{"b"}}}},
			}},
			{Sequential: []string{"after"}},
		},
		map[time.Duration]stageTransitionWire{time.Minute: done("b"), 3 * time.Hour: done("after")},
	)
	if out.Status != domain.InstanceStatusCompleted || !contains(created, "after/after_task") {
		t.Errorf("status %v, tasks %v; want the join to proceed to after", out.Status, created)
	}
}

// A force-back that re-runs the department before a fork, whose boundary then
// ends its path, leaves the fork in place: the branches resume and the join
// proceeds.
func TestExecute_ForceBackBeforeAForkEndingAtABoundaryResumesTheBranches(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	pre := dept("pre")
	pre.Stages[0].BoundaryTimer = &dsl.BoundaryTimer{Duration: "1h", Interrupting: true}
	collab := &dsl.CompiledCollaboration{MainPlan: "main", Plans: []*dsl.CompiledPlan{{
		Name:        "main",
		Departments: []dsl.DepartmentDef{pre, dept("a"), dept("b"), dept("after")},
		Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
			{Sequential: []string{"pre"}},
			{Parallel: []dsl.ParallelBranch{
				{DeptID: "a", Steps: []dsl.ExecutionStep{{Sequential: []string{"a"}}}},
				{DeptID: "b", Steps: []dsl.ExecutionStep{{Sequential: []string{"b"}}}},
			}},
			{Sequential: []string{"after"}},
		}},
	}}}
	var mu sync.Mutex
	var created []domain.NodeKey
	registerFakeActivities(env, collab, &activityHooks{createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		mu.Lock()
		defer mu.Unlock()
		created = append(created, in.NodeKey)
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	}})
	env.RegisterDelayedCallback(func() { env.SignalWorkflow("stage-transition:instance-1", done("pre")) }, 10*time.Minute)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-back:instance-1", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 1})
	}, 20*time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow("stage-transition:instance-1", done("a")) }, 2*time.Hour)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow("stage-transition:instance-1", done("b")) }, 2*time.Hour+time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow("stage-transition:instance-1", done("after")) }, 3*time.Hour)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult: %v", err)
	}
	if out.Status != domain.InstanceStatusCompleted || !contains(created, "after/after_task") {
		t.Errorf("status %v, tasks %v; want the branches to finish and after to run", out.Status, created)
	}
}
