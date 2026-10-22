package workflow_test

import (
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// pathRun is what a plan run left behind: the instance's outcome, the node
// keys of the tasks created and the status writes made to tasks.
type pathRun struct {
	out      wfengine.ExecuteOutput
	err      error
	created  []domain.NodeKey
	statuses map[string]domain.TaskStatus
	// createdAt is the workflow time each task was created at.
	createdAt map[domain.NodeKey]time.Time
}

// runPathPlan runs steps over depts like runBoundaryPlan, but returns a
// workflow error instead of failing on one, and records the task status
// writes, signals going out at their delays.
func runPathPlan(t *testing.T, depts []dsl.DepartmentDef, steps []dsl.ExecutionStep, signals map[time.Duration]any, signalName map[time.Duration]string) pathRun {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans:    []*dsl.CompiledPlan{{Name: "main", Departments: depts, Execution: dsl.ExecutionPlan{Steps: steps}}},
	}
	var mu sync.Mutex
	run := pathRun{statuses: map[string]domain.TaskStatus{}, createdAt: map[domain.NodeKey]time.Time{}}
	registerFakeActivities(env, collab, &activityHooks{
		createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			mu.Lock()
			defer mu.Unlock()
			run.created = append(run.created, in.NodeKey)
			run.createdAt[in.NodeKey] = env.Now()
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
		updateTaskStatus: func(in port.UpdateTaskStatusInput) error {
			mu.Lock()
			defer mu.Unlock()
			run.statuses[in.TaskID] = in.Status
			return nil
		},
	})
	for delay, payload := range signals {
		env.RegisterDelayedCallback(func() { env.SignalWorkflow(signalName[delay]+":instance-1", payload) }, delay)
	}
	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1"})
	run.err = env.GetWorkflowError()
	mu.Lock()
	defer mu.Unlock()
	if run.err == nil {
		if err := env.GetWorkflowResult(&run.out); err != nil {
			t.Fatalf("GetWorkflowResult: %v", err)
		}
	}
	return run
}

func transition(delay time.Duration, deptID string) (map[time.Duration]any, map[time.Duration]string) {
	return map[time.Duration]any{delay: done(deptID)}, map[time.Duration]string{delay: "stage-transition"}
}

func merge(a, b map[time.Duration]any, an, bn map[time.Duration]string) (map[time.Duration]any, map[time.Duration]string) {
	for k, v := range b {
		a[k] = v
		an[k] = bn[k]
	}
	return a, an
}

// An interrupting boundary runs the whole path it leads to, in order, then
// ends its path: the steps after the host do not run.
func TestExecute_InterruptingBoundaryRunsItsWholePathThenEndsIt(t *testing.T) {
	path := []dsl.ExecutionStep{{Sequential: []string{"esc1"}}, {Sequential: []string{"esc2"}}}

	for name, tc := range map[string]struct {
		depts []dsl.DepartmentDef
		steps []dsl.ExecutionStep
	}{
		"on a call": {
			depts: []dsl.DepartmentDef{dept("review"), dept("esc1"), dept("esc2"), dept("after")},
			steps: []dsl.ExecutionStep{
				{SubWorkflow: &dsl.SubWorkflowStep{
					NodeID: "sub1", Name: "sub",
					Plan:       dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"review"}}}},
					TimerPaths: []dsl.TimerPath{{Duration: "1h", Interrupting: true, TargetDept: "esc1", Steps: path}},
				}},
				{Sequential: []string{"after"}},
			},
		},
		"on a task": {
			depts: func() []dsl.DepartmentDef {
				review := dept("review")
				review.Stages[0].BoundaryTimer = &dsl.BoundaryTimer{Duration: "1h", Interrupting: true, TargetDept: "esc1", Steps: path}
				return []dsl.DepartmentDef{review, dept("esc1"), dept("esc2"), dept("after")}
			}(),
			steps: []dsl.ExecutionStep{{Sequential: []string{"review", "after"}}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			s1, n1 := transition(2*time.Hour, "esc1")
			s2, n2 := transition(3*time.Hour, "esc2")
			signals, names := merge(s1, s2, n1, n2)

			run := runPathPlan(t, tc.depts, tc.steps, signals, names)

			if run.err != nil || run.out.Status != domain.InstanceStatusCompleted {
				t.Fatalf("status %v, error %v; want COMPLETED", run.out.Status, run.err)
			}
			if !contains(run.created, "esc1/esc1_task") || !contains(run.created, "esc2/esc2_task") {
				t.Errorf("tasks %v, want the whole path: esc1 then esc2", run.created)
			}
			if contains(run.created, "after/after_task") {
				t.Errorf("tasks %v: the steps after the host ran after its interrupting boundary", run.created)
			}
		})
	}
}

// A non-interrupting boundary runs its path beside the host, which finishes
// and lets the flow go on.
func TestExecute_NonInterruptingBoundaryRunsItsPathBesideTheHost(t *testing.T) {
	review := dept("review")
	review.Stages[0].BoundaryTimer = &dsl.BoundaryTimer{
		Duration: "1h", Interrupting: false, TargetDept: "esc1",
		Steps: []dsl.ExecutionStep{{Sequential: []string{"esc1"}}, {Sequential: []string{"esc2"}}},
	}
	s1, n1 := transition(2*time.Hour, "esc1")
	s2, n2 := transition(3*time.Hour, "esc2")
	s3, n3 := transition(4*time.Hour, "review")
	signals, names := merge(s1, s2, n1, n2)
	signals, names = merge(signals, s3, names, n3)

	run := runPathPlan(t, []dsl.DepartmentDef{review, dept("esc1"), dept("esc2")}, []dsl.ExecutionStep{{Sequential: []string{"review"}}}, signals, names)

	if run.err != nil || run.out.Status != domain.InstanceStatusCompleted {
		t.Fatalf("status %v, error %v; want COMPLETED", run.out.Status, run.err)
	}
	if !contains(run.created, "esc1/esc1_task") || !contains(run.created, "esc2/esc2_task") {
		t.Errorf("tasks %v, want the whole path beside the host", run.created)
	}
}

// A task still open inside a call whose interrupting boundary fires is marked
// SUPERSEDED, so it cannot be claimed after the instance has moved on.
func TestExecute_TaskOpenInAnInterruptedCallIsSuperseded(t *testing.T) {
	run := runPathPlan(t,
		[]dsl.DepartmentDef{dept("review")},
		[]dsl.ExecutionStep{{SubWorkflow: &dsl.SubWorkflowStep{
			NodeID: "sub1", Name: "sub",
			Plan:       dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"review"}}}},
			TimerPaths: []dsl.TimerPath{{Duration: "1h", Interrupting: true, Terminates: true}},
		}}},
		nil, nil)

	if run.err != nil || run.out.Status != domain.InstanceStatusCompleted {
		t.Fatalf("status %v, error %v; want COMPLETED", run.out.Status, run.err)
	}
	if got := run.statuses["review/review_task"]; got != domain.TaskStatusSuperseded {
		t.Errorf("the abandoned task's status = %q, want SUPERSEDED (writes: %v)", got, run.statuses)
	}
}

// A connector or stage that fails inside a call raises its error class to the
// call's error boundaries: the one that names it, else the catch-all, runs
// its path. With none, the instance fails as before.
func TestExecute_FailedStageInACallFiresItsErrorBoundary(t *testing.T) {
	work := dept("work")
	work.Stages[0].Type = "connector"
	build := func(paths []dsl.ErrorPath) ([]dsl.DepartmentDef, []dsl.ExecutionStep) {
		return []dsl.DepartmentDef{work, dept("handler"), dept("after")}, []dsl.ExecutionStep{
			{SubWorkflow: &dsl.SubWorkflowStep{
				NodeID: "sub1", Name: "sub",
				Plan:       dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"work"}}}},
				ErrorPaths: paths,
			}},
			{Sequential: []string{"after"}},
		}
	}
	fail := func(class string) (map[time.Duration]any, map[time.Duration]string) {
		return map[time.Duration]any{time.Hour: stageFailWire{DeptID: "work", NodeID: "work_task", ErrorClass: class, RecordVersion: 1, VisitCount: 1}},
			map[time.Duration]string{time.Hour: "stage-fail"}
	}
	handled := []dsl.ErrorPath{{ErrorCode: "upstream_timeout", TargetDept: "handler", Steps: []dsl.ExecutionStep{{Sequential: []string{"handler"}}}}}

	t.Run("a path naming the class runs", func(t *testing.T) {
		depts, steps := build(handled)
		signals, names := fail("upstream_timeout")
		h, hn := transition(2*time.Hour, "handler")
		signals, names = merge(signals, h, names, hn)

		run := runPathPlan(t, depts, steps, signals, names)

		if run.err != nil || run.out.Status != domain.InstanceStatusCompleted || !contains(run.created, "handler/handler_task") {
			t.Fatalf("status %v, error %v, tasks %v; want COMPLETED through the handler", run.out.Status, run.err, run.created)
		}
		failedAt := run.createdAt["work/work_task"].Add(time.Hour)
		if late := run.createdAt["handler/handler_task"].Sub(failedAt); late > 5*time.Second {
			t.Errorf("the error path started %v after the failure, want it at once: the body had already finished, so nothing is left to wait for", late)
		}
		if contains(run.created, "after/after_task") {
			t.Errorf("tasks %v: the flow after the failed call ran", run.created)
		}
	})

	t.Run("no path for the class fails the instance", func(t *testing.T) {
		depts, steps := build(handled)
		signals, names := fail("something_else")

		run := runPathPlan(t, depts, steps, signals, names)

		if run.err == nil {
			t.Fatalf("status %v; want the instance to fail when no error path matches", run.out.Status)
		}
		if contains(run.created, "handler/handler_task") {
			t.Errorf("tasks %v: an unrelated error ran the handler", run.created)
		}
	})

	t.Run("the catch-all runs when no path names the class", func(t *testing.T) {
		depts, steps := build([]dsl.ErrorPath{{TargetDept: "handler", Steps: []dsl.ExecutionStep{{Sequential: []string{"handler"}}}}})
		signals, names := fail("anything")
		h, hn := transition(2*time.Hour, "handler")
		signals, names = merge(signals, h, names, hn)

		run := runPathPlan(t, depts, steps, signals, names)

		if run.err != nil || run.out.Status != domain.InstanceStatusCompleted || !contains(run.created, "handler/handler_task") {
			t.Fatalf("status %v, error %v, tasks %v; want COMPLETED through the catch-all", run.out.Status, run.err, run.created)
		}
	})
}

// After an admin force-route the steps run again from where the admin sent
// them; a boundary that interrupts a call in that run supersedes the call's
// open task like any other, and is not mistaken for a force-route still under way.
func TestExecute_BoundaryAfterAForceRouteStillSupersedesItsOpenTasks(t *testing.T) {
	signals := map[time.Duration]any{
		10 * tick: adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "a/a_task", RecordVersion: 1},
		20 * tick: done("a"),
	}
	names := map[time.Duration]string{10 * tick: "instance-force-forward", 20 * tick: "stage-transition"}
	// The first run of "a" is abandoned by the force-forward, so the second
	// visit's completion carries VisitCount 2.
	signals[20*tick] = stageTransitionWire{DeptID: "a", NodeID: "a_task", ToStage: "review", ResultJSON: "{}", RecordVersion: 1, VisitCount: 2}

	run := runPathPlan(t,
		[]dsl.DepartmentDef{dept("a"), dept("review")},
		[]dsl.ExecutionStep{
			{Sequential: []string{"a"}},
			{SubWorkflow: &dsl.SubWorkflowStep{
				NodeID: "sub1", Name: "sub",
				Plan:       dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"review"}}}},
				TimerPaths: []dsl.TimerPath{{Duration: "1h", Interrupting: true, Terminates: true}},
			}},
		},
		signals, names)

	if run.err != nil || run.out.Status != domain.InstanceStatusCompleted {
		t.Fatalf("status %v, error %v; want COMPLETED", run.out.Status, run.err)
	}
	if got := run.statuses["review/review_task"]; got != domain.TaskStatusSuperseded {
		t.Errorf("the open task's status = %q, want SUPERSEDED (writes: %v)", got, run.statuses)
	}
}
