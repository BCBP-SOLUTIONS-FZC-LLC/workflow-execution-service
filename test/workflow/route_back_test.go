package workflow_test

import (
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
)

type deferWire struct {
	DeptID        string
	FromStage     string
	Reason        string
	UserID        string
	RecordVersion int64
	TaskID        string
	AssignmentID  string
}

func finish(t *testing.T, env *testsuite.TestWorkflowEnvironment, instance string) wfengine.ExecuteOutput {
	t.Helper()
	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: instance, VersionID: "version-1"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult error: %v", err)
	}
	return out
}

func transitionAt(env *testsuite.TestWorkflowEnvironment, at time.Duration, dept string, visit int64) {
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: dept, ToStage: "prep", ResultJSON: "{}", RecordVersion: 1, VisitCount: visit,
		})
	}, at)
}

// A defer sends the instance one completed step back, as a force-back does:
// the earlier step's department runs again and creates a new task, and the
// deferred task is closed with the earlier node named, with no admin route
// recorded and no replacement task made at the deferred node.
func TestExecute_DeferSendsTheInstanceOneStepBack(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var mu sync.Mutex
	var created []domain.NodeKey
	var deferred []port.DeferTaskInput
	forceRoutes := 0
	registerFakeActivities(env, twoDeptCollaboration(), &activityHooks{
		createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			mu.Lock()
			defer mu.Unlock()
			created = append(created, in.NodeKey)
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
		deferTask:        func(in port.DeferTaskInput) { mu.Lock(); deferred = append(deferred, in); mu.Unlock() },
		recordForceRoute: func(port.RecordForceRouteInput) { mu.Lock(); forceRoutes++; mu.Unlock() },
	})

	transitionAt(env, time.Millisecond, "deptA", 1)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-defer:instance-1", deferWire{
			DeptID: "deptB", FromStage: "prep", Reason: "need more info", UserID: "user-1", RecordVersion: 1, TaskID: "deptB/prep", AssignmentID: "assign-1",
		})
	}, 5*time.Millisecond)
	transitionAt(env, 10*time.Millisecond, "deptA", 2)
	transitionAt(env, 15*time.Millisecond, "deptB", 2)

	out := finish(t, env, "instance-1")

	mu.Lock()
	defer mu.Unlock()
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED", out.Status)
	}
	if len(deferred) != 1 || deferred[0].DeferredToNodeKey != "deptA/prep" {
		t.Errorf("DeferTask inputs = %+v, want one naming deptA/prep as the node the instance goes back to", deferred)
	}
	count := map[domain.NodeKey]int{}
	for _, k := range created {
		count[k]++
	}
	if count["deptA/prep"] != 2 || count["deptB/prep"] != 2 {
		t.Errorf("tasks created = %v, want deptA twice (the step back) and deptB twice (before and after it)", count)
	}
	if forceRoutes != 0 {
		t.Errorf("RecordForceRoute called %d times, want none: no admin routed anything", forceRoutes)
	}
}

// A task with no completed step before it has nowhere to go back to; the
// service refuses it synchronously, and a signal that gets past it anyway
// changes nothing.
func TestExecute_DeferAtTheFirstStepGoesNowhere(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	deferCalls := 0
	registerFakeActivities(env, twoDeptCollaboration(), &activityHooks{deferTask: func(port.DeferTaskInput) { deferCalls++ }})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-defer:instance-1", deferWire{DeptID: "deptA", FromStage: "prep", UserID: "user-1", RecordVersion: 1, TaskID: "deptA/prep", AssignmentID: "assign-1"})
	}, time.Millisecond)
	transitionAt(env, 5*time.Millisecond, "deptA", 1)
	transitionAt(env, 10*time.Millisecond, "deptB", 1)

	out := finish(t, env, "instance-1")

	if out.Status != domain.InstanceStatusCompleted || deferCalls != 0 {
		t.Errorf("status %v, DeferTask calls %d; want COMPLETED with the defer dropped", out.Status, deferCalls)
	}
}

// An admin force-forward carries only a node key over HTTP; inside a parallel
// step, the branch it applies to is the one running that node's department.
func TestExecute_ForceForwardInAParallelStepNeedsOnlyTheNodeKey(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerFakeActivities(env, parallelCollaboration(), nil)

	transitionAt(env, time.Millisecond, "warehouse", 1)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-forward:instance-1", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 1})
	}, 5*time.Millisecond)

	out := finish(t, env, "instance-1")

	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED: the force-forward was dropped, so billing never settled", out.Status)
	}
}

// The same for a DEGRADED instance's failed branch.
func TestExecute_DegradedForceForwardNeedsOnlyTheNodeKey(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerFakeActivities(env, parallelCollaboration(), &activityHooks{createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		if in.NodeKey == "billing/prep" {
			return port.CreateTaskOutput{}, temporal.NewApplicationError("card declined", "ValidationError")
		}
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	}})

	transitionAt(env, time.Millisecond, "warehouse", 1)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-forward:instance-1", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 1})
	}, 10*time.Millisecond)

	out := finish(t, env, "instance-1")

	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED", out.Status)
	}
}

// A force-back of a DEGRADED instance names the failed branch by one of its
// nodes, which need not be the department the branch is keyed by.
func TestExecute_DegradedForceBackNamesTheBranchByANodeKey(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	invoiceCalls := 0
	registerFakeActivities(env, multiDeptBranchCollaboration(), &activityHooks{createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		if in.NodeKey == "billing_invoice/prep" {
			invoiceCalls++
			if invoiceCalls == 1 {
				return port.CreateTaskOutput{}, temporal.NewApplicationError("invoice service down", "ValidationError")
			}
		}
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	}})

	transitionAt(env, time.Millisecond, "warehouse", 1)
	transitionAt(env, 2*time.Millisecond, "billing_charge", 1)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-back:instance-1", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "billing_invoice/prep", RecordVersion: 1})
	}, 10*time.Millisecond)
	transitionAt(env, 20*time.Millisecond, "billing_invoice", 2)

	out := finish(t, env, "instance-1")

	if out.Status != domain.InstanceStatusCompleted || invoiceCalls != 2 {
		t.Errorf("status %v, invoice attempts %d; want COMPLETED after one respawn", out.Status, invoiceCalls)
	}
}

// A branch that fails again after a respawn is parked with its steps, so the
// next force-back runs what was still owed after the department that failed.
func TestExecute_ARespawnedBranchThatFailsAgainKeepsItsSteps(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	invoiceCalls := 0
	registerFakeActivities(env, multiDeptBranchCollaboration(), &activityHooks{createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		if in.NodeKey == "billing_invoice/prep" {
			invoiceCalls++
			if invoiceCalls <= 2 {
				return port.CreateTaskOutput{}, temporal.NewApplicationError("invoice service down", "ValidationError")
			}
		}
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	}})

	transitionAt(env, time.Millisecond, "warehouse", 1)
	transitionAt(env, 2*time.Millisecond, "billing_charge", 1)
	for _, at := range []time.Duration{10 * time.Millisecond, 20 * time.Millisecond} {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("instance-force-back:instance-1", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "billing_invoice/prep", RecordVersion: 1})
		}, at)
	}
	transitionAt(env, 30*time.Millisecond, "billing_invoice", 3)

	out := finish(t, env, "instance-1")

	if out.Status != domain.InstanceStatusCompleted || invoiceCalls != 3 {
		t.Errorf("status %v, invoice attempts %d; want COMPLETED on the third attempt, the invoice being retried after each failure", out.Status, invoiceCalls)
	}
}
