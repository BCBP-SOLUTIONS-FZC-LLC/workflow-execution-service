package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// wflOlderHistory makes the interpreter read changeID as a history recorded
// before that change: the marker is absent, as in a replay of an older run.
func wflOlderHistory(env *testsuite.TestWorkflowEnvironment, changeID string) {
	env.OnGetVersion(changeID, wf.DefaultVersion, 1).Return(wf.DefaultVersion)
}

func wflQueryWaitingAt(t *testing.T, env *testsuite.TestWorkflowEnvironment, delay time.Duration, into *[]string, taken *bool) {
	t.Helper()
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow(port.QueryWaitingExternal)
		if err != nil {
			t.Errorf("QueryWorkflow(%s): %v", port.QueryWaitingExternal, err)
			return
		}
		if err := val.Get(into); err != nil {
			t.Errorf("decoding %s: %v", port.QueryWaitingExternal, err)
			return
		}
		*taken = true
	}, delay)
}

func wflQueryStatusAt(t *testing.T, env *testsuite.TestWorkflowEnvironment, delay time.Duration, into *wfengine.WorkflowStatusQuery) {
	t.Helper()
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow(wfengine.QueryGetWorkflowStatus)
		if err != nil {
			t.Errorf("QueryWorkflow(%s): %v", wfengine.QueryGetWorkflowStatus, err)
			return
		}
		if err := val.Get(into); err != nil {
			t.Errorf("decoding %s: %v", wfengine.QueryGetWorkflowStatus, err)
		}
	}, delay)
}

// An instance whose history predates the outside-answer wait: a receive task
// the plan lists as an outside wait waits on the message buffer, so nothing is
// registered to answer, the waiting-external query is empty, and an answer
// that names the node is dropped; the step after the receive is never reached.
func TestExecute_ReceiveTaskOfAnOlderHistoryWaitsOnTheMessageBufferAndDropsAnOutsideAnswer(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	wflOlderHistory(env, "external-wait-by-key")
	collab := wflOneDept("Ops",
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
		dsl.StageDef{Type: "prep", NodeID: "Award"},
	)
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)

	var waitingBefore, waitingAfter []string
	var tookBefore, tookAfter bool
	wflQueryWaitingAt(t, env, 1*time.Second, &waitingBefore, &tookBefore)
	wflSignalAt(env, 2*time.Second, port.SignalInstanceExternalAnswer, wflAnswerWire{
		Key: "Ops/Receive_Bid", Variables: map[string]any{"bid": "42"},
	})
	wflQueryWaitingAt(t, env, 4*time.Second, &waitingAfter, &tookAfter)
	wflSignalAt(env, 8*time.Second, "instance-cancel", adminSignalWire{AdminUserID: "admin-1", Reason: "stuck", RecordVersion: 3})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status, "the answer did not complete the receive, so only the cancel ended the run")
	require.True(t, tookBefore && tookAfter)
	assert.Empty(t, waitingBefore, "nothing is registered for an answer to find")
	assert.Empty(t, waitingAfter)
	assert.Empty(t, wflInputs[port.CreateTaskInput](fakes, port.ActivityCreateTask), "Award must not be reached on the dropped answer")
	cancels := wflInputs[port.CancelInstanceInput](fakes, port.ActivityCancelInstance)
	require.Len(t, cancels, 1)
	assert.Equal(t, wflTenant, cancels[0].TenantID)
	assert.Equal(t, wflInstance, cancels[0].InstanceID)
}

// An instance whose history predates the outside-answer wait: a message
// boundary no send task feeds listens on the message buffer, so firing it from
// outside finds nothing waiting. The boundary does not fire; the task it
// guards is not superseded and completes normally.
func TestExecute_MessageBoundaryOfAnOlderHistoryIsNotFiredByAnOutsideAnswer(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	wflOlderHistory(env, "external-wait-by-key")
	collab := wflOneDept("Ops", dsl.StageDef{
		Type: "prep", NodeID: "Prepare",
		BoundaryMessage: &dsl.MessagePath{MessageName: "withdraw", BoundaryID: "Boundary_Withdraw", Interrupting: true, Terminates: true},
	})
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)

	var waiting []string
	var took bool
	wflQueryWaitingAt(t, env, 1*time.Second, &waiting, &took)
	wflSignalAt(env, 2*time.Second, port.SignalInstanceExternalAnswer, wflAnswerWire{Key: "Ops/Prepare#Boundary_Withdraw"})
	var midStatus wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 4*time.Second, &midStatus)
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("Ops", "Prepare", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	require.True(t, took)
	assert.Empty(t, waiting, "the older boundary registers no outside wait")
	assert.Equal(t, []domain.NodeKey{"Ops/Prepare"}, midStatus.ActiveTasks, "the guarded task is still open after the answer")
	var statuses []domain.TaskStatus
	for _, in := range wflInputs[port.UpdateTaskStatusInput](fakes, port.ActivityUpdateTaskStatus) {
		assert.Equal(t, wflTenant, in.TenantID)
		assert.Equal(t, "Ops/Prepare", in.TaskID)
		statuses = append(statuses, in.Status)
	}
	assert.Equal(t, []domain.TaskStatus{domain.TaskStatusCompleted}, statuses, "never superseded by the boundary")
}

// A force-forward that names its target by node key only, the way older
// callers did, reaches a parallel step and a degraded park.
// A history recorded before the node-key routing reads such a signal by
// department id alone, so it is dropped; a current one accepts it.
func TestExecute_ForceForwardNamingOnlyANodeKeyIsDroppedByAnOlderHistoryAndTakenByACurrentOne(t *testing.T) {
	for _, older := range []bool{true, false} {
		name := "current history"
		if older {
			name = "older history"
		}
		t.Run(name+"/parallel step open", func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			if older {
				wflOlderHistory(env, "force-route-by-node-key")
			}
			collab := parallelCollaboration()
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, collab)

			wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{
				AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 7,
			})
			var routesSoon int
			env.RegisterDelayedCallback(func() {
				routesSoon = fakes.attemptsOf(port.ActivityRecordForceRoute)
			}, 10*time.Second)
			wflSignalAt(env, 20*time.Second, "stage-transition", wflDoneStage("warehouse", "prep", 1))
			if older {
				wflSignalAt(env, 25*time.Second, "stage-transition", wflDoneStage("billing", "prep", 1))
			}

			status, err := wflStart(t, env, fakes, "")

			require.NoError(t, err)
			assert.Equal(t, domain.InstanceStatusCompleted, status)
			routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
			if older {
				assert.Zero(t, routesSoon, "the signal names no department, so it matches no branch")
				assert.Empty(t, routes)
				return
			}
			require.Len(t, routes, 1)
			assert.Equal(t, port.RecordForceRouteInput{
				InstanceID: wflInstance, TenantID: wflTenant, OldNodeKeys: []domain.NodeKey{"billing/prep"},
				TargetNodeID: "billing/prep", Direction: domain.ForceRouteDirectionForward, AdminUserID: "admin-1", RecordVersion: 7,
			}, routes[0])
		})

		t.Run(name+"/degraded park", func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			if older {
				wflOlderHistory(env, "force-route-by-node-key")
			}
			collab := parallelCollaboration()
			fakes := wflNewFakes()
			fakes.failFor = func(name string, in any) error {
				if ct, ok := in.(port.CreateTaskInput); ok && name == port.ActivityCreateTask && ct.NodeKey == "billing/prep" {
					return temporal.NewApplicationError("card declined", "ValidationError")
				}
				return nil
			}
			wflRegisterAll(env, fakes, collab)

			wflSignalAt(env, 2*time.Second, "stage-transition", wflDoneStage("warehouse", "prep", 1))
			wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{
				AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 7,
			})
			var routesSoon int
			var parked wfengine.WorkflowStatusQuery
			env.RegisterDelayedCallback(func() {
				routesSoon = fakes.attemptsOf(port.ActivityRecordForceRoute)
			}, 10*time.Second)
			wflQueryStatusAt(t, env, 10*time.Second, &parked)
			if older {
				wflSignalAt(env, 20*time.Second, "instance-force-forward", adminSignalWire{
					AdminUserID: "admin-1", TargetDeptID: "billing", TargetNodeKey: "billing/prep", RecordVersion: 8,
				})
			}

			status, err := wflStart(t, env, fakes, "")

			require.NoError(t, err)
			assert.Equal(t, domain.InstanceStatusCompleted, status)
			routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
			require.Len(t, routes, 1)
			assert.Equal(t, "billing/prep", routes[0].TargetNodeID)
			assert.Equal(t, wflTenant, routes[0].TenantID)
			assert.Equal(t, wflInstance, routes[0].InstanceID)
			if older {
				assert.Zero(t, routesSoon, "no force-route was recorded for the node-key-only signal")
				assert.Equal(t, domain.InstanceStatusDegraded, parked.Status, "the branch is still failed after the dropped signal")
				assert.EqualValues(t, 8, routes[0].RecordVersion, "only the department-addressed signal was applied")
				return
			}
			assert.EqualValues(t, 7, routes[0].RecordVersion)
		})
	}
}

// In a history recorded before the node-key routing, a degraded park finds the
// failed branch by department id only: a force-forward naming no target and a
// force-back naming only a node key are both dropped, and the branch stays
// failed (not re-run, nothing recorded) until a signal names its department.
func TestExecute_DegradedParkOfAnOlderHistoryFindsTheFailedBranchByDepartmentIDOnly(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	wflOlderHistory(env, "force-route-by-node-key")
	collab := parallelCollaboration()
	fakes := wflNewFakes()
	fakes.failFor = func(name string, in any) error {
		if ct, ok := in.(port.CreateTaskInput); ok && name == port.ActivityCreateTask && ct.NodeKey == "billing/prep" {
			return temporal.NewApplicationError("card declined", "ValidationError")
		}
		return nil
	}
	wflRegisterAll(env, fakes, collab)

	wflSignalAt(env, 2*time.Second, "stage-transition", wflDoneStage("warehouse", "prep", 1))
	wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 7})
	wflSignalAt(env, 8*time.Second, "instance-force-back", adminSignalWire{
		AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 7,
	})
	var routesBefore, billingTasksBefore int
	var parked wfengine.WorkflowStatusQuery
	env.RegisterDelayedCallback(func() {
		routesBefore = fakes.attemptsOf(port.ActivityRecordForceRoute)
		for _, in := range wflInputs[port.CreateTaskInput](fakes, port.ActivityCreateTask) {
			if in.NodeKey == "billing/prep" {
				billingTasksBefore++
			}
		}
	}, 12*time.Second)
	wflQueryStatusAt(t, env, 12*time.Second, &parked)
	wflSignalAt(env, 20*time.Second, "instance-force-forward", adminSignalWire{
		AdminUserID: "admin-1", TargetDeptID: "billing", TargetNodeKey: "billing/prep", RecordVersion: 9,
	})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Zero(t, routesBefore, "neither unaddressed signal was recorded as a force-route")
	assert.Equal(t, 1, billingTasksBefore, "the failed branch was not re-run by the force-back")
	assert.Equal(t, domain.InstanceStatusDegraded, parked.Status)
	routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, routes, 1)
	assert.EqualValues(t, 9, routes[0].RecordVersion)
	assert.Equal(t, domain.ForceRouteDirectionForward, routes[0].Direction)
	assert.Equal(t, wflTenant, routes[0].TenantID)
	assert.Equal(t, wflInstance, routes[0].InstanceID)
}
