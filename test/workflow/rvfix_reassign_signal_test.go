package workflow_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/wflclock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

type rvfixReassignRun struct {
	mu       sync.Mutex
	reassign []port.ReassignAssignmentInput
	statuses []wfengine.WorkflowStatusQuery
}

func (r *rvfixReassignRun) activityInputs() []port.ReassignAssignmentInput {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]port.ReassignAssignmentInput(nil), r.reassign...)
}

// rvfixRunWithReassigns sends each signal one tick apart (querying the status after
// each), then completes the task, and records every ReassignAssignment activity run.
func rvfixRunWithReassigns(t *testing.T, signals ...func(env *testsuite.TestWorkflowEnvironment)) *rvfixReassignRun {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := singleStageCollaboration(dsl.StageDef{Type: "approve", Activity: "approve_order"})
	run := &rvfixReassignRun{}
	registerFakeActivities(env, collab, &activityHooks{
		reassignAssignment: func(in port.ReassignAssignmentInput) error {
			run.mu.Lock()
			defer run.mu.Unlock()
			run.reassign = append(run.reassign, in)
			return nil
		},
	})

	query := func() {
		result, err := env.QueryWorkflow(wfengine.QueryGetWorkflowStatus)
		if err != nil {
			t.Errorf("QueryWorkflow error: %v", err)
			return
		}
		var status wfengine.WorkflowStatusQuery
		if err := result.Get(&status); err != nil {
			t.Errorf("decoding query result: %v", err)
			return
		}
		run.mu.Lock()
		run.statuses = append(run.statuses, status)
		run.mu.Unlock()
	}
	at := tick
	for _, send := range signals {
		wflclock.After(env, func() { send(env) }, at)
		at += tick
		wflclock.After(env, query, at)
		at += tick
	}
	wflclock.After(env, func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "sales", ToStage: "approve", ResultJSON: `{"decision":"approved"}`, RecordVersion: 1, VisitCount: 1,
		})
	}, at)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1"})
	require.NoError(t, env.GetWorkflowError())
	return run
}

func rvfixOutboxReassign(from, to string, recordedOffset time.Duration) func(env *testsuite.TestWorkflowEnvironment) {
	return func(env *testsuite.TestWorkflowEnvironment) {
		env.SignalWorkflow("instance-reassign:instance-1", stampedReassignWire{
			TaskID: vacatedTaskID, OldUserID: from, NewUserID: to, RecordVersion: 1, RecordedAt: vacatedBase.Add(recordedOffset),
		})
	}
}

func rvfixAdminReassign(from, to, admin string) func(env *testsuite.TestWorkflowEnvironment) {
	return func(env *testsuite.TestWorkflowEnvironment) {
		env.SignalWorkflow("instance-reassign:instance-1", reassignSignalWire{
			TaskID: vacatedTaskID, OldUserID: from, NewUserID: to, AdminUserID: admin, RecordVersion: 1,
		})
	}
}

func TestExecute_ADelegationReassignSignalDoesNotRunTheReassignActivity(t *testing.T) {
	run := rvfixRunWithReassigns(t, sendVacated(0), rvfixOutboxReassign("user-1", "user-2", time.Second))

	assert.Empty(t, run.activityInputs(), "the delegation already moved the database; running the activity again could only undo or fail it")
	assert.Empty(t, run.statuses[1].UnassignedTasks, "the signal still tells the workflow the task has an assignee")
}

func TestExecute_AStaleDelegationSignalChangesNothing(t *testing.T) {
	run := rvfixRunWithReassigns(t,
		rvfixOutboxReassign("user-2", "user-1", time.Second),
		rvfixOutboxReassign("user-1", "user-2", 0),
	)

	assert.Empty(t, run.activityInputs(), "a late A to B signal after the B to A reversal must not move the assignment back")
}

func TestExecute_AStaleDelegationSignalDoesNotHideALaterVacate(t *testing.T) {
	run := rvfixRunWithReassigns(t, sendVacated(time.Second), rvfixOutboxReassign("user-1", "user-2", 0))

	assert.Len(t, run.statuses[1].UnassignedTasks, 1, "the older reassign must not hide the later vacate")
	assert.Empty(t, run.activityInputs())
}

func TestExecute_AnAdminReassignSignalStillRunsTheReassignActivityOnce(t *testing.T) {
	run := rvfixRunWithReassigns(t, rvfixAdminReassign("user-1", "user-2", "admin-1"))

	got := run.activityInputs()
	require.Len(t, got, 1)
	assert.Equal(t, port.ReassignAssignmentInput{
		TaskID: vacatedTaskID, TenantID: "tenant-1", OldUserID: "user-1", NewUserID: "user-2", AdminUserID: "admin-1", RecordVersion: 1,
	}, got[0])
}
