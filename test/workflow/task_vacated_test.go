package workflow_test

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// taskVacatedWire mirrors internal/workflow's unexported task-vacated signal payload.
type taskVacatedWire struct {
	TaskID        string
	VacatedUserID string
	RecordedAt    time.Time
}

// stampedReassignWire is the reassign payload a delegation sends: it carries the
// time the move was recorded.
type stampedReassignWire struct {
	TaskID        string
	OldUserID     string
	NewUserID     string
	AdminUserID   string
	RecordVersion int64
	RecordedAt    time.Time
}

const vacatedTaskID = "sales/approve"

var vacatedBase = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

type vacatedRun struct {
	statuses []wfengine.WorkflowStatusQuery
}

// runWithAssigneeSignals starts a one-task workflow, sends each signal in order
// (one tick apart), queries the status after each, and finally completes the task.
func runWithAssigneeSignals(t *testing.T, signals ...func(env *testsuite.TestWorkflowEnvironment)) vacatedRun {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := singleStageCollaboration(dsl.StageDef{Type: "approve", Activity: "approve_order"})
	registerFakeActivities(env, collab, nil)

	var run vacatedRun
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
		run.statuses = append(run.statuses, status)
	}
	at := tick
	for _, send := range signals {
		send := send
		env.RegisterDelayedCallback(func() { send(env) }, at)
		at += tick
		env.RegisterDelayedCallback(query, at)
		at += tick
	}
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "sales", ToStage: "approve", ResultJSON: `{"decision":"approved"}`, RecordVersion: 1, VisitCount: 1,
		})
	}, at)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	return run
}

func sendVacated(offset time.Duration) func(env *testsuite.TestWorkflowEnvironment) {
	return func(env *testsuite.TestWorkflowEnvironment) {
		env.SignalWorkflow(port.SignalInstanceTaskVacated+":instance-1", taskVacatedWire{
			TaskID: vacatedTaskID, VacatedUserID: "user-1", RecordedAt: vacatedBase.Add(offset),
		})
	}
}

func sendReassign(offset time.Duration) func(env *testsuite.TestWorkflowEnvironment) {
	return func(env *testsuite.TestWorkflowEnvironment) {
		env.SignalWorkflow("instance-reassign:instance-1", stampedReassignWire{
			TaskID: vacatedTaskID, OldUserID: "user-1", NewUserID: "user-2", RecordVersion: 1, RecordedAt: vacatedBase.Add(offset),
		})
	}
}

func TestExecute_ATaskThatLostItsLastAssigneeIsReportedAndTheInstanceKeepsWaiting(t *testing.T) {
	run := runWithAssigneeSignals(t, sendVacated(0))

	if len(run.statuses) != 1 {
		t.Fatalf("queried %d times, want 1", len(run.statuses))
	}
	got := run.statuses[0]
	if len(got.UnassignedTasks) != 1 || got.UnassignedTasks[0] != vacatedTaskID {
		t.Errorf("UnassignedTasks = %v, want [%s]: the workflow was not told the task lost its assignee", got.UnassignedTasks, vacatedTaskID)
	}
	if got.Status != domain.InstanceStatusRunning {
		t.Errorf("Status = %v, want RUNNING: a task with no assignee is waited on, it does not stop the instance", got.Status)
	}
	if len(got.CurrentNodeKeys) != 1 || got.CurrentNodeKeys[0] != "sales/approve" {
		t.Errorf("CurrentNodeKeys = %v, want [sales/approve]: the task is still the one being waited on", got.CurrentNodeKeys)
	}
}

func TestExecute_AReassignmentGivesTheVacatedTaskAnAssigneeAgain(t *testing.T) {
	run := runWithAssigneeSignals(t, sendVacated(0), sendReassign(time.Second))

	if len(run.statuses[0].UnassignedTasks) != 1 {
		t.Fatalf("after the vacate UnassignedTasks = %v, want the task", run.statuses[0].UnassignedTasks)
	}
	if got := run.statuses[1].UnassignedTasks; len(got) != 0 {
		t.Errorf("after the reassign UnassignedTasks = %v, want none", got)
	}
}

func TestExecute_ASignalThatArrivesLateDoesNotUndoANewerOne(t *testing.T) {
	t.Run("a vacate recorded before the reassign, delivered after it", func(t *testing.T) {
		run := runWithAssigneeSignals(t, sendReassign(time.Second), sendVacated(0))

		if got := run.statuses[1].UnassignedTasks; len(got) != 0 {
			t.Errorf("UnassignedTasks = %v, want none: the older vacate must not mark a task that was reassigned after it", got)
		}
	})
	t.Run("a reassign recorded before the vacate, delivered after it", func(t *testing.T) {
		run := runWithAssigneeSignals(t, sendVacated(time.Second), sendReassign(0))

		if got := run.statuses[1].UnassignedTasks; len(got) != 1 {
			t.Errorf("UnassignedTasks = %v, want the task: the older reassign must not hide the later vacate", got)
		}
	})
}

func TestExecute_APausedInstanceStillLearnsThatATaskLostItsAssignee(t *testing.T) {
	pause := func(env *testsuite.TestWorkflowEnvironment) {
		env.SignalWorkflow(port.SignalInstancePause+":instance-1", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 1})
	}
	resume := func(env *testsuite.TestWorkflowEnvironment) {
		env.SignalWorkflow(port.SignalInstanceResume+":instance-1", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 2})
	}

	run := runWithAssigneeSignals(t, pause, sendVacated(0), resume)

	if run.statuses[0].Status != domain.InstanceStatusPaused {
		t.Fatalf("Status = %v, want PAUSED", run.statuses[0].Status)
	}
	if got := run.statuses[1].UnassignedTasks; len(got) != 1 {
		t.Errorf("UnassignedTasks = %v, want the task: the vacate happened in the database whatever state the instance is in", got)
	}
}
