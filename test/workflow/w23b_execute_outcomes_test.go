package workflow_test

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// w23bBase is the workflow start time wflStart sets.
var w23bBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func w23bOneTask() *dsl.CompiledCollaboration {
	return wflOneDept("sales", dsl.StageDef{Type: "approve", NodeID: "n1"})
}

func w23bNewEnv() *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	return suite.NewTestWorkflowEnvironment()
}

func TestExecute_AllStepsDone_WritesCompletedOnceWithWorkflowTime(t *testing.T) {
	env := w23bNewEnv()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, w23bOneTask())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("sales", "n1", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	writes := wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus)
	require.Len(t, writes, 1, "the final status is written exactly once")
	assert.Equal(t, domain.InstanceStatusCompleted, writes[0].Status)
	assert.Equal(t, wflTenant, writes[0].TenantID)
	assert.Equal(t, wflInstance, writes[0].InstanceID)
	require.NotNil(t, writes[0].CompletedAt)
	assert.True(t, writes[0].CompletedAt.Equal(w23bBase.Add(2*time.Second)), "CompletedAt = %v, want the workflow time of the last answer", writes[0].CompletedAt)

	val, qerr := env.QueryWorkflow(wfengine.QueryGetWorkflowStatus)
	require.NoError(t, qerr, "the status query still answers after completion")
	var after wfengine.WorkflowStatusQuery
	require.NoError(t, val.Get(&after))
	assert.Equal(t, domain.InstanceStatusCompleted, after.Status)
	assert.Empty(t, after.CurrentNodeKeys)
	assert.Empty(t, after.ActiveTasks)
	assert.Empty(t, after.SavedSiblingBranches)
}

func TestExecute_StepFails_FinalisesFailedWithCompletedAtAndReturnsTheStepError(t *testing.T) {
	cases := []struct {
		name        string
		refuseWrite bool
	}{
		{name: "status write succeeds"},
		{name: "status write also refused", refuseWrite: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := w23bNewEnv()
			fakes := wflNewFakes()
			if tc.refuseWrite {
				fakes.failFor = func(name string, _ any) error {
					if name == port.ActivityUpdateInstanceStatus {
						return wflRefuse("status write")
					}
					return nil
				}
			}
			wflRegisterAll(env, fakes, w23bOneTask())
			wflSignalAt(env, 2*time.Second, "stage-fail", stageFailWire{DeptID: "sales", NodeID: "n1", ErrorClass: "boom", RecordVersion: 5, VisitCount: 1})

			_, err := wflStart(t, env, fakes, "")

			require.Error(t, err)
			assert.ErrorContains(t, err, `stage "sales/n1" failed: boom`)
			writes := wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus)
			require.Len(t, writes, 1)
			assert.Equal(t, domain.InstanceStatusFailed, writes[0].Status)
			assert.Equal(t, wflTenant, writes[0].TenantID)
			assert.Equal(t, wflInstance, writes[0].InstanceID)
			require.NotNil(t, writes[0].CompletedAt)
			assert.True(t, writes[0].CompletedAt.Equal(w23bBase.Add(2*time.Second)), "CompletedAt = %v", writes[0].CompletedAt)
			tasks := wflInputs[port.UpdateTaskStatusInput](fakes, port.ActivityUpdateTaskStatus)
			require.Len(t, tasks, 1)
			assert.Equal(t, "sales/n1", tasks[0].TaskID)
			assert.Equal(t, domain.TaskStatusFailed, tasks[0].Status)
		})
	}
}

func TestExecute_InstanceCancel_PassesTheReasonAsAPointerOnlyWhenOneIsGiven(t *testing.T) {
	cases := []struct {
		name   string
		reason string
	}{
		{name: "empty reason", reason: ""},
		{name: "reason given", reason: "duplicate request"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := w23bNewEnv()
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, w23bOneTask())
			wflSignalAt(env, 2*time.Second, "instance-cancel", adminSignalWire{AdminUserID: "admin-9", Reason: tc.reason, RecordVersion: 4})

			status, err := wflStart(t, env, fakes, "")

			require.NoError(t, err)
			assert.Equal(t, domain.InstanceStatusTerminated, status)
			cancels := wflInputs[port.CancelInstanceInput](fakes, port.ActivityCancelInstance)
			require.Len(t, cancels, 1)
			assert.Equal(t, wflTenant, cancels[0].TenantID)
			assert.Equal(t, wflInstance, cancels[0].InstanceID)
			assert.Equal(t, "admin-9", cancels[0].AdminUserID)
			assert.Equal(t, int64(4), cancels[0].RecordVersion)
			if tc.reason == "" {
				assert.Nil(t, cancels[0].Reason)
			} else {
				require.NotNil(t, cancels[0].Reason)
				assert.Equal(t, tc.reason, *cancels[0].Reason)
			}
			assert.Empty(t, wflStatuses(fakes), "a cancelled instance gets no completed or failed write")
		})
	}
}

func TestExecute_InstanceCancelWhilePaused_TerminatesAndDropsTheResultHeldDuringThePause(t *testing.T) {
	env := w23bNewEnv()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, w23bOneTask())
	wflSignalAt(env, 2*time.Second, "instance-pause", adminSignalWire{AdminUserID: "admin-1", Initiator: domain.InitiatorAdmin, RecordVersion: 2})
	wflSignalAt(env, 4*time.Second, "stage-transition", wflDone("sales", "n1", 1))
	wflSignalAt(env, 6*time.Second, "instance-cancel", adminSignalWire{AdminUserID: "admin-9", RecordVersion: 3})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	pauses := wflInputs[port.PauseInstanceInput](fakes, port.ActivityPauseInstance)
	require.Len(t, pauses, 1)
	assert.Equal(t, wflInstance, pauses[0].InstanceID)
	cancels := wflInputs[port.CancelInstanceInput](fakes, port.ActivityCancelInstance)
	require.Len(t, cancels, 1)
	assert.Equal(t, "admin-9", cancels[0].AdminUserID)
	assert.Equal(t, int64(3), cancels[0].RecordVersion)
	assert.Empty(t, wflInputs[port.CompleteAssignmentInput](fakes, port.ActivityCompleteAssignment), "the held result was never applied")
	assert.Empty(t, wflStatuses(fakes), "no completed write after the cancel")
}

func w23bWire(dept string, visit, recordVersion int64) stageTransitionWire {
	return stageTransitionWire{DeptID: dept, ToStage: "prep", ResultJSON: "{}", RecordVersion: recordVersion, VisitCount: visit}
}

func TestExecute_SignalForAStaleVisit_IsIgnoredAndNeverResolvesTheRevisitedTask(t *testing.T) {
	env := w23bNewEnv()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, twoDeptCollaboration())
	wflSignalAt(env, 2*time.Second, "stage-transition", w23bWire("deptA", 1, 11))
	wflSignalAt(env, 4*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-1", TargetDeptID: "deptA", TargetNodeKey: "deptA/prep", RecordVersion: 1})
	// The revisit of deptA/prep is on visit 2: these answer visit 1.
	wflSignalAt(env, 6*time.Second, "stage-transition", w23bWire("deptA", 1, 91))
	wflSignalAt(env, 8*time.Second, "stage-fail", stageFailWire{DeptID: "deptA", NodeID: "prep", ErrorClass: "stale", RecordVersion: 92, VisitCount: 1})
	var afterStale wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 9*time.Second, &afterStale)
	wflSignalAt(env, 10*time.Second, "stage-transition", w23bWire("deptA", 2, 12))
	wflSignalAt(env, 12*time.Second, "stage-transition", w23bWire("deptB", 2, 13))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []domain.NodeKey{"deptA/prep"}, afterStale.CurrentNodeKeys, "the stale answers left the revisit waiting")
	var versions []int64
	for _, c := range wflInputs[port.CompleteAssignmentInput](fakes, port.ActivityCompleteAssignment) {
		versions = append(versions, c.RecordVersion)
	}
	assert.Equal(t, []int64{11, 12, 13}, versions, "only the answers for the visits that waited were applied")
	for _, s := range wflInputs[port.UpdateTaskStatusInput](fakes, port.ActivityUpdateTaskStatus) {
		assert.NotEqual(t, domain.TaskStatusFailed, s.Status, "the stale failure did not fail the revisit")
	}
}

func TestExecute_StageDeferRefusedByStatus_FailsWithAnUpdateRefusedErrorAndWritesNothing(t *testing.T) {
	cases := []struct {
		name     string
		pause    bool
		deferAt  time.Duration
		taskID   string
		wantText string
	}{
		{name: "instance paused", pause: true, deferAt: 4 * time.Second, taskID: "deptB/prep", wantText: "defer refused: workflow: signal \"stage-defer\" rejected: instance status is PAUSED"},
		{name: "no completed step to go back to", deferAt: 4 * time.Second, taskID: "deptA/prep", wantText: "defer refused: there is no completed step to go back to"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := w23bNewEnv()
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, twoDeptCollaboration())
			if tc.pause {
				wflSignalAt(env, 1*time.Second, "stage-transition", w23bWire("deptA", 1, 1))
				wflSignalAt(env, 2*time.Second, "instance-pause", adminSignalWire{AdminUserID: "admin-1", Initiator: domain.InitiatorAdmin, RecordVersion: 2})
				wflSignalAt(env, 8*time.Second, "instance-resume", adminSignalWire{AdminUserID: "admin-1", Initiator: domain.InitiatorAdmin, RecordVersion: 3})
			} else {
				wflSignalAt(env, 8*time.Second, "stage-transition", w23bWire("deptA", 1, 1))
			}
			wflSignalAt(env, 12*time.Second, "stage-transition", w23bWire("deptB", 1, 1))
			answer := &updateAnswer{}
			env.RegisterDelayedCallback(func() {
				env.UpdateWorkflow("stage-defer:"+wflInstance, "defer-1", answer, deferWire{
					DeptID: strings.Split(tc.taskID, "/")[0], FromStage: "prep", UserID: "user-1", RecordVersion: 1, TaskID: tc.taskID, AssignmentID: "assign-1",
				})
			}, tc.deferAt)

			status, err := wflStart(t, env, fakes, "")

			require.NoError(t, err)
			assert.Equal(t, domain.InstanceStatusCompleted, status)
			require.Error(t, answer.rejected)
			var appErr *temporal.ApplicationError
			require.True(t, errors.As(answer.rejected, &appErr), "the refusal is an application error: %v", answer.rejected)
			assert.Equal(t, port.UpdateRefusedErrorType, appErr.Type())
			assert.True(t, appErr.NonRetryable())
			assert.Contains(t, appErr.Message(), tc.wantText)
			assert.Zero(t, fakes.attemptsOf(port.ActivityDeferTask), "a refused defer writes nothing")
		})
	}
}

func TestExecute_ForceBackAtTopLevel_PopsOnlyTheTopEntryEachTimeAndAuditsTheBackRoute(t *testing.T) {
	env := w23bNewEnv()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, threeDeptCollaboration())
	wflSignalAt(env, 2*time.Second, "stage-transition", w23bWire("deptA", 1, 1))
	wflSignalAt(env, 4*time.Second, "stage-transition", w23bWire("deptB", 1, 1))
	// deptC waits; history is [deptA/prep, deptB/prep].
	wflSignalAt(env, 6*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-5", RecordVersion: 7})
	wflSignalAt(env, 8*time.Second, "stage-transition", w23bWire("deptB", 2, 1))
	// deptB is done again, so history is [deptA/prep, deptB/prep] again.
	wflSignalAt(env, 10*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-6", RecordVersion: 8})
	wflSignalAt(env, 12*time.Second, "stage-transition", w23bWire("deptB", 3, 1))
	wflSignalAt(env, 14*time.Second, "stage-transition", w23bWire("deptC", 3, 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 2)
	for i, admin := range []string{"admin-5", "admin-6"} {
		assert.Equal(t, domain.ForceRouteDirectionBack, audits[i].Direction)
		assert.Equal(t, admin, audits[i].AdminUserID)
		assert.Equal(t, wflTenant, audits[i].TenantID)
		assert.Equal(t, wflInstance, audits[i].InstanceID)
		assert.Equal(t, "deptB/prep", audits[i].TargetNodeID, "only the top entry is popped, so the run restarts at deptB, never deptA")
	}
	assert.Equal(t, []domain.NodeKey{"deptC/prep"}, audits[0].OldNodeKeys, "the audit names the nodes that were pending")
	assert.Equal(t, int64(7), audits[0].RecordVersion)
	assert.Equal(t, int64(8), audits[1].RecordVersion)
	var visitsA []int64
	for _, c := range wflInputs[port.CreateTaskInput](fakes, port.ActivityCreateTask) {
		if c.NodeKey == "deptA/prep" {
			visitsA = append(visitsA, c.VisitCount)
		}
	}
	assert.Equal(t, []int64{1}, visitsA, "deptA is never rerun")
}

// w23bLog records the info and warning lines of a test workflow environment.
type w23bLog struct {
	mu    sync.Mutex
	infos []w23bLogLine
}

type w23bLogLine struct {
	msg     string
	keyvals []any
}

func (l *w23bLog) Debug(string, ...any) {}
func (l *w23bLog) Warn(string, ...any)  {}
func (l *w23bLog) Error(string, ...any) {}
func (l *w23bLog) Info(msg string, keyvals ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, w23bLogLine{msg: msg, keyvals: keyvals})
}

func (l *w23bLog) withText(text string) []w23bLogLine {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []w23bLogLine
	for _, line := range l.infos {
		if strings.Contains(line.msg, text) {
			out = append(out, line)
		}
	}
	return out
}

func TestExecute_AssigneeSignalRecordedBeforeTheLastAppliedOne_IsDroppedWithAnInfoLog(t *testing.T) {
	vacated := func(at time.Time) any {
		return taskVacatedWire{TaskID: vacatedTaskID, VacatedUserID: "user-1", RecordedAt: at}
	}
	reassigned := func(at time.Time) any {
		return stampedReassignWire{TaskID: vacatedTaskID, OldUserID: "user-1", NewUserID: "user-2", RecordVersion: 1, RecordedAt: at}
	}
	cases := []struct {
		name           string
		first, second  any
		firstSignal    string
		secondSignal   string
		wantUnassigned []string
	}{
		{
			name:  "older reassign after a newer vacate",
			first: vacated(vacatedBase.Add(10 * time.Second)), firstSignal: port.SignalInstanceTaskVacated,
			second: reassigned(vacatedBase.Add(5 * time.Second)), secondSignal: "instance-reassign",
			wantUnassigned: []string{vacatedTaskID},
		},
		{
			name:  "older vacate after a newer reassign",
			first: reassigned(vacatedBase.Add(10 * time.Second)), firstSignal: "instance-reassign",
			second: vacated(vacatedBase.Add(5 * time.Second)), secondSignal: port.SignalInstanceTaskVacated,
			wantUnassigned: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			log := &w23bLog{}
			suite.SetLogger(log)
			env := suite.NewTestWorkflowEnvironment()
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, wflOneDept("sales", dsl.StageDef{Type: "approve", NodeID: "approve"}))
			wflSignalAt(env, 2*time.Second, tc.firstSignal, tc.first)
			wflSignalAt(env, 4*time.Second, tc.secondSignal, tc.second)
			var seen wfengine.WorkflowStatusQuery
			wflQueryStatusAt(t, env, 6*time.Second, &seen)
			wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("sales", "approve", 1))

			status, err := wflStart(t, env, fakes, "")

			require.NoError(t, err)
			assert.Equal(t, domain.InstanceStatusCompleted, status)
			assert.Equal(t, tc.wantUnassigned, seen.UnassignedTasks)
			dropped := log.withText("dropping an assignee signal recorded before a later one")
			require.Len(t, dropped, 1)
			assert.Contains(t, dropped[0].keyvals, vacatedTaskID)
		})
	}
}
