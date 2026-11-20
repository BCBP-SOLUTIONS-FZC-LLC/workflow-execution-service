package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

var (
	errW13bRepo     = errors.New("w13b: repository failed")
	errW13bSignal   = errors.New("w13b: workflow unreachable")
	errW13bUpdate   = errors.New("w13b: update failed")
	errW13bReadBack = errors.New("w13b: read back failed")
)

// w13bTasks answers every read with a copy of the stored task, the way the database does. It records the
// tenant and id of each read, fails the reads its script names, and, for an instance of the tenant that holds
// a task, lists one completed task.
type w13bTasks struct {
	*taTasks
	script []error // one entry per GetByID call, in order; nil lets the call through
	gets   []csdIDCall
	lists  []csdIDCall
}

func (r *w13bTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.gets = append(r.gets, csdIDCall{TenantID: tenantID, ID: id})
	if len(r.script) > 0 {
		err := r.script[0]
		r.script = r.script[1:]
		if err != nil {
			return nil, err
		}
	}
	task, err := r.taTasks.GetByID(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	copied := *task
	return &copied, nil
}

func (r *w13bTasks) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, _ port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.lists = append(r.lists, csdIDCall{TenantID: tenantID, ID: instanceID})
	for _, task := range r.byID {
		if task.TenantID == tenantID && task.WorkflowInstanceID == instanceID {
			return []*domain.Task{{
				ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/intake", Status: domain.TaskStatusCompleted,
			}}, nil, nil
		}
	}
	return nil, nil, nil
}

// w13bTemporal lets a test fail the stage-defer update; its calls are recorded by the wrapped fake.
type w13bTemporal struct {
	*taTemporal
	updateErr error
}

func (c *w13bTemporal) UpdateWorkflow(ctx context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	if err := c.taTemporal.UpdateWorkflow(ctx, workflowID, instanceID, name, payload); err != nil {
		return err
	}
	return c.updateErr
}

// w13bClaimBump runs a hook after a claim commits, to change the stored task as a later writer would.
type w13bClaimBump struct {
	*taCountingAssignments
	after func()
}

func (r *w13bClaimBump) ClaimLead(ctx context.Context, tenantID, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	assignment, err := r.taCountingAssignments.ClaimLead(ctx, tenantID, taskID, id, version)
	if err == nil {
		r.after()
	}
	return assignment, err
}

// w13bEligibility records every question and answers yes only for the users it was told about.
type w13bEligibility struct {
	port.EligibilityChecker
	eligible map[uuid.UUID]bool
	calls    []csdEligibilityCall
}

func (e *w13bEligibility) CheckEligibility(_ context.Context, tenantID, userID, departmentID uuid.UUID, level string) (bool, error) {
	e.calls = append(e.calls, csdEligibilityCall{TenantID: tenantID, UserID: userID, DepartmentID: departmentID, Level: level})
	return e.eligible[userID], nil
}

func w13bNewHarness(t *testing.T, mode string) (*taHarness, *w13bTasks) {
	t.Helper()
	h := newTaHarness(t, mode)
	tasks := &w13bTasks{taTasks: h.taskRepo}
	h.svc.Tasks = tasks
	return h, tasks
}

func w13bDecode[T any](t *testing.T, payload any) T {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	var out T
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

// w13bRefusals is every refusal a pre-check or an eligibility check can give.
func w13bRefusals() []error {
	return []error{
		port.ErrTaskNotFound, port.ErrTaskNotHumanActionable, port.ErrRecordVersionConflict, port.ErrInvalidTaskState,
		port.ErrNotAssignee, port.ErrAssigneeIneligible, port.ErrClaimNotApplicable, port.ErrTaskAlreadyClaimed,
		port.ErrInvalidInstanceState, port.ErrInstanceNotFound, port.ErrOverrideNoOp,
	}
}

func w13bRequireOnly(t *testing.T, err, want error) {
	t.Helper()
	require.ErrorIs(t, err, want)
	for _, other := range w13bRefusals() {
		if !errors.Is(want, other) {
			assert.NotErrorIs(t, err, other, "an earlier refusal hides every later one")
		}
	}
}

func w13bConnectorTyped(h *taHarness) {
	kind := "http"
	h.task.ConnectorType = &kind
}

func TestTaskCompleteAndDefer_SeveralChecksFail_ReturnsTheEarliestRefusalAndReadsNoLaterData(t *testing.T) {
	ops := []struct {
		name string
		call func(h *taHarness, taskID, userID uuid.UUID, version int64) error
	}{
		{"complete", func(h *taHarness, taskID, userID uuid.UUID, version int64) error {
			_, err := h.svc.Complete(context.Background(), h.tenantID, taskID, userID, json.RawMessage(`{}`), version)
			return err
		}},
		{"defer", func(h *taHarness, taskID, userID uuid.UUID, version int64) error {
			_, err := h.svc.Defer(context.Background(), h.tenantID, taskID, userID, "waiting", version)
			return err
		}},
	}
	cases := []struct {
		name            string
		prepare         func(h *taHarness)
		unknownTask     bool
		stale           bool
		stranger        bool
		wantErr         error
		wantLists       int
		wantInstances   int
		wantEligibility int
	}{
		{name: "a missing task hides everything after it", unknownTask: true, stale: true, stranger: true, wantErr: port.ErrTaskNotFound},
		{
			name: "a connector task hides a stale version, a closed status and a stranger", prepare: func(h *taHarness) {
				w13bConnectorTyped(h)
				h.task.Status = domain.TaskStatusCompleted
			}, stale: true, stranger: true, wantErr: port.ErrTaskNotHumanActionable,
		},
		{
			name: "a stale version hides a closed status and a stranger", prepare: func(h *taHarness) { h.task.Status = domain.TaskStatusCompleted },
			stale: true, stranger: true, wantErr: port.ErrRecordVersionConflict,
		},
		{
			name: "a closed status hides a stranger and an ineligible caller", prepare: func(h *taHarness) {
				h.task.Status = domain.TaskStatusCompleted
				h.elig.eligible = false
			}, stranger: true, wantErr: port.ErrInvalidTaskState,
		},
		{
			name: "a stranger hides the eligibility check", prepare: func(h *taHarness) { h.elig.eligible = false },
			stranger: true, wantErr: port.ErrNotAssignee, wantLists: 1,
		},
		{
			name: "an ineligible assignee is refused last, after the instance and plan are read", prepare: func(h *taHarness) { h.elig.eligible = false },
			wantErr: port.ErrAssigneeIneligible, wantLists: 1, wantInstances: 1, wantEligibility: 1,
		},
		{name: "control: every check passes", wantLists: 1, wantInstances: 2, wantEligibility: 1},
	}
	for _, op := range ops {
		for _, tc := range cases {
			t.Run(op.name+"/"+tc.name, func(t *testing.T) {
				h, tasks := w13bNewHarness(t, "single")
				if tc.prepare != nil {
					tc.prepare(h)
				}
				taskID, userID, version := h.task.ID, h.callerID, int64(csdTaskVersion)
				if tc.unknownTask {
					taskID = uuid.New()
				}
				if tc.stranger {
					userID = uuid.New()
				}
				if tc.stale {
					version--
				}

				err := op.call(h, taskID, userID, version)

				assert.Equal(t, csdIDCall{TenantID: h.tenantID, ID: taskID}, tasks.gets[0], "the task is read for the caller's tenant")
				wantLists := tc.wantLists
				if tc.wantErr == nil && op.name == "defer" {
					wantLists++ // a defer reads the assignees again to find the assignment it defers
				}
				assert.Len(t, h.assignments.lists, wantLists, "assignee reads")
				if tc.wantErr == nil && op.name == "defer" {
					assert.Equal(t, []csdIDCall{{TenantID: h.tenantID, ID: h.instanceID}}, tasks.lists, "a defer looks for a completed step of the instance, for the caller's tenant")
				} else {
					assert.Empty(t, tasks.lists, "no step is looked up for a refusal or a completion")
				}
				assert.Len(t, h.instances.gets, tc.wantInstances, "the instance is read only once every earlier check passed")
				assert.Len(t, h.elig.calls, tc.wantEligibility)
				assert.Len(t, h.definitions.calls, tc.wantEligibility, "the plan is read only for the eligibility question")
				if tc.wantErr == nil {
					require.NoError(t, err)
					assert.Len(t, h.temporalLog.signals, map[string]int{"complete": 1, "defer": 0}[op.name])
					assert.Len(t, h.temporalLog.updates, map[string]int{"complete": 0, "defer": 1}[op.name])
					return
				}
				w13bRequireOnly(t, err, tc.wantErr)
				assert.Empty(t, h.temporalLog.signals)
				assert.Empty(t, h.temporalLog.updates)
				h.requireNothingChanged(t)
			})
		}
	}
}

func TestTaskComplete_Success_SignalsOnceAndAnswersWithTheTaskAsRead(t *testing.T) {
	for _, status := range []domain.TaskStatus{domain.TaskStatusReady, domain.TaskStatusInProgress} {
		t.Run(string(status), func(t *testing.T) {
			h, tasks := w13bNewHarness(t, "single")
			h.task.Status = status
			h.task.VisitCount = 4

			got, err := h.svc.Complete(context.Background(), h.tenantID, h.task.ID, h.callerID, json.RawMessage(`{"approved":true}`), csdTaskVersion)

			require.NoError(t, err)
			assert.Equal(t, h.task.ID, got.ID)
			assert.Equal(t, port.TaskStatus(status), got.Status, "the task is still open: the workflow, not this call, completes it")
			assert.EqualValues(t, csdTaskVersion, got.RecordVersion)
			assert.Equal(t, 2, got.AssigneeCount)
			assert.False(t, got.AssigneeCountUnavailable)
			assert.Equal(t, []csdIDCall{{TenantID: h.tenantID, ID: h.task.ID}}, tasks.gets, "the task is read once, before the signal")
			assert.Empty(t, tasks.lists)

			require.Len(t, h.temporalLog.signals, 1)
			sent := h.temporalLog.signals[0]
			assert.Equal(t, "stage-transition", sent.Name)
			assert.Equal(t, h.instanceID, sent.InstanceID)
			assert.Equal(t, "wf-"+h.instanceID.String(), sent.TemporalWorkflowID)
			assert.False(t, sent.InTx, "the signal is sent outside any transaction")
			wire := w13bDecode[struct {
				DeptID, ToStage, NodeID, UserID, ResultJSON string
				RecordVersion, VisitCount                   int64
				Failed                                      bool
				Reason                                      string
			}](t, sent.Payload)
			assert.Equal(t, "finance", wire.DeptID)
			assert.Equal(t, "review", wire.NodeID)
			assert.Equal(t, h.callerID.String(), wire.UserID)
			assert.Equal(t, `{"approved":true}`, wire.ResultJSON)
			assert.EqualValues(t, csdTaskVersion, wire.RecordVersion)
			assert.EqualValues(t, 4, wire.VisitCount, "the visit count of the task rides along")
			assert.Empty(t, wire.ToStage)
			assert.False(t, wire.Failed)
			assert.Empty(t, wire.Reason)

			assert.Equal(t, []string{"signal"}, h.log.events, "no transaction, version bump, override or outbox row is written")
			assert.Empty(t, h.taskRepo.bumps)
			assert.Empty(t, h.overrides.creates)
			assert.Empty(t, h.outbox.signals)
			assert.Empty(t, h.assignments.setLeads)
			assert.EqualValues(t, csdTaskVersion, h.task.RecordVersion)
			assert.Empty(t, h.temporalLog.updates)
			assert.Equal(t, status, h.task.Status)
			assert.True(t, h.caller.IsActive)
		})
	}
}

func TestTaskComplete_SignalFails_ReturnsTheWrappedCauseAndFreesTheCompletion(t *testing.T) {
	cases := []struct {
		name     string
		cause    error
		sentinel error
	}{
		{name: "a plain failure", cause: errW13bSignal},
		{name: "a workflow that is gone", cause: fmt.Errorf("%w: closed", port.ErrWorkflowNotFound), sentinel: port.ErrWorkflowNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := w13bNewHarness(t, "single")
			cache := newFakeCacheStore()
			h.svc.Cache = cache
			h.temporalLog.err = tc.cause

			got, err := h.svc.Complete(context.Background(), h.tenantID, h.task.ID, h.callerID, json.RawMessage(`{}`), csdTaskVersion)

			require.Error(t, err)
			assert.Nil(t, got)
			require.ErrorIs(t, err, tc.cause)
			if tc.sentinel != nil {
				require.ErrorIs(t, err, tc.sentinel, "a gone workflow stays recognisable through the wrapping")
			}
			assert.EqualError(t, err, "signal stage-transition: "+tc.cause.Error())
			require.Len(t, h.temporalLog.signals, 1)
			assert.Equal(t, h.instanceID, h.temporalLog.signals[0].InstanceID)
			assert.NotContains(t, cache.values, "task-complete:"+h.task.ID.String(), "a signal that never arrived leaves the completion free to retry")

			h.temporalLog.err = nil
			_, err = h.svc.Complete(context.Background(), h.tenantID, h.task.ID, h.callerID, json.RawMessage(`{}`), csdTaskVersion)
			require.NoError(t, err, "control: the retry is accepted once the workflow answers")
			assert.Len(t, h.temporalLog.signals, 2)
		})
	}
}

func TestTaskClaimAndDefer_ReadBackAfterTheCommit_IsRetriedOnceAndFallsBackToTheCommittedCopy(t *testing.T) {
	ops := []struct {
		name          string
		mode          string
		committedVer  int64 // the caller's own copy: +1 for a claim, +2 for a defer
		storedVer     int64 // what a successful read-back returns
		committedStat port.TaskStatus
		storedStat    port.TaskStatus
		call          func(h *taHarness) (*port.Task, error)
		writes        func(h *taHarness) int
		arrange       func(h *taHarness) // makes the stored task differ from the caller's own copy once the change commits
		looksUpStep   bool               // a defer looks for a completed step of the instance
	}{
		{
			name: "claim", mode: "all", committedVer: csdTaskVersion + 1, storedVer: csdTaskVersion + 2,
			committedStat: port.TaskStatusReady, storedStat: port.TaskStatusReady,
			call: func(h *taHarness) (*port.Task, error) {
				return h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)
			},
			writes: func(h *taHarness) int { return len(h.assignments.claims) },
			arrange: func(h *taHarness) {
				// the claim bumps the stored version once; a later writer bumps it again
				h.svc.Assignments = &w13bClaimBump{taCountingAssignments: h.counting, after: func() { h.task.RecordVersion++ }}
			},
		},
		{
			name: "defer", mode: "single", committedVer: csdTaskVersion + 2, storedVer: csdTaskVersion + 3,
			committedStat: port.TaskStatusDeferred, storedStat: port.TaskStatusDeferred,
			call: func(h *taHarness) (*port.Task, error) {
				return h.svc.Defer(context.Background(), h.tenantID, h.task.ID, h.callerID, "waiting", csdTaskVersion)
			},
			writes: func(h *taHarness) int { return len(h.temporalLog.updates) },
			arrange: func(h *taHarness) {
				h.temporalLog.onUpdate = func() {
					h.task.Status = domain.TaskStatusDeferred
					h.task.RecordVersion += 3 // not the +2 the caller assumes
				}
			},
			looksUpStep: true,
		},
	}
	cases := []struct {
		name      string
		script    []error // the load first, then the read-backs
		wantErr   error
		wantReads int
		wantWarns int
		committed bool // the answer is the caller's copy, not a read
	}{
		{name: "one failed read-back is retried", script: []error{nil, errW13bReadBack}, wantReads: 3, wantWarns: 1},
		{name: "two failed read-backs answer with the committed copy", script: []error{nil, errW13bReadBack, errW13bReadBack}, wantReads: 3, wantWarns: 2, committed: true},
		{name: "a task gone at the first read-back is not found", script: []error{nil, domain.ErrNotFound}, wantErr: port.ErrTaskNotFound, wantReads: 2},
		{name: "a task gone at the retry is not found", script: []error{nil, errW13bReadBack, domain.ErrNotFound}, wantErr: port.ErrTaskNotFound, wantReads: 3, wantWarns: 1},
	}
	for _, op := range ops {
		for _, tc := range cases {
			t.Run(op.name+"/"+tc.name, func(t *testing.T) {
				h, tasks := w13bNewHarness(t, op.mode)
				log := &fakeLogger{}
				h.svc.Log = log
				tasks.script = tc.script
				op.arrange(h)

				got, err := op.call(h)

				if !op.looksUpStep {
					assert.Empty(t, tasks.lists)
				} else {
					assert.Equal(t, []csdIDCall{{TenantID: h.tenantID, ID: h.instanceID}}, tasks.lists, "the completed step is looked up once, before the update")
				}

				assert.Equal(t, 1, op.writes(h), "the change is committed once and never repeated")
				require.Len(t, tasks.gets, tc.wantReads)
				for _, read := range tasks.gets {
					assert.Equal(t, csdIDCall{TenantID: h.tenantID, ID: h.task.ID}, read)
				}
				require.Len(t, log.warnFields, tc.wantWarns, "one warning per failed read")
				for _, fields := range log.warnFields {
					assert.Equal(t, h.task.ID, fields["task_id"])
					assert.Equal(t, errW13bReadBack.Error(), fields["error"])
				}
				if tc.wantErr != nil {
					require.ErrorIs(t, err, tc.wantErr)
					assert.Nil(t, got)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, h.task.ID, got.ID)
				assert.Equal(t, 2, got.AssigneeCount)
				if tc.committed {
					assert.Equal(t, op.committedVer, got.RecordVersion, "the caller's copy carries the version the change committed")
					assert.Equal(t, op.committedStat, got.Status)
					return
				}
				assert.Equal(t, op.storedVer, got.RecordVersion, "the answer is the task as read on the retry")
				assert.Equal(t, op.storedStat, got.Status)
			})
		}
	}
}

func TestTaskDefer_WorkflowUpdate_SendsTheDeferAndAnswersWithTheTaskReReadOrTheMappedError(t *testing.T) {
	t.Run("success sends every field and answers with the re-read task", func(t *testing.T) {
		h, tasks := w13bNewHarness(t, "single")
		h.temporalLog.onUpdate = func() {
			h.task.Status = domain.TaskStatusDeferred
			h.task.RecordVersion += 3
		}

		got, err := h.svc.Defer(context.Background(), h.tenantID, h.task.ID, h.callerID, "waiting for documents", csdTaskVersion)

		require.NoError(t, err)
		assert.Equal(t, port.TaskStatusDeferred, got.Status)
		assert.EqualValues(t, csdTaskVersion+3, got.RecordVersion, "the answer is what the database holds after the update")
		assert.Equal(t, []csdIDCall{{h.tenantID, h.task.ID}, {h.tenantID, h.task.ID}}, tasks.gets, "the task is read before the update and again after it")
		assert.Equal(t, []csdIDCall{{TenantID: h.tenantID, ID: h.instanceID}}, tasks.lists, "the completed step is looked up before the update")
		require.Len(t, h.temporalLog.updates, 1)
		sent := h.temporalLog.updates[0]
		assert.Equal(t, "stage-defer", sent.Name)
		assert.Equal(t, h.instanceID, sent.InstanceID)
		assert.Equal(t, "wf-"+h.instanceID.String(), sent.TemporalWorkflowID)
		wire := w13bDecode[struct {
			DeptID, FromStage, Reason, UserID, TaskID, AssignmentID string
			RecordVersion                                           int64
		}](t, sent.Payload)
		assert.Equal(t, "finance", wire.DeptID)
		assert.Equal(t, "review", wire.FromStage)
		assert.Equal(t, "waiting for documents", wire.Reason)
		assert.Equal(t, h.callerID.String(), wire.UserID)
		assert.Equal(t, h.task.ID.String(), wire.TaskID)
		assert.Equal(t, h.caller.ID.String(), wire.AssignmentID, "the caller's own assignment, not the other assignee's")
		assert.EqualValues(t, csdTaskVersion, wire.RecordVersion)
		assert.Empty(t, h.temporalLog.signals)
	})

	cases := []struct {
		name    string
		cause   error
		mapped  error
		message string
	}{
		{name: "a refused update is an invalid task state that keeps the refusal", cause: fmt.Errorf("%w: the stage is not the last", port.ErrUpdateRefused), mapped: port.ErrInvalidTaskState},
		{name: "any other failure is wrapped with the update name", cause: errW13bUpdate, message: "update stage-defer: " + errW13bUpdate.Error()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, tasks := w13bNewHarness(t, "single")
			h.svc.Temporal = &w13bTemporal{taTemporal: h.temporalLog, updateErr: tc.cause}

			got, err := h.svc.Defer(context.Background(), h.tenantID, h.task.ID, h.callerID, "waiting", csdTaskVersion)

			assert.Nil(t, got)
			require.ErrorIs(t, err, tc.cause)
			if tc.mapped != nil {
				require.ErrorIs(t, err, tc.mapped)
				require.ErrorIs(t, err, port.ErrUpdateRefused)
			} else {
				assert.NotErrorIs(t, err, port.ErrInvalidTaskState)
				assert.EqualError(t, err, tc.message)
			}
			assert.Len(t, h.temporalLog.updates, 1)
			assert.Len(t, tasks.gets, 1, "a failed update is not followed by a re-read")
			assert.Equal(t, []csdIDCall{{TenantID: h.tenantID, ID: h.instanceID}}, tasks.lists)
			assert.EqualValues(t, csdTaskVersion, h.task.RecordVersion)
			assert.Equal(t, domain.TaskStatusReady, h.task.Status)
		})
	}
}

func TestTaskReassign_SeveralChecksFail_ReturnsTheEarliestRefusal(t *testing.T) {
	cases := []struct {
		name            string
		prepare         func(h *taHarness)
		stale           bool
		noOp            bool
		newUserEligible bool
		wantErr         error
		wantLists       int
		wantInstances   int
		wantEligibility int
	}{
		{
			name: "a connector task hides a stale version and a closed status", prepare: func(h *taHarness) {
				w13bConnectorTyped(h)
				h.task.Status = domain.TaskStatusCompleted
			}, stale: true, wantErr: port.ErrTaskNotHumanActionable,
		},
		{
			name: "a stale version hides a closed status", prepare: func(h *taHarness) { h.task.Status = domain.TaskStatusCompleted },
			stale: true, wantErr: port.ErrRecordVersionConflict,
		},
		{
			name: "a closed status hides the assignee lookup", prepare: func(h *taHarness) {
				h.task.Status = domain.TaskStatusCompleted
				h.assignments.listErr = errW13bRepo
			}, wantErr: port.ErrInvalidTaskState,
		},
		{
			name: "a failed assignee lookup hides the instance read", prepare: func(h *taHarness) {
				h.assignments.listErr = errW13bRepo
				delete(h.instances.byID, h.instanceID)
			}, wantErr: errW13bRepo, wantLists: 1,
		},
		{
			name: "a no-op hides the instance state", prepare: func(h *taHarness) { h.instances.byID[h.instanceID].Status = domain.InstanceStatusPaused },
			noOp: true, wantErr: port.ErrOverrideNoOp, wantLists: 1,
		},
		{
			name: "a missing instance hides the eligibility of the new user", prepare: func(h *taHarness) { delete(h.instances.byID, h.instanceID) },
			wantErr: port.ErrInstanceNotFound, wantLists: 1, wantInstances: 1,
		},
		{
			name: "an instance that is not running hides the eligibility of the new user", prepare: func(h *taHarness) { h.instances.byID[h.instanceID].Status = domain.InstanceStatusPaused },
			wantErr: port.ErrInvalidInstanceState, wantLists: 1, wantInstances: 1,
		},
		{name: "an ineligible new user is refused after the instance checks", wantErr: port.ErrAssigneeIneligible, wantLists: 1, wantInstances: 1, wantEligibility: 1},
		{name: "control: only the new user is eligible", newUserEligible: true, wantLists: 1, wantInstances: 1, wantEligibility: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, tasks := w13bNewHarness(t, "single")
			if tc.prepare != nil {
				tc.prepare(h)
			}
			newUser, actor := uuid.New(), uuid.New()
			if tc.noOp {
				newUser = h.callerID // the first active assignee is the one the task is taken from
			}
			elig := &w13bEligibility{eligible: map[uuid.UUID]bool{newUser: tc.newUserEligible}}
			h.svc.Eligibility = elig
			version := int64(csdTaskVersion)
			if tc.stale {
				version--
			}

			got, err := h.svc.Reassign(context.Background(), h.tenantID, h.task.ID, actor, newUser, version)

			assert.Equal(t, csdIDCall{TenantID: h.tenantID, ID: h.task.ID}, tasks.gets[0])
			assert.Len(t, h.assignments.lists, tc.wantLists)
			assert.Len(t, h.instances.gets, tc.wantInstances)
			require.Len(t, elig.calls, tc.wantEligibility)
			if tc.wantEligibility > 0 {
				assert.Equal(t, csdEligibilityCall{TenantID: h.tenantID, UserID: newUser, DepartmentID: h.deptID, Level: "approver"}, elig.calls[0],
					"the new user is asked about, never the acting admin")
			}
			if tc.wantErr != nil {
				assert.Nil(t, got)
				w13bRequireOnly(t, err, tc.wantErr)
				assert.Empty(t, h.temporalLog.signals)
				h.requireNothingChanged(t)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, h.task.ID, got.ID)
			assert.Equal(t, port.TaskStatusReady, got.Status)
			assert.EqualValues(t, csdTaskVersion, got.RecordVersion, "the answer is the task as read before the signal")
			assert.Equal(t, 2, got.AssigneeCount)
			assert.Len(t, tasks.gets, 1, "the task is not read again after the signal")
			require.Len(t, h.temporalLog.signals, 1)
			sent := h.temporalLog.signals[0]
			assert.Equal(t, port.SignalInstanceReassign, sent.Name)
			assert.Equal(t, h.instanceID, sent.InstanceID)
			wire := taDecodeReassign(t, sent)
			assert.Equal(t, h.callerID.String(), wire.OldUserID)
			assert.Equal(t, newUser.String(), wire.NewUserID)
			assert.Equal(t, actor.String(), wire.AdminUserID)
			assert.EqualValues(t, csdTaskVersion, wire.RecordVersion)
			assert.Empty(t, h.outbox.signals)
			assert.Empty(t, h.taskRepo.bumps)
			assert.Empty(t, h.overrides.creates)
		})
	}
}

func TestTaskClaim_SeveralChecksFail_ReturnsTheEarliestRefusalBeforeAnyLeadIsSet(t *testing.T) {
	cases := []struct {
		name          string
		prepare       func(h *taHarness)
		stale         bool
		stranger      bool
		wantErr       error
		wantLists     int
		wantInstances int
		wantElig      int
	}{
		{
			name: "a connector task hides a single-assignee task, a stale version and a closed status", prepare: func(h *taHarness) {
				w13bConnectorTyped(h)
				h.task.AssigneeMode = "single"
				h.task.Status = domain.TaskStatusCompleted
			}, stale: true, wantErr: port.ErrTaskNotHumanActionable,
		},
		{
			name: "a single-assignee task hides a stale version and a closed status", prepare: func(h *taHarness) {
				h.task.AssigneeMode = "single"
				h.task.Status = domain.TaskStatusCompleted
			}, stale: true, wantErr: port.ErrClaimNotApplicable, wantLists: 1,
		},
		{
			name: "a stale version hides a closed status", prepare: func(h *taHarness) { h.task.Status = domain.TaskStatusCompleted },
			stale: true, wantErr: port.ErrRecordVersionConflict, wantLists: 1,
		},
		{
			name: "a closed status hides the instance state", prepare: func(h *taHarness) {
				h.task.Status = domain.TaskStatusCompleted
				h.instances.byID[h.instanceID].Status = domain.InstanceStatusPaused
			}, wantErr: port.ErrInvalidTaskState, wantLists: 1,
		},
		{
			name: "an instance that is not running hides the lead refusal", prepare: func(h *taHarness) {
				h.instances.byID[h.instanceID].Status = domain.InstanceStatusPaused
				h.other.IsLead = true
			}, wantErr: port.ErrInvalidInstanceState, wantLists: 1, wantInstances: 1,
		},
		{
			name: "a stranger is refused before the instance is read", prepare: func(h *taHarness) { h.other.IsLead = true },
			stranger: true, wantErr: port.ErrNotAssignee, wantLists: 1,
		},
		{
			name: "a lead held by another refuses an assignee who is not the lead", prepare: func(h *taHarness) { h.other.IsLead = true },
			wantErr: port.ErrTaskAlreadyClaimed, wantLists: 1, wantInstances: 1,
		},
		{
			name: "a stranger hides the eligibility check", prepare: func(h *taHarness) { h.elig.eligible = false },
			stranger: true, wantErr: port.ErrNotAssignee, wantLists: 1,
		},
		{
			name: "an ineligible assignee is refused last", prepare: func(h *taHarness) { h.elig.eligible = false },
			wantErr: port.ErrAssigneeIneligible, wantLists: 1, wantInstances: 2, wantElig: 1,
		},
	}
	obstest.Init(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, tasks := w13bNewHarness(t, "all")
			tc.prepare(h)
			userID, version := h.callerID, int64(csdTaskVersion)
			if tc.stranger {
				userID = uuid.New()
			}
			if tc.stale {
				version--
			}
			before := conflictCount(t, "workflow_task")
			leadBefore := h.assignments.leadOf(h.task.ID)

			got, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, userID, version)

			assert.Nil(t, got)
			w13bRequireOnly(t, err, tc.wantErr)
			assert.Equal(t, csdIDCall{TenantID: h.tenantID, ID: h.task.ID}, tasks.gets[0])
			assert.Len(t, h.assignments.lists, tc.wantLists)
			assert.Len(t, h.instances.gets, tc.wantInstances)
			assert.Len(t, h.elig.calls, tc.wantElig)
			assert.Empty(t, h.assignments.claims, "every refusal returns before the lead is set")
			assert.Empty(t, h.assignments.setLeads)
			assert.Equal(t, leadBefore, h.assignments.leadOf(h.task.ID), "nobody gained the lead")
			assert.Len(t, tasks.gets, 1, "a refused claim does not read the task again")
			wantConflicts := 0.0
			if errors.Is(tc.wantErr, port.ErrRecordVersionConflict) {
				wantConflicts = 1
			}
			assert.Equal(t, before+wantConflicts, conflictCount(t, "workflow_task"), "only a stale version is counted as a task version conflict")
		})
	}

	t.Run("control: with every check passing the claim sets the lead once", func(t *testing.T) {
		h, _ := w13bNewHarness(t, "all")

		_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

		require.NoError(t, err)
		assert.Equal(t, []csdSetLeadCall{{h.tenantID, h.task.ID, h.caller.ID, csdTaskVersion}}, h.assignments.claims)
	})
}
