package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var (
	errW14aEndedWrite = errors.New("the ended record could not be written")
	errW14aLeadRead   = errors.New("the lead read failed")
	errW14aPlanRead   = errors.New("the plan could not be read")
)

// w14aEnded records the tenant and id of every attempt to record an ended delegation, and fails the ones it is told to.
type w14aEnded struct {
	*fakeEndedDelegations
	delegateErr      error
	delegationErr    error
	delegateAttempts []endedDelegateKey
	endedAttempts    []endedDelegationKey
}

func (e *w14aEnded) MarkDelegateEnded(ctx context.Context, tenantID, delegateID uuid.UUID, at time.Time) error {
	e.delegateAttempts = append(e.delegateAttempts, endedDelegateKey{tenantID, delegateID})
	if e.delegateErr != nil {
		return e.delegateErr
	}
	return e.fakeEndedDelegations.MarkDelegateEnded(ctx, tenantID, delegateID, at)
}

func (e *w14aEnded) MarkEnded(ctx context.Context, tenantID, delegationID uuid.UUID) error {
	e.endedAttempts = append(e.endedAttempts, endedDelegationKey{tenantID, delegationID})
	if e.delegationErr != nil {
		return e.delegationErr
	}
	return e.fakeEndedDelegations.MarkEnded(ctx, tenantID, delegationID)
}

func TestDelegationCancel_ARecordThatCannotBeWrittenFailsTheCancelNamingWhatItWasFor(t *testing.T) {
	cases := []struct {
		name  string
		named bool
		holds bool
	}{
		{"every delegation to a delegate that holds work", false, true},
		{"every delegation to a delegate that holds nothing", false, false},
		{"a named delegation whose work the delegate holds", true, true},
		{"a named delegation whose work the delegate does not hold", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := dlgfixNewClient(t)
			ended := &w14aEnded{fakeEndedDelegations: newFakeEndedDelegations()}
			h.svc.Ended = ended
			delegationID := uuid.New()
			in := port.CancelByDelegateInput{TenantID: h.tenantID, DelegateUserID: h.oldDelegate}
			if tc.named {
				in.DelegationID = &delegationID
				ended.delegationErr = errW14aEndedWrite
			} else {
				ended.delegateErr = errW14aEndedWrite
			}
			var held dlgfixHeld
			if tc.holds {
				held = h.seed(t, "reviewer")
				if tc.named {
					held.assignment.Reason = "delegation:" + delegationID.String()
				}
			}

			count, err := h.svc.CancelByDelegate(t.Context(), in)

			require.ErrorIs(t, err, errW14aEndedWrite)
			if tc.named {
				assert.ErrorContains(t, err, delegationID.String(), "the error names the delegation")
				assert.Equal(t, []endedDelegationKey{{h.tenantID, delegationID}}, ended.endedAttempts)
				assert.Empty(t, ended.delegateAttempts)
			} else {
				assert.ErrorContains(t, err, h.oldDelegate.String(), "the error names the delegate")
				assert.Equal(t, []endedDelegateKey{{h.tenantID, h.oldDelegate}}, ended.delegateAttempts)
				assert.Empty(t, ended.endedAttempts)
			}
			assert.Zero(t, count, "nothing is reported cancelled")
			if tc.holds {
				assert.True(t, held.assignment.IsActive, "nothing is vacated")
			}
			assert.Empty(t, h.outbox.enqueued)
			assert.Empty(t, h.outbox.signals)
			assert.Zero(t, h.tx.commits, "no transaction commits")
		})
	}

	t.Run("control: with the record writable the delegate's work is vacated and the delegate recorded", func(t *testing.T) {
		h := dlgfixNewClient(t)
		ended := &w14aEnded{fakeEndedDelegations: newFakeEndedDelegations()}
		h.svc.Ended = ended
		held := h.seed(t, "reviewer")

		count, err := h.cancel()

		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.False(t, held.assignment.IsActive)
		assert.Equal(t, []endedDelegateKey{{h.tenantID, h.oldDelegate}}, ended.delegateAttempts)
		assert.Empty(t, ended.endedAttempts)
	})

	t.Run("control: with the record writable a named delegation is recorded as ended and its work vacated", func(t *testing.T) {
		h := dlgfixNewClient(t)
		ended := &w14aEnded{fakeEndedDelegations: newFakeEndedDelegations()}
		h.svc.Ended = ended
		delegationID := uuid.New()
		held := h.seed(t, "reviewer")
		held.assignment.Reason = "delegation:" + delegationID.String()

		count, err := h.svc.CancelByDelegate(t.Context(), port.CancelByDelegateInput{TenantID: h.tenantID, DelegateUserID: h.oldDelegate, DelegationID: &delegationID})

		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.False(t, held.assignment.IsActive)
		assert.Equal(t, []endedDelegationKey{{h.tenantID, delegationID}}, ended.endedAttempts)
		assert.Equal(t, []endedDelegationKey{{h.tenantID, delegationID}}, ended.marked, "the end was written for this tenant and delegation")
		assert.Empty(t, ended.delegateAttempts)
	})
}

func w14aPlanOfDepartment(t *testing.T, h *dlgfixRecon, row dlgfixRecRow, departmentID uuid.UUID) {
	t.Helper()
	h.definitions.byVersion[row.versionID] = publishedCompiledWorkflow(uuid.New(), row.versionID, w14aPlanWithDept(t, dsl.DepartmentDef{
		ID: "finance", IAMDepartmentID: departmentID.String(), Stages: []dsl.StageDef{{Type: "userTask", NodeID: "reviewer", Role: "reviewer"}},
	}))
	row.task.DepartmentID = departmentID
}

func TestDelegationReroute_DepartmentScopeReadsEveryPlanBeforeItMovesAnyRow(t *testing.T) {
	reroute := func(h *dlgfixRecon, departmentID uuid.UUID) error {
		scopeID := departmentID.String()
		return h.svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: h.tenantID, DelegationID: uuid.New(), DelegatorID: h.delegator, DelegateID: h.delegate, Scope: "department", ScopeID: &scopeID,
		})
	}

	t.Run("control: with every plan readable both rows of the department move", func(t *testing.T) {
		h := dlgfixNewRecon()
		departmentID := uuid.New()
		first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")
		w14aPlanOfDepartment(t, h, first, departmentID)
		w14aPlanOfDepartment(t, h, second, departmentID)
		plans := &w14aDefinitions{DefinitionServiceClient: h.definitions}
		h.svc.Definitions = plans

		require.NoError(t, reroute(h, departmentID))

		assert.False(t, first.assignment.IsActive)
		assert.False(t, second.assignment.IsActive)
		assert.Len(t, h.outbox.enqueued, 2)
		assert.ElementsMatch(t, []w14aPlanRead{{h.tenantID, first.versionID}, {h.tenantID, second.versionID}}, plans.reads, "each plan is read once for the tenant")
	})

	t.Run("a later row whose plan cannot be read fails the event before the earlier row moves", func(t *testing.T) {
		h := dlgfixNewRecon()
		departmentID := uuid.New()
		good, bad := h.seed(t, "reviewer"), h.seed(t, "reviewer")
		w14aPlanOfDepartment(t, h, good, departmentID)
		w14aPlanOfDepartment(t, h, bad, departmentID)
		h.definitions.failing[bad.versionID] = errW14aPlanRead
		plans := &w14aDefinitions{DefinitionServiceClient: h.definitions}
		h.svc.Definitions = plans

		err := reroute(h, departmentID)

		require.ErrorIs(t, err, errW14aPlanRead, "the event fails, so the producer redelivers it")
		h.requireNothingMoved(t, good, bad)
		assert.ElementsMatch(t, []w14aPlanRead{{h.tenantID, good.versionID}, {h.tenantID, bad.versionID}}, plans.reads,
			"both plans were read for the tenant before anything moved, the good row's included")
	})
}

// w14aLeadReads records, in order, the reads of a task and of a task's active assignments made inside the move's
// transaction, and fails the lead move's own: the task read, or the assignments read that follows a task read.
type w14aLeadReads struct {
	tx *dlgfixTx

	order        []string
	taskReads    []w14aTaskRead
	listReads    []w14aTaskRead
	failTask     bool
	failListNext bool
	taskSeen     bool
}

type w14aTaskRead struct{ tenantID, taskID uuid.UUID }

type w14aTasksInTx struct {
	port.TaskRepository
	reads *w14aLeadReads
}

func (r *w14aTasksInTx) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	if r.reads.tx.running {
		r.reads.order = append(r.reads.order, "task")
		r.reads.taskReads = append(r.reads.taskReads, w14aTaskRead{tenantID, id})
		r.reads.taskSeen = true
		if r.reads.failTask {
			return nil, errW14aLeadRead
		}
	}
	return r.TaskRepository.GetByID(ctx, tenantID, id)
}

type w14aAssignmentsInTx struct {
	port.TaskAssignmentRepository
	reads *w14aLeadReads
}

func (r *w14aAssignmentsInTx) ListActiveByTask(ctx context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	if r.reads.tx.running {
		r.reads.order = append(r.reads.order, "list")
		r.reads.listReads = append(r.reads.listReads, w14aTaskRead{tenantID, taskID})
		if r.reads.failListNext && r.reads.taskSeen {
			return nil, errW14aLeadRead
		}
	}
	return r.TaskAssignmentRepository.ListActiveByTask(ctx, tenantID, taskID)
}

func TestDelegationStarted_ALeadMoveThatCannotReadTheTaskOrItsAssignmentsFailsTheEvent(t *testing.T) {
	// The lead move reads the task, then re-reads the task's active assignments; the vacate's own read of those
	// assignments comes before both.
	cases := []struct {
		name         string
		failTask     bool
		failListNext bool
	}{
		{"control: no read fails", false, false},
		{"the task read fails", true, false},
		{"the re-read of the task's active assignments fails", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := dlrNewUnclaimed(t, "all")
			tx := &dlgfixTx{repo: w.h.assignments, outbox: w.h.outbox}
			reads := &w14aLeadReads{tx: tx, failTask: tc.failTask, failListNext: tc.failListNext}
			w.h.svc.Transactor = tx
			w.h.svc.Tasks = &w14aTasksInTx{TaskRepository: w.h.tasks, reads: reads}
			w.h.svc.Assignments = &w14aAssignmentsInTx{TaskAssignmentRepository: w.leads, reads: reads}

			err := w.h.reroute()

			wantRead := w14aTaskRead{w.h.tenantID, w.row.task.ID}
			if !tc.failTask && !tc.failListNext {
				require.NoError(t, err)
				assert.True(t, w.delegate.IsLead, "the lead move ran")
				assert.Equal(t, 1, tx.commits)
				require.GreaterOrEqual(t, len(reads.order), 2)
				assert.Equal(t, []string{"task", "list"}, reads.order[len(reads.order)-2:], "the lead move reads the task, then the assignments")
				assert.Equal(t, []w14aTaskRead{wantRead}, reads.taskReads)
				assert.Equal(t, wantRead, reads.listReads[len(reads.listReads)-1])
				return
			}
			require.ErrorIs(t, err, errW14aLeadRead, "the event fails so it is retried")
			if tc.failTask {
				assert.Equal(t, []w14aTaskRead{wantRead}, reads.taskReads, "the task read that failed is of this task in this tenant")
				assert.Equal(t, "task", reads.order[len(reads.order)-1], "the failing read is the last one made")
			} else {
				require.GreaterOrEqual(t, len(reads.order), 2)
				assert.Equal(t, []string{"task", "list"}, reads.order[len(reads.order)-2:], "the failing read is the assignments read after the task read")
				assert.Equal(t, wantRead, reads.listReads[len(reads.listReads)-1], "the re-read that failed is of this task in this tenant")
			}
			assert.Equal(t, 1, tx.rollbacks, "the move is rolled back")
			assert.Zero(t, tx.commits)
			assert.True(t, w.row.assignment.IsActive, "the delegator keeps the task")
			assert.False(t, w.delegate.IsLead, "the delegate was not made lead")
			assert.Empty(t, w.leads.setLead, "no lead move is attempted without the reads")
			assert.Empty(t, w.h.outbox.enqueued)
			assert.Empty(t, w.h.outbox.signals)
			assert.Equal(t, []string{w.h.tenantID.String()}, tx.tenants)
		})
	}
}

func w14aHolderOf(a *fakeAssignmentRepo, taskID uuid.UUID) []*domain.TaskAssignment {
	var out []*domain.TaskAssignment
	for _, row := range a.byID {
		if row.TaskID == taskID && row.IsActive {
			out = append(out, row)
		}
	}
	return out
}

func TestDelegationReverse_AReplayOfTheSameEndFindsNothingLeftToRestore(t *testing.T) {
	h := dlgfixNewRecon()
	delegationID, earlierID, earlierDelegator := uuid.New(), uuid.New(), uuid.New()
	hop := "delegation:" + delegationID.String() + ":" + h.delegator.String()
	earlierHop := "delegation:" + earlierID.String() + ":" + earlierDelegator.String()
	single, chained := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	single.assignment.UserID, single.assignment.Reason = h.delegate, hop
	chained.assignment.UserID, chained.assignment.Reason = h.delegate, earlierHop+";"+hop
	in := port.DelegationReversalInput{
		TenantID: h.tenantID, DelegationID: delegationID, DelegatorID: h.delegator, DelegateID: h.delegate, EndedReason: "expired",
	}
	holderOf := func(task *domain.Task) *domain.TaskAssignment {
		t.Helper()
		active := w14aHolderOf(h.assignments, task.ID)
		require.Len(t, active, 1, "a task has exactly one holder")
		return active[0]
	}

	require.NoError(t, h.svc.Reverse(context.Background(), in))

	singleHolder, chainedHolder := holderOf(single.task), holderOf(chained.task)
	assert.Equal(t, h.delegator, singleHolder.UserID, "the work returns to the delegator")
	assert.Empty(t, singleHolder.Reason, "a row with one hop comes back with no reason")
	assert.Equal(t, h.delegator, chainedHolder.UserID)
	assert.Equal(t, earlierHop, chainedHolder.Reason, "a chained row keeps the hops before the ended one")
	events, signals, rows := len(h.outbox.enqueued), len(h.outbox.signals), len(h.assignments.byID)
	require.Equal(t, 2, events, "one event for each restored task")
	require.Len(t, h.outbox.signalsNamed(port.SignalInstanceReassign), 2)
	for _, tenantID := range h.outbox.signalTenants {
		assert.Equal(t, h.tenantID, tenantID)
	}

	require.NoError(t, h.svc.Reverse(context.Background(), in), "the replayed event is acknowledged")

	assert.Len(t, h.outbox.enqueued, events, "no second event")
	assert.Len(t, h.outbox.signals, signals, "no second signal")
	assert.Len(t, h.assignments.byID, rows, "no assignment is created or moved")
	assert.Equal(t, singleHolder.ID, holderOf(single.task).ID, "the same row still holds the task")
	assert.Equal(t, chainedHolder.ID, holderOf(chained.task).ID)
	assert.Empty(t, holderOf(single.task).Reason)
	assert.Equal(t, earlierHop, holderOf(chained.task).Reason, "the earlier hop is untouched by the replay")
	assert.Empty(t, h.temporal.signals, "nothing is sent straight to the workflow")
	ended, err := h.svc.Ended.IsEnded(context.Background(), h.tenantID, delegationID)
	require.NoError(t, err)
	assert.True(t, ended, "the delegation stays recorded as ended")
}
