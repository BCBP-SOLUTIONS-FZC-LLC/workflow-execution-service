package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w7bDelegateEndedReads fails the read of the delegate's cancel time and records its arguments.
type w7bDelegateEndedReads struct {
	*fakeEndedDelegations
	err   error
	reads []endedDelegateKey
}

func (e *w7bDelegateEndedReads) DelegateEndedAt(ctx context.Context, tenantID, delegateID uuid.UUID) (time.Time, bool, error) {
	e.reads = append(e.reads, endedDelegateKey{tenantID, delegateID})
	if e.err != nil {
		return time.Time{}, false, e.err
	}
	return e.fakeEndedDelegations.DelegateEndedAt(ctx, tenantID, delegateID)
}

// w7bNthReadTasks fails every read of a task from the failAt-th on.
type w7bNthReadTasks struct {
	*fakeTaskRepo
	reads   int
	failAt  int
	err     error
	tenants []uuid.UUID
}

func (r *w7bNthReadTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads++
	r.tenants = append(r.tenants, tenantID)
	if r.reads >= r.failAt {
		return nil, r.err
	}
	return r.fakeTaskRepo.GetByID(ctx, tenantID, id)
}

// w7bNthListAssignments fails every read of a task's active assignments from the failAt-th on.
type w7bNthListAssignments struct {
	*fakeAssignmentRepo
	reads   int
	failAt  int
	err     error
	tenants []uuid.UUID
}

func (r *w7bNthListAssignments) ListActiveByTask(ctx context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.reads++
	r.tenants = append(r.tenants, tenantID)
	if r.reads >= r.failAt {
		return nil, r.err
	}
	return r.fakeAssignmentRepo.ListActiveByTask(ctx, tenantID, taskID)
}

// w7bRollingTx makes the reconciler's transaction roll assignment rows and the outbox back on failure.
func w7bRollingTx(h *dlgfixRecon) *dlgfixTx {
	tx := &dlgfixTx{repo: h.assignments, outbox: h.outbox}
	h.svc.Transactor = tx
	return tx
}

func TestDelegationReversed_RowHeldByTheDelegatorWithTheDelegationsHop_IsLeftAlone(t *testing.T) {
	cases := []struct {
		name         string
		withOwedRow  bool
		wantRestored int
	}{
		{name: "it is the only row carrying the hop", withOwedRow: false, wantRestored: 0},
		{name: "another row carrying the hop is held by the delegate", withOwedRow: true, wantRestored: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := dlgfixNewRecon()
			var asked [][]port.EligibilityCheckRequest
			h.eligibility.batchCheck = func(_ context.Context, _ uuid.UUID, requests []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
				asked = append(asked, requests)
				results := make([]port.EligibilityResult, len(requests))
				for i := range results {
					results[i] = port.EligibilityResult{Eligible: true}
				}
				return results, nil
			}
			delegationID, earlierDelegator := uuid.New(), uuid.New()
			hop := dcrHop(delegationID, earlierDelegator)
			heldByDelegator := h.seed(t, "reviewer")
			heldByDelegator.assignment.Reason = hop
			var owed dlgfixRecRow
			if tc.withOwedRow {
				owed = h.seed(t, "reviewer")
				owed.assignment.UserID = h.delegate
				owed.assignment.Reason = hop
			}

			err := h.svc.Reverse(t.Context(), port.DelegationReversalInput{
				TenantID: h.tenantID, DelegationID: delegationID, DelegatorID: h.delegator, DelegateID: h.delegate, EndedReason: "expired",
			})

			require.NoError(t, err)
			assert.True(t, heldByDelegator.assignment.IsActive, "the delegator's own row is not vacated")
			assert.Equal(t, hop, heldByDelegator.assignment.Reason, "and not rewritten")
			assert.Len(t, h.outbox.enqueued, tc.wantRestored)
			assert.Len(t, h.outbox.signalsNamed(port.SignalInstanceReassign), tc.wantRestored)
			assert.Empty(t, h.temporal.signals)
			for _, s := range h.outbox.signals {
				assert.NotEqual(t, heldByDelegator.instance.ID, s.InstanceID, "nothing is signalled for the delegator's own row")
			}
			ended := h.svc.Ended.(*fakeEndedDelegations)
			assert.Equal(t, []endedDelegationKey{{h.tenantID, delegationID}}, ended.marked, "the end is still recorded")
			if !tc.withOwedRow {
				assert.Empty(t, asked, "no one is asked about eligibility when nothing is owed back")
				assert.Len(t, h.locker.keys, 1, "and no restore lock is taken")
				return
			}
			require.Len(t, asked, 1)
			assert.Len(t, asked[0], 1, "only the owed row is checked")
			assert.Equal(t, h.delegator, asked[0][0].NewUserID)
			assert.False(t, owed.assignment.IsActive, "the delegate's row is vacated")
			assert.True(t, w7bHolds(h, owed.task.ID, h.delegator))
			assert.Equal(t, owed.instance.ID, h.outbox.signals[0].InstanceID)
		})
	}
}

func w7bHolds(h *dlgfixRecon, taskID, userID uuid.UUID) bool {
	for _, a := range h.assignments.byID {
		if a.TaskID == taskID && a.UserID == userID && a.IsActive {
			return true
		}
	}
	return false
}

func TestDelegationStarted_ReadOfTheDelegatesCancelTimeFails_MovesNothingAndFailsTheEvent(t *testing.T) {
	h := dlgfixNewRecon()
	tx := w7bRollingTx(h)
	ended := &w7bDelegateEndedReads{fakeEndedDelegations: newFakeEndedDelegations(), err: assert.AnError}
	h.svc.Ended = ended
	first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")

	err := h.reroute()

	require.ErrorIs(t, err, assert.AnError, "the event fails so it is redelivered")
	h.requireNothingMoved(t, first, second)
	assert.Equal(t, []endedDelegateKey{{h.tenantID, h.delegate}}, ended.reads)
	assert.Empty(t, tx.tenants, "no transaction was opened")
	assert.Zero(t, tx.commits)
	assert.Len(t, h.locker.keys, 3, "the read ran under the start's locks")
	assert.Empty(t, h.temporal.signals)
}

func TestDelegationStarted_ReadsForTheLeadDecisionFail_RollBackTheMove(t *testing.T) {
	cases := []struct {
		name string
		// breakIt wraps the failing read and returns the tenant ids that read was asked with.
		breakIt   func(h *dlgfixRecon) func() []uuid.UUID
		wantReads int
	}{
		{"the task cannot be read", func(h *dlgfixRecon) func() []uuid.UUID {
			// the first read is the candidate read, the second the re-list under the lock; the lead decision is the third
			tasks := &w7bNthReadTasks{fakeTaskRepo: h.tasks, failAt: 3, err: assert.AnError}
			h.svc.Tasks = tasks
			return func() []uuid.UUID { return tasks.tenants }
		}, 3},
		{"the task's active assignments cannot be re-read", func(h *dlgfixRecon) func() []uuid.UUID {
			// the first read is the vacate's; the lead decision's is the second
			lists := &w7bNthListAssignments{fakeAssignmentRepo: h.assignments, failAt: 2, err: assert.AnError}
			h.svc.Assignments = lists
			return func() []uuid.UUID { return lists.tenants }
		}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := dlgfixNewRecon()
			tx := w7bRollingTx(h)
			row := h.seed(t, "reviewer")
			row.task.AssigneeMode = "all"
			delegateRow := &domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: row.task.ID, UserID: h.delegate, IsActive: true}
			h.assignments.add(delegateRow)
			askedTenants := tc.breakIt(h)

			err := h.reroute()

			require.ErrorIs(t, err, assert.AnError)
			assert.True(t, row.assignment.IsActive, "the delegator's vacate is rolled back")
			assert.True(t, delegateRow.IsActive)
			assert.False(t, delegateRow.IsLead, "no lead is set")
			assert.Equal(t, 1, tx.rollbacks)
			assert.Zero(t, tx.commits)
			assert.Equal(t, []string{h.tenantID.String()}, tx.tenants)
			wantTenants := make([]uuid.UUID, tc.wantReads)
			for i := range wantTenants {
				wantTenants[i] = h.tenantID
			}
			assert.Equal(t, wantTenants, askedTenants(), "every read, the failing one included, names the event's tenant")
			assert.Empty(t, h.outbox.enqueued)
			assert.Empty(t, h.outbox.signals)
			assert.Empty(t, h.temporal.signals)
		})
	}
}
