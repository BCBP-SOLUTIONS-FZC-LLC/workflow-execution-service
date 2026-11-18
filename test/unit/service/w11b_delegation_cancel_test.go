package service_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var (
	errW11bInstanceRead = errors.New("w11b: instance read failed")
	errW11bSignalRecord = errors.New("w11b: signal could not be recorded")
)

// w11bNamedSignalOutbox fails only the signals of one name and records the name
// of every signal it was asked to record.
type w11bNamedSignalOutbox struct {
	*fakeOutbox
	failName string
	err      error
	attempts []string
}

func (o *w11bNamedSignalOutbox) EnqueueSignal(ctx context.Context, sig port.WorkflowSignal) error {
	o.attempts = append(o.attempts, sig.Name)
	if sig.Name == o.failName {
		return o.err
	}
	return o.fakeOutbox.EnqueueSignal(ctx, sig)
}

type w11bInstanceRead struct {
	tenantID uuid.UUID
	id       uuid.UUID
}

// w11bReadFailInstances fails the read of chosen instances and records every read.
type w11bReadFailInstances struct {
	*fakeInstanceRepo
	failing map[uuid.UUID]error
	reads   []w11bInstanceRead
}

func (r *w11bReadFailInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, w11bInstanceRead{tenantID: tenantID, id: id})
	if err := r.failing[id]; err != nil {
		return nil, err
	}
	return r.fakeInstanceRepo.GetByID(ctx, tenantID, id)
}

// w11bCancelAssignments records the listings by user and the vacates of a cancel.
type w11bCancelAssignments struct {
	*dlgfixAssignments
	lists   []w11bUserListCall
	vacates []w11bIDCall
}

func (r *w11bCancelAssignments) ListActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.lists = append(r.lists, w11bUserListCall{tenantID: tenantID, userID: userID})
	return r.dlgfixAssignments.ListActiveByUser(ctx, tenantID, userID)
}

func (r *w11bCancelAssignments) VacateActive(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error) {
	r.vacates = append(r.vacates, w11bIDCall{tenantID: tenantID, id: id})
	return r.dlgfixAssignments.VacateActive(ctx, tenantID, id)
}

type w11bCancelRecorders struct {
	tasks       *w11bRecordingTasks
	instances   *w11bReadFailInstances
	assignments *w11bCancelAssignments
}

// w11bRecordCancelReads puts recording wrappers between the client and its repositories.
func w11bRecordCancelReads(h *dlgfixClient) *w11bCancelRecorders {
	rec := &w11bCancelRecorders{
		tasks:       &w11bRecordingTasks{fakeTaskRepo: h.tasks},
		instances:   &w11bReadFailInstances{fakeInstanceRepo: h.instances, failing: map[uuid.UUID]error{}},
		assignments: &w11bCancelAssignments{dlgfixAssignments: h.assignments},
	}
	h.svc.Tasks, h.svc.Instances, h.svc.Assignments = rec.tasks, rec.instances, rec.assignments
	return rec
}

func TestDelegationCancel_NamedEarlierHopOfAChain_ReturnsTheRowToThatHopsDelegator(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		hops      int
		named     int
		wantChain []int
	}{
		{name: "the earlier of two hops", hops: 2, named: 0, wantChain: nil},
		{name: "the middle of three hops keeps the hop before it", hops: 3, named: 1, wantChain: []int{0}},
		{name: "the latest of three hops keeps both earlier ones", hops: 3, named: 2, wantChain: []int{0, 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := dlgfixNewClient(t)
			rec := w11bRecordCancelReads(h)
			delegations := make([]uuid.UUID, tc.hops)
			delegators := make([]uuid.UUID, tc.hops)
			hopTexts := make([]string, tc.hops)
			for i := range tc.hops {
				delegations[i], delegators[i] = uuid.New(), uuid.New()
				hopTexts[i] = dcrHop(delegations[i], delegators[i])
			}
			held := h.seed(t, "reviewer")
			held.assignment.Reason = strings.Join(hopTexts, ";")
			var wantReason []string
			for _, i := range tc.wantChain {
				wantReason = append(wantReason, hopTexts[i])
			}
			named := delegations[tc.named]

			count, err := h.svc.CancelByDelegate(t.Context(), port.CancelByDelegateInput{
				TenantID: h.tenantID, DelegateUserID: h.oldDelegate, DelegationID: &named,
			})

			require.NoError(t, err)
			assert.Equal(t, 1, count)
			assert.Equal(t, []w11bUserListCall{{tenantID: h.tenantID, userID: h.oldDelegate}}, rec.assignments.lists)
			assert.Equal(t, []w11bIDCall{{tenantID: h.tenantID, id: held.task.ID}}, rec.tasks.gets)
			assert.Equal(t, []w11bInstanceRead{{tenantID: h.tenantID, id: held.instance.ID}}, rec.instances.reads)
			assert.Equal(t, []w11bIDCall{{tenantID: h.tenantID, id: held.assignment.ID}}, rec.assignments.vacates)
			assert.False(t, h.holdsActive(held.task.ID, h.oldDelegate), "the present holder lost the row")
			for i, delegator := range delegators {
				assert.Equal(t, i == tc.named, h.holdsActive(held.task.ID, delegator), "only the named hop's delegator holds the row")
			}
			var restored []*domain.TaskAssignment
			for _, a := range h.assignments.byID {
				if a.TaskID == held.task.ID && a.UserID == delegators[tc.named] && a.IsActive {
					restored = append(restored, a)
				}
			}
			require.Len(t, restored, 1)
			assert.Equal(t, strings.Join(wantReason, ";"), restored[0].Reason, "the row keeps the hops before the named one")

			require.Len(t, h.outbox.signals, 1)
			assert.Equal(t, port.SignalInstanceReassign, h.outbox.signals[0].SignalName)
			assert.Equal(t, csaReassignWire{
				TaskID: held.task.ID.String(), OldUserID: h.oldDelegate.String(), NewUserID: delegators[tc.named].String(),
			}, csaDecodeReassign(t, h.outbox.signals[0].Payload))
			assert.Equal(t, []uuid.UUID{h.tenantID}, h.outbox.signalTenants)
			require.Len(t, h.outbox.enqueued, 1)
			event := ivfixDecodeReassigned(t, h.outbox.enqueued[0])
			assert.Equal(t, delegators[tc.named], event.NewUserID)
			require.NotNil(t, event.DelegationID)
			assert.Equal(t, named, *event.DelegationID)
		})
	}
}

func TestDelegationCancel_DelegateWithNothingToCancel_ReturnsZeroAndRecordsTheEnd(t *testing.T) {
	t.Parallel()

	delegationID := uuid.New()
	cases := []struct {
		name         string
		delegationID *uuid.UUID
		wantScope    func(tenantID, delegateID uuid.UUID) string
	}{
		{
			name:      "no delegation named",
			wantScope: port.DelegateScope,
		},
		{
			name:         "a delegation named",
			delegationID: &delegationID,
			wantScope:    func(tenantID, _ uuid.UUID) string { return port.DelegationScope(tenantID, delegationID) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := dlgfixNewClient(t)
			rec := w11bRecordCancelReads(h)
			ended := newFakeEndedDelegations()
			locker := &fakeLocker{}
			h.svc.Ended, h.svc.Locks = ended, locker

			count, err := h.svc.CancelByDelegate(t.Context(), port.CancelByDelegateInput{
				TenantID: h.tenantID, DelegateUserID: h.oldDelegate, DelegationID: tc.delegationID,
			})

			require.NoError(t, err)
			assert.Zero(t, count, "nothing was cancelled, which is not an error")
			assert.Equal(t, []w11bUserListCall{{tenantID: h.tenantID, userID: h.oldDelegate}}, rec.assignments.lists)
			assert.Empty(t, rec.tasks.gets)
			assert.Empty(t, rec.instances.reads)
			assert.Empty(t, rec.assignments.vacates)
			assert.Equal(t, []string{tc.wantScope(h.tenantID, h.oldDelegate)}, locker.keys, "the cancel runs under the lock a start of the same delegation takes")
			if tc.delegationID != nil {
				assert.Equal(t, []endedDelegationKey{{h.tenantID, delegationID}}, ended.marked)
				assert.Empty(t, ended.delegates)
			} else {
				_, found, readErr := ended.DelegateEndedAt(t.Context(), h.tenantID, h.oldDelegate)
				require.NoError(t, readErr)
				assert.True(t, found, "a start delivered later finds every delegation to the delegate ended")
				assert.Empty(t, ended.marked)
			}
			assert.Zero(t, h.tx.commits, "no transaction is opened when there is nothing to move")
			assert.Zero(t, h.tx.rollbacks)
			assert.Empty(t, h.outbox.signals)
			assert.Empty(t, h.outbox.enqueued)
			assert.Empty(t, h.assignments.byID)
		})
	}
}

func TestDelegationCancel_AnInstanceThatCannotBeRead_FailsTheCallBeforeAnyChange(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		sabotage func(h *dlgfixClient, bad dlgfixHeld, instances *w11bReadFailInstances)
		want     error
	}{
		{
			name: "the instance does not exist",
			sabotage: func(h *dlgfixClient, bad dlgfixHeld, _ *w11bReadFailInstances) {
				delete(h.instances.byID, bad.instance.ID)
			},
			want: domain.ErrNotFound,
		},
		{
			name: "the instance read fails",
			sabotage: func(_ *dlgfixClient, bad dlgfixHeld, instances *w11bReadFailInstances) {
				instances.failing[bad.instance.ID] = errW11bInstanceRead
			},
			want: errW11bInstanceRead,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := dlgfixNewClient(t)
			rec := w11bRecordCancelReads(h)
			instances := rec.instances
			ended := newFakeEndedDelegations()
			h.svc.Ended = ended
			good, bad := h.seed(t, "reviewer"), h.seed(t, "reviewer")
			tc.sabotage(h, bad, instances)

			count, err := h.cancel()

			require.ErrorIs(t, err, tc.want)
			assert.Zero(t, count)
			assert.Equal(t, []w11bInstanceRead{
				{tenantID: h.tenantID, id: good.instance.ID}, {tenantID: h.tenantID, id: bad.instance.ID},
			}, instances.reads, "every selected row is read in the delegate's tenant before any change")
			assert.Equal(t, []w11bIDCall{
				{tenantID: h.tenantID, id: good.task.ID}, {tenantID: h.tenantID, id: bad.task.ID},
			}, rec.tasks.gets)
			assert.Equal(t, []w11bUserListCall{{tenantID: h.tenantID, userID: h.oldDelegate}}, rec.assignments.lists)
			assert.Empty(t, rec.assignments.vacates, "nothing was vacated")
			h.requireUntouched(t, good, bad)
			assert.Len(t, h.assignments.byID, 2, "no assignment was created")
			assert.Zero(t, h.tx.commits, "the transaction never opened")
			assert.Zero(t, h.tx.rollbacks)
			assert.Empty(t, h.outbox.enqueued)
			assert.Empty(t, ended.marked)
			assert.Empty(t, ended.delegates, "the cancel is not recorded as ended, so the caller's retry is a full cancel")
		})
	}
}

func TestDelegationCancel_ASignalThatCannotBeRecorded_RollsBackTheWholeCancel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		failName     string
		wantAttempts []string
		vacatesBoth  bool
	}{
		{
			name:         "the task-vacated signal of a row that is not given back",
			failName:     port.SignalInstanceTaskVacated,
			wantAttempts: []string{port.SignalInstanceReassign, port.SignalInstanceTaskVacated},
			vacatesBoth:  true,
		},
		{
			name:         "the reassign signal of a row given back to its delegator",
			failName:     port.SignalInstanceReassign,
			wantAttempts: []string{port.SignalInstanceReassign},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := dlgfixNewClient(t)
			rec := w11bRecordCancelReads(h)
			signals := &w11bNamedSignalOutbox{fakeOutbox: h.outbox, failName: tc.failName, err: errW11bSignalRecord}
			h.svc.Signals = signals
			delegator := uuid.New()
			given, vacated := h.seed(t, "reviewer"), h.seed(t, "reviewer")
			given.assignment.Reason = dcrHop(uuid.New(), delegator)

			count, err := h.cancel()

			require.ErrorIs(t, err, errW11bSignalRecord, "a cancel whose signal cannot be recorded is not committed, so the caller retries it")
			assert.Zero(t, count)
			assert.Equal(t, tc.wantAttempts, signals.attempts, "the failing signal is the one under test")
			assert.Equal(t, []w11bUserListCall{{tenantID: h.tenantID, userID: h.oldDelegate}}, rec.assignments.lists)
			wantVacates := []w11bIDCall{{tenantID: h.tenantID, id: given.assignment.ID}}
			if tc.vacatesBoth {
				wantVacates = append(wantVacates, w11bIDCall{tenantID: h.tenantID, id: vacated.assignment.ID})
			}
			assert.Equal(t, wantVacates, rec.assignments.vacates, "each vacate is made in the delegate's tenant")
			assert.True(t, given.assignment.IsActive, "the row given back is put back with the delegate")
			assert.True(t, vacated.assignment.IsActive)
			assert.False(t, h.holdsActive(given.task.ID, delegator), "the delegator holds nothing the call created")
			assert.Empty(t, h.outbox.signals, "no signal outlives the rollback")
			assert.Empty(t, h.outbox.enqueued, "no event outlives the rollback")
			assert.Equal(t, 1, h.tx.rollbacks)
			assert.Zero(t, h.tx.commits)
			assert.Equal(t, []string{h.tenantID.String()}, h.tx.tenants, "the transaction ran on the delegate's tenant")
		})
	}
}
