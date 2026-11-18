package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// fxcLocker runs a step after the locks are taken and before the locked work,
// to change rows the caller read before the lock.
type fxcLocker struct {
	*fakeLocker
	afterLock func()
}

func (l *fxcLocker) WithLocks(ctx context.Context, scopeKeys []string, fn func(context.Context) error) error {
	return l.fakeLocker.WithLocks(ctx, scopeKeys, func(ctx context.Context) error {
		if l.afterLock != nil {
			l.afterLock()
		}
		return fn(ctx)
	})
}

func TestDelegationReroute_ATaskTheDelegatorGainedAfterTheReadKeepsTheInstancePaused(t *testing.T) {
	h := dlgfixNewRecon()
	row := h.seed(t, "reviewer")
	row.instance.Status = domain.InstanceStatusPaused
	require.NoError(t, h.store.byRecord(h.tenantID, h.delegator, "ooo"))
	var gained dlgfixRecRow
	h.svc.Locks = &fxcLocker{fakeLocker: h.locker, afterLock: func() {
		gained = h.seedOn(t, row.instance, "approver")
	}}
	h.eligibility.batchCheck = func(_ context.Context, _ uuid.UUID, reqs []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
		out := make([]port.EligibilityResult, len(reqs))
		for i, r := range reqs {
			out[i].Eligible = r.RequiredLevel == "reviewer"
		}
		return out, nil
	}

	require.NoError(t, h.reroute())

	assert.False(t, row.assignment.IsActive, "the task read before the lock moved")
	require.NotNil(t, gained.assignment)
	assert.True(t, gained.assignment.IsActive, "the task gained after the read stays with the delegator")
	assert.Zero(t, dlgfixResumeCount(h, row.instance.ID), "the delegator still holds work on the instance, so it is not resumed")
}

func TestDelegationReroute_ATaskThatLeftTheDelegatorAfterTheReadIsNotCountedAsMoved(t *testing.T) {
	h := dlgfixNewRecon()
	row := h.seed(t, "reviewer")
	other := h.seedOn(t, row.instance, "reviewer")
	row.instance.Status = domain.InstanceStatusPaused
	require.NoError(t, h.store.byRecord(h.tenantID, h.delegator, "ooo"))
	h.svc.Locks = &fxcLocker{fakeLocker: h.locker, afterLock: func() { other.assignment.IsActive = false }}

	require.NoError(t, h.reroute())

	assert.False(t, row.assignment.IsActive)
	assert.Equal(t, 1, dlgfixResumeCount(h, row.instance.ID), "the one task that really moved settles the instance")
	assert.Len(t, h.outbox.signalsNamed(port.SignalInstanceReassign), 1, "only the row that was still held is moved")
}

func TestDelegationReverse_AnInstanceReadAsRunningStillGetsTheResumeWhenItsPauseLandsAfterTheRead(t *testing.T) {
	h := dlgfixNewRecon()
	delegationID := uuid.New()
	row := h.seed(t, "reviewer")
	row.assignment.UserID = h.delegate
	row.assignment.Reason = "delegation:" + delegationID.String()
	h.svc.Instances = &dlgfixSnapshotInstances{fakeInstanceRepo: h.instances}
	h.store.onGet = func() { row.instance.Status = domain.InstanceStatusPaused }

	require.NoError(t, h.svc.Reverse(context.Background(), port.DelegationReversalInput{
		TenantID: h.tenantID, DelegationID: delegationID, DelegatorID: h.delegator, DelegateID: h.delegate, EndedReason: "expired",
	}))

	assert.False(t, row.assignment.IsActive, "the delegate's row ended and the work went back to the delegator")
	assert.Equal(t, 1, dlgfixResumeCount(h, row.instance.ID), "the workflow drops a resume it cannot use, so none is held back")
}

// fxcFailOnceSignals fails the first recording of the named signal.
type fxcFailOnceSignals struct {
	inner  *fakeOutbox
	name   string
	failed bool
}

func (s *fxcFailOnceSignals) EnqueueSignal(ctx context.Context, sig port.WorkflowSignal) error {
	if !s.failed && sig.Name == s.name {
		s.failed = true
		return errors.New("outbox down")
	}
	return s.inner.EnqueueSignal(ctx, sig)
}

// fxcRetryTx runs the work again, from the rolled back state, when it fails, as a transactor
// that retries on a serialization failure does.
type fxcRetryTx struct{ *dlgfixTx }

func (t fxcRetryTx) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	if err := t.dlgfixTx.RunInTx(ctx, fn); err == nil {
		return nil
	}
	return t.dlgfixTx.RunInTx(ctx, fn)
}

func TestDelegationReverse_ASignalThatFailedToRecordIsRecordedByTheRetryOfTheSameMove(t *testing.T) {
	h := dlgfixNewRecon()
	tx, _ := h.transactional()
	h.svc.Transactor = fxcRetryTx{tx}
	flaky := &fxcFailOnceSignals{inner: h.outbox, name: port.SignalInstancePause}
	h.svc.Signals = flaky
	delegationID := uuid.New()
	row := h.seed(t, "reviewer")
	row.assignment.UserID = h.delegate
	row.assignment.Reason = "delegation:" + delegationID.String()
	require.NoError(t, h.store.byRecord(h.tenantID, h.delegator, "ooo"))

	require.NoError(t, h.svc.Reverse(context.Background(), port.DelegationReversalInput{
		TenantID: h.tenantID, DelegationID: delegationID, DelegatorID: h.delegator, DelegateID: h.delegate, EndedReason: "expired",
	}))

	assert.True(t, flaky.failed, "the first recording failed")
	assert.Equal(t, 1, tx.rollbacks)
	pauses := h.outbox.signalsNamed(port.SignalInstancePause)
	require.Len(t, pauses, 1, "the retry records the pause once")
	assert.Equal(t, row.instance.ID, pauses[0].InstanceID)
}
