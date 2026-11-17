package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

type endedFirstWorld struct {
	svc         *service.DelegationReconciler
	ended       *fakeEndedDelegations
	assignments *fakeAssignmentRepo
	outbox      *fakeOutbox
	task        *domain.Task
	held        *domain.TaskAssignment
	tenantID    uuid.UUID
	delegator   uuid.UUID
	delegate    uuid.UUID
}

func newEndedFirstWorld(t *testing.T) *endedFirstWorld {
	t.Helper()
	svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
	w := &endedFirstWorld{svc: svc, assignments: assignments, outbox: outbox, tenantID: uuid.New(), delegator: uuid.New(), delegate: uuid.New()}
	w.ended = svc.Ended.(*fakeEndedDelegations)
	_, w.task, w.held = seedDelegationFixture(instances, tasks, assignments, definitions, w.tenantID, w.delegator, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
	return w
}

func (w *endedFirstWorld) start(delegationID uuid.UUID) error {
	return w.svc.Reroute(context.Background(), port.DelegationRerouteInput{
		TenantID: w.tenantID, DelegationID: delegationID, DelegatorID: w.delegator, DelegateID: w.delegate, Scope: "all",
	})
}

func (w *endedFirstWorld) end(delegationID uuid.UUID) error {
	return w.svc.Reverse(context.Background(), port.DelegationReversalInput{
		TenantID: w.tenantID, DelegationID: delegationID, DelegatorID: w.delegator, DelegateID: w.delegate, EndedReason: "expired",
	})
}

func TestDelegationStarted_AfterItsDelegationEndedMovesNothing(t *testing.T) {
	w := newEndedFirstWorld(t)
	delegationID := uuid.New()

	require.NoError(t, w.end(delegationID), "an end that finds nothing tagged is still recorded")
	require.NoError(t, w.start(delegationID))

	assert.True(t, w.held.IsActive, "the delegator keeps the work")
	assert.Empty(t, w.outbox.enqueued)
	assert.Empty(t, w.outbox.signals)
	assert.Equal(t, []endedDelegationKey{{w.tenantID, delegationID}}, w.ended.marked)
}

func TestDelegationStarted_AnotherDelegationStillMovesTheWork(t *testing.T) {
	w := newEndedFirstWorld(t)

	require.NoError(t, w.end(uuid.New()))
	require.NoError(t, w.start(uuid.New()))

	assert.False(t, w.held.IsActive, "an end for one delegation does not stop another")
	assert.NotNil(t, assignmentOf(w.assignments, w.task.ID, w.delegate))
}

func TestDelegationStarted_EndedInAnotherTenantStillMovesTheWork(t *testing.T) {
	w := newEndedFirstWorld(t)
	delegationID := uuid.New()
	require.NoError(t, w.ended.MarkEnded(context.Background(), uuid.New(), delegationID))

	require.NoError(t, w.start(delegationID))

	assert.False(t, w.held.IsActive, "an ended delegation is remembered per tenant")
}

func TestDelegationStarted_AnEndThatCannotBeReadFailsSoTheEventIsRetried(t *testing.T) {
	w := newEndedFirstWorld(t)
	w.ended.checkErr = assert.AnError

	err := w.start(uuid.New())

	require.ErrorIs(t, err, assert.AnError)
	assert.True(t, w.held.IsActive)
	assert.Empty(t, w.outbox.enqueued)
}

func TestDelegationEnded_ARecordThatCannotBeWrittenFailsSoTheEventIsRetried(t *testing.T) {
	w := newEndedFirstWorld(t)
	w.ended.markErr = assert.AnError

	err := w.end(uuid.New())

	require.ErrorIs(t, err, assert.AnError)
}

func TestDelegationEnded_AfterItsStartStillReturnsTheWork(t *testing.T) {
	w := newEndedFirstWorld(t)
	delegationID := uuid.New()
	require.NoError(t, w.start(delegationID))

	require.NoError(t, w.end(delegationID))

	assert.NotNil(t, assignmentOf(w.assignments, w.task.ID, w.delegator), "the usual order is unchanged")
	assert.Nil(t, assignmentOf(w.assignments, w.task.ID, w.delegate))
}

func TestDelegationStartAndEnd_TakeTheDelegatorsLockAroundTheEndRecord(t *testing.T) {
	w := newEndedFirstWorld(t)
	locker := &fakeLocker{}
	w.svc.Locks = locker
	var checkHeld, markHeld bool
	w.ended.onCheck = func() { checkHeld = locker.held }
	w.ended.onMark = func() { markHeld = locker.held }
	delegationID := uuid.New()

	require.NoError(t, w.start(delegationID))
	require.NoError(t, w.end(delegationID))

	assert.True(t, checkHeld, "a start looks for an end while the delegator's lock is held, so an end cannot slip in before its move commits")
	assert.True(t, markHeld, "an end is recorded while the delegator's lock is held")
	scope := port.UserAvailabilityScope(w.tenantID, w.delegator)
	assert.Equal(t, []string{scope, scope, scope}, locker.keys)
}
