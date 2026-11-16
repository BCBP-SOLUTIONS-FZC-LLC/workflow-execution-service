package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
)

// ivfixEndAfterRead ends the assignment the way a concurrent complete does: after
// the caller read the task's active rows and before it writes.
func ivfixEndAfterRead(assignment *domain.TaskAssignment) func(taskID uuid.UUID) {
	return func(taskID uuid.UUID) {
		if taskID == assignment.TaskID {
			assignment.IsActive = false
		}
	}
}

func ivfixDecodeReassigned(t *testing.T, env events.Envelope[json.RawMessage]) domain.WorkflowTaskReassignedPayload {
	t.Helper()
	assert.Equal(t, domain.EventWorkflowTaskReassigned, env.Type)
	var payload domain.WorkflowTaskReassignedPayload
	require.NoError(t, json.Unmarshal(env.Payload, &payload))
	return payload
}

func ivfixSignalledInstances(signals []signalCall, name string) []uuid.UUID {
	var out []uuid.UUID
	for _, s := range signals {
		if s.SignalName == name {
			out = append(out, s.InstanceID)
		}
	}
	return out
}

func TestDelegationReassign_ARowCompletedAfterTheReReadGetsNoHandOverEventOrSignal(t *testing.T) {
	h := dlgfixNewClient(t)
	completed, live := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	h.assignments.afterList = ivfixEndAfterRead(completed.assignment)

	count, err := h.reassign()

	require.NoError(t, err)
	assert.Equal(t, 1, count, "the completed row is not counted")
	assert.False(t, h.holdsActive(completed.task.ID, h.newDelegate), "no assignment is created for the finished task")
	require.Len(t, h.assignments.created, 1)
	assert.Equal(t, live.task.ID, h.assignments.created[0].TaskID)
	assert.Equal(t, []uuid.UUID{live.instance.ID}, ivfixSignalledInstances(h.outbox.signals, port.SignalInstanceReassign), "only the moved row is signalled")
	require.Len(t, h.outbox.enqueued, 1, "no reassigned event for the finished task")
	assert.Equal(t, live.task.ID, ivfixDecodeReassigned(t, h.outbox.enqueued[0]).TaskID)
	assert.Equal(t, 1, h.tx.commits)
}

func TestDelegationCancel_ARowCompletedAfterTheReReadIsNotCountedOrSignalled(t *testing.T) {
	h := dlgfixNewClient(t)
	completed, live := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	h.assignments.afterList = ivfixEndAfterRead(completed.assignment)

	count, err := h.cancel()

	require.NoError(t, err)
	assert.Equal(t, 1, count, "the completed row is not counted")
	assert.Equal(t, []uuid.UUID{live.instance.ID}, ivfixSignalledInstances(h.outbox.signals, port.SignalInstanceTaskVacated),
		"the workflow is not told that a finished task lost its assignee")
	assert.Equal(t, 1, h.tx.commits)
}

func TestDelegationReroute_ARowCompletedAfterTheReReadGetsNoHandOverEventOrSignal(t *testing.T) {
	h := dlgfixNewRecon()
	tx, repo := h.transactional()
	completed, live := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	repo.afterList = ivfixEndAfterRead(completed.assignment)

	require.NoError(t, h.reroute())

	for _, a := range h.assignments.byID {
		assert.False(t, a.TaskID == completed.task.ID && a.UserID == h.delegate, "no delegate row for the finished task")
	}
	require.Len(t, repo.created, 1)
	assert.Equal(t, live.task.ID, repo.created[0].TaskID)
	require.Len(t, h.outbox.enqueued, 1, "no reassigned event for the finished task")
	assert.Equal(t, live.task.ID, ivfixDecodeReassigned(t, h.outbox.enqueued[0]).TaskID)
	assert.Equal(t, []uuid.UUID{live.instance.ID}, ivfixSignalledInstances(h.outbox.signals, port.SignalInstanceReassign))
	assert.Equal(t, 1, tx.commits)
}

// ivfixSecondEnqueueFails lets the first event through and fails the second.
type ivfixSecondEnqueueFails struct {
	*fakeOutbox
	calls int
}

func (o *ivfixSecondEnqueueFails) Enqueue(ctx context.Context, env events.Envelope[json.RawMessage]) error {
	o.calls++
	if o.calls == 2 {
		return assert.AnError
	}
	return o.fakeOutbox.Enqueue(ctx, env)
}

func TestDelegationReassign_RecordsOneReassignedEventPerMovedRowInTheMoveTransaction(t *testing.T) {
	h := dlgfixNewClient(t)
	h.withEligibility(func(level string) (bool, error) { return level == "reviewer", nil })
	first, second, skipped := h.seed(t, "reviewer"), h.seed(t, "reviewer"), h.seed(t, "approver")

	count, err := h.reassign()

	require.NoError(t, err)
	assert.Equal(t, 2, count)
	require.Len(t, h.outbox.enqueued, 2, "one event per moved row, none for the row that stayed")
	byTask := map[uuid.UUID]domain.WorkflowTaskReassignedPayload{}
	for _, env := range h.outbox.enqueued {
		assert.Equal(t, h.tenantID.String(), env.TenantID)
		p := ivfixDecodeReassigned(t, env)
		byTask[p.TaskID] = p
	}
	require.Contains(t, byTask, first.task.ID)
	require.Contains(t, byTask, second.task.ID)
	assert.NotContains(t, byTask, skipped.task.ID)
	for _, held := range []dlgfixHeld{first, second} {
		p := byTask[held.task.ID]
		assert.Equal(t, held.instance.ID, p.WorkflowInstanceID)
		assert.Equal(t, h.oldDelegate, p.OldUserID)
		assert.Equal(t, h.newDelegate, p.NewUserID)
		assert.Equal(t, []uuid.UUID{h.newDelegate}, p.AssigneeUserIDs)
		assert.Equal(t, domain.ReassignInitiatorDelegation, p.Initiator)
		assert.Nil(t, p.DelegationID, "no delegation id came with the call")
	}
	assert.Equal(t, []string{h.tenantID.String()}, h.tx.tenants, "the move runs once, on the tenant context")
	assert.Equal(t, 1, h.tx.commits)
}

func TestDelegationReassign_TheReassignedEventCarriesTheDelegationIDOfTheCall(t *testing.T) {
	h := dlgfixNewClient(t)
	delegationID := uuid.New()
	held := h.seed(t, "reviewer")
	held.assignment.Reason = ivfixDelegationReason(delegationID)

	count, err := h.svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{
		TenantID: h.tenantID, OldDelegateID: h.oldDelegate, NewDelegateID: h.newDelegate, DelegationID: &delegationID,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, count)
	require.Len(t, h.outbox.enqueued, 1)
	p := ivfixDecodeReassigned(t, h.outbox.enqueued[0])
	require.NotNil(t, p.DelegationID)
	assert.Equal(t, delegationID, *p.DelegationID)
}

func ivfixDelegationReason(id uuid.UUID) string { return "delegation:" + id.String() }

func TestDelegationReassign_AFailedEventEnqueueRollsTheWholeMoveBackAndFailsTheCall(t *testing.T) {
	h := dlgfixNewClient(t)
	failing := &ivfixSecondEnqueueFails{fakeOutbox: h.outbox}
	h.svc.Outbox = failing
	first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")

	count, err := h.reassign()

	require.ErrorIs(t, err, assert.AnError, "the caller sees the failure and retries")
	assert.Zero(t, count)
	h.requireUntouched(t, first, second)
	assert.Empty(t, h.outbox.enqueued, "the first row's event is rolled back with its move")
	assert.Zero(t, h.tx.commits)
	assert.Equal(t, 1, h.tx.rollbacks)
}
