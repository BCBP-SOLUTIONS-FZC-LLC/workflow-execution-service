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

var errTaReadBack = errors.New("ta: read back failed")

// taFlakyTasks hands out copies, as the repository does, and refuses the read-backs that come after the
// first read of a task: failReads of them fail, the rest succeed.
type taFlakyTasks struct {
	*taTasks
	reads     int
	failReads int
}

func (r *taFlakyTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads++
	if r.reads > 1 && r.failReads > 0 {
		r.failReads--
		return nil, errTaReadBack
	}
	task, err := r.taTasks.GetByID(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	copied := *task
	return &copied, nil
}

func (r *taFlakyTasks) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, _ port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	return []*domain.Task{{
		ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/intake", Status: domain.TaskStatusCompleted,
	}}, nil, nil
}

func newTaFlakyHarness(t *testing.T, mode string, failReads int) (*taHarness, *taFlakyTasks) {
	t.Helper()
	h := newTaHarness(t, mode)
	flaky := &taFlakyTasks{taTasks: h.taskRepo, failReads: failReads}
	h.svc.Tasks = flaky
	return h, flaky
}

func TestTaskClaim_LeadRepeatsItsOwnClaim_ReturnsTheTaskUnchanged(t *testing.T) {
	h := newTaHarness(t, "all")
	h.caller.IsLead = true

	got, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.NoError(t, err)
	assert.Equal(t, h.task.ID, got.ID)
	assert.EqualValues(t, csdTaskVersion, got.RecordVersion)
	assert.Empty(t, h.assignments.setLeads, "a repeated claim writes nothing")
	assert.EqualValues(t, csdTaskVersion, h.task.RecordVersion)
	assert.Equal(t, h.callerID, h.assignments.leadOf(h.task.ID))
}

func TestTaskClaim_AnotherAssigneeHoldsTheLead_ReturnsAlreadyClaimed(t *testing.T) {
	h := newTaHarness(t, "all")
	h.other.IsLead = true

	_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.ErrorIs(t, err, port.ErrTaskAlreadyClaimed)
	assert.Empty(t, h.assignments.setLeads)
	assert.Equal(t, h.otherID, h.assignments.leadOf(h.task.ID))
}

func TestTaskClaim_ReadBackFailsOnce_ReturnsTheTaskReadOnTheRetry(t *testing.T) {
	h, flaky := newTaFlakyHarness(t, "all", 1)

	got, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.NoError(t, err)
	assert.Equal(t, h.task.ID, got.ID)
	assert.EqualValues(t, csdTaskVersion+1, got.RecordVersion)
	assert.Equal(t, 3, flaky.reads, "load, the failed read-back, and one retry")
	assert.Equal(t, h.callerID, h.assignments.leadOf(h.task.ID))
	assert.Len(t, h.assignments.setLeads, 1)
}

func TestTaskClaim_ReadBackKeepsFailing_ReturnsTheCommittedClaim(t *testing.T) {
	h, _ := newTaFlakyHarness(t, "all", 10)

	got, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.NoError(t, err, "the lead is recorded, so the claim answers success")
	assert.Equal(t, h.task.ID, got.ID)
	assert.EqualValues(t, csdTaskVersion+1, got.RecordVersion, "the answer carries the version the claim committed")
	assert.Equal(t, h.callerID, h.assignments.leadOf(h.task.ID))
	assert.Len(t, h.assignments.setLeads, 1)
}

func newTaDeferHarness(t *testing.T, failReads int) (*taHarness, *taFlakyTasks) {
	t.Helper()
	h, flaky := newTaFlakyHarness(t, "single", failReads)
	h.temporalLog.onUpdate = func() {
		h.task.Status = domain.TaskStatusDeferred
		h.task.RecordVersion += 2
	}
	return h, flaky
}

func TestTaskDefer_ReadBackFailsOnce_ReturnsTheTaskReadOnTheRetry(t *testing.T) {
	h, _ := newTaDeferHarness(t, 1)

	got, err := h.svc.Defer(context.Background(), h.tenantID, h.task.ID, h.callerID, "waiting for documents", csdTaskVersion)

	require.NoError(t, err)
	assert.Equal(t, h.task.ID, got.ID)
	assert.Equal(t, port.TaskStatusDeferred, got.Status)
	assert.EqualValues(t, csdTaskVersion+2, got.RecordVersion)
	require.Len(t, h.temporalLog.updates, 1)
}

func TestTaskDefer_ReadBackKeepsFailing_ReturnsTheDeferredTask(t *testing.T) {
	h, _ := newTaDeferHarness(t, 10)

	got, err := h.svc.Defer(context.Background(), h.tenantID, h.task.ID, h.callerID, "waiting for documents", csdTaskVersion)

	require.NoError(t, err, "the workflow applied the defer, so the call answers success")
	assert.Equal(t, h.task.ID, got.ID)
	assert.Equal(t, port.TaskStatusDeferred, got.Status)
	assert.EqualValues(t, csdTaskVersion+2, got.RecordVersion, "the status change and the assignment completion each bump the version")
	require.Len(t, h.temporalLog.updates, 1, "the defer is applied once, not again")
}
