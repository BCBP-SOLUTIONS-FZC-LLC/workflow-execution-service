package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// fxhConflictingUpdates answers the first conflicts status updates with a version conflict, as a
// concurrent writer that bumped the row after it was read would cause.
type fxhConflictingUpdates struct {
	port.InstanceRepository
	conflicts int
	updates   []uuid.UUID
	versions  []int64
}

func (r *fxhConflictingUpdates) UpdateStatus(ctx context.Context, tenantID, id uuid.UUID, status domain.InstanceStatus, version int64) (*domain.Instance, error) {
	r.updates = append(r.updates, tenantID)
	r.versions = append(r.versions, version)
	if len(r.updates) <= r.conflicts {
		return nil, domain.ErrRecordVersionConflict
	}
	return r.InstanceRepository.UpdateStatus(ctx, tenantID, id, status, version)
}

func fxhTerminateWorld(conflicts int) (*cscHarness, *fxhConflictingUpdates, *domain.Instance, uuid.UUID) {
	tenantID := uuid.New()
	inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 4)
	h := newCscHarness(inst)
	wrapped := &fxhConflictingUpdates{InstanceRepository: h.instances, conflicts: conflicts}
	h.svc.Instances = wrapped
	return h, wrapped, inst, tenantID
}

func TestInstanceService_Terminate_OneConcurrentChangeIsRetriedAndTheInstanceIsTerminated(t *testing.T) {
	h, repo, inst, tenantID := fxhTerminateWorld(1)

	require.NoError(t, h.svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "stuck"))

	assert.Equal(t, domain.InstanceStatusTerminated, h.instances.byID[inst.ID].Status)
	assert.Len(t, repo.updates, 2, "the transaction ran twice")
	for _, got := range repo.updates {
		assert.Equal(t, tenantID, got)
	}
	require.Len(t, h.outbox.attempts, 1, "the terminated event is written once")
	assert.Equal(t, domain.EventWorkflowInstanceTerminated, h.outbox.attempts[0].Envelope.Type)
}

// fxhRollbackTx puts the instance back as it was when a transaction callback fails, as a real rollback does.
type fxhRollbackTx struct {
	port.Transactor
	inst *domain.Instance
	runs int
}

func (t *fxhRollbackTx) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	t.runs++
	before := *t.inst
	err := t.Transactor.RunInTx(ctx, fn)
	if err != nil {
		*t.inst = before
	}
	return err
}

func TestInstanceService_Terminate_ATaskConflictInTheCascadeIsRetriedThenAnswersAVersionConflict(t *testing.T) {
	tenantID := uuid.New()
	inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 4)
	h, tasks, _ := w16aTerminateRig(inst)
	tasks.add(&domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusInProgress, RecordVersion: 3})
	tasks.updateErr = domain.ErrRecordVersionConflict
	tx := &fxhRollbackTx{Transactor: h.tx, inst: inst}
	h.svc.Transactor = tx

	err := h.svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "stuck")

	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.Equal(t, 3, tx.runs)
	assert.Len(t, tasks.lists, 3, "the tasks are read again on every attempt")
	assert.Equal(t, domain.InstanceStatusRunning, inst.Status)
	assert.Empty(t, h.outbox.attempts)
}

func TestInstanceService_Terminate_AConflictOnEveryAttemptAnswersAVersionConflictAndWritesNothing(t *testing.T) {
	h, repo, inst, tenantID := fxhTerminateWorld(100)

	err := h.svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "stuck")

	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.Len(t, repo.updates, 3, "three attempts, no more")
	assert.Equal(t, domain.InstanceStatusRunning, h.instances.byID[inst.ID].Status)
	assert.Empty(t, h.outbox.attempts, "no event was written")
}
