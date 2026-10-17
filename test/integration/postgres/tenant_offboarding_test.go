//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type recordingTemporal struct {
	port.TemporalClient
	terminated []string
	failWith   error
}

func (r *recordingTemporal) TerminateWorkflow(_ context.Context, id, _ string) error {
	if r.failWith != nil {
		return r.failWith
	}
	r.terminated = append(r.terminated, id)
	return nil
}

func TestTenantOffboarding_FailsEveryOpenTaskOfALargeInstance(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	temporal := &recordingTemporal{}
	reconciler := &service.TenantLifecycleReconciler{
		Instances:   instanceRepo,
		Tasks:       taskRepo,
		Assignments: postgres.NewTaskAssignmentRepo(appPool),
		Queues:      postgres.NewActiveTaskQueueRepo(appPool),
		Outbox:      postgres.NewOutboxRepo(appPool),
		Transactor:  postgres.NewTransactor(appPool),
		Temporal:    temporal,
		Validator:   validator,
	}
	ctx := context.Background()

	const taskCount = 1000
	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	for i := 0; i < taskCount; i++ {
		require.NoError(t, taskRepo.Create(ctx, newTask(tenantID, inst.ID)))
	}

	start := time.Now()
	require.NoError(t, reconciler.Apply(ctx, port.TenantLifecycleInput{TenantID: tenantID, Status: "offboarded", PreviousStatus: "active"}))
	t.Logf("offboarding an instance with %d open tasks took %s", taskCount, time.Since(start))

	got, err := instanceRepo.GetByID(ctx, tenantID, inst.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, got.Status)
	assert.Contains(t, temporal.terminated, inst.TemporalWorkflowID)

	var cursor *port.Cursor
	failed := 0
	for {
		page, next, err := taskRepo.ListByInstance(ctx, tenantID, inst.ID, port.PageRequest{After: cursor, Limit: 100})
		require.NoError(t, err)
		for _, task := range page {
			if task.Status == domain.TaskStatusFailed {
				failed++
			}
		}
		if next == nil {
			break
		}
		cursor = next
	}
	assert.Equal(t, taskCount, failed)
}

func TestTenantOffboarding_AFailedExecutionCloseLeavesTheInstanceForRedelivery(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	temporal := &recordingTemporal{failWith: assert.AnError}
	reconciler := &service.TenantLifecycleReconciler{
		Instances:   instanceRepo,
		Tasks:       taskRepo,
		Assignments: postgres.NewTaskAssignmentRepo(appPool),
		Queues:      postgres.NewActiveTaskQueueRepo(appPool),
		Outbox:      postgres.NewOutboxRepo(appPool),
		Transactor:  postgres.NewTransactor(appPool),
		Temporal:    temporal,
		Validator:   validator,
	}
	ctx := context.Background()

	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	task := newTask(tenantID, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, task))
	input := port.TenantLifecycleInput{TenantID: tenantID, Status: "offboarded", PreviousStatus: "active"}

	require.ErrorIs(t, reconciler.Apply(ctx, input), assert.AnError)
	got, err := instanceRepo.GetByID(ctx, tenantID, inst.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusRunning, got.Status)
	openTask, err := taskRepo.GetByID(ctx, tenantID, task.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusReady, openTask.Status)

	temporal.failWith = nil
	require.NoError(t, reconciler.Apply(ctx, input))
	got, err = instanceRepo.GetByID(ctx, tenantID, inst.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, got.Status)
	assert.Contains(t, temporal.terminated, inst.TemporalWorkflowID)
}
