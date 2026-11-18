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

// Offboarding one tenant through the real reconciler and the row-level-security role terminates that tenant's
// running instances and leaves another tenant's running instances running.
func TestTenantOffboarding_OneTenant_LeavesAnotherTenantsRunningInstancesRunning(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	temporal := &recordingTemporal{}
	reconciler := &service.TenantLifecycleReconciler{
		Instances:   instances,
		Tasks:       postgres.NewTaskRepo(appPool),
		Assignments: postgres.NewTaskAssignmentRepo(appPool),
		Queues:      postgres.NewActiveTaskQueueRepo(appPool),
		Outbox:      postgres.NewOutboxRepo(appPool),
		Transactor:  postgres.NewTransactor(appPool),
		Temporal:    temporal,
		Validator:   validator,
	}
	ctx := context.Background()

	tenantA, tenantB := uuid.New(), uuid.New()
	seed := func(tenantID uuid.UUID) *domain.Instance {
		inst := newInstance(tenantID, time.Now().UTC())
		require.NoError(t, instances.Create(ctx, inst))
		return inst
	}
	aFirst, aSecond := seed(tenantA), seed(tenantA)
	bFirst, bSecond := seed(tenantB), seed(tenantB)

	require.NoError(t, reconciler.Apply(ctx, port.TenantLifecycleInput{TenantID: tenantA, Status: "offboarded", PreviousStatus: "active"}))

	for _, inst := range []*domain.Instance{aFirst, aSecond} {
		got, err := instances.GetByID(ctx, tenantA, inst.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusTerminated, got.Status, "the offboarded tenant's instance %s is terminated", inst.ID)
	}
	for _, inst := range []*domain.Instance{bFirst, bSecond} {
		got, err := instances.GetByID(ctx, tenantB, inst.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusRunning, got.Status, "the other tenant's instance %s keeps running", inst.ID)
	}
	for _, inst := range []*domain.Instance{aFirst, aSecond} {
		assert.Contains(t, temporal.terminated, inst.TemporalWorkflowID, "the offboarded tenant's workflow is closed")
	}
	for _, inst := range []*domain.Instance{bFirst, bSecond} {
		assert.NotContains(t, temporal.terminated, inst.TemporalWorkflowID, "the other tenant's workflow is not closed")
	}

	running, _, err := instances.ListByTenant(ctx, tenantB, port.InstanceListFilter{Statuses: []domain.InstanceStatus{domain.InstanceStatusRunning}}, port.PageRequest{Limit: 100})
	require.NoError(t, err)
	gotRunning := make([]uuid.UUID, 0, len(running))
	for _, inst := range running {
		gotRunning = append(gotRunning, inst.ID)
	}
	assert.ElementsMatch(t, []uuid.UUID{bFirst.ID, bSecond.ID}, gotRunning, "the other tenant still lists both instances as running")

	assert.Equal(t, 0, countWorkflowInstance(t, appPool, withGUC(ctx, tenantA), bFirst.ID), "a connection bound to the offboarded tenant cannot see the other tenant's instance")
	assert.Equal(t, 1, countWorkflowInstance(t, appPool, withGUC(ctx, tenantB), bFirst.ID), "the other tenant's own connection still sees it")
}
