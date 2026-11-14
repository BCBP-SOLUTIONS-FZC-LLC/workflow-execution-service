//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestTaskList_ClosedPool_ReturnsTheFailureWithNoItems(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	svc := &service.TaskService{Tasks: taskRepo}
	ctx := context.Background()

	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	for i := 0; i < 3; i++ {
		require.NoError(t, taskRepo.Create(ctx, newTask(tenantID, inst.ID)))
	}
	admin := port.ReadScope{CallerUserID: uuid.New(), IsAdmin: true}

	healthy, err := svc.List(ctx, tenantID, admin, port.TaskFilter{}, port.Page{})
	require.NoError(t, err)
	require.Len(t, healthy.Items, 3, "the tenant's tasks are listed while the pool is open")

	appPool.Close()

	t.Run("the repository returns the failure and no tasks or cursor", func(t *testing.T) {
		tasks, next, err := taskRepo.ListByTenant(ctx, tenantID, port.TaskListFilter{}, port.PageRequest{})

		require.Error(t, err)
		assert.NotErrorIs(t, err, domain.ErrNotFound)
		assert.NotErrorIs(t, err, domain.ErrRecordVersionConflict)
		assert.Empty(t, tasks)
		assert.Nil(t, next)
	})

	t.Run("the service passes the failure up unmapped with an empty result", func(t *testing.T) {
		result, err := svc.List(ctx, tenantID, admin, port.TaskFilter{}, port.Page{})

		require.Error(t, err)
		for _, sentinel := range []error{
			port.ErrTaskNotFound, port.ErrRecordVersionConflict, port.ErrNotAuthorizedForRead, port.ErrInvalidTaskState,
		} {
			assert.NotErrorIs(t, err, sentinel, "an outage must not look like a client error")
		}
		assert.Empty(t, result.Items)
		assert.Empty(t, result.NextCursor)
	})
}
