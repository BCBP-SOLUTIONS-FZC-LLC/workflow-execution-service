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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestTaskRepo_GetByInstanceAndNode_SQLMetacharactersMatchNothing(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	ctx := context.Background()

	tenant := uuid.New()
	inst := newInstance(tenant, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	task := newTask(tenant, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, task))

	hostileKeys := []struct {
		name string
		key  string
	}{
		{name: "always-true predicate", key: `' OR 1=1 --`},
		{name: "quote breakout of the real key", key: `finance/review' OR '1'='1`},
		{name: "statement stacking", key: `x'; DROP TABLE workflow_task; --`},
		{name: "LIKE wildcard percent", key: `finance/%`},
		{name: "LIKE wildcard underscore", key: `finance/_eview`},
	}
	for _, tc := range hostileKeys {
		t.Run(tc.name, func(t *testing.T) {
			got, err := taskRepo.GetByInstanceAndNode(ctx, tenant, inst.ID, tc.key)

			assert.ErrorIs(t, err, domain.ErrNotFound)
			assert.Nil(t, got)
		})
	}

	t.Run("the stored task is untouched and still found by its real key", func(t *testing.T) {
		got, err := taskRepo.GetByInstanceAndNode(ctx, tenant, inst.ID, task.NodeKey)
		require.NoError(t, err)
		assert.Equal(t, task.ID, got.ID)
		assert.Equal(t, domain.TaskStatusReady, got.Status)
		assert.Equal(t, task.RecordVersion, got.RecordVersion)
	})
}
