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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestTaskAssignmentRepo_CountActiveByTasks(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	tenantA, tenantB := uuid.New(), uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	two, ended, none := newTask(tenantA, inst.ID), newTask(tenantA, inst.ID), newTask(tenantA, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, two))
	require.NoError(t, taskRepo.Create(ctx, ended))
	require.NoError(t, taskRepo.Create(ctx, none))
	require.NoError(t, assignmentRepo.Create(ctx, newTaskAssignment(tenantA, two.ID, uuid.New())))
	require.NoError(t, assignmentRepo.Create(ctx, newTaskAssignment(tenantA, two.ID, uuid.New())))
	vacated := newTaskAssignment(tenantA, ended.ID, uuid.New())
	require.NoError(t, assignmentRepo.Create(ctx, vacated))
	_, err := assignmentRepo.Vacate(ctx, tenantA, vacated.ID)
	require.NoError(t, err)

	counts, err := assignmentRepo.CountActiveByTasks(ctx, tenantA, []uuid.UUID{two.ID, ended.ID, none.ID})
	require.NoError(t, err)
	assert.Equal(t, map[uuid.UUID]int{two.ID: 2}, counts, "only active assignments count; a task with none is absent")

	other, err := assignmentRepo.CountActiveByTasks(ctx, tenantB, []uuid.UUID{two.ID})
	require.NoError(t, err)
	assert.Empty(t, other, "another tenant sees no counts")

	// The owner role bypasses row-level security, so only the query's own tenant filter can hide the rows.
	unfiltered, err := postgres.NewTaskAssignmentRepo(superPool).CountActiveByTasks(ctx, tenantB, []uuid.UUID{two.ID})
	require.NoError(t, err)
	assert.Empty(t, unfiltered, "the query filters by tenant itself, beside row-level security")

	sameTenant, err := postgres.NewTaskAssignmentRepo(superPool).CountActiveByTasks(ctx, tenantA, []uuid.UUID{two.ID})
	require.NoError(t, err)
	assert.Equal(t, map[uuid.UUID]int{two.ID: 2}, sameTenant)

	empty, err := assignmentRepo.CountActiveByTasks(ctx, tenantA, nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}
