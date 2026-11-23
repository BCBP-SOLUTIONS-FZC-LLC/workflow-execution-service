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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestInstanceRepo_ListByTenant_AFormerAssigneeWithNoDepartmentNoLongerSeesTheInstance(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()
	tenantID, userID := uuid.New(), uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instances.Create(ctx, inst))
	task := newTask(tenantID, inst.ID)
	require.NoError(t, postgres.NewTaskRepo(appPool).Create(ctx, task))
	assignment := newTaskAssignment(tenantID, task.ID, userID)
	require.NoError(t, assignments.Create(ctx, assignment))
	filter := port.InstanceListFilter{InstanceID: &inst.ID, Scope: &port.ScopeFilter{CallerUserID: userID}}

	seen, _, err := instances.ListByTenant(ctx, tenantID, filter, port.PageRequest{Limit: 1})
	require.NoError(t, err)
	require.Len(t, seen, 1, "an assignee with an active assignment sees the instance")

	_, err = assignments.Vacate(ctx, tenantID, assignment.ID)
	require.NoError(t, err)
	seen, _, err = instances.ListByTenant(ctx, tenantID, filter, port.PageRequest{Limit: 1})

	require.NoError(t, err)
	assert.Empty(t, seen, "once the assignment is vacated the former assignee has no read access")
}
