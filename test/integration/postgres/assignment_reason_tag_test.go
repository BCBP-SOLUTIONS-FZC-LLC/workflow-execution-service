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

func TestTaskAssignmentRepo_ListActiveByReasonTagFindsTheTagWhereverTheRowSits(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	tenantA, tenantB := uuid.New(), uuid.New()
	tag := "delegation:" + uuid.NewString()
	seed := func(tenant uuid.UUID, reason string, active bool) uuid.UUID {
		inst := newInstance(tenant, time.Now().UTC())
		require.NoError(t, instanceRepo.Create(ctx, inst))
		task := newTask(tenant, inst.ID)
		require.NoError(t, taskRepo.Create(ctx, task))
		a := newTaskAssignment(tenant, task.ID, uuid.New())
		a.Reason = reason
		require.NoError(t, assignmentRepo.Create(ctx, a))
		if !active {
			_, err := assignmentRepo.Vacate(ctx, tenant, a.ID)
			require.NoError(t, err)
		}
		return a.ID
	}
	first := seed(tenantA, tag+":"+uuid.NewString(), true)
	later := seed(tenantA, "delegation:"+uuid.NewString()+":"+uuid.NewString()+";"+tag+":"+uuid.NewString(), true)
	seed(tenantA, "delegation:"+uuid.NewString(), true)
	seed(tenantA, "", true)
	seed(tenantA, tag, false)
	seed(tenantB, tag, true)

	found, err := assignmentRepo.ListActiveByReasonTag(ctx, tenantA, tag)

	require.NoError(t, err)
	var ids []uuid.UUID
	for _, a := range found {
		ids = append(ids, a.ID)
	}
	assert.ElementsMatch(t, []uuid.UUID{first, later}, ids, "only the tenant's active rows that carry the tag, at any position in the chain")
}
