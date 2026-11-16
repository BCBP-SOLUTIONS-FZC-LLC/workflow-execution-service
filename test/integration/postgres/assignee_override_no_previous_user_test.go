//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestAssigneeOverrideRepo_PreviousUserIsStoredAsNullWhenNobodyWasReplaced(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	overrideRepo := postgres.NewAssigneeOverrideRepo(appPool)
	ctx := context.Background()

	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	replaced := uuid.New()
	ownerless := &domain.AssigneeOverride{
		ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: "finance/review",
		NewUserID: uuid.New(), ActorUserID: uuid.New(),
	}
	replacing := &domain.AssigneeOverride{
		ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: "finance/review",
		PreviousUserID: &replaced, NewUserID: uuid.New(), ActorUserID: uuid.New(),
	}
	require.NoError(t, overrideRepo.Create(ctx, ownerless))
	require.NoError(t, overrideRepo.Create(ctx, replacing))

	listed, err := overrideRepo.ListByInstance(ctx, tenantID, inst.ID)

	require.NoError(t, err)
	require.Len(t, listed, 2)
	byID := map[uuid.UUID]*domain.AssigneeOverride{}
	for _, o := range listed {
		byID[o.ID] = o
	}
	assert.Nil(t, byID[ownerless.ID].PreviousUserID)
	require.NotNil(t, byID[replacing.ID].PreviousUserID)
	assert.Equal(t, replaced, *byID[replacing.ID].PreviousUserID)

	var storedNull bool
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			`SELECT previous_user_id IS NULL FROM assignee_overrides WHERE id = $1`, ownerless.ID).Scan(&storedNull)
	}))
	assert.True(t, storedNull, "the column holds NULL, not the all-zero id")
}
