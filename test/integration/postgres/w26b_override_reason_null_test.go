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

func TestAssigneeOverrideRepo_ReasonIsStoredAsNullWhenNoneWasGiven(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	overrideRepo := postgres.NewAssigneeOverrideRepo(appPool)
	ctx := context.Background()

	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	withoutReason := &domain.AssigneeOverride{
		ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: "finance/review",
		NewUserID: uuid.New(), ActorUserID: uuid.New(),
	}
	withReason := &domain.AssigneeOverride{
		ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: "finance/review",
		NewUserID: uuid.New(), ActorUserID: uuid.New(), Reason: "covering leave",
	}
	require.NoError(t, overrideRepo.Create(ctx, withoutReason))
	require.NoError(t, overrideRepo.Create(ctx, withReason))

	assert.Empty(t, withoutReason.Reason, "the row read back after the insert carries no reason")
	assert.Equal(t, "covering leave", withReason.Reason)
	listed, err := overrideRepo.ListByInstance(ctx, tenantID, inst.ID)
	require.NoError(t, err)
	byID := map[uuid.UUID]*domain.AssigneeOverride{}
	for _, o := range listed {
		byID[o.ID] = o
	}
	require.Len(t, byID, 2)
	assert.Empty(t, byID[withoutReason.ID].Reason)
	assert.Equal(t, "covering leave", byID[withReason.ID].Reason)

	var emptyStoredAsNull, givenStoredAsText bool
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		if err := conn.QueryRow(ctx,
			`SELECT reason IS NULL FROM assignee_overrides WHERE id = $1`, withoutReason.ID).Scan(&emptyStoredAsNull); err != nil {
			return err
		}
		return conn.QueryRow(ctx,
			`SELECT reason = 'covering leave' FROM assignee_overrides WHERE id = $1`, withReason.ID).Scan(&givenStoredAsText)
	}))
	assert.True(t, emptyStoredAsNull, "the column holds NULL, not an empty string")
	assert.True(t, givenStoredAsText, "a given reason is stored as text")
}
