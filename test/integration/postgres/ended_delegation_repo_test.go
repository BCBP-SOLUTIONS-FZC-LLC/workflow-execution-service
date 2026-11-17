//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestEndedDelegationRepo_RemembersAnEndPerTenant(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewEndedDelegationRepo(appPool)
	ctx := context.Background()
	tenantA, tenantB, delegationID := uuid.New(), uuid.New(), uuid.New()

	ended, err := repo.IsEnded(ctx, tenantA, delegationID)
	require.NoError(t, err)
	assert.False(t, ended, "a delegation nobody ended is not ended")

	require.NoError(t, repo.MarkEnded(ctx, tenantA, delegationID))
	require.NoError(t, repo.MarkEnded(ctx, tenantA, delegationID), "an end delivered twice is recorded once")

	ended, err = repo.IsEnded(ctx, tenantA, delegationID)
	require.NoError(t, err)
	assert.True(t, ended)
	ended, err = repo.IsEnded(ctx, tenantB, delegationID)
	require.NoError(t, err)
	assert.False(t, ended, "another tenant's delegation with the same id is not ended")
	other, err := repo.IsEnded(ctx, tenantA, uuid.New())
	require.NoError(t, err)
	assert.False(t, other)
}

func TestEndedDelegationTable_ForcesRowLevelSecurityAndIsolatesTenants(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ctx := context.Background()

	enabled, forced := rlsFlags(t, superPool, ctx, "ended_delegation")
	assert.True(t, enabled)
	assert.True(t, forced)

	tenantA, tenantB := uuid.New(), uuid.New()
	repo := postgres.NewEndedDelegationRepo(appPool)
	delegationID := uuid.New()
	require.NoError(t, repo.MarkEnded(ctx, tenantA, delegationID))

	var visibleToB int
	require.NoError(t, appPool.WithConn(withGUC(ctx, tenantB), func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, "SELECT count(*) FROM ended_delegation WHERE delegation_id = $1", delegationID).Scan(&visibleToB)
	}))
	assert.Zero(t, visibleToB, "a connection scoped to another tenant sees nothing")

	var visibleWithoutTenant int
	require.NoError(t, appPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, "SELECT count(*) FROM ended_delegation").Scan(&visibleWithoutTenant)
	}))
	assert.Zero(t, visibleWithoutTenant, "a connection with no tenant sees nothing")
}
