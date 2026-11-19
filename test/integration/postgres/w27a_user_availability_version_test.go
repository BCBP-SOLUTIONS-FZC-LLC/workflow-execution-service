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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func w27aAvailabilityRow(t *testing.T, superPool *pgcommon.Pool, tenantID, userID uuid.UUID) (status string, version int64) {
	t.Helper()
	require.NoError(t, superPool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			`SELECT status::text, record_version FROM user_availability WHERE tenant_id = $1 AND user_id = $2`,
			tenantID, userID).Scan(&status, &version)
	}))
	return status, version
}

func TestUserAvailabilityRepo_OutOfOfficeThenAvailable_AppliesBothInOrderAndCountsEachWrite(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	repo := postgres.NewUserAvailabilityRepo(newAppRolePool(t, superPool, superDSN))
	ctx := context.Background()
	tenant, otherTenant, user := uuid.New(), uuid.New(), uuid.New()
	t0 := time.Now().UTC().Truncate(time.Microsecond)

	written, err := repo.Record(ctx, tenant, user, "ooo", t0)
	require.NoError(t, err)
	require.True(t, written)
	status, version := w27aAvailabilityRow(t, superPool, tenant, user)
	assert.Equal(t, "ooo", status)
	assert.EqualValues(t, 1, version, "the first write creates the row at version 1")

	written, err = repo.Record(ctx, tenant, user, "available", t0.Add(time.Second))
	require.NoError(t, err)
	require.True(t, written)
	status, version = w27aAvailabilityRow(t, superPool, tenant, user)
	assert.Equal(t, "available", status, "the later change replaced the earlier one")
	assert.EqualValues(t, 2, version, "each applied change adds one to the version")

	written, err = repo.Record(ctx, tenant, user, "ooo", t0)
	require.NoError(t, err)
	assert.False(t, written, "replaying the earlier change is refused")
	status, version = w27aAvailabilityRow(t, superPool, tenant, user)
	assert.Equal(t, "available", status)
	assert.EqualValues(t, 2, version, "a refused change leaves the version alone")

	written, err = repo.Record(ctx, otherTenant, user, "busy", t0.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, written)
	_, otherVersion := w27aAvailabilityRow(t, superPool, otherTenant, user)
	assert.EqualValues(t, 1, otherVersion, "the same user id under another tenant counts separately")
	status, version = w27aAvailabilityRow(t, superPool, tenant, user)
	assert.Equal(t, "available", status)
	assert.EqualValues(t, 2, version)
}
