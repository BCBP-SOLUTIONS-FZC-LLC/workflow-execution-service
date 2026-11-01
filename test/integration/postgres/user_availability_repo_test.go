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

func TestUserAvailabilityRepo_RecordsAndReadsAStatus(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	repo := postgres.NewUserAvailabilityRepo(newAppRolePool(t, superPool, superDSN))
	ctx := context.Background()
	tenantID, userID := uuid.New(), uuid.New()

	_, found, err := repo.Get(ctx, tenantID, userID)
	require.NoError(t, err)
	assert.False(t, found, "a user with no row has no recorded status")

	written, err := repo.Record(ctx, tenantID, userID, "ooo", time.Now().UTC())
	require.NoError(t, err)
	assert.True(t, written)
	status, found, err := repo.Get(ctx, tenantID, userID)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "ooo", status)
}

func TestUserAvailabilityRepo_AnOlderChangeNeverOverwritesANewerOne(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	repo := postgres.NewUserAvailabilityRepo(newAppRolePool(t, superPool, superDSN))
	ctx := context.Background()
	tenantID, userID := uuid.New(), uuid.New()
	now := time.Now().UTC()

	written, err := repo.Record(ctx, tenantID, userID, "available", now)
	require.NoError(t, err)
	require.True(t, written)

	written, err = repo.Record(ctx, tenantID, userID, "ooo", now.Add(-time.Hour))
	require.NoError(t, err)
	assert.False(t, written, "the caller must learn that an older change was not stored")
	status, _, err := repo.Get(ctx, tenantID, userID)
	require.NoError(t, err)
	assert.Equal(t, "available", status)

	written, err = repo.Record(ctx, tenantID, userID, "busy", now.Add(time.Minute))
	require.NoError(t, err)
	assert.True(t, written)
	status, _, err = repo.Get(ctx, tenantID, userID)
	require.NoError(t, err)
	assert.Equal(t, "busy", status)
}

func TestUserAvailabilityRepo_AUnknownStatusIsRefusedByTheDatabase(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	repo := postgres.NewUserAvailabilityRepo(newAppRolePool(t, superPool, superDSN))

	_, err := repo.Record(context.Background(), uuid.New(), uuid.New(), "retired", time.Now().UTC())

	require.Error(t, err)
}

func TestUserAvailabilityRepo_KeysTheRowByTenantAndUser(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	repo := postgres.NewUserAvailabilityRepo(newAppRolePool(t, superPool, superDSN))
	ctx := context.Background()
	tenantA, tenantB, userID := uuid.New(), uuid.New(), uuid.New()
	_, err := repo.Record(ctx, tenantA, userID, "ooo", time.Now().UTC())
	require.NoError(t, err)

	_, found, err := repo.Get(ctx, tenantB, userID)

	require.NoError(t, err)
	assert.False(t, found, "the same user id under another tenant is a different row")
}
