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

func TestProcessedEventRepo_RecordIfNew_FirstThenDuplicate(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewProcessedEventRepo(pool)
	ctx := context.Background()

	eventID := uuid.New()

	isNew, err := repo.RecordIfNew(ctx, eventID, "membership-execution", "DelegationStarted")
	require.NoError(t, err)
	assert.True(t, isNew)

	isNew, err = repo.RecordIfNew(ctx, eventID, "membership-execution", "DelegationStarted")
	require.NoError(t, err)
	assert.False(t, isNew, "a duplicate (event_id, consumer) pair must not be recorded as new")
}

func TestProcessedEventRepo_RecordIfNew_SameEventDifferentConsumer_BothNew(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewProcessedEventRepo(pool)
	ctx := context.Background()

	eventID := uuid.New()

	isNew, err := repo.RecordIfNew(ctx, eventID, "membership-execution", "DelegationStarted")
	require.NoError(t, err)
	assert.True(t, isNew)

	isNew, err = repo.RecordIfNew(ctx, eventID, "user-execution", "DelegationStarted")
	require.NoError(t, err)
	assert.True(t, isNew, "the same event id dedupes independently per consumer string (LLD §6.3)")
}

func TestProcessedEventRepo_IsProcessed(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewProcessedEventRepo(pool)
	ctx := context.Background()

	eventID := uuid.New()

	processed, err := repo.IsProcessed(ctx, eventID, "membership-execution")
	require.NoError(t, err)
	assert.False(t, processed)

	_, err = repo.RecordIfNew(ctx, eventID, "membership-execution", "DelegationStarted")
	require.NoError(t, err)

	processed, err = repo.IsProcessed(ctx, eventID, "membership-execution")
	require.NoError(t, err)
	assert.True(t, processed)
}

func TestProcessedEventRepo_PruneOlderThan(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewProcessedEventRepo(pool)
	ctx := context.Background()

	eventID := uuid.New()
	_, err := repo.RecordIfNew(ctx, eventID, "membership-execution", "DelegationStarted")
	require.NoError(t, err)

	deleted, err := repo.PruneOlderThan(ctx, -time.Hour) // cutoff in the future relative to processed_at
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	processed, err := repo.IsProcessed(ctx, eventID, "membership-execution")
	require.NoError(t, err)
	assert.False(t, processed, "pruned rows must no longer be considered processed")
}
