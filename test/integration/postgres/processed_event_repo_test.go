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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestProcessedEventRepo_RecordIfNew(t *testing.T) {
	superPool, _ := fixtures.NewTestPoolAndDSN(t)
	repo := postgres.NewProcessedEventRepo(superPool)
	ctx := context.Background()

	eventID := uuid.New()

	isNew, err := repo.RecordIfNew(ctx, eventID, "membership-execution", "workflow.task.created")
	require.NoError(t, err)
	assert.True(t, isNew, "first record should be new")

	isNew, err = repo.RecordIfNew(ctx, eventID, "membership-execution", "workflow.task.created")
	require.NoError(t, err)
	assert.False(t, isNew, "redelivery of the same (event_id, consumer) should not be new")

	isNew, err = repo.RecordIfNew(ctx, eventID, "user-execution", "workflow.task.created")
	require.NoError(t, err)
	assert.True(t, isNew, "same event_id under a different consumer dedups independently")
}

func TestProcessedEventRepo_IsProcessed(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewProcessedEventRepo(pool)
	ctx := context.Background()

	eventID := uuid.New()

	processed, err := repo.IsProcessed(ctx, eventID, "membership-execution")
	require.NoError(t, err)
	assert.False(t, processed)

	_, err = repo.RecordIfNew(ctx, eventID, "membership-execution", "delegation.started")
	require.NoError(t, err)

	processed, err = repo.IsProcessed(ctx, eventID, "membership-execution")
	require.NoError(t, err)
	assert.True(t, processed)
}

func TestProcessedEventRepo_PruneOlderThan(t *testing.T) {
	superPool, _ := fixtures.NewTestPoolAndDSN(t)
	repo := postgres.NewProcessedEventRepo(superPool)
	ctx := context.Background()

	oldID := uuid.New()
	freshID := uuid.New()
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx,
			`INSERT INTO processed_event (event_id, consumer, event_type, processed_at) VALUES ($1, 'membership-execution', 'x', $2)`,
			oldID, time.Now().UTC().Add(-8*24*time.Hour))
		return err
	}))
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx,
			`INSERT INTO processed_event (event_id, consumer, event_type, processed_at) VALUES ($1, 'membership-execution', 'x', now())`,
			freshID)
		return err
	}))

	deleted, err := repo.PruneOlderThan(ctx, 7*24*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	var freshStillThere bool
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM processed_event WHERE event_id = $1)", freshID).Scan(&freshStillThere)
	}))
	assert.True(t, freshStillThere, "a row inside the retention window must survive the prune")

	processed, err := repo.IsProcessed(ctx, oldID, "membership-execution")
	require.NoError(t, err)
	assert.False(t, processed, "pruned rows must no longer be considered processed")
}
