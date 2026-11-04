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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestInbox_RedeliveredEventIsSeen(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	ledger, err := eventbus.NewInbox(pool, port.ConsumerMembership, port.ConsumerUser)
	require.NoError(t, err)
	ctx := context.Background()
	eventID := uuid.New()

	processed, err := ledger.IsProcessed(ctx, eventID, port.ConsumerMembership)
	require.NoError(t, err)
	assert.False(t, processed)

	require.NoError(t, ledger.MarkProcessed(ctx, eventID, port.ConsumerMembership))
	require.NoError(t, ledger.MarkProcessed(ctx, eventID, port.ConsumerMembership), "recording twice is a no-op")

	processed, err = ledger.IsProcessed(ctx, eventID, port.ConsumerMembership)
	require.NoError(t, err)
	assert.True(t, processed, "a redelivery of the same (event_id, consumer) is skipped")

	processed, err = ledger.IsProcessed(ctx, eventID, port.ConsumerUser)
	require.NoError(t, err)
	assert.False(t, processed, "the same event_id under a different consumer dedups independently")
}

func TestInbox_UnknownConsumerIsRefused(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	ledger, err := eventbus.NewInbox(pool, port.ConsumerMembership)
	require.NoError(t, err)

	_, err = ledger.IsProcessed(context.Background(), uuid.New(), "nobody")
	require.Error(t, err)
	require.Error(t, ledger.MarkProcessed(context.Background(), uuid.New(), "nobody"))
}

func TestInbox_PruneDropsRowsOutsideTheRetentionWindow(t *testing.T) {
	superPool, _ := fixtures.NewTestPoolAndDSN(t)
	ledger, err := eventbus.NewInbox(superPool, port.ConsumerMembership, port.ConsumerUser)
	require.NoError(t, err)
	ctx := context.Background()

	oldID, freshID := uuid.New(), uuid.New()
	insert := func(id uuid.UUID, consumer string, at time.Time) {
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx,
				`INSERT INTO processed_events (event_id, consumer, processed_at) VALUES ($1, $2, $3)`, id, consumer, at)
			return err
		}))
	}
	insert(oldID, port.ConsumerMembership, time.Now().UTC().Add(-8*24*time.Hour))
	insert(uuid.New(), port.ConsumerUser, time.Now().UTC().Add(-9*24*time.Hour))
	insert(freshID, port.ConsumerMembership, time.Now().UTC())

	deleted, err := ledger.Prune(ctx, 7*24*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted, "every consumer's old rows go")

	processed, err := ledger.IsProcessed(ctx, freshID, port.ConsumerMembership)
	require.NoError(t, err)
	assert.True(t, processed, "a row inside the window survives")
	processed, err = ledger.IsProcessed(ctx, oldID, port.ConsumerMembership)
	require.NoError(t, err)
	assert.False(t, processed, "a pruned row is no longer processed")
}

func TestInbox_AppRoleWithNoTenantRecordsAndPrunes(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ledger, err := eventbus.NewInbox(appPool, port.ConsumerMembership)
	require.NoError(t, err)
	ctx := context.Background()

	id := uuid.New()
	require.NoError(t, ledger.MarkProcessed(ctx, id, port.ConsumerMembership))
	processed, err := ledger.IsProcessed(ctx, id, port.ConsumerMembership)
	require.NoError(t, err)
	assert.True(t, processed, "the table has no row-level security, so the app role needs no tenant")

	deleted, err := ledger.Prune(ctx, time.Hour)
	require.NoError(t, err)
	assert.Zero(t, deleted)
}
