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

	eventsmock "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events/mock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

const (
	w8aOutboxRetention = 7 * 24 * time.Hour
	w8aOutboxBatch     = 2
)

// w8aInsertOutboxEvent stores an event; publishedAgo nil leaves it unpublished.
func w8aInsertOutboxEvent(t *testing.T, pool *pgcommon.Pool, tenantID uuid.UUID, createdAgo time.Duration, publishedAgo *time.Duration) uuid.UUID {
	t.Helper()
	id := uuid.New()
	now := time.Now().UTC()
	var publishedAt *time.Time
	if publishedAgo != nil {
		at := now.Add(-*publishedAgo)
		publishedAt = &at
	}
	require.NoError(t, pool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx,
			`INSERT INTO outbox_events (id, event_type, payload, tenant_id, created_at, scheduled_at, published_at) VALUES ($1, 'SLAWarning', '{}', $2, $3, $3, $4)`,
			id, tenantID.String(), now.Add(-createdAgo), publishedAt)
		return err
	}))
	return id
}

func w8aOutboxIDs(t *testing.T, pool *pgcommon.Pool) map[uuid.UUID]bool {
	t.Helper()
	ids := map[uuid.UUID]bool{}
	require.NoError(t, pool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		rows, err := conn.Query(ctx, `SELECT id FROM outbox_events`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids[id] = true
		}
		return rows.Err()
	}))
	return ids
}

func TestRetentionSweeper_SweepsPublishedOutboxEventsPastRetentionAndNothingElse(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	relayPool := newOutboxRelayRolePool(t, superPool, superDSN)
	runner, err := outbox.NewRunner(outbox.Config{
		Pool: relayPool, Publisher: &eventsmock.Publisher{}, PollInterval: time.Minute, BatchSize: 50, Logger: testLogger{t},
	})
	require.NoError(t, err)
	inbox, err := eventbus.NewInbox(superPool, port.ConsumerMembership)
	require.NoError(t, err)
	recorded := map[string]int64{}
	sweeper := service.NewRetentionSweeper(service.RetentionSweeperDeps{
		Outbox: runner, ProcessedEvents: inbox,
		OutboxRetention: w8aOutboxRetention, ProcessedEventRetention: w8aOutboxRetention,
		Batch: w8aOutboxBatch, Interval: time.Hour, Logger: testLogger{t},
		Record: func(table string, deleted int64) { recorded[table] += deleted },
	})

	tenantA, tenantB := uuid.New(), uuid.New()
	day := 24 * time.Hour
	publishedTenDaysAgo, publishedOneDayAgo := 10*day, day
	var expiredIDs []uuid.UUID
	for range 3 {
		expiredIDs = append(expiredIDs, w8aInsertOutboxEvent(t, superPool, tenantA, 11*day, &publishedTenDaysAgo))
	}
	expiredIDs = append(expiredIDs, w8aInsertOutboxEvent(t, superPool, tenantB, 11*day, &publishedTenDaysAgo))
	insideWindow := w8aInsertOutboxEvent(t, superPool, tenantA, 2*day, &publishedOneDayAgo)
	createdLongAgoPublishedRecently := w8aInsertOutboxEvent(t, superPool, tenantA, 400*day, &publishedOneDayAgo)
	unpublishedOld := w8aInsertOutboxEvent(t, superPool, tenantA, 400*day, nil)
	unpublishedOtherTenant := w8aInsertOutboxEvent(t, superPool, tenantB, 30*day, nil)

	t.Run("the same sweep on the app role deletes nothing of another tenant", func(t *testing.T) {
		appPool := newAppRolePool(t, superPool, superDSN)
		appRunner, err := outbox.NewRunner(outbox.Config{
			Pool: appPool, Publisher: &eventsmock.Publisher{}, PollInterval: time.Minute, BatchSize: 50, Logger: testLogger{t},
		})
		require.NoError(t, err)
		appRecorded := map[string]int64{}
		appSweeper := service.NewRetentionSweeper(service.RetentionSweeperDeps{
			Outbox: appRunner, ProcessedEvents: inbox,
			OutboxRetention: w8aOutboxRetention, ProcessedEventRetention: w8aOutboxRetention,
			Batch: w8aOutboxBatch, Interval: time.Hour, Logger: testLogger{t},
			Record: func(table string, deleted int64) { appRecorded[table] += deleted },
		})
		before := w8aOutboxIDs(t, superPool)

		require.NoError(t, appSweeper.Tick(context.Background()), "no tenant bound")
		require.NoError(t, appSweeper.Tick(withGUC(context.Background(), uuid.New())), "a tenant that owns none of the rows")

		assert.Equal(t, before, w8aOutboxIDs(t, superPool), "row-level security hides every other tenant's rows from the delete")
		assert.Zero(t, appRecorded["outbox_events"])
	})

	require.NoError(t, sweeper.Tick(context.Background()))

	left := w8aOutboxIDs(t, superPool)
	for _, id := range expiredIDs {
		assert.False(t, left[id], "a published event past retention is deleted, in every tenant")
	}
	assert.True(t, left[insideWindow], "a published event inside the retention window is kept")
	assert.True(t, left[createdLongAgoPublishedRecently], "retention counts from publication, not from creation")
	assert.True(t, left[unpublishedOld], "an unpublished event is never deleted, however old")
	assert.True(t, left[unpublishedOtherTenant], "an unpublished event of another tenant is never deleted")
	assert.Len(t, left, 4)
	assert.EqualValues(t, len(expiredIDs), recorded["outbox_events"], "the sweep counts what it deleted across its batches")

	require.NoError(t, sweeper.Tick(context.Background()), "a second sweep finds nothing more to do")
	assert.Equal(t, left, w8aOutboxIDs(t, superPool))
	assert.EqualValues(t, len(expiredIDs), recorded["outbox_events"])
}
