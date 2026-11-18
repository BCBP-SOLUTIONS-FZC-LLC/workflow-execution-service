//go:build integration

package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type w14bIdleOutboxPruner struct{}

func (w14bIdleOutboxPruner) PrunePublished(context.Context, time.Duration, int) (int64, error) {
	return 0, nil
}

type w14bSweepLog struct {
	mu      sync.Mutex
	deleted map[string]int64
}

func (l *w14bSweepLog) record(table string, deleted int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.deleted[table] += deleted
}

type w14bNoLogger struct{}

func (w14bNoLogger) Debug(string, map[string]any) {}
func (w14bNoLogger) Info(string, map[string]any)  {}
func (w14bNoLogger) Warn(string, map[string]any)  {}
func (w14bNoLogger) Error(string, map[string]any) {}

func w14bSeedProcessed(t *testing.T, pool *pgcommon.Pool, consumer string, age time.Duration) uuid.UUID {
	t.Helper()
	id := uuid.New()
	require.NoError(t, pool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, `INSERT INTO processed_events (event_id, consumer, processed_at) VALUES ($1, $2, $3)`,
			id, consumer, time.Now().UTC().Add(-age))
		return err
	}))
	return id
}

func TestRetentionSweeper_ProcessedEventsOlderThanTheWindowAreForgottenInBatchSizedStatements(t *testing.T) {
	pool, _ := fixtures.NewTestPoolAndDSN(t)
	ctx := context.Background()
	ledger, err := eventbus.NewInbox(pool, port.ConsumerMembership)
	require.NoError(t, err)
	exec := func(sql string) {
		require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx, sql)
			return err
		}))
	}
	exec(`CREATE TABLE w14b_delete_statements (at timestamptz NOT NULL DEFAULT now())`)
	exec(`CREATE FUNCTION w14b_count_delete() RETURNS trigger LANGUAGE plpgsql AS
		$$ BEGIN INSERT INTO w14b_delete_statements DEFAULT VALUES; RETURN NULL; END $$`)
	exec(`CREATE TRIGGER w14b_count_delete AFTER DELETE ON processed_events
		FOR EACH STATEMENT EXECUTE FUNCTION w14b_count_delete()`)

	const day = 24 * time.Hour
	var expired, kept []uuid.UUID
	for range 3 {
		expired = append(expired, w14bSeedProcessed(t, pool, port.ConsumerMembership, 17*day))
	}
	for range 2 {
		kept = append(kept, w14bSeedProcessed(t, pool, port.ConsumerMembership, 15*day))
	}
	kept = append(kept, w14bSeedProcessed(t, pool, port.ConsumerMembership, time.Hour))

	log := &w14bSweepLog{deleted: map[string]int64{}}
	sweeper := service.NewRetentionSweeper(service.RetentionSweeperDeps{
		Outbox: w14bIdleOutboxPruner{}, ProcessedEvents: ledger,
		OutboxRetention: time.Hour, ProcessedEventRetention: 16 * day,
		Batch: 2, Interval: time.Hour, Record: log.record, Logger: w14bNoLogger{},
	})

	require.NoError(t, sweeper.Tick(ctx))

	assert.EqualValues(t, 3, log.deleted["processed_events"], "the rows past the window are the ones deleted")
	var statements int
	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, `SELECT count(*) FROM w14b_delete_statements`).Scan(&statements)
	}))
	assert.Equal(t, 2, statements, "3 rows at 2 per statement are deleted by 2 statements")
	for _, id := range expired {
		seen, err := ledger.IsProcessed(ctx, id, port.ConsumerMembership)
		require.NoError(t, err)
		assert.False(t, seen, "an event redelivered after the window is not found, so it would be processed again")
	}
	for _, id := range kept {
		seen, err := ledger.IsProcessed(ctx, id, port.ConsumerMembership)
		require.NoError(t, err)
		assert.True(t, seen, "an event inside the window, even one older than the queue's own 14 days, is still found")
	}
}
