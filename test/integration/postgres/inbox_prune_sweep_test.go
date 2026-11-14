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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func strfixExec(t *testing.T, pool *pgcommon.Pool, sql string, args ...any) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, sql, args...)
		return err
	}))
}

func strfixInsertOld(t *testing.T, pool *pgcommon.Pool, consumer string, n int) {
	t.Helper()
	for range n {
		strfixExec(t, pool, `INSERT INTO processed_events (event_id, consumer, processed_at) VALUES ($1, $2, $3)`,
			uuid.New(), consumer, time.Now().UTC().Add(-30*24*time.Hour))
	}
}

func TestInbox_PruneBatch_DeletesInStatementsOfTheGivenSize(t *testing.T) {
	pool, _ := fixtures.NewTestPoolAndDSN(t)
	ledger, err := eventbus.NewInbox(pool, port.ConsumerMembership)
	require.NoError(t, err)
	ctx := context.Background()
	strfixExec(t, pool, `CREATE TABLE strfix_delete_statements (at timestamptz NOT NULL DEFAULT now())`)
	strfixExec(t, pool, `CREATE FUNCTION strfix_count_delete() RETURNS trigger LANGUAGE plpgsql AS
		$$ BEGIN INSERT INTO strfix_delete_statements DEFAULT VALUES; RETURN NULL; END $$`)
	strfixExec(t, pool, `CREATE TRIGGER strfix_count_delete AFTER DELETE ON processed_events
		FOR EACH STATEMENT EXECUTE FUNCTION strfix_count_delete()`)
	strfixInsertOld(t, pool, port.ConsumerMembership, 5)

	deleted, err := ledger.PruneBatch(ctx, 7*24*time.Hour, 2)

	require.NoError(t, err)
	assert.EqualValues(t, 5, deleted)
	var statements int
	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, `SELECT count(*) FROM strfix_delete_statements`).Scan(&statements)
	}))
	assert.Equal(t, 3, statements, "5 rows at 2 per statement are deleted by 3 statements")
}

func TestInbox_Prune_OneConsumerFailing_OthersAreStillPrunedAndTheErrorNamesIt(t *testing.T) {
	pool, _ := fixtures.NewTestPoolAndDSN(t)
	ctx := context.Background()
	strfixExec(t, pool, `CREATE FUNCTION strfix_refuse_delete() RETURNS trigger LANGUAGE plpgsql AS
		$$ BEGIN RAISE EXCEPTION 'strfix refused'; END $$`)
	strfixExec(t, pool, `CREATE TRIGGER strfix_refuse_delete BEFORE DELETE ON processed_events
		FOR EACH ROW WHEN (OLD.consumer = 'membership-execution') EXECUTE FUNCTION strfix_refuse_delete()`)
	ledger, err := eventbus.NewInbox(pool, port.ConsumerMembership, port.ConsumerUser, port.ConsumerConnector)
	require.NoError(t, err)

	for round := range 20 {
		strfixInsertOld(t, pool, port.ConsumerMembership, 1)
		strfixInsertOld(t, pool, port.ConsumerUser, 1)
		strfixInsertOld(t, pool, port.ConsumerConnector, 1)

		deleted, err := ledger.Prune(ctx, 7*24*time.Hour)

		require.Error(t, err, "round %d", round)
		assert.Contains(t, err.Error(), `prune "`+port.ConsumerMembership+`"`)
		assert.EqualValues(t, 2, deleted, "round %d: the user and connector rows went although the membership prune failed", round)
		var left int
		require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE consumer <> 'membership-execution'`).Scan(&left)
		}))
		require.Zero(t, left, "round %d", round)
	}
}
