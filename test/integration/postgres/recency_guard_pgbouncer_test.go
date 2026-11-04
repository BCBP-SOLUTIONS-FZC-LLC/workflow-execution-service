//go:build integration

package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func advisoryLocksHeld(t *testing.T, direct *pgcommon.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, direct.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'`).Scan(&n)
	}))
	return n
}

func TestRecencyGuardWithLock_BehindATransactionPoolingPgBouncerLeavesNoLockBehind(t *testing.T) {
	bouncerDSN, directDSN := fixtures.NewPgBouncer(t)
	ctx := context.Background()
	bouncer, err := pgcommon.NewPool(ctx, pgcommon.Config{DSN: bouncerDSN, PGBouncerMode: true, MaxConns: 8})
	require.NoError(t, err)
	t.Cleanup(bouncer.Close)
	direct, err := pgcommon.NewPool(ctx, pgcommon.Config{DSN: directDSN, MaxConns: 3})
	require.NoError(t, err)
	t.Cleanup(direct.Close)
	repo := postgres.NewRecencyGuardRepo(bouncer, direct)

	stop := make(chan struct{})
	var load sync.WaitGroup
	for i := 0; i < 4; i++ {
		load.Add(1)
		go func() {
			defer load.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = bouncer.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
					_, err := conn.Exec(ctx, `SELECT pg_sleep(0.005)`)
					return err
				})
			}
		}()
	}

	for i := 0; i < 20; i++ {
		lockCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := repo.WithLock(lockCtx, "tenant:pgbouncer-test", func(context.Context) error {
			time.Sleep(40 * time.Millisecond)
			return nil
		})
		cancel()
		require.NoError(t, err, "iteration %d: the lock from an earlier iteration must have been released", i)
	}
	close(stop)
	load.Wait()

	assert.Zero(t, advisoryLocksHeld(t, direct), "no session lock may outlive its WithLock call")
}
