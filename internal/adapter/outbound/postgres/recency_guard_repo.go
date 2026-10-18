package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.RecencyGuard = (*RecencyGuardRepo)(nil)

type RecencyGuardRepo struct {
	pool     *pgcommon.Pool
	lockPool *pgcommon.Pool
}

func NewRecencyGuardRepo(pool, lockPool *pgcommon.Pool) *RecencyGuardRepo {
	return &RecencyGuardRepo{pool: pool, lockPool: lockPool}
}

func (r *RecencyGuardRepo) ShouldApply(ctx context.Context, scopeKey string, eventTime time.Time) (bool, error) {
	var shouldApply bool
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		v, err := db.New(dbtx).RecencyGuardShouldApply(ctx, db.RecencyGuardShouldApplyParams{
			ScopeKey:  scopeKey,
			EventTime: eventTime,
		})
		if err != nil {
			return err
		}
		shouldApply = v
		return nil
	})
	return shouldApply, err
}

func (r *RecencyGuardRepo) CheckAndCommit(ctx context.Context, scopeKey string, eventTime time.Time) (bool, error) {
	var applied bool
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		_, err := db.New(dbtx).RecencyGuardCheckAndCommit(ctx, db.RecencyGuardCheckAndCommitParams{
			ScopeKey:  scopeKey,
			EventTime: eventTime,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			applied = false
			return nil
		}
		if err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

func (r *RecencyGuardRepo) Commit(ctx context.Context, scopeKey string, eventTime time.Time) error {
	return exec(ctx, r.pool, func(dbtx db.DBTX) error {
		return db.New(dbtx).RecencyGuardCommit(ctx, db.RecencyGuardCommitParams{
			ScopeKey:  scopeKey,
			EventTime: eventTime,
		})
	})
}

func (r *RecencyGuardRepo) WithLock(ctx context.Context, scopeKey string, fn func(context.Context) error) error {
	err := r.lockPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtext($1))", scopeKey); err != nil {
			return fmt.Errorf("acquire advisory lock: %w", err)
		}
		defer func() {
			_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock(hashtext($1))", scopeKey)
		}()
		return fn(ctx)
	})
	if err != nil {
		return fmt.Errorf("recency guard: with lock: %w", err)
	}
	return nil
}
