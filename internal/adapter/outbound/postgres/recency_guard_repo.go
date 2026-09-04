package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.RecencyGuard = (*RecencyGuardRepo)(nil)

type RecencyGuardRepo struct {
	pool *pgcommon.Pool
}

func NewRecencyGuardRepo(pool *pgcommon.Pool) *RecencyGuardRepo {
	return &RecencyGuardRepo{pool: pool}
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

// CheckAndCommit's RETURNING clause yields zero rows (pgx.ErrNoRows) exactly
// when the conditional UPDATE's WHERE clause didn't match — i.e. eventTime
// was not strictly newer than what's stored, so applied is false.
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

// WithLock holds scopeKey's session-level advisory lock on one dedicated
// connection for fn's entire duration. fn's own calls to ShouldApply/Commit
// each acquire their own connection from the same pool rather than reusing
// this one — safe as long as the pool has more than one connection (true of
// every real deployment; PG_MIN_CONNS/PG_MAX_CONNS default to 2/10), since
// the lock only needs to be held by this session, not by whichever
// connection issues the guarded statements.
func (r *RecencyGuardRepo) WithLock(ctx context.Context, scopeKey string, fn func(context.Context) error) error {
	err := r.pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
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
