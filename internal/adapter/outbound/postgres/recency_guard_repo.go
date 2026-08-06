package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

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
