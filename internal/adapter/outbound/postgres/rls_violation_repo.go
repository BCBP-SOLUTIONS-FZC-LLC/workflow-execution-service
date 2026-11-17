package postgres

import (
	"context"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.RLSViolationLog = (*RLSViolationRepo)(nil)

type RLSViolationRepo struct {
	pool *pgcommon.Pool
}

func NewRLSViolationRepo(pool *pgcommon.Pool) *RLSViolationRepo {
	return &RLSViolationRepo{pool: pool}
}

func (r *RLSViolationRepo) Horizon(ctx context.Context, lag time.Duration) (time.Time, error) {
	var horizon time.Time
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		v, err := db.New(dbtx).RLSViolationHorizon(ctx, lag.Seconds())
		horizon = v
		return mapErr(err)
	})
	return horizon, err
}

func (r *RLSViolationRepo) CountsBetween(ctx context.Context, after, upTo time.Time) ([]domain.RLSViolationCount, error) {
	var counts []domain.RLSViolationCount
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).CountRLSViolationsBetween(ctx, db.CountRLSViolationsBetweenParams{After: after, UpTo: upTo})
		if err != nil {
			return mapErr(err)
		}
		for _, row := range rows {
			counts = append(counts, domain.RLSViolationCount{ViolationType: row.ViolationType, Count: row.Total})
		}
		return nil
	})
	return counts, err
}

func (r *RLSViolationRepo) PruneBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	var n int64
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		v, err := db.New(dbtx).PruneRLSViolationsBefore(ctx, cutoff)
		n = v
		return mapErr(err)
	})
	return n, err
}
