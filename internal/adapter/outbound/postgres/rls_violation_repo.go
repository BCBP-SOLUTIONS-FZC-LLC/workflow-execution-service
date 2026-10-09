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

func (r *RLSViolationRepo) LatestID(ctx context.Context) (int64, error) {
	var id int64
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		v, err := db.New(dbtx).LatestRLSViolationID(ctx)
		id = v
		return mapErr(err)
	})
	return id, err
}

func (r *RLSViolationRepo) CountsAfter(ctx context.Context, afterID int64) ([]domain.RLSViolationCount, int64, error) {
	var counts []domain.RLSViolationCount
	newest := afterID
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).CountRLSViolationsAfter(ctx, afterID)
		if err != nil {
			return mapErr(err)
		}
		for _, row := range rows {
			counts = append(counts, domain.RLSViolationCount{ViolationType: row.ViolationType, Count: row.Total})
			newest = max(newest, row.NewestID)
		}
		return nil
	})
	return counts, newest, err
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
