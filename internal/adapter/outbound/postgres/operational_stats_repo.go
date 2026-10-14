package postgres

import (
	"context"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.OperationalStatsReader = (*OperationalStatsRepo)(nil)

type OperationalStatsRepo struct {
	pool *pgcommon.Pool
}

func NewOperationalStatsRepo(pool *pgcommon.Pool) *OperationalStatsRepo {
	return &OperationalStatsRepo{pool: pool}
}

func (r *OperationalStatsRepo) Read(ctx context.Context) (domain.OperationalStats, error) {
	var stats domain.OperationalStats
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		row, err := db.New(dbtx).OperationalStats(ctx)
		if err != nil {
			return mapErr(err)
		}
		stats = domain.OperationalStats{
			DegradedInstances:         row.DegradedInstances,
			OldestDegradedAgeSeconds:  row.OldestDegradedAgeSeconds,
			OldestReadyTaskAgeSeconds: row.OldestReadyTaskAgeSeconds,
		}
		return nil
	})
	return stats, err
}
