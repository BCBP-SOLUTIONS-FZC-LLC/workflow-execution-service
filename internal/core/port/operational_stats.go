package port

import (
	"context"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

type RLSViolationLog interface {
	LatestID(ctx context.Context) (int64, error)
	CountsAfter(ctx context.Context, afterID int64) ([]domain.RLSViolationCount, int64, error)
	PruneBefore(ctx context.Context, cutoff time.Time) (int64, error)
}

type OperationalStatsReader interface {
	Read(ctx context.Context) (domain.OperationalStats, error)
}
