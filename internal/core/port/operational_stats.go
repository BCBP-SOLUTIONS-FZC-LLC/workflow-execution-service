package port

import (
	"context"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

type RLSViolationLog interface {
	Horizon(ctx context.Context, lag time.Duration) (time.Time, error)
	CountsBetween(ctx context.Context, after, upTo time.Time) ([]domain.RLSViolationCount, error)
	PruneBefore(ctx context.Context, cutoff time.Time) (int64, error)
}

type OperationalStatsReader interface {
	Read(ctx context.Context) (domain.OperationalStats, error)
}
