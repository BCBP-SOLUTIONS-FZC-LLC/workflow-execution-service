package port

import (
	"context"
	"time"
)

type RecencyGuard interface {
	ShouldApply(ctx context.Context, scopeKey string, eventTime time.Time) (bool, error)

	CheckAndCommit(ctx context.Context, scopeKey string, eventTime time.Time) (applied bool, err error)

	Commit(ctx context.Context, scopeKey string, eventTime time.Time) error

	WithLock(ctx context.Context, scopeKey string, fn func(ctx context.Context) error) error
}
