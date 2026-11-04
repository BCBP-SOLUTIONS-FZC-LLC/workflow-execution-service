package port

import (
	"context"
	"time"
)

type ScopeLocker interface {
	WithLock(ctx context.Context, scopeKey string, fn func(ctx context.Context) error) error
}

type RecencyGuard interface {
	ScopeLocker

	ShouldApply(ctx context.Context, scopeKey string, eventTime time.Time) (bool, error)

	CheckAndCommit(ctx context.Context, scopeKey string, eventTime time.Time) (applied bool, err error)

	Commit(ctx context.Context, scopeKey string, eventTime time.Time) error
}
