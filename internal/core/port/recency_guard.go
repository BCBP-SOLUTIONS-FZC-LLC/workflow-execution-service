package port

import (
	"context"
	"time"
)

type ScopeLocker interface {
	WithLock(ctx context.Context, scopeKey string, fn func(ctx context.Context) error) error
}

// ScopeSetLocker takes several scopes at once on one connection, in a fixed
// order, so callers that need more than one lock neither hold several pooled
// connections nor risk taking them in opposite orders.
type ScopeSetLocker interface {
	ScopeLocker

	WithLocks(ctx context.Context, scopeKeys []string, fn func(ctx context.Context) error) error
}

type RecencyGuard interface {
	ScopeLocker

	ShouldApply(ctx context.Context, scopeKey string, eventTime time.Time) (bool, error)

	ShouldApplyUnlessOlder(ctx context.Context, scopeKey string, eventTime time.Time) (bool, error)

	CheckAndCommit(ctx context.Context, scopeKey string, eventTime time.Time) (applied bool, err error)

	Commit(ctx context.Context, scopeKey string, eventTime time.Time) error
}
