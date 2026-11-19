package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var (
	_ port.RecencyGuard   = (*RecencyGuardRepo)(nil)
	_ port.ScopeSetLocker = (*RecencyGuardRepo)(nil)
	_ port.ScopeTryLocker = (*RecencyGuardRepo)(nil)
)

type RecencyGuardRepo struct {
	pool     *pgcommon.Pool
	lockPool *pgcommon.Pool
}

func NewRecencyGuardRepo(pool, lockPool *pgcommon.Pool) *RecencyGuardRepo {
	return &RecencyGuardRepo{pool: pool, lockPool: lockPool}
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

func (r *RecencyGuardRepo) ShouldApplyUnlessOlder(ctx context.Context, scopeKey string, eventTime time.Time) (bool, error) {
	var shouldApply bool
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		v, err := db.New(dbtx).RecencyGuardShouldApplyUnlessOlder(ctx, db.RecencyGuardShouldApplyUnlessOlderParams{
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

const (
	lockRetryInterval  = 50 * time.Millisecond
	lockReleaseTimeout = 5 * time.Second
)

func (r *RecencyGuardRepo) WithLock(ctx context.Context, scopeKey string, fn func(context.Context) error) error {
	return r.WithLocks(ctx, []string{scopeKey}, fn)
}

// WithLocks takes every scope on one connection, in sorted order and without
// repeats, and retries from the start when any of them is held by someone else.
func (r *RecencyGuardRepo) WithLocks(ctx context.Context, scopeKeys []string, fn func(context.Context) error) error {
	keys := sortedUnique(scopeKeys)
	for {
		acquired, err := r.withLocksOnce(ctx, keys, fn)
		if err != nil {
			return fmt.Errorf("recency guard: with lock: %w", err)
		}
		if acquired {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("recency guard: with lock: waiting for %q: %w", strings.Join(keys, ","), ctx.Err())
		case <-time.After(lockRetryInterval):
		}
	}
}

func (r *RecencyGuardRepo) TryWithLock(ctx context.Context, scopeKey string, fn func(context.Context) error) (bool, error) {
	acquired, err := r.withLocksOnce(ctx, []string{scopeKey}, fn)
	if err != nil {
		return false, fmt.Errorf("recency guard: try lock %q: %w", scopeKey, err)
	}
	return acquired, nil
}

func sortedUnique(keys []string) []string {
	out := slices.Clone(keys)
	slices.Sort(out)
	return slices.Compact(out)
}

func (r *RecencyGuardRepo) withLocksOnce(ctx context.Context, keys []string, fn func(context.Context) error) (bool, error) {
	var acquired bool
	var fnErr error
	err := r.lockPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		held, err := tryLockAll(ctx, conn, keys)
		if err != nil {
			return err
		}
		if len(held) == len(keys) {
			acquired = true
			fnErr = fn(ctx)
		}
		releaseAll(ctx, conn, held)
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("lock connection: %w", err)
	}
	return acquired, fnErr
}

// tryLockAll returns the keys it took; it stops at the first one that is held
// by someone else, so fewer keys than asked for means nothing may run.
func tryLockAll(ctx context.Context, conn *pgxpool.Conn, keys []string) ([]string, error) {
	held := make([]string, 0, len(keys))
	for _, key := range keys {
		var got bool
		if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtext($1))", key).Scan(&got); err != nil {
			releaseAll(ctx, conn, held)
			return nil, fmt.Errorf("acquire advisory lock: %w", err)
		}
		if !got {
			return held, nil
		}
		held = append(held, key)
	}
	return held, nil
}

// releaseAll closes the connection when an unlock fails, which drops every lock
// the session still holds.
func releaseAll(ctx context.Context, conn *pgxpool.Conn, held []string) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), lockReleaseTimeout)
	defer cancel()
	for _, key := range held {
		var released bool
		if err := conn.QueryRow(releaseCtx, "SELECT pg_advisory_unlock(hashtext($1))", key).Scan(&released); err != nil || !released {
			_ = conn.Conn().Close(context.WithoutCancel(ctx))
			return
		}
	}
}
