package postgres

import (
	"context"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/jackc/pgx/v5"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.Transactor = (*Transactor)(nil)

// serializableRetry backs RunInTxWithRetry: SERIALIZABLE isolation with
// bounded exponential backoff on 40001/40P01, matching definition_service's
// own transactor exactly.
var serializableRetry = pgcommon.RetryOptions{
	MaxAttempts:    5,
	InitialWait:    10 * time.Millisecond,
	MaxWait:        200 * time.Millisecond,
	Multiplier:     2,
	JitterFraction: 0.2,
}

type Transactor struct {
	pool *pgcommon.Pool
}

func NewTransactor(pool *pgcommon.Pool) *Transactor {
	return &Transactor{pool: pool}
}

func (t *Transactor) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	return pgcommon.RunInTx(ctx, t.pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error { //nolint:wrapcheck
		return fn(withTx(ctx, tx))
	})
}

func (t *Transactor) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return pgcommon.RunInTxWithRetryOpts( //nolint:wrapcheck
		ctx, t.pool,
		pgx.TxOptions{IsoLevel: pgx.Serializable},
		serializableRetry,
		func(ctx context.Context, tx pgx.Tx) error {
			return fn(withTx(ctx, tx))
		},
	)
}
