package port

import "context"

type Transactor interface {
	RunInTx(ctx context.Context, fn func(ctx context.Context) error) error
	RunInTxWithRetry(ctx context.Context, fn func(ctx context.Context) error) error
}
