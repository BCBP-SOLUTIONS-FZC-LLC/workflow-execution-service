package port

import (
	"context"
	"time"
)

type CacheStore interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
	Del(ctx context.Context, keys ...string) error
	SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)
	Ping(ctx context.Context) error
}
