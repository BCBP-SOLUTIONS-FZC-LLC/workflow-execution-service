package valkey

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache struct {
	client redis.Cmdable
}

func NewCache(client redis.Cmdable) *Cache {
	return &Cache{client: client}
}

func (c *Cache) Get(ctx context.Context, key string) (string, error) {
	val, err := c.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) { // redis.Nil signals a cache miss, not an error
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("valkey get: %w", err)
	}
	return val, nil
}

func (c *Cache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	if err := c.client.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("valkey set: %w", err)
	}
	return nil
}

func (c *Cache) Del(ctx context.Context, keys ...string) error {
	if err := c.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("valkey del: %w", err)
	}
	return nil
}

func (c *Cache) SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	val, err := c.client.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("valkey setnx: %w", err)
	}
	return val, nil
}

func (c *Cache) Ping(ctx context.Context) error {
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("valkey ping: %w", err)
	}
	return nil
}
