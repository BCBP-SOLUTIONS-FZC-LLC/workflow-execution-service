package main

import (
	"context"
	"sync"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func dispatchEntry(ctx context.Context, d *deps, wg *sync.WaitGroup, entry valkeystream.Entry, wlog port.Logger) {
	dispatchBatch(ctx, d, wg, []valkeystream.Entry{entry}, wlog)
}

type fakeCacheStore struct {
	mu     sync.Mutex
	values map[string]string
	nxErr  error
}

func newFakeCacheStore() *fakeCacheStore { return &fakeCacheStore{values: map[string]string{}} }

func (c *fakeCacheStore) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.values[key], nil
}
func (c *fakeCacheStore) Set(_ context.Context, key, value string, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[key] = value
	return nil
}
func (c *fakeCacheStore) Del(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		delete(c.values, k)
	}
	return nil
}
func (c *fakeCacheStore) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nxErr != nil {
		return false, c.nxErr
	}
	if _, ok := c.values[key]; ok {
		return false, nil
	}
	c.values[key] = value
	return true, nil
}
func (c *fakeCacheStore) Ping(context.Context) error { return nil }

type noopLogger struct{}

func (noopLogger) Debug(string, map[string]any) {}
func (noopLogger) Info(string, map[string]any)  {}
func (noopLogger) Warn(string, map[string]any)  {}
func (noopLogger) Error(string, map[string]any) {}

var _ port.CacheStore = (*fakeCacheStore)(nil)
var _ port.Logger = noopLogger{}
