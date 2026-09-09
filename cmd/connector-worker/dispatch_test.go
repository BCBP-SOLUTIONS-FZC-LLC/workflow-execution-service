package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type fakeCacheStore struct {
	values map[string]string
	nxErr  error
}

func newFakeCacheStore() *fakeCacheStore { return &fakeCacheStore{values: map[string]string{}} }

func (c *fakeCacheStore) Get(_ context.Context, key string) (string, error) {
	return c.values[key], nil
}
func (c *fakeCacheStore) Set(_ context.Context, key, value string, _ time.Duration) error {
	c.values[key] = value
	return nil
}
func (c *fakeCacheStore) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		delete(c.values, k)
	}
	return nil
}
func (c *fakeCacheStore) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
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

func TestClaimDispatch_FirstCallClaimsSecondIsBlocked(t *testing.T) {
	d := &deps{dedup: newFakeCacheStore()}
	taskID := uuid.New()

	assert.True(t, claimDispatch(context.Background(), d, taskID, noopLogger{}), "the first claim for a task_id must succeed")
	assert.False(t, claimDispatch(context.Background(), d, taskID, noopLogger{}), "a second claim for the same task_id within the TTL must be blocked")
}

func TestClaimDispatch_DifferentTaskIDsClaimIndependently(t *testing.T) {
	d := &deps{dedup: newFakeCacheStore()}
	require.True(t, claimDispatch(context.Background(), d, uuid.New(), noopLogger{}))
	require.True(t, claimDispatch(context.Background(), d, uuid.New(), noopLogger{}))
}

func TestClaimDispatch_CacheErrorFailsOpen(t *testing.T) {
	cache := newFakeCacheStore()
	cache.nxErr = assert.AnError
	d := &deps{dedup: cache}

	assert.True(t, claimDispatch(context.Background(), d, uuid.New(), noopLogger{}), "a cache error must fail open, not block a real dispatch")
}

func TestClaimDispatch_NilDedupFailsOpen(t *testing.T) {
	d := &deps{}
	assert.True(t, claimDispatch(context.Background(), d, uuid.New(), noopLogger{}))
}

var _ port.CacheStore = (*fakeCacheStore)(nil)
var _ port.Logger = noopLogger{}
