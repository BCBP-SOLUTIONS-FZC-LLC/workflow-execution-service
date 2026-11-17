package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

type levelLogger struct {
	silentLogger
	mu    sync.Mutex
	warns []map[string]any
}

func (l *levelLogger) Warn(_ string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, fields)
}

func TestNewCacheStore_AnUnreachableValkeyDoesNotStopTheStartAndIsWarnedAbout(t *testing.T) {
	cfg := &config.Config{
		ValkeyAddr:         "127.0.0.1:1",
		ValkeyDialTimeout:  200 * time.Millisecond,
		ValkeyReadTimeout:  200 * time.Millisecond,
		ValkeyWriteTimeout: 200 * time.Millisecond,
	}
	log := &levelLogger{}

	store, client := newCacheStore(context.Background(), cfg, log)

	require.NotNil(t, store)
	require.NotNil(t, client)
	t.Cleanup(func() { _ = client.Close() })
	require.Len(t, log.warns, 1)
	assert.NotEmpty(t, log.warns[0]["error"])
	assert.Equal(t, "127.0.0.1:1", log.warns[0]["addr"])
}
