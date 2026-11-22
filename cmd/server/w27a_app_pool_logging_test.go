//go:build integration

package main

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

type w27aPoolLogEntry struct {
	level  string
	msg    string
	fields map[string]any
}

type w27aPoolLogger struct {
	mu      sync.Mutex
	entries []w27aPoolLogEntry
}

func (l *w27aPoolLogger) add(level, msg string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, w27aPoolLogEntry{level: level, msg: msg, fields: fields})
}

func (l *w27aPoolLogger) Debug(msg string, fields map[string]any) { l.add("debug", msg, fields) }
func (l *w27aPoolLogger) Info(msg string, fields map[string]any)  { l.add("info", msg, fields) }
func (l *w27aPoolLogger) Warn(msg string, fields map[string]any)  { l.add("warn", msg, fields) }
func (l *w27aPoolLogger) Error(msg string, fields map[string]any) { l.add("error", msg, fields) }

func (l *w27aPoolLogger) snapshot() []w27aPoolLogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]w27aPoolLogEntry(nil), l.entries...)
}

func TestNewAppPool_SlowQueryWithASensitiveArgument_ReachesTheServiceLoggerWithTheCountOnly(t *testing.T) {
	dsn := w9aEmptyPostgres(t)
	ctx := context.Background()
	log := &w27aPoolLogger{}
	const secret = "s3cret-token-5a4b3c2d"
	cfg := &config.Config{DatabaseURL: dsn, PGMaxConns: 2, PGMinConns: 1, PGSlowQueryThresholdMS: 50, OTELServiceName: "w27a"}

	pool, err := newAppPool(ctx, cfg, log)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, "SELECT pg_sleep($1), $2::text", 0.2, secret)
		return err
	}))

	var warns []w27aPoolLogEntry
	for _, e := range log.snapshot() {
		assert.NotContains(t, fmt.Sprint(e.msg, e.fields), secret, "no log line carries the argument value")
		if e.level == "warn" && e.msg == "slow query" {
			warns = append(warns, e)
		}
	}
	require.Len(t, warns, 1, "the pool built by the server logs a slow statement through the service logger")
	assert.Equal(t, 2, warns[0].fields["args_count"])
	assert.NotContains(t, warns[0].fields, "args")
}
