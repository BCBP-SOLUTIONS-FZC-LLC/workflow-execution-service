//go:build integration

package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/pglogger"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type vsxLogEntry struct {
	level  string
	msg    string
	fields map[string]any
}

type vsxRecordingLogger struct {
	mu      sync.Mutex
	entries []vsxLogEntry
}

func (l *vsxRecordingLogger) record(level, msg string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, vsxLogEntry{level: level, msg: msg, fields: fields})
}

func (l *vsxRecordingLogger) Debug(msg string, fields map[string]any) { l.record("debug", msg, fields) }
func (l *vsxRecordingLogger) Info(msg string, fields map[string]any)  { l.record("info", msg, fields) }
func (l *vsxRecordingLogger) Warn(msg string, fields map[string]any)  { l.record("warn", msg, fields) }
func (l *vsxRecordingLogger) Error(msg string, fields map[string]any) { l.record("error", msg, fields) }

func (l *vsxRecordingLogger) snapshot() []vsxLogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]vsxLogEntry(nil), l.entries...)
}

func TestPGLogger_SlowQueryOnPoolBuiltWithAdapter_LogsOneWarnWithQueryFields(t *testing.T) {
	_, dsn := fixtures.NewTestPoolAndDSN(t)
	ctx := context.Background()
	log := &vsxRecordingLogger{}
	const threshold = 50 * time.Millisecond

	pool, err := pgcommon.NewPool(ctx, pgcommon.Config{DSN: dsn, Logger: pglogger.New(log), SlowQueryThreshold: threshold})
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, "SELECT pg_sleep($1)", 0.2)
		return err
	}))

	var warns []vsxLogEntry
	for _, e := range log.snapshot() {
		if e.level == "warn" && e.msg == "slow query" {
			warns = append(warns, e)
		}
	}
	require.Len(t, warns, 1, "one slow statement logs exactly one warning")
	fields := warns[0].fields
	assert.Equal(t, "SELECT", fields["query"], "only the statement verb is logged by default")
	assert.Equal(t, 1, fields["args_count"])
	duration, ok := fields["duration_ms"].(int64)
	require.True(t, ok, "duration_ms is a whole number of milliseconds, got %T", fields["duration_ms"])
	assert.GreaterOrEqual(t, duration, threshold.Milliseconds())
	caller, ok := fields["caller"].(string)
	require.True(t, ok)
	assert.Contains(t, caller, "pglogger_slow_query_test.go", "the caller is the first frame outside pgx and pgcommon")
}
