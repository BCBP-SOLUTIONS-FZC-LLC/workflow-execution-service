package main

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var (
	errW10bConnectionRefused = errors.New("valkeystream read group: connection refused")
	errW10bLostGroup         = errors.New("valkeystream read group: NOGROUP No such key 'dspfix-stream' or consumer group 'dspfix-group' in XREADGROUP with GROUP option")
)

type w10bLogEntry struct {
	level  string
	msg    string
	fields map[string]any
}

// w10bReadFailureLogger records every entry and signals once the read-failure warning is logged.
type w10bReadFailureLogger struct {
	mu      sync.Mutex
	entries []w10bLogEntry
	warned  chan struct{}
	once    sync.Once
}

func newW10bReadFailureLogger() *w10bReadFailureLogger {
	return &w10bReadFailureLogger{warned: make(chan struct{})}
}

func (l *w10bReadFailureLogger) record(level, msg string, fields map[string]any) {
	l.mu.Lock()
	l.entries = append(l.entries, w10bLogEntry{level: level, msg: msg, fields: fields})
	l.mu.Unlock()
	if level == "warn" && msg == "connector-worker: read group failed" {
		l.once.Do(func() { close(l.warned) })
	}
}

func (l *w10bReadFailureLogger) Debug(msg string, f map[string]any) { l.record("debug", msg, f) }
func (l *w10bReadFailureLogger) Info(msg string, f map[string]any)  { l.record("info", msg, f) }
func (l *w10bReadFailureLogger) Warn(msg string, f map[string]any)  { l.record("warn", msg, f) }
func (l *w10bReadFailureLogger) Error(msg string, f map[string]any) { l.record("error", msg, f) }

func (l *w10bReadFailureLogger) all() []w10bLogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]w10bLogEntry(nil), l.entries...)
}

var _ port.Logger = (*w10bReadFailureLogger)(nil)

// w10bNoEnsureConsumer hides the group-recreation method of the stream it wraps, like a consumer that cannot
// recreate its group.
type w10bNoEnsureConsumer struct{ streamConsumer }

func TestDispatchLoop_ReadFailureThatDoesNotRecreateTheGroup_LogsTheFailureAndLeavesTheGroupAlone(t *testing.T) {
	cases := []struct {
		name       string
		readErr    error
		hideEnsure bool
	}{
		{name: "read fails for a reason other than a missing group", readErr: errW10bConnectionRefused},
		{name: "group is missing but the consumer cannot recreate it", readErr: errW10bLostGroup, hideEnsure: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newDspfixWorker(t, http.StatusNoContent, map[string]*typePool{})
			if tc.hideEnsure {
				w.d.consumer = w10bNoEnsureConsumer{w.stream}
				_, recreates := w.d.consumer.(groupEnsurer)
				require.False(t, recreates, "the wrapped consumer must not offer group recreation")
			}
			w.stream.script = []dspfixReadStep{{err: tc.readErr}}
			logger := newW10bReadFailureLogger()
			ctx, stop := context.WithCancel(context.Background())
			t.Cleanup(stop)
			ended := make(chan struct{})
			go func() {
				defer close(ended)
				var wg sync.WaitGroup
				runDispatchLoop(ctx, w.d, &wg, logger)
				wg.Wait()
			}()

			select {
			case <-logger.warned:
			case <-time.After(dspfixGuard):
				require.FailNow(t, "the read failure was not logged")
			}
			stop()
			select {
			case <-ended:
			case <-time.After(dspfixGuard):
				require.FailNow(t, "the loop did not end on shutdown")
			}

			assert.Empty(t, w.stream.ensureList(), "the group must not be touched")
			assert.Empty(t, w.stream.ackList())
			var warnings []w10bLogEntry
			for _, e := range logger.all() {
				assert.NotEqual(t, "error", e.level, "no error entry: the group was not recreated and not failed to recreate")
				if e.level == "warn" {
					warnings = append(warnings, e)
				}
			}
			require.Len(t, warnings, 1, "one warning for the failed read and no recreation notice")
			assert.Equal(t, "connector-worker: read group failed", warnings[0].msg)
			assert.Equal(t, tc.readErr.Error(), warnings[0].fields["error"])
		})
	}
}
