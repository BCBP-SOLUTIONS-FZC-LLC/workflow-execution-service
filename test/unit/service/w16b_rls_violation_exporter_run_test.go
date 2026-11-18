package service_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var errW16bViolationLogDown = errors.New("w16b violation log down")

type w16bWindow struct{ after, upTo time.Time }

// w16bViolationLog answers Horizon from a script of results, one per call, and records every call it gets.
type w16bViolationLog struct {
	mu       sync.Mutex
	horizons []time.Duration
	script   []w16bHorizonResult
	windows  []w16bWindow
	pruned   []time.Time
	counts   []domain.RLSViolationCount
	onCall   func(call int)
}

type w16bHorizonResult struct {
	at  time.Time
	err error
}

func (l *w16bViolationLog) Horizon(_ context.Context, lag time.Duration) (time.Time, error) {
	l.mu.Lock()
	l.horizons = append(l.horizons, lag)
	call := len(l.horizons)
	var res w16bHorizonResult
	if call <= len(l.script) {
		res = l.script[call-1]
	} else {
		res = l.script[len(l.script)-1]
	}
	hook := l.onCall
	l.mu.Unlock()
	if hook != nil {
		hook(call)
	}
	if res.err != nil {
		return time.Time{}, res.err
	}
	return res.at.Add(-lag), nil
}

func (l *w16bViolationLog) CountsBetween(_ context.Context, after, upTo time.Time) ([]domain.RLSViolationCount, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.windows = append(l.windows, w16bWindow{after, upTo})
	return l.counts, nil
}

func (l *w16bViolationLog) PruneBefore(_ context.Context, cutoff time.Time) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pruned = append(l.pruned, cutoff)
	return 0, nil
}

func (l *w16bViolationLog) snapshot() (horizons []time.Duration, windows []w16bWindow, pruned []time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]time.Duration(nil), l.horizons...), append([]w16bWindow(nil), l.windows...), append([]time.Time(nil), l.pruned...)
}

type w16bRecorded struct {
	violationType string
	n             float64
}

func TestRLSViolationExporter_RunOnAFreshExporterOnlyReadsItsStartPositionBeforeTheFirstInterval(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	base := time.Unix(1_000, 0)
	log := &w16bViolationLog{
		script: []w16bHorizonResult{{at: base}},
		counts: []domain.RLSViolationCount{{ViolationType: "cross_tenant_access", Count: 3}},
		onCall: func(int) { cancel() },
	}
	var mu sync.Mutex
	var recorded []w16bRecorded
	exp := service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log: log,
		Record: func(typ string, n float64) {
			mu.Lock()
			defer mu.Unlock()
			recorded = append(recorded, w16bRecorded{typ, n})
		},
		Interval:  time.Hour,
		Retention: time.Hour,
		Logger:    &w16bLogger{},
	})

	done := make(chan struct{})
	go func() {
		exp.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("Run did not return after its context was cancelled")
	}

	horizons, windows, pruned := log.snapshot()
	assert.Equal(t, []time.Duration{0}, horizons, "the only read is the start position, taken with no commit lag")
	assert.Empty(t, windows, "no window is counted before the first interval")
	assert.Empty(t, pruned)
	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, recorded, "nothing is exported before the first interval")
}

func TestRLSViolationExporter_RunLogsAFailedExportAndExportsTheSameWindowOnTheNextTick(t *testing.T) {
	t.Parallel()

	base := time.Unix(1_000, 0)
	later := base.Add(10 * time.Minute)
	// Horizon call 1 is Start, call 2 is the failing tick, call 3 onwards read a later horizon.
	log := &w16bViolationLog{
		script: []w16bHorizonResult{{at: base}, {err: errW16bViolationLogDown}, {at: later}},
		counts: []domain.RLSViolationCount{{ViolationType: "cross_tenant_access", Count: 2}},
	}
	logger := &w16bLogger{}
	exported := make(chan w16bRecorded, 4)
	exp := service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log:       log,
		Record:    func(typ string, n float64) { exported <- w16bRecorded{typ, n} },
		Interval:  time.Millisecond,
		Retention: time.Hour,
		Logger:    logger,
	})
	require.NoError(t, exp.Start(context.Background()))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		exp.Run(ctx)
		close(done)
	}()

	select {
	case got := <-exported:
		assert.Equal(t, w16bRecorded{"cross_tenant_access", 2}, got)
	case <-time.After(10 * time.Second):
		cancel()
		<-done
		t.Fatal("the exporter did not export after a failed tick")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}

	entries := logger.errorEntries()
	require.Len(t, entries, 1, "only the failed tick is logged")
	assert.Equal(t, "rls violation export failed", entries[0].msg)
	cause, ok := entries[0].fields["error"].(string)
	require.True(t, ok, "the log entry carries the error text")
	assert.Contains(t, cause, errW16bViolationLogDown.Error())

	_, windows, _ := log.snapshot()
	require.NotEmpty(t, windows)
	assert.Equal(t, w16bWindow{after: base, upTo: later.Add(-time.Minute)}, windows[0],
		"the window the failed tick could not read starts at the position kept from the start read")
}
