package service_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type windowRow struct {
	violationType string
	occurredAt    time.Duration
	committed     bool
}

type windowViolationLog struct {
	now        time.Duration
	rows       []*windowRow
	horizonErr error
	horizons   int
	windows    [][2]time.Duration
	pruned     []time.Time
	pruneErr   error
}

func (l *windowViolationLog) clock(d time.Duration) time.Time { return time.Unix(0, 0).Add(d) }

func (l *windowViolationLog) Horizon(_ context.Context, lag time.Duration) (time.Time, error) {
	l.horizons++
	if l.horizonErr != nil {
		return time.Time{}, l.horizonErr
	}
	return l.clock(l.now - lag), nil
}

func (l *windowViolationLog) CountsBetween(_ context.Context, after, upTo time.Time) ([]domain.RLSViolationCount, error) {
	l.windows = append(l.windows, [2]time.Duration{after.Sub(l.clock(0)), upTo.Sub(l.clock(0))})
	byType := map[string]int64{}
	var order []string
	for _, r := range l.rows {
		at := l.clock(r.occurredAt)
		if !r.committed || !at.After(after) || at.After(upTo) {
			continue
		}
		if _, ok := byType[r.violationType]; !ok {
			order = append(order, r.violationType)
		}
		byType[r.violationType]++
	}
	var counts []domain.RLSViolationCount
	for _, typ := range order {
		counts = append(counts, domain.RLSViolationCount{ViolationType: typ, Count: byType[typ]})
	}
	return counts, nil
}

func (l *windowViolationLog) PruneBefore(_ context.Context, cutoff time.Time) (int64, error) {
	l.pruned = append(l.pruned, cutoff)
	return 0, l.pruneErr
}

func newWindowExporter(log *windowViolationLog, out *[]recorded) *service.RLSViolationExporter {
	return service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log:       log,
		Record:    func(t string, n float64) { *out = append(*out, recorded{t, n}) },
		Interval:  time.Millisecond,
		Retention: time.Hour,
		Logger:    &fakeLogger{},
	})
}

func TestRLSViolationExporter_CountsARowThatCommitsAfterALaterRowWasExported(t *testing.T) {
	log := &windowViolationLog{now: 100 * time.Second}
	var out []recorded
	exp := newWindowExporter(log, &out)
	require.NoError(t, exp.Start(context.Background()))

	slow := &windowRow{violationType: "cross_tenant_access", occurredAt: 101 * time.Second}
	fast := &windowRow{violationType: "missing_or_invalid_guc", occurredAt: 102 * time.Second, committed: true}
	log.rows = append(log.rows, slow, fast)

	log.now = 103 * time.Second
	require.NoError(t, exp.Tick(context.Background()))
	assert.Empty(t, out, "rows younger than the commit lag are not counted yet")

	slow.committed = true
	log.now = 400 * time.Second
	require.NoError(t, exp.Tick(context.Background()))

	assert.ElementsMatch(t, []recorded{{"cross_tenant_access", 1}, {"missing_or_invalid_guc", 1}}, out)
}

func TestRLSViolationExporter_CountsEachRowOnceAcrossWindows(t *testing.T) {
	log := &windowViolationLog{now: 100 * time.Second}
	var out []recorded
	exp := newWindowExporter(log, &out)
	require.NoError(t, exp.Start(context.Background()))
	log.rows = append(log.rows, &windowRow{violationType: "cross_tenant_access", occurredAt: 150 * time.Second, committed: true})

	log.now = 500 * time.Second
	require.NoError(t, exp.Tick(context.Background()))
	log.now = 600 * time.Second
	require.NoError(t, exp.Tick(context.Background()))

	assert.Equal(t, []recorded{{"cross_tenant_access", 1}}, out)
	require.Len(t, log.windows, 2)
	assert.Equal(t, log.windows[0][1], log.windows[1][0], "the second window starts where the first ended")
}

func TestRLSViolationExporter_DoesNotCountRowsFromBeforeStart(t *testing.T) {
	log := &windowViolationLog{now: 100 * time.Second, rows: []*windowRow{{violationType: "cross_tenant_access", occurredAt: 90 * time.Second, committed: true}}}
	var out []recorded
	exp := newWindowExporter(log, &out)
	require.NoError(t, exp.Start(context.Background()))

	log.now = 500 * time.Second
	require.NoError(t, exp.Tick(context.Background()))

	assert.Empty(t, out)
}

func TestRLSViolationExporter_FailedWindowReadKeepsThePosition(t *testing.T) {
	log := &windowViolationLog{now: 100 * time.Second}
	var out []recorded
	exp := newWindowExporter(log, &out)
	require.NoError(t, exp.Start(context.Background()))
	log.rows = append(log.rows, &windowRow{violationType: "cross_tenant_access", occurredAt: 150 * time.Second, committed: true})

	log.horizonErr = errors.New("db down")
	log.now = 500 * time.Second
	require.Error(t, exp.Tick(context.Background()))
	log.horizonErr = nil
	require.NoError(t, exp.Tick(context.Background()))

	assert.Equal(t, []recorded{{"cross_tenant_access", 1}}, out)
}

type flakyStartLog struct {
	failures atomic.Int32
	calls    atomic.Int32
	okCalls  atomic.Int32
}

func (l *flakyStartLog) Horizon(_ context.Context, lag time.Duration) (time.Time, error) {
	l.calls.Add(1)
	if l.failures.Add(-1) >= 0 {
		return time.Time{}, errors.New("db down")
	}
	n := l.okCalls.Add(1)
	return time.Unix(0, 0).Add(time.Duration(n)*time.Hour - lag), nil
}

func (l *flakyStartLog) CountsBetween(_ context.Context, after, upTo time.Time) ([]domain.RLSViolationCount, error) {
	row := time.Unix(0, 0).Add(90 * time.Minute)
	if row.After(after) && !row.After(upTo) {
		return []domain.RLSViolationCount{{ViolationType: "cross_tenant_access", Count: 1}}, nil
	}
	return nil, nil
}

func (l *flakyStartLog) PruneBefore(context.Context, time.Time) (int64, error) { return 0, nil }

func TestRLSViolationExporter_RunRetriesTheStartReadUntilItSucceeds(t *testing.T) {
	log := &flakyStartLog{}
	log.failures.Store(3)
	recordedCh := make(chan recorded, 4)
	exp := service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log:       log,
		Record:    func(typ string, n float64) { recordedCh <- recorded{typ, n} },
		Interval:  time.Millisecond,
		Retention: time.Hour,
		Logger:    &fakeLogger{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		exp.Run(ctx)
		close(done)
	}()

	select {
	case got := <-recordedCh:
		assert.Equal(t, recorded{"cross_tenant_access", 1}, got)
	case <-time.After(30 * time.Second):
		t.Fatal("exporter did not recover after the start read failed")
	}
	cancel()
	<-done
	assert.GreaterOrEqual(t, log.calls.Load(), int32(4), "the failed start reads were retried")
}
