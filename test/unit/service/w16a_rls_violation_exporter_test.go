package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var (
	errW16aHorizonDown = errors.New("w16a: horizon read failed")
	errW16aCountsDown  = errors.New("w16a: window count failed")
)

// w16aViolationLog wraps the window log: it can fail the window count, and it
// signals each horizon read so a looping test can wait for it.
type w16aViolationLog struct {
	*windowViolationLog
	countsErr error
	horizonCh chan struct{}
}

func newW16aViolationLog(now time.Duration) *w16aViolationLog {
	return &w16aViolationLog{windowViolationLog: &windowViolationLog{now: now}, horizonCh: make(chan struct{}, 16)}
}

func (l *w16aViolationLog) CountsBetween(ctx context.Context, after, upTo time.Time) ([]domain.RLSViolationCount, error) {
	if l.countsErr != nil {
		l.windows = append(l.windows, [2]time.Duration{after.Sub(l.clock(0)), upTo.Sub(l.clock(0))})
		return nil, l.countsErr
	}
	return l.windowViolationLog.CountsBetween(ctx, after, upTo)
}

func (l *w16aViolationLog) Horizon(ctx context.Context, lag time.Duration) (time.Time, error) {
	t, err := l.windowViolationLog.Horizon(ctx, lag)
	select {
	case l.horizonCh <- struct{}{}:
	default:
	}
	return t, err
}

func w16aNewExporter(log *w16aViolationLog, out *[]recorded, interval time.Duration, logger *w11aLogger) *service.RLSViolationExporter {
	return service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log:       log,
		Record:    func(typ string, n float64) { *out = append(*out, recorded{typ, n}) },
		Interval:  interval,
		Retention: time.Hour,
		Logger:    logger,
	})
}

func TestRLSViolationExporter_StartReadFailure_NamesThePositionRead(t *testing.T) {
	log := newW16aViolationLog(100 * time.Second)
	log.horizonErr = errW16aHorizonDown
	var out []recorded
	exp := w16aNewExporter(log, &out, time.Hour, &w11aLogger{})

	err := exp.Start(context.Background())

	require.ErrorIs(t, err, errW16aHorizonDown)
	assert.Equal(t, "rls violation log position: "+errW16aHorizonDown.Error(), err.Error())
	assert.Equal(t, 1, log.horizons, "the position is read once")
	assert.Empty(t, log.windows)
	assert.Empty(t, log.pruned)
}

func TestRLSViolationExporter_Run_AfterAFailedStartItRetriesTheStartAndNeitherCountsNorPrunes(t *testing.T) {
	log := newW16aViolationLog(100 * time.Second)
	log.horizonErr = errW16aHorizonDown
	var out []recorded
	logger := &w11aLogger{}
	exp := w16aNewExporter(log, &out, time.Hour, logger)
	require.Error(t, exp.Start(context.Background()))
	<-log.horizonCh

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { exp.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case <-log.horizonCh:
	case <-time.After(w16aLoopWait):
		t.Fatal("Run did not retry the start read")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(w16aLoopWait):
		t.Fatal("Run did not stop after cancel")
	}

	errs := logger.at("error")
	require.Len(t, errs, 1)
	assert.Equal(t, "rls violation exporter not started, retrying", errs[0].Msg, "the pass retried the start, it did not run an export")
	assert.Equal(t, map[string]any{"error": "rls violation log position: " + errW16aHorizonDown.Error()}, errs[0].Fields)
	assert.Equal(t, 2, log.horizons, "the failed Start and the one retry, each a position read")
	assert.Empty(t, log.windows, "nothing is counted before the exporter has a starting position")
	assert.Empty(t, log.pruned, "nothing is pruned either")
	assert.Empty(t, out)
}

func TestRLSViolationExporter_Tick_AFailedReadRecordsNothingKeepsThePositionAndTheNextTickCountsTheRowsOnce(t *testing.T) {
	cases := []struct {
		name        string
		fail        func(l *w16aViolationLog)
		heal        func(l *w16aViolationLog)
		wantWindows int
	}{
		{
			name:        "the horizon read fails",
			fail:        func(l *w16aViolationLog) { l.horizonErr = errW16aHorizonDown },
			heal:        func(l *w16aViolationLog) { l.horizonErr = nil },
			wantWindows: 1,
		},
		{
			name:        "the window count fails",
			fail:        func(l *w16aViolationLog) { l.countsErr = errW16aCountsDown },
			heal:        func(l *w16aViolationLog) { l.countsErr = nil },
			wantWindows: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := newW16aViolationLog(100 * time.Second)
			var out []recorded
			exp := w16aNewExporter(log, &out, time.Hour, &w11aLogger{})
			require.NoError(t, exp.Start(context.Background()))
			log.rows = append(log.rows, &windowRow{violationType: "cross_tenant_access", occurredAt: 150 * time.Second, committed: true})
			log.now = 500 * time.Second
			tc.fail(log)

			err := exp.Tick(context.Background())

			require.Error(t, err)
			cause := errW16aHorizonDown
			if log.countsErr != nil {
				cause = errW16aCountsDown
			}
			require.ErrorIs(t, err, cause)
			assert.Equal(t, "rls violation counts: "+cause.Error(), err.Error())
			assert.Empty(t, out, "nothing is recorded")
			assert.Empty(t, log.pruned, "a failed read is not followed by a prune")

			tc.heal(log)
			require.NoError(t, exp.Tick(context.Background()))

			assert.Equal(t, []recorded{{"cross_tenant_access", 1}}, out, "the same row is counted once")
			require.Len(t, log.windows, tc.wantWindows)
			assert.Equal(t, 100*time.Second, log.windows[0][0], "the window still starts at the position Start read")
			assert.Equal(t, 100*time.Second, log.windows[len(log.windows)-1][0], "the retried window starts where the failed one did")
			assert.Equal(t, 440*time.Second, log.windows[len(log.windows)-1][1], "and ends at the horizon, one commit lag behind the database clock")
			assert.Len(t, log.pruned, 1, "the prune runs once the window has been counted")
		})
	}
}
