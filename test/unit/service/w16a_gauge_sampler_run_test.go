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

var errW16aStatsDown = errors.New("w16a: stats database unavailable")

const w16aLoopWait = 30 * time.Second

// w16aStatsReader fails its first failFirst reads, then answers stats.
type w16aStatsReader struct {
	stats     domain.OperationalStats
	failFirst int32
	reads     atomic.Int32
}

func (r *w16aStatsReader) Read(context.Context) (domain.OperationalStats, error) {
	if n := r.reads.Add(1); n <= r.failFirst {
		return domain.OperationalStats{}, errW16aStatsDown
	}
	return r.stats, nil
}

// w16aRunSampler runs the sampler until it has recorded wantRecords readings, then cancels it and waits for Run to return.
func w16aRunSampler(t *testing.T, reader *w16aStatsReader, interval time.Duration, log *w11aLogger, wantRecords int) []domain.OperationalStats {
	t.Helper()
	recorded := make(chan domain.OperationalStats, 16)
	sampler := service.NewOperationalGaugeSampler(service.OperationalGaugeSamplerDeps{
		Stats: reader, Interval: interval, Logger: log,
		Record: func(s domain.OperationalStats) {
			select {
			case recorded <- s:
			default:
			}
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { sampler.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	var got []domain.OperationalStats
	for len(got) < wantRecords {
		select {
		case s := <-recorded:
			got = append(got, s)
		case <-time.After(w16aLoopWait):
			t.Fatalf("the sampler recorded %d readings, want %d", len(got), wantRecords)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(w16aLoopWait):
		t.Fatal("Run did not return after cancel")
	}
	return got
}

func TestOperationalGaugeSampler_Run_SamplesOnceAtStartBeforeTheFirstInterval(t *testing.T) {
	reader := &w16aStatsReader{stats: domain.OperationalStats{DegradedInstances: 2, OldestDegradedAgeSeconds: 40, OldestReadyTaskAgeSeconds: 9}}
	log := &w11aLogger{}

	got := w16aRunSampler(t, reader, time.Hour, log, 1)

	assert.Equal(t, []domain.OperationalStats{reader.stats}, got, "the reading is recorded without waiting for the interval")
	assert.Equal(t, int32(1), reader.reads.Load(), "an hour-long interval has not elapsed, so the one sample is the start-up one")
	assert.Empty(t, log.at("error"))
}

func TestOperationalGaugeSampler_Run_AFailingTickIsLoggedAndTheNextTickRecovers(t *testing.T) {
	reader := &w16aStatsReader{stats: domain.OperationalStats{DegradedInstances: 5}, failFirst: 1}
	log := &w11aLogger{}

	got := w16aRunSampler(t, reader, time.Millisecond, log, 1)

	assert.Equal(t, []domain.OperationalStats{reader.stats}, got, "the first reading that is recorded is the one after the failure")
	assert.GreaterOrEqual(t, reader.reads.Load(), int32(2))
	errs := log.at("error")
	require.Len(t, errs, 1, "only the failing tick is logged")
	assert.Equal(t, "operational gauge sampling failed", errs[0].Msg)
	assert.Equal(t, map[string]any{"error": "operational stats: " + errW16aStatsDown.Error()}, errs[0].Fields)
}
