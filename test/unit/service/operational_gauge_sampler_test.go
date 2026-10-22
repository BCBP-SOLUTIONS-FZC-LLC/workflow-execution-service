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

type fakeStatsReader struct {
	stats domain.OperationalStats
	err   error
	reads atomic.Int32
}

func (f *fakeStatsReader) Read(context.Context) (domain.OperationalStats, error) {
	f.reads.Add(1)
	return f.stats, f.err
}

func TestOperationalGaugeSampler_RecordsEachReading(t *testing.T) {
	reader := &fakeStatsReader{stats: domain.OperationalStats{DegradedInstances: 3, OldestDegradedAgeSeconds: 90, OldestReadyTaskAgeSeconds: 12}}
	var got []domain.OperationalStats
	sampler := service.NewOperationalGaugeSampler(service.OperationalGaugeSamplerDeps{
		Stats: reader, Record: func(s domain.OperationalStats) { got = append(got, s) }, Interval: time.Hour, Logger: &fakeLogger{},
	})

	require.NoError(t, sampler.Tick(context.Background()))

	assert.Equal(t, []domain.OperationalStats{reader.stats}, got)
}

func TestOperationalGaugeSampler_ReadFailureRecordsNothing(t *testing.T) {
	reader := &fakeStatsReader{err: errors.New("db down")}
	recorded := false
	sampler := service.NewOperationalGaugeSampler(service.OperationalGaugeSamplerDeps{
		Stats: reader, Record: func(domain.OperationalStats) { recorded = true }, Interval: time.Hour, Logger: &fakeLogger{},
	})

	require.Error(t, sampler.Tick(context.Background()))
	assert.False(t, recorded, "a failed read must not overwrite the last good gauge values")
}

func TestOperationalGaugeSampler_RunSamplesImmediatelyAndStopsOnCancel(t *testing.T) {
	reader := &fakeStatsReader{}
	sampler := service.NewOperationalGaugeSampler(service.OperationalGaugeSamplerDeps{
		Stats: reader, Record: func(domain.OperationalStats) {}, Interval: time.Millisecond, Logger: &fakeLogger{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { sampler.Run(ctx); close(done) }()

	require.Eventually(t, func() bool { return reader.reads.Load() >= 2 }, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
