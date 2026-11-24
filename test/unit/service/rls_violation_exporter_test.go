package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type recorded struct {
	violationType string
	n             float64
}

func TestRLSViolationExporter_PrunesRowsPastRetention(t *testing.T) {
	log := &windowViolationLog{now: 100 * time.Second}
	exp := service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log: log, Record: func(string, float64) {}, Retention: 24 * time.Hour, Logger: &fakeLogger{},
	})
	require.NoError(t, exp.Start(context.Background()))

	before := time.Now().Add(-24 * time.Hour)
	require.NoError(t, exp.Tick(context.Background()))

	require.Len(t, log.pruned, 1)
	assert.WithinDuration(t, before, log.pruned[0], 5*time.Second)
}

func TestRLSViolationExporter_StartFailureIsReported(t *testing.T) {
	log := &windowViolationLog{horizonErr: errors.New("db down")}
	exp := service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log: log, Record: func(string, float64) {}, Retention: time.Hour, Logger: &fakeLogger{},
	})
	require.Error(t, exp.Start(context.Background()))
}

func TestRLSViolationExporter_RunExportsOnEveryIntervalUntilCancelled(t *testing.T) {
	log := &flakyStartLog{}
	recordedCh := make(chan recorded, 4)
	exp := service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log:       log,
		Record:    func(typ string, n float64) { recordedCh <- recorded{typ, n} },
		Interval:  5 * time.Millisecond,
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
		t.Fatal("no export within 2s")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}
