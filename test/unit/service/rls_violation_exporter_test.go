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

type fakeViolationLog struct {
	latest    int64
	counts    []domain.RLSViolationCount
	newest    int64
	afterIDs  []int64
	pruned    []time.Time
	countsErr error
}

func (f *fakeViolationLog) LatestID(context.Context) (int64, error) { return f.latest, nil }

func (f *fakeViolationLog) CountsAfter(_ context.Context, afterID int64) ([]domain.RLSViolationCount, int64, error) {
	f.afterIDs = append(f.afterIDs, afterID)
	if f.countsErr != nil {
		return nil, afterID, f.countsErr
	}
	counts, newest := f.counts, f.newest
	f.counts = nil
	return counts, newest, nil
}

func (f *fakeViolationLog) PruneBefore(_ context.Context, cutoff time.Time) (int64, error) {
	f.pruned = append(f.pruned, cutoff)
	return 0, nil
}

type recorded struct {
	violationType string
	n             float64
}

func newExporter(log *fakeViolationLog, out *[]recorded) *service.RLSViolationExporter {
	return service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log:       log,
		Record:    func(t string, n float64) { *out = append(*out, recorded{t, n}) },
		Retention: 24 * time.Hour,
		Logger:    &fakeLogger{},
	})
}

func TestRLSViolationExporter_RecordsRowsWrittenAfterStart(t *testing.T) {
	log := &fakeViolationLog{latest: 40, newest: 45, counts: []domain.RLSViolationCount{
		{ViolationType: "cross_tenant_access", Count: 3},
		{ViolationType: "missing_or_invalid_guc", Count: 2},
	}}
	var out []recorded
	exp := newExporter(log, &out)

	require.NoError(t, exp.Start(context.Background()))
	require.NoError(t, exp.Tick(context.Background()))

	assert.Equal(t, []int64{40}, log.afterIDs, "rows present at start are not counted")
	assert.ElementsMatch(t, []recorded{{"cross_tenant_access", 3}, {"missing_or_invalid_guc", 2}}, out)
}

func TestRLSViolationExporter_NeverCountsARowTwice(t *testing.T) {
	log := &fakeViolationLog{latest: 0, newest: 7, counts: []domain.RLSViolationCount{{ViolationType: "cross_tenant_access", Count: 7}}}
	var out []recorded
	exp := newExporter(log, &out)
	require.NoError(t, exp.Start(context.Background()))

	require.NoError(t, exp.Tick(context.Background()))
	require.NoError(t, exp.Tick(context.Background()))

	assert.Equal(t, []int64{0, 7}, log.afterIDs)
	assert.Len(t, out, 1)
}

func TestRLSViolationExporter_FailedReadKeepsPosition(t *testing.T) {
	log := &fakeViolationLog{latest: 5, countsErr: errors.New("db down")}
	var out []recorded
	exp := newExporter(log, &out)
	require.NoError(t, exp.Start(context.Background()))

	require.Error(t, exp.Tick(context.Background()))
	log.countsErr = nil
	log.newest = 9
	log.counts = []domain.RLSViolationCount{{ViolationType: "cross_tenant_access", Count: 4}}
	require.NoError(t, exp.Tick(context.Background()))

	assert.Equal(t, []int64{5, 5}, log.afterIDs)
	assert.Equal(t, []recorded{{"cross_tenant_access", 4}}, out)
}

func TestRLSViolationExporter_PrunesRowsPastRetention(t *testing.T) {
	log := &fakeViolationLog{}
	var out []recorded
	exp := newExporter(log, &out)
	require.NoError(t, exp.Start(context.Background()))

	before := time.Now().Add(-24 * time.Hour)
	require.NoError(t, exp.Tick(context.Background()))

	require.Len(t, log.pruned, 1)
	assert.WithinDuration(t, before, log.pruned[0], 5*time.Second)
}

type failingStartLog struct{ fakeViolationLog }

func (*failingStartLog) LatestID(context.Context) (int64, error) { return 0, errors.New("db down") }

func TestRLSViolationExporter_StartFailureIsReported(t *testing.T) {
	var out []recorded
	exp := service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log: &failingStartLog{}, Record: func(string, float64) {}, Retention: time.Hour, Logger: &fakeLogger{},
	})
	require.Error(t, exp.Start(context.Background()))
	assert.Empty(t, out)
}

func TestRLSViolationExporter_RunExportsOnEveryIntervalUntilCancelled(t *testing.T) {
	log := &fakeViolationLog{latest: 1, newest: 3, counts: []domain.RLSViolationCount{{ViolationType: "cross_tenant_access", Count: 2}}}
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
		assert.Equal(t, recorded{"cross_tenant_access", 2}, got)
	case <-time.After(2 * time.Second):
		t.Fatal("no export within 2s")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

func TestRLSViolationExporter_RunDoesNotStartWhenThePositionIsUnreadable(t *testing.T) {
	exp := service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log: &failingStartLog{}, Record: func(string, float64) { t.Error("nothing may be recorded") },
		Interval: time.Millisecond, Retention: time.Hour, Logger: &fakeLogger{},
	})
	done := make(chan struct{})
	go func() {
		exp.Run(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run must return when Start fails")
	}
}
