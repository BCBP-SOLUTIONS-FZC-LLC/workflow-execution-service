package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type fakeRetention struct {
	due       []uuid.UUID
	listErr   error
	listedAt  time.Time
	purged    []uuid.UUID
	recorded  []recordedOffboard
	recordErr error
	markErr   error
	onList    func()
}

type recordedOffboard struct {
	tenantID     uuid.UUID
	offboardedAt time.Time
	purgeAfter   time.Time
}

func (f *fakeRetention) RecordOffboarded(_ context.Context, tenantID uuid.UUID, offboardedAt, purgeAfter time.Time) error {
	f.recorded = append(f.recorded, recordedOffboard{tenantID, offboardedAt, purgeAfter})
	return f.recordErr
}

func (f *fakeRetention) ListDue(_ context.Context, now time.Time, _ int) ([]uuid.UUID, error) {
	f.listedAt = now
	if f.onList != nil {
		f.onList()
	}
	return f.due, f.listErr
}

func (f *fakeRetention) MarkPurged(_ context.Context, tenantID uuid.UUID, _ time.Time) error {
	if f.markErr != nil {
		return f.markErr
	}
	f.purged = append(f.purged, tenantID)
	return nil
}

type fakeDataPurger struct {
	script  map[uuid.UUID][]map[string]int64
	errFor  map[uuid.UUID]error
	calls   map[uuid.UUID]int
	limits  []int
	rows    map[uuid.UUID]bool
	rowsErr error
}

func (f *fakeDataPurger) HasRows(_ context.Context, tenantID uuid.UUID) (bool, error) {
	return f.rows[tenantID], f.rowsErr
}

func (f *fakeDataPurger) PurgeBatch(_ context.Context, tenantID uuid.UUID, limit int) (map[string]int64, error) {
	if f.calls == nil {
		f.calls = map[uuid.UUID]int{}
	}
	f.limits = append(f.limits, limit)
	if err := f.errFor[tenantID]; err != nil {
		return nil, err
	}
	i := f.calls[tenantID]
	f.calls[tenantID]++
	steps := f.script[tenantID]
	if i >= len(steps) {
		return map[string]int64{}, nil
	}
	return steps[i], nil
}

func newPurger(r *fakeRetention, d *fakeDataPurger, rec map[string]int64, failed *int) *service.TenantPurger {
	return service.NewTenantPurger(service.TenantPurgerDeps{
		Retention: r, Data: d, Batch: 50, Interval: time.Hour,
		Now:    func() time.Time { return time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC) },
		Record: func(table string, n int64) { rec[table] += n },
		Failed: func() { *failed++ },
	})
}

func TestTenantPurger_PurgesADueTenantInBatchesThenMarksIt(t *testing.T) {
	tenant := uuid.New()
	r := &fakeRetention{due: []uuid.UUID{tenant}}
	d := &fakeDataPurger{script: map[uuid.UUID][]map[string]int64{tenant: {
		{"workflow_task": 50}, {"workflow_task": 7}, {"workflow_instance": 3},
	}}}
	rec, failed := map[string]int64{}, 0

	require.NoError(t, newPurger(r, d, rec, &failed).Tick(context.Background()))

	assert.Equal(t, []uuid.UUID{tenant}, r.purged)
	assert.Equal(t, 4, d.calls[tenant], "three batches with rows, then one that finds the tenant empty")
	assert.Equal(t, map[string]int64{"workflow_task": 57, "workflow_instance": 3}, rec)
	assert.Equal(t, time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC), r.listedAt)
	for _, l := range d.limits {
		assert.Equal(t, 50, l)
	}
	assert.Zero(t, failed)
}

func TestTenantPurger_NothingDueDeletesNothing(t *testing.T) {
	r := &fakeRetention{}
	d := &fakeDataPurger{}
	rec, failed := map[string]int64{}, 0

	require.NoError(t, newPurger(r, d, rec, &failed).Tick(context.Background()))
	assert.Empty(t, d.calls)
	assert.Empty(t, r.purged)
}

func TestTenantPurger_AFailingTenantIsNotMarkedAndDoesNotStopTheOthers(t *testing.T) {
	bad, good := uuid.New(), uuid.New()
	r := &fakeRetention{due: []uuid.UUID{bad, good}}
	d := &fakeDataPurger{
		errFor: map[uuid.UUID]error{bad: errors.New("fk violation")},
		script: map[uuid.UUID][]map[string]int64{good: {{"workflow_instance": 1}}},
	}
	rec, failed := map[string]int64{}, 0

	err := newPurger(r, d, rec, &failed).Tick(context.Background())

	require.Error(t, err)
	assert.Equal(t, []uuid.UUID{good}, r.purged)
	assert.Equal(t, 1, failed)
}

func TestTenantPurger_ADueListFailureIsReturned(t *testing.T) {
	r := &fakeRetention{listErr: errors.New("db down")}
	rec, failed := map[string]int64{}, 0
	require.Error(t, newPurger(r, &fakeDataPurger{}, rec, &failed).Tick(context.Background()))
}

func TestTenantPurger_ACancelledRunLeavesTheTenantUnmarked(t *testing.T) {
	tenant := uuid.New()
	r := &fakeRetention{due: []uuid.UUID{tenant}}
	d := &fakeDataPurger{script: map[uuid.UUID][]map[string]int64{tenant: {{"workflow_task": 50}, {"workflow_task": 50}, {"workflow_task": 50}}}}
	rec, failed := map[string]int64{}, 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := newPurger(r, d, rec, &failed).Tick(ctx)

	require.Error(t, err)
	assert.Empty(t, r.purged)
}

func TestTenantPurger_AFailureToMarkIsReturnedAndTheTenantIsRetriedNextSweep(t *testing.T) {
	tenant := uuid.New()
	r := &fakeRetention{due: []uuid.UUID{tenant}, markErr: errors.New("db down")}
	rec, failed := map[string]int64{}, 0

	err := newPurger(r, &fakeDataPurger{}, rec, &failed).Tick(context.Background())

	require.Error(t, err)
	assert.Equal(t, 1, failed)
}

func TestTenantPurger_RunSweepsAndStopsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sweeps := 0
	r := &fakeRetention{onList: func() { sweeps++; cancel() }}
	rec, failed := map[string]int64{}, 0
	purger := newPurger(r, &fakeDataPurger{}, rec, &failed)

	done := make(chan struct{})
	go func() { purger.Run(ctx); close(done) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after its context was cancelled")
	}
	assert.Equal(t, 1, sweeps, "one sweep at start, none after the cancel")
}

func TestTenantPurger_RunLogsAFailedSweepAndKeepsGoing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &fakeRetention{listErr: errors.New("db down"), onList: cancel}
	rec, failed := map[string]int64{}, 0
	done := make(chan struct{})
	go func() { newPurger(r, &fakeDataPurger{}, rec, &failed).Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestTenantPurger_DefaultsTheClockAndLoggerWhenNoneAreGiven(t *testing.T) {
	tenant := uuid.New()
	r := &fakeRetention{due: []uuid.UUID{tenant}}
	p := service.NewTenantPurger(service.TenantPurgerDeps{
		Retention: r, Data: &fakeDataPurger{}, Batch: 10, Interval: time.Hour,
		Record: func(string, int64) {}, Failed: func() {},
	})

	require.NoError(t, p.Tick(context.Background()))
	assert.WithinDuration(t, time.Now(), r.listedAt, time.Minute)
	assert.Equal(t, []uuid.UUID{tenant}, r.purged)
}

func TestTenantPurger_ATenantWithRowsLeftIsNotMarkedPurged(t *testing.T) {
	tenant := uuid.New()
	r := &fakeRetention{due: []uuid.UUID{tenant}}
	d := &fakeDataPurger{rows: map[uuid.UUID]bool{tenant: true}}
	rec, failed := map[string]int64{}, 0

	err := newPurger(r, d, rec, &failed).Tick(context.Background())

	require.Error(t, err, "a batch that deleted nothing can mean another replica took the rows, not that none are left")
	assert.Empty(t, r.purged)
	assert.Equal(t, 1, failed)
}

func TestTenantPurger_AFailureToCheckForRemainingRowsIsReturned(t *testing.T) {
	tenant := uuid.New()
	r := &fakeRetention{due: []uuid.UUID{tenant}}
	d := &fakeDataPurger{rowsErr: errors.New("db down")}
	rec, failed := map[string]int64{}, 0

	require.Error(t, newPurger(r, d, rec, &failed).Tick(context.Background()))
	assert.Empty(t, r.purged)
}
