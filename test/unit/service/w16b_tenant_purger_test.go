package service_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var (
	errW16bListDown  = errors.New("w16b due list down")
	errW16bBatchDown = errors.New("w16b purge batch down")
	errW16bMarkDown  = errors.New("w16b mark down")
)

var w16bPurgerNow = time.Date(2026, 11, 18, 3, 0, 0, 0, time.UTC)

type w16bListCall struct {
	now   time.Time
	limit int
}

type w16bMarkCall struct {
	tenantID uuid.UUID
	version  int64
	at       time.Time
}

type w16bFailureCall struct {
	tenantID uuid.UUID
	version  int64
	attempts int
	next     time.Time
}

// w16bRetention answers ListDue from a script (one answer per sweep, nothing once the script ends)
// and records every call it gets, with the tenant, version and arguments.
type w16bRetention struct {
	mu        sync.Mutex
	dueBySwp  [][]port.TenantPurgeDue
	listErrs  []error
	markErrs  []error
	listCalls []w16bListCall
	marks     []w16bMarkCall
	purged    []uuid.UUID
	failures  []w16bFailureCall
	onPurged  func(tenantID uuid.UUID)
	onList    func()
}

func (r *w16bRetention) RecordOffboarded(context.Context, uuid.UUID, time.Time, time.Time) error {
	return nil
}
func (r *w16bRetention) RecordReturned(context.Context, uuid.UUID, time.Time) error { return nil }

func (r *w16bRetention) ListDue(_ context.Context, now time.Time, limit int) ([]port.TenantPurgeDue, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listCalls = append(r.listCalls, w16bListCall{now, limit})
	sweep := len(r.listCalls) - 1
	if r.onList != nil {
		r.onList()
	}
	if sweep < len(r.listErrs) && r.listErrs[sweep] != nil {
		return nil, r.listErrs[sweep]
	}
	if sweep < len(r.dueBySwp) {
		return r.dueBySwp[sweep], nil
	}
	return nil, nil
}

func (r *w16bRetention) MarkPurged(_ context.Context, tenantID uuid.UUID, version int64, at time.Time) (int64, error) {
	r.mu.Lock()
	r.marks = append(r.marks, w16bMarkCall{tenantID, version, at})
	call := len(r.marks) - 1
	var err error
	if call < len(r.markErrs) {
		err = r.markErrs[call]
	}
	hook := r.onPurged
	if err == nil {
		r.purged = append(r.purged, tenantID)
	}
	r.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if hook != nil {
		hook(tenantID)
	}
	return version + 1, nil
}

func (r *w16bRetention) RecordFailure(_ context.Context, tenantID uuid.UUID, version int64, attempts int, next time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, w16bFailureCall{tenantID, version, attempts, next})
	return nil
}

type w16bSnapshot struct {
	listCalls []w16bListCall
	marks     []w16bMarkCall
	purged    []uuid.UUID
	failures  []w16bFailureCall
}

func (r *w16bRetention) snapshot() w16bSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return w16bSnapshot{
		listCalls: append([]w16bListCall(nil), r.listCalls...),
		marks:     append([]w16bMarkCall(nil), r.marks...),
		purged:    append([]uuid.UUID(nil), r.purged...),
		failures:  append([]w16bFailureCall(nil), r.failures...),
	}
}

type w16bPurgeCall struct {
	tenantID uuid.UUID
	version  int64
	limit    int
}

// w16bPurgeData holds a number of rows per tenant in one table and deletes up to the batch limit per call;
// failOn maps a 1-based PurgeBatch call number to the error that call returns.
type w16bPurgeData struct {
	mu      sync.Mutex
	rows    map[uuid.UUID]int64
	failOn  map[int]error
	calls   []w16bPurgeCall
	hasRows []uuid.UUID
}

func (d *w16bPurgeData) PurgeBatch(_ context.Context, tenantID uuid.UUID, version int64, limit int) (map[string]int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, w16bPurgeCall{tenantID, version, limit})
	if err := d.failOn[len(d.calls)]; err != nil {
		return nil, err
	}
	n := min(int64(limit), d.rows[tenantID])
	d.rows[tenantID] -= n
	if n == 0 {
		return map[string]int64{}, nil
	}
	return map[string]int64{"workflow_task": n}, nil
}

func (d *w16bPurgeData) HasRows(_ context.Context, tenantID uuid.UUID) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.hasRows = append(d.hasRows, tenantID)
	return d.rows[tenantID] > 0, nil
}

func (d *w16bPurgeData) remaining(tenantID uuid.UUID) int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.rows[tenantID]
}

type w16bTableCount struct {
	table string
	n     int64
}

type w16bPurgerRig struct {
	purger  *service.TenantPurger
	ret     *w16bRetention
	data    *w16bPurgeData
	logger  *w16bLogger
	mu      sync.Mutex
	deleted []w16bTableCount
	failed  int
}

func w16bNewPurger(ret *w16bRetention, data *w16bPurgeData, interval time.Duration) *w16bPurgerRig {
	rig := &w16bPurgerRig{ret: ret, data: data, logger: &w16bLogger{}}
	rig.purger = service.NewTenantPurger(service.TenantPurgerDeps{
		Retention: ret, Data: data, Batch: 50, Interval: interval,
		Now: func() time.Time { return w16bPurgerNow },
		Record: func(table string, n int64) {
			rig.mu.Lock()
			defer rig.mu.Unlock()
			rig.deleted = append(rig.deleted, w16bTableCount{table, n})
		},
		Failed: func() {
			rig.mu.Lock()
			defer rig.mu.Unlock()
			rig.failed++
		},
		Logger: rig.logger,
	})
	return rig
}

func (g *w16bPurgerRig) recorded() ([]w16bTableCount, int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]w16bTableCount(nil), g.deleted...), g.failed
}

func w16bRunUntilStopped(t *testing.T, ctx context.Context, cancel context.CancelFunc, p *service.TenantPurger) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		cancel()
		<-done
		t.Fatal("Run did not stop")
	}
}

func TestTenantPurger_RunSweepsAgainAfterEachIntervalAndStopsOnCancel(t *testing.T) {
	t.Parallel()

	first, second := uuid.New(), uuid.New()
	ret := &w16bRetention{dueBySwp: [][]port.TenantPurgeDue{
		{{TenantID: first, RecordVersion: 4}},
		{{TenantID: second, RecordVersion: 9}},
	}}
	data := &w16bPurgeData{rows: map[uuid.UUID]int64{first: 10, second: 10}}
	rig := w16bNewPurger(ret, data, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ret.onPurged = func(id uuid.UUID) {
		if id == second {
			cancel()
		}
	}

	w16bRunUntilStopped(t, ctx, cancel, rig.purger)

	got := ret.snapshot()
	assert.Equal(t, []w16bMarkCall{{first, 4, w16bPurgerNow}, {second, 9, w16bPurgerNow}}, got.marks, "the sweep at start purged the first tenant, the sweep one interval later the second")
	assert.Equal(t, []uuid.UUID{first, second}, got.purged)
	for _, c := range got.listCalls {
		assert.Equal(t, w16bListCall{w16bPurgerNow, 100}, c)
	}
	assert.GreaterOrEqual(t, len(got.listCalls), 2)
	deleted, failed := rig.recorded()
	assert.Equal(t, []w16bTableCount{{"workflow_task", 10}, {"workflow_task", 10}}, deleted)
	assert.Zero(t, failed)
}

func TestTenantPurger_RunSweepsOnceAtStartBeforeTheFirstInterval(t *testing.T) {
	t.Parallel()

	tenant := uuid.New()
	ret := &w16bRetention{dueBySwp: [][]port.TenantPurgeDue{{{TenantID: tenant, RecordVersion: 8}}}}
	data := &w16bPurgeData{rows: map[uuid.UUID]int64{tenant: 5}}
	rig := w16bNewPurger(ret, data, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ret.onList = cancel

	w16bRunUntilStopped(t, ctx, cancel, rig.purger)

	got := ret.snapshot()
	assert.Equal(t, []w16bListCall{{w16bPurgerNow, 100}}, got.listCalls, "one sweep, at start, before any interval passed")
	assert.Empty(t, got.marks, "the cancelled sweep marks no tenant")
}

func TestTenantPurger_RunLogsAFailedSweepAndSweepsAgainOnTheNextInterval(t *testing.T) {
	t.Parallel()

	tenant := uuid.New()
	ret := &w16bRetention{
		listErrs: []error{errW16bListDown},
		dueBySwp: [][]port.TenantPurgeDue{nil, {{TenantID: tenant, RecordVersion: 2}}},
	}
	data := &w16bPurgeData{rows: map[uuid.UUID]int64{tenant: 5}}
	rig := w16bNewPurger(ret, data, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ret.onPurged = func(uuid.UUID) { cancel() }

	w16bRunUntilStopped(t, ctx, cancel, rig.purger)

	entries := rig.logger.errorEntries()
	require.Len(t, entries, 1, "only the failed sweep is logged")
	assert.Equal(t, "tenant purge sweep failed", entries[0].msg)
	cause, ok := entries[0].fields["error"].(string)
	require.True(t, ok)
	assert.Contains(t, cause, errW16bListDown.Error())
	got := ret.snapshot()
	assert.Equal(t, []w16bMarkCall{{tenant, 2, w16bPurgerNow}}, got.marks, "the loop went on after the failed sweep")
}

func TestTenantPurger_ADueListFailureTouchesNoTenantAndCountsNoFailure(t *testing.T) {
	t.Parallel()

	ret := &w16bRetention{listErrs: []error{errW16bListDown}}
	data := &w16bPurgeData{rows: map[uuid.UUID]int64{}}
	rig := w16bNewPurger(ret, data, time.Hour)

	err := rig.purger.Tick(context.Background())

	require.ErrorIs(t, err, errW16bListDown)
	assert.ErrorContains(t, err, "list tenants due for purge")
	got := ret.snapshot()
	assert.Equal(t, []w16bListCall{{w16bPurgerNow, 100}}, got.listCalls)
	assert.Empty(t, got.marks)
	assert.Empty(t, got.failures, "no failure is recorded: no tenant was read")
	assert.Empty(t, data.calls, "no tenant data is touched")
	deleted, failed := rig.recorded()
	assert.Empty(t, deleted)
	assert.Zero(t, failed)
}

func TestTenantPurger_BackoffAfterAFailureDoublesFromTheIntervalUpToSevenDays(t *testing.T) {
	t.Parallel()

	hour := time.Hour
	cases := []struct {
		name         string
		interval     time.Duration
		storedTries  int
		wantAttempts int
		wantDelay    time.Duration
	}{
		{"attempts below zero wait one interval", hour, -5, -4, 1 * hour},
		{"attempts of zero wait one interval", hour, -1, 0, 1 * hour},
		{"second attempt waits two intervals", hour, 1, 2, 2 * hour},
		{"fourth attempt waits eight intervals", hour, 3, 4, 8 * hour},
		{"fifth attempt", hour, 4, 5, 16 * hour},
		{"sixth attempt", hour, 5, 6, 32 * hour},
		{"seventh attempt", hour, 6, 7, 64 * hour},
		{"eighth attempt", hour, 7, 8, 128 * hour},
		{"ninth attempt is capped at seven days", hour, 8, 9, 168 * hour},
		{"tenth attempt stays at seven days", hour, 9, 10, 168 * hour},
		{"an interval above seven days is cut to seven days", 240 * hour, 0, 1, 168 * hour},
		{"a doubled interval above seven days stays at seven days", 100 * hour, 1, 2, 168 * hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tenant := uuid.New()
			ret := &w16bRetention{dueBySwp: [][]port.TenantPurgeDue{{{TenantID: tenant, Attempts: tc.storedTries, RecordVersion: 6}}}}
			data := &w16bPurgeData{rows: map[uuid.UUID]int64{tenant: 1}, failOn: map[int]error{1: errW16bBatchDown}}
			rig := w16bNewPurger(ret, data, tc.interval)

			err := rig.purger.Tick(context.Background())

			require.ErrorIs(t, err, errW16bBatchDown)
			assert.Equal(t, []w16bFailureCall{{tenant, 6, tc.wantAttempts, w16bPurgerNow.Add(tc.wantDelay)}}, ret.snapshot().failures)
		})
	}
}

func TestTenantPurger_ASecondBatchFailureKeepsTheFirstBatchDeletedAndTheNextSweepResumes(t *testing.T) {
	t.Parallel()

	tenant := uuid.New()
	due := []port.TenantPurgeDue{{TenantID: tenant, RecordVersion: 7}}
	ret := &w16bRetention{dueBySwp: [][]port.TenantPurgeDue{due, due}}
	data := &w16bPurgeData{rows: map[uuid.UUID]int64{tenant: 120}, failOn: map[int]error{2: errW16bBatchDown}}
	rig := w16bNewPurger(ret, data, time.Hour)

	err := rig.purger.Tick(context.Background())

	require.ErrorIs(t, err, errW16bBatchDown)
	assert.ErrorContains(t, err, "purge tenant "+tenant.String())
	deleted, failed := rig.recorded()
	assert.Equal(t, []w16bTableCount{{"workflow_task", 50}}, deleted, "the first batch is deleted and recorded")
	assert.Equal(t, int64(70), data.remaining(tenant))
	assert.Empty(t, ret.snapshot().marks, "an unfinished tenant is not marked purged")
	assert.Equal(t, 1, failed)
	assert.Equal(t, []w16bFailureCall{{tenant, 7, 1, w16bPurgerNow.Add(time.Hour)}}, ret.snapshot().failures)

	require.NoError(t, rig.purger.Tick(context.Background()))

	deleted, failed = rig.recorded()
	assert.Equal(t, []w16bTableCount{{"workflow_task", 50}, {"workflow_task", 50}, {"workflow_task", 20}}, deleted,
		"the rerun deletes only what the first sweep left")
	assert.Equal(t, 1, failed)
	assert.Equal(t, []w16bMarkCall{{tenant, 7, w16bPurgerNow}}, ret.snapshot().marks)
	for _, c := range data.calls {
		assert.Equal(t, w16bPurgeCall{tenant, 7, 50}, c)
	}
}

func TestTenantPurger_AFailedMarkIsRetriedOnTheNextSweepWithoutDeletingAgain(t *testing.T) {
	t.Parallel()

	tenant := uuid.New()
	due := []port.TenantPurgeDue{{TenantID: tenant, RecordVersion: 3}}
	ret := &w16bRetention{dueBySwp: [][]port.TenantPurgeDue{due, due}, markErrs: []error{errW16bMarkDown, nil}}
	data := &w16bPurgeData{rows: map[uuid.UUID]int64{tenant: 30}}
	rig := w16bNewPurger(ret, data, time.Hour)

	err := rig.purger.Tick(context.Background())

	require.ErrorIs(t, err, errW16bMarkDown)
	assert.ErrorContains(t, err, "mark tenant "+tenant.String()+" purged")
	deleted, failed := rig.recorded()
	assert.Equal(t, []w16bTableCount{{"workflow_task", 30}}, deleted)
	assert.Equal(t, 1, failed)
	assert.Empty(t, ret.snapshot().purged)

	require.NoError(t, rig.purger.Tick(context.Background()))

	got := ret.snapshot()
	assert.Equal(t, []w16bMarkCall{{tenant, 3, w16bPurgerNow}, {tenant, 3, w16bPurgerNow}}, got.marks, "the mark is attempted again for the same tenant and version")
	assert.Equal(t, []uuid.UUID{tenant}, got.purged)
	assert.Equal(t, []uuid.UUID{tenant, tenant}, data.hasRows, "each sweep checks the empty pass for remaining rows")
	deleted, failed = rig.recorded()
	assert.Equal(t, []w16bTableCount{{"workflow_task", 30}}, deleted, "the rerun finds an empty pass and records no deletion")
	assert.Equal(t, 1, failed)
}
