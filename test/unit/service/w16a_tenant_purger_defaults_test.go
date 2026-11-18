package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var errW16aPurgeRefused = errors.New("w16a: purge refused")

type w16aMarkCall struct {
	tenantID uuid.UUID
	version  int64
	at       time.Time
}

type w16aFailureCall struct {
	tenantID uuid.UUID
	version  int64
	attempts int
	next     time.Time
}

type w16aRetention struct {
	port.TenantRetentionRepository
	due      []port.TenantPurgeDue
	listedAt time.Time
	marks    []w16aMarkCall
	failures []w16aFailureCall
}

func (r *w16aRetention) ListDue(_ context.Context, now time.Time, _ int) ([]port.TenantPurgeDue, error) {
	r.listedAt = now
	return r.due, nil
}

func (r *w16aRetention) MarkPurged(_ context.Context, tenantID uuid.UUID, version int64, at time.Time) (int64, error) {
	r.marks = append(r.marks, w16aMarkCall{tenantID, version, at})
	return version + 1, nil
}

func (r *w16aRetention) RecordFailure(_ context.Context, tenantID uuid.UUID, version int64, attempts int, next time.Time) error {
	r.failures = append(r.failures, w16aFailureCall{tenantID, version, attempts, next})
	return nil
}

type w16aPurgeCall struct {
	tenantID uuid.UUID
	version  int64
	limit    int
}

type w16aPurgeData struct {
	port.TenantDataPurger
	errFor map[uuid.UUID]error
	rows   map[uuid.UUID][]map[string]int64
	calls  []w16aPurgeCall
	used   map[uuid.UUID]int
}

func (d *w16aPurgeData) PurgeBatch(_ context.Context, tenantID uuid.UUID, version int64, limit int) (map[string]int64, error) {
	d.calls = append(d.calls, w16aPurgeCall{tenantID, version, limit})
	if err := d.errFor[tenantID]; err != nil {
		return nil, err
	}
	if d.used == nil {
		d.used = map[uuid.UUID]int{}
	}
	i := d.used[tenantID]
	d.used[tenantID]++
	if i >= len(d.rows[tenantID]) {
		return map[string]int64{}, nil
	}
	return d.rows[tenantID][i], nil
}

func (d *w16aPurgeData) HasRows(context.Context, uuid.UUID) (bool, error) { return false, nil }

func TestTenantPurger_WithoutAClockOrLoggerEveryOutcomeStillRunsAgainstTheWallClock(t *testing.T) {
	purged, failing, skipped := uuid.New(), uuid.New(), uuid.New()
	retention := &w16aRetention{due: []port.TenantPurgeDue{
		{TenantID: purged, RecordVersion: 3},
		{TenantID: failing, RecordVersion: 5, Attempts: 1},
		{TenantID: skipped, RecordVersion: 8},
	}}
	data := &w16aPurgeData{
		errFor: map[uuid.UUID]error{failing: errW16aPurgeRefused, skipped: port.ErrTenantRetentionChanged},
		rows:   map[uuid.UUID][]map[string]int64{purged: {{"workflow_task": 4, "workflow_instance": 1}}},
	}
	recorded := map[string]int64{}
	failedCalls := 0
	purger := service.NewTenantPurger(service.TenantPurgerDeps{
		Retention: retention, Data: data, Batch: 25, Interval: time.Hour,
		Record: func(table string, n int64) { recorded[table] += n },
		Failed: func() { failedCalls++ },
	})

	before := time.Now()
	var err error
	require.NotPanics(t, func() { err = purger.Tick(context.Background()) }, "the info and error log lines run on the no-op logger")
	after := time.Now()

	require.ErrorIs(t, err, errW16aPurgeRefused)
	assert.NotErrorIs(t, err, port.ErrTenantRetentionChanged, "a tenant another replica is purging is skipped, not reported")
	assert.False(t, retention.listedAt.Before(before) || retention.listedAt.After(after), "the due list is read at the wall clock")

	assert.Equal(t, map[string]int64{"workflow_task": 4, "workflow_instance": 1}, recorded)
	assert.Equal(t, 1, failedCalls, "only the failing tenant counts as a failure")

	assert.Equal(t, []w16aPurgeCall{
		{purged, 3, 25}, {purged, 3, 25}, {failing, 5, 25}, {skipped, 8, 25},
	}, data.calls)

	require.Len(t, retention.marks, 1)
	assert.Equal(t, purged, retention.marks[0].tenantID)
	assert.Equal(t, int64(3), retention.marks[0].version)
	assert.False(t, retention.marks[0].at.Before(before) || retention.marks[0].at.After(after), "the purge time is the wall clock")

	require.Len(t, retention.failures, 1)
	failure := retention.failures[0]
	assert.Equal(t, failing, failure.tenantID)
	assert.Equal(t, int64(5), failure.version)
	assert.Equal(t, 2, failure.attempts)
	assert.False(t, failure.next.Before(before.Add(2*time.Hour)) || failure.next.After(after.Add(2*time.Hour)),
		"the retry is scheduled from the wall clock, one doubling after the interval")
}
