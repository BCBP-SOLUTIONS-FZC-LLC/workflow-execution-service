package service_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type csbPurgeMark struct {
	tenantID uuid.UUID
	version  int64
}

// csbPagedRetention lists the tenants not yet purged, at most limit of them
// per call, in a fixed order, and records every limit it was asked for.
type csbPagedRetention struct {
	port.TenantRetentionRepository
	all    []port.TenantPurgeDue
	limits []int
	marked []csbPurgeMark
}

func (r *csbPagedRetention) ListDue(_ context.Context, _ time.Time, limit int) ([]port.TenantPurgeDue, error) {
	r.limits = append(r.limits, limit)
	done := map[uuid.UUID]bool{}
	for _, m := range r.marked {
		done[m.tenantID] = true
	}
	var out []port.TenantPurgeDue
	for _, d := range r.all {
		if len(out) == limit {
			break
		}
		if !done[d.TenantID] {
			out = append(out, d)
		}
	}
	return out, nil
}

func (r *csbPagedRetention) MarkPurged(_ context.Context, tenantID uuid.UUID, version int64, _ time.Time) (int64, error) {
	r.marked = append(r.marked, csbPurgeMark{tenantID, version})
	return version + 1, nil
}

// csbEmptyTenants has no rows for any tenant, so a purge pass finds nothing to delete.
type csbEmptyTenants struct {
	port.TenantDataPurger
	passes []csbPurgeMark
}

func (p *csbEmptyTenants) PurgeBatch(_ context.Context, tenantID uuid.UUID, version int64, _ int) (map[string]int64, error) {
	p.passes = append(p.passes, csbPurgeMark{tenantID, version})
	return map[string]int64{}, nil
}

func (p *csbEmptyTenants) HasRows(context.Context, uuid.UUID) (bool, error) { return false, nil }

func TestTenantPurger_Tick_OnlyOnePageOfDueTenantsIsPurgedPerTick(t *testing.T) {
	const due = 150
	all := make([]port.TenantPurgeDue, due)
	wantMarks := make([]csbPurgeMark, due)
	for i := range all {
		all[i] = port.TenantPurgeDue{TenantID: uuid.New(), RecordVersion: int64(i + 1)}
		wantMarks[i] = csbPurgeMark{all[i].TenantID, int64(i + 1)}
	}
	retention := &csbPagedRetention{all: all}
	data := &csbEmptyTenants{}
	purger := service.NewTenantPurger(service.TenantPurgerDeps{
		Retention: retention, Data: data, Batch: 50, Interval: time.Hour,
		Now:    func() time.Time { return time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC) },
		Record: func(string, int64) {},
		Failed: func() { t.Error("no tenant fails in this test") },
	})

	require.NoError(t, purger.Tick(context.Background()))

	assert.Equal(t, []int{100}, retention.limits, "a tick asks for 100 tenants at most")
	assert.Equal(t, wantMarks[:100], retention.marked, "the first 100 listed are purged, in order, each at its own record version")
	assert.Equal(t, wantMarks[:100], data.passes[:100])

	require.NoError(t, purger.Tick(context.Background()))

	assert.Equal(t, []int{100, 100}, retention.limits)
	assert.Equal(t, wantMarks, retention.marked, "the next tick purges the 50 that waited")
}

// csbFailingRetention lists the given tenants and fails RecordFailure with err.
type csbFailingRetention struct {
	port.TenantRetentionRepository
	due        []port.TenantPurgeDue
	recordErr  error
	failures   []csbRecordedFailure
	markedDone []uuid.UUID
}

type csbRecordedFailure struct {
	tenantID uuid.UUID
	version  int64
	attempts int
}

func (r *csbFailingRetention) ListDue(context.Context, time.Time, int) ([]port.TenantPurgeDue, error) {
	return r.due, nil
}

func (r *csbFailingRetention) RecordFailure(_ context.Context, tenantID uuid.UUID, version int64, attempts int, _ time.Time) error {
	r.failures = append(r.failures, csbRecordedFailure{tenantID, version, attempts})
	return r.recordErr
}

func (r *csbFailingRetention) MarkPurged(_ context.Context, tenantID uuid.UUID, version int64, _ time.Time) (int64, error) {
	r.markedDone = append(r.markedDone, tenantID)
	return version + 1, nil
}

// csbBrokenTenant fails the purge of one tenant and finds nothing to delete for the rest.
type csbBrokenTenant struct {
	port.TenantDataPurger
	broken uuid.UUID
	err    error
}

func (p *csbBrokenTenant) PurgeBatch(_ context.Context, tenantID uuid.UUID, _ int64, _ int) (map[string]int64, error) {
	if tenantID == p.broken {
		return nil, p.err
	}
	return map[string]int64{}, nil
}

func (p *csbBrokenTenant) HasRows(context.Context, uuid.UUID) (bool, error) { return false, nil }

func TestTenantPurger_Tick_AFailureThatCannotBeRecordedIsJoinedUnlessTheRecordChanged(t *testing.T) {
	errPurge := errors.New("fk violation")
	errRecord := errors.New("retention write failed")

	cases := []struct {
		name          string
		recordErr     error
		wantRecordErr error
	}{
		{"a failing RecordFailure is part of the result", errRecord, errRecord},
		{"a record that changed in the meantime is dropped silently", fmt.Errorf("record failure: %w", port.ErrTenantRetentionChanged), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad, good := uuid.New(), uuid.New()
			retention := &csbFailingRetention{
				due:       []port.TenantPurgeDue{{TenantID: bad, Attempts: 2, RecordVersion: 7}, {TenantID: good, RecordVersion: 1}},
				recordErr: tc.recordErr,
			}
			failed := 0
			purger := service.NewTenantPurger(service.TenantPurgerDeps{
				Retention: retention, Data: &csbBrokenTenant{broken: bad, err: errPurge}, Batch: 50, Interval: time.Hour,
				Now:    func() time.Time { return time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC) },
				Record: func(string, int64) {},
				Failed: func() { failed++ },
			})

			err := purger.Tick(context.Background())

			require.ErrorIs(t, err, errPurge, "the purge failure is always reported")
			assert.Equal(t, []csbRecordedFailure{{bad, 7, 3}}, retention.failures, "the failure is recorded for the tenant at its version with one more attempt")
			assert.Equal(t, []uuid.UUID{good}, retention.markedDone, "the loop goes on to the next tenant")
			assert.Equal(t, 1, failed)
			if tc.wantRecordErr != nil {
				require.ErrorIs(t, err, tc.wantRecordErr)
				assert.ErrorContains(t, err, fmt.Sprintf("record tenant %s purge failure: %s", bad, tc.wantRecordErr))
				return
			}
			assert.NotErrorIs(t, err, port.ErrTenantRetentionChanged)
			assert.NotContains(t, err.Error(), "record tenant")
		})
	}
}
