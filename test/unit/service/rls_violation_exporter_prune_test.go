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

type csbViolationRow struct {
	id            int64
	violationType string
}

// csbPruneFailLog is a violation log of numbered rows that fails every prune.
// CountsAfter counts the rows numbered above the position it is given, as the
// real log does, so an exporter that kept an old position would count rows again.
type csbPruneFailLog struct {
	rows     []csbViolationRow
	pruneErr error
	afterIDs []int64
	cutoffs  []time.Time
}

func (l *csbPruneFailLog) LatestID(context.Context) (int64, error) {
	var latest int64
	for _, r := range l.rows {
		latest = max(latest, r.id)
	}
	return latest, nil
}

func (l *csbPruneFailLog) CountsAfter(_ context.Context, afterID int64) ([]domain.RLSViolationCount, int64, error) {
	l.afterIDs = append(l.afterIDs, afterID)
	newest := afterID
	byType := map[string]int64{}
	var order []string
	for _, r := range l.rows {
		if r.id <= afterID {
			continue
		}
		if _, ok := byType[r.violationType]; !ok {
			order = append(order, r.violationType)
		}
		byType[r.violationType]++
		newest = max(newest, r.id)
	}
	var counts []domain.RLSViolationCount
	for _, typ := range order {
		counts = append(counts, domain.RLSViolationCount{ViolationType: typ, Count: byType[typ]})
	}
	return counts, newest, nil
}

func (l *csbPruneFailLog) PruneBefore(_ context.Context, cutoff time.Time) (int64, error) {
	l.cutoffs = append(l.cutoffs, cutoff)
	return 0, l.pruneErr
}

func TestRLSViolationExporter_Tick_AFailedPruneKeepsTheCountsAndThePosition(t *testing.T) {
	errPrune := errors.New("prune failed")
	log := &csbPruneFailLog{pruneErr: errPrune, rows: []csbViolationRow{{40, "old"}}}
	var out []recorded
	exp := service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log:       log,
		Record:    func(violationType string, n float64) { out = append(out, recorded{violationType, n}) },
		Retention: 24 * time.Hour,
		Logger:    &fakeLogger{},
	})
	require.NoError(t, exp.Start(context.Background()))
	log.rows = append(log.rows,
		csbViolationRow{41, "cross_tenant_access"}, csbViolationRow{42, "cross_tenant_access"}, csbViolationRow{43, "cross_tenant_access"},
		csbViolationRow{44, "missing_or_invalid_guc"}, csbViolationRow{45, "missing_or_invalid_guc"},
	)

	before := time.Now()
	err := exp.Tick(context.Background())
	after := time.Now()

	require.ErrorIs(t, err, errPrune)
	assert.EqualError(t, err, "rls violation prune: prune failed")
	assert.ElementsMatch(t, []recorded{{"cross_tenant_access", 3}, {"missing_or_invalid_guc", 2}}, out, "the counts read before the failed prune are recorded")
	require.Len(t, log.cutoffs, 1, "the prune was attempted")
	assert.False(t, log.cutoffs[0].Before(before.Add(-24*time.Hour)), "the cutoff is not older than the retention before the call")
	assert.False(t, log.cutoffs[0].After(after.Add(-24*time.Hour)), "the cutoff is not newer than the retention before the end of the call")

	log.rows = append(log.rows, csbViolationRow{46, "cross_tenant_access"})
	err = exp.Tick(context.Background())

	require.ErrorIs(t, err, errPrune)
	assert.Equal(t, []int64{40, 45}, log.afterIDs, "the next tick reads after the newest row of the failed tick")
	assert.ElementsMatch(t, []recorded{{"cross_tenant_access", 3}, {"missing_or_invalid_guc", 2}, {"cross_tenant_access", 1}}, out,
		"the rows of the failed tick are not counted again and the new row is counted once")
}
