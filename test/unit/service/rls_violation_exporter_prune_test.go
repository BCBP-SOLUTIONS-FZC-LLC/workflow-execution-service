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

func TestRLSViolationExporter_Tick_AFailedPruneKeepsTheCountsAndThePosition(t *testing.T) {
	errPrune := errors.New("prune failed")
	log := &windowViolationLog{now: 100 * time.Second, pruneErr: errPrune}
	var out []recorded
	exp := service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log:       log,
		Record:    func(violationType string, n float64) { out = append(out, recorded{violationType, n}) },
		Retention: 24 * time.Hour,
		Logger:    &fakeLogger{},
	})
	require.NoError(t, exp.Start(context.Background()))
	log.rows = append(log.rows,
		&windowRow{"cross_tenant_access", 110 * time.Second, true},
		&windowRow{"cross_tenant_access", 120 * time.Second, true},
		&windowRow{"missing_or_invalid_guc", 130 * time.Second, true},
	)

	before := time.Now()
	log.now = 500 * time.Second
	err := exp.Tick(context.Background())
	after := time.Now()

	require.ErrorIs(t, err, errPrune)
	assert.EqualError(t, err, "rls violation prune: prune failed")
	assert.ElementsMatch(t, []recorded{{"cross_tenant_access", 2}, {"missing_or_invalid_guc", 1}}, out, "the counts read before the failed prune are recorded")
	require.Len(t, log.pruned, 1, "the prune was attempted")
	assert.False(t, log.pruned[0].Before(before.Add(-24*time.Hour)), "the cutoff is not older than the retention before the call")
	assert.False(t, log.pruned[0].After(after.Add(-24*time.Hour)), "the cutoff is not newer than the retention before the end of the call")

	log.rows = append(log.rows, &windowRow{"cross_tenant_access", 480 * time.Second, true})
	log.now = 700 * time.Second
	err = exp.Tick(context.Background())

	require.ErrorIs(t, err, errPrune)
	assert.ElementsMatch(t, []recorded{{"cross_tenant_access", 2}, {"missing_or_invalid_guc", 1}, {"cross_tenant_access", 1}}, out,
		"the rows of the failed tick are not counted again and the new row is counted once")
}
