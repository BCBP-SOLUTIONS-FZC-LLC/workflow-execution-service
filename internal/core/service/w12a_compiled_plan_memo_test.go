package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

var errW12aPlanFetch = errors.New("definitions answered 502")

type w12aPlanFetch struct{ tenantID, versionID uuid.UUID }

// w12aMemoDefinitions records every fetch with its tenant and version, answers a
// readable plan unless told otherwise, and fails a version it is told to fail.
type w12aMemoDefinitions struct {
	fetches []w12aPlanFetch
	failing map[uuid.UUID]error
	garbage map[uuid.UUID]bool
}

func (d *w12aMemoDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.fetches = append(d.fetches, w12aPlanFetch{tenantID: tenantID, versionID: versionID})
	if err := d.failing[versionID]; err != nil {
		return nil, err
	}
	if d.garbage[versionID] {
		return &port.CompiledWorkflow{Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: "not json"}, nil
	}
	collab := dsl.CompiledCollaboration{
		MainPlan:      "main",
		SchemaVersion: dsl.CurrentSchemaVersion,
		Plans:         []*dsl.CompiledPlan{{Name: "main", Departments: []dsl.DepartmentDef{{ID: "Ops", Stages: []dsl.StageDef{{Type: "approve", NodeID: "T0"}}}}}},
	}
	raw, err := json.Marshal(collab)
	if err != nil {
		return nil, err
	}
	return &port.CompiledWorkflow{Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: string(raw)}, nil
}

const (
	w12aHitsMetric   = "workflow_execution_service_wf_cache_hits_total"
	w12aMissesMetric = "workflow_execution_service_wf_cache_misses_total"
)

func TestCompiledPlanCache_ARepeatedVersionIsServedFromMemoryAndANewOneIsLoadedOnce(t *testing.T) {
	obstest.Init(t)
	defs := &w12aMemoDefinitions{}
	cache := newCompiledPlanCache(defs, noopLogger{})
	tenantID, first, second := uuid.New(), uuid.New(), uuid.New()
	hits, misses := obstest.Sum(t, w12aHitsMetric, nil), obstest.Sum(t, w12aMissesMetric, nil)
	read := func(version uuid.UUID) *dsl.CompiledPlan {
		t.Helper()
		plan, err := cache.mainPlan(context.Background(), tenantID, version)
		require.NoError(t, err)
		require.NotNil(t, plan)
		return plan
	}

	firstPlan := read(first)
	assert.Equal(t, []w12aPlanFetch{{tenantID: tenantID, versionID: first}}, defs.fetches, "a version read for the first time is loaded for its tenant")
	assert.Equal(t, misses+1, obstest.Sum(t, w12aMissesMetric, nil))
	assert.Equal(t, hits, obstest.Sum(t, w12aHitsMetric, nil))

	assert.Same(t, firstPlan, read(first), "the repeat is the plan already held")
	assert.Len(t, defs.fetches, 1, "and loads nothing")
	assert.Equal(t, misses+1, obstest.Sum(t, w12aMissesMetric, nil))
	assert.Equal(t, hits+1, obstest.Sum(t, w12aHitsMetric, nil))

	secondPlan := read(second)
	assert.NotSame(t, firstPlan, secondPlan)
	assert.Equal(t, []w12aPlanFetch{{tenantID: tenantID, versionID: first}, {tenantID: tenantID, versionID: second}}, defs.fetches, "another version is its own miss and load")
	assert.Equal(t, misses+2, obstest.Sum(t, w12aMissesMetric, nil))
	assert.Equal(t, hits+1, obstest.Sum(t, w12aHitsMetric, nil))

	assert.Same(t, firstPlan, read(first))
	assert.Same(t, secondPlan, read(second))
	assert.Len(t, defs.fetches, 2, "both versions are now held")
	assert.Equal(t, misses+2, obstest.Sum(t, w12aMissesMetric, nil))
	assert.Equal(t, hits+3, obstest.Sum(t, w12aHitsMetric, nil))
}

func TestCompiledPlanCache_AVersionThatCouldNotBeLoaded_IsAMissEachTimeAndNeverHeld(t *testing.T) {
	obstest.Init(t)
	version, garbled := uuid.New(), uuid.New()
	defs := &w12aMemoDefinitions{failing: map[uuid.UUID]error{version: errW12aPlanFetch}, garbage: map[uuid.UUID]bool{garbled: true}}
	cache := newCompiledPlanCache(defs, noopLogger{})
	tenantID := uuid.New()
	hits, misses := obstest.Sum(t, w12aHitsMetric, nil), obstest.Sum(t, w12aMissesMetric, nil)

	for attempt := 1; attempt <= 2; attempt++ {
		_, err := cache.mainPlan(context.Background(), tenantID, version)
		require.ErrorIs(t, err, errW12aPlanFetch)
		assert.Len(t, defs.fetches, attempt, "a failed load is tried again, not remembered")
	}
	assert.Equal(t, misses+2, obstest.Sum(t, w12aMissesMetric, nil))
	assert.Equal(t, hits, obstest.Sum(t, w12aHitsMetric, nil), "a failure is never a hit")

	_, err := cache.mainPlan(context.Background(), tenantID, garbled)
	require.Error(t, err)
	_, err = cache.mainPlan(context.Background(), tenantID, garbled)
	require.Error(t, err)
	assert.Equal(t, misses+4, obstest.Sum(t, w12aMissesMetric, nil), "a plan that cannot be read is not held either")
	assert.Equal(t, hits, obstest.Sum(t, w12aHitsMetric, nil))

	delete(defs.failing, version)
	plan, err := cache.mainPlan(context.Background(), tenantID, version)
	require.NoError(t, err)
	again, err := cache.mainPlan(context.Background(), tenantID, version)
	require.NoError(t, err)
	assert.Same(t, plan, again)
	assert.Equal(t, misses+5, obstest.Sum(t, w12aMissesMetric, nil), "the load that works is the next miss")
	assert.Equal(t, hits+1, obstest.Sum(t, w12aHitsMetric, nil), "and is then held")
}
