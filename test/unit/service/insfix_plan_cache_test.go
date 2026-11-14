package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type insfixCountingDefinitions struct {
	inner *fakeDefinitionClient
	reads []uuid.UUID
}

func (d *insfixCountingDefinitions) GetCompiledWorkflow(ctx context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.reads = append(d.reads, tenantID)
	return d.inner.GetCompiledWorkflow(ctx, tenantID, versionID)
}

func TestAnswerOutsideWait_ASharedCacheEntryThatIsNotACompiledPlanIsReadAgainAndOverwritten(t *testing.T) {
	entries := map[string]string{
		"null":                       `null`,
		"empty object":               `{}`,
		"plan text is empty":         `{"WorkflowID":"00000000-0000-0000-0000-000000000000","CompiledPlanJSON":""}`,
		"plan text is not json":      `{"CompiledPlanJSON":"not json"}`,
		"plan text has no main plan": `{"CompiledPlanJSON":"{}"}`,
	}
	for name, entry := range entries {
		t.Run(name, func(t *testing.T) {
			svc, inst, temporal := outsideAnswerHarness(t, "RUNNING", outsideKey)
			good, err := svc.Definitions.GetCompiledWorkflow(context.Background(), inst.TenantID, inst.WorkflowVersionID)
			require.NoError(t, err)
			definitions := &insfixCountingDefinitions{inner: &fakeDefinitionClient{resp: good}}
			svc.Definitions = definitions
			cache := newFakeCacheStore()
			key := "compiled_plan:" + inst.TenantID.String() + ":" + inst.WorkflowVersionID.String()
			cache.values[key] = entry
			svc.Cache = cache

			require.NoError(t, svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, nil))

			assert.Equal(t, []uuid.UUID{inst.TenantID}, definitions.reads, "the Definition Service is asked once, for this tenant")
			require.Len(t, temporal.signals, 1)
			var stored port.CompiledWorkflow
			require.NoError(t, json.Unmarshal([]byte(cache.values[key]), &stored))
			assert.Equal(t, good.CompiledPlanJSON, stored.CompiledPlanJSON, "the entry is overwritten with the plan just read")
			assert.Equal(t, []string{key}, cache.setCalls)
		})
	}
}

func TestAnswerOutsideWait_AUsableSharedCacheEntryIsNotReadAgain(t *testing.T) {
	svc, inst, _ := outsideAnswerHarness(t, "RUNNING", outsideKey)
	good, err := svc.Definitions.GetCompiledWorkflow(context.Background(), inst.TenantID, inst.WorkflowVersionID)
	require.NoError(t, err)
	raw, err := json.Marshal(good)
	require.NoError(t, err)
	definitions := &insfixCountingDefinitions{inner: &fakeDefinitionClient{resp: good}}
	svc.Definitions = definitions
	cache := newFakeCacheStore()
	cache.values["compiled_plan:"+inst.TenantID.String()+":"+inst.WorkflowVersionID.String()] = string(raw)
	svc.Cache = cache

	require.NoError(t, svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, nil))

	assert.Empty(t, definitions.reads)
	assert.Empty(t, cache.setCalls)
}
