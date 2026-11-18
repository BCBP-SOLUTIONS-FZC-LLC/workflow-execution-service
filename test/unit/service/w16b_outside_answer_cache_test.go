package service_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type w16bPlanRead struct{ tenantID, versionID uuid.UUID }

type w16bCacheSet struct {
	key   string
	value string
	ttl   time.Duration
}

type w16bPlanCache struct {
	port.CacheStore
	values map[string]string
	gets   []string
	sets   []w16bCacheSet
}

func (c *w16bPlanCache) Get(_ context.Context, key string) (string, error) {
	c.gets = append(c.gets, key)
	return c.values[key], nil
}

func (c *w16bPlanCache) Set(_ context.Context, key, value string, ttl time.Duration) error {
	c.sets = append(c.sets, w16bCacheSet{key, value, ttl})
	c.values[key] = value
	return nil
}

type w16bPlanDefinitions struct {
	resp  *port.CompiledWorkflow
	calls []w16bPlanRead
}

func (d *w16bPlanDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.calls = append(d.calls, w16bPlanRead{tenantID, versionID})
	return d.resp, nil
}

type w16bPlanInstances struct {
	port.InstanceRepository
	inst *domain.Instance
}

func (r *w16bPlanInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	if r.inst.TenantID != tenantID || r.inst.ID != id {
		return nil, domain.ErrNotFound
	}
	return r.inst, nil
}

type w16bPlanSignal struct {
	workflowID string
	instanceID uuid.UUID
	name       string
}

type w16bPlanQuery struct{ workflowID, queryType string }

type w16bPlanTemporal struct {
	port.TemporalClient
	waiting []string
	queries []w16bPlanQuery
	signals []w16bPlanSignal
}

func (c *w16bPlanTemporal) QueryWorkflow(_ context.Context, workflowID, queryType string, out any) error {
	c.queries = append(c.queries, w16bPlanQuery{workflowID, queryType})
	raw, err := json.Marshal(c.waiting)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

func (c *w16bPlanTemporal) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, _ any) error {
	c.signals = append(c.signals, w16bPlanSignal{workflowID, instanceID, name})
	return nil
}

func TestAnswerOutsideWait_WithNoCachedPlanReadsDefinitionsOnceAndWritesThePlanToTheCache(t *testing.T) {
	t.Parallel()

	const key = "finance/Receive_Bid"
	planJSON, err := json.Marshal(dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{{ID: "finance", Label: "Finance", Stages: []dsl.StageDef{
				{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
			}}},
		}},
	})
	require.NoError(t, err)
	tenantID, versionID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, WorkflowVersionID: versionID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:subject:w16b"}
	definitions := &w16bPlanDefinitions{resp: &port.CompiledWorkflow{VersionID: versionID, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: string(planJSON)}}
	cache := &w16bPlanCache{values: map[string]string{}}
	temporal := &w16bPlanTemporal{waiting: []string{key}}
	svc := &service.InstanceService{Instances: &w16bPlanInstances{inst: inst}, Temporal: temporal, Definitions: definitions, Cache: cache}
	admin := port.ReadScope{CallerUserID: uuid.New(), IsAdmin: true}

	require.NoError(t, svc.AnswerOutsideWait(context.Background(), tenantID, inst.ID, admin, key, nil))

	wantKey := "compiled_plan:" + tenantID.String() + ":" + versionID.String()
	assert.Equal(t, []string{wantKey}, cache.gets, "the cache is asked first, under the caller's tenant and the instance's version")
	assert.Equal(t, []w16bPlanRead{{tenantID, versionID}}, definitions.calls)
	require.Len(t, cache.sets, 1, "the plan read from Definitions is written to the cache")
	assert.Equal(t, wantKey, cache.sets[0].key)
	assert.Positive(t, cache.sets[0].ttl, "the cached plan expires")
	var cached port.CompiledWorkflow
	require.NoError(t, json.Unmarshal([]byte(cache.sets[0].value), &cached))
	assert.Equal(t, versionID, cached.VersionID)
	assert.JSONEq(t, string(planJSON), cached.CompiledPlanJSON)
	assert.Equal(t, []w16bPlanSignal{{"tenant:subject:w16b", inst.ID, port.SignalInstanceExternalAnswer}}, temporal.signals)
	assert.Equal(t, []w16bPlanQuery{{"tenant:subject:w16b", port.QueryWaitingExternal}}, temporal.queries)

	require.NoError(t, svc.AnswerOutsideWait(context.Background(), tenantID, inst.ID, admin, key, nil))

	assert.Equal(t, []w16bPlanRead{{tenantID, versionID}}, definitions.calls, "the second answer is served from the cache")
	assert.Len(t, cache.sets, 1)
	assert.Len(t, temporal.signals, 2)
}
