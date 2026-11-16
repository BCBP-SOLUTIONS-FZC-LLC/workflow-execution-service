package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type tenantScopedInstances struct {
	*fakeInstanceRepo
	lookups []uuid.UUID
}

func (r *tenantScopedInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.lookups = append(r.lookups, tenantID)
	inst, err := r.fakeInstanceRepo.GetByID(ctx, tenantID, id)
	if err == nil && inst.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return inst, err
}

type countingOutbox struct {
	*fakeOutbox
	listCalls int
}

func (o *countingOutbox) ListByInstance(ctx context.Context, tenantID, instanceID uuid.UUID, page port.PageRequest) ([]*domain.OutboxEventRecord, *port.Cursor, error) {
	o.listCalls++
	return o.fakeOutbox.ListByInstance(ctx, tenantID, instanceID, page)
}

func TestInstanceService_ListEvents_AMissingOrOtherTenantInstanceIsNotFoundForEveryone(t *testing.T) {
	scopes := map[string]port.ReadScope{
		"admin":     {IsAdmin: true},
		"non-admin": {CallerUserID: uuid.New()},
	}
	for name, scope := range scopes {
		t.Run(name+" asks for a missing instance", func(t *testing.T) {
			svc, instances, _, _, fake, _, _, _ := newInstanceServiceHarness()
			scoped := &tenantScopedInstances{fakeInstanceRepo: instances}
			outbox := &countingOutbox{fakeOutbox: fake}
			svc.Instances = scoped
			svc.Outbox = outbox
			tenantID := uuid.New()

			_, err := svc.ListEvents(context.Background(), tenantID, uuid.New(), scope, port.Page{Limit: 10})

			require.ErrorIs(t, err, port.ErrInstanceNotFound)
			assert.Equal(t, []uuid.UUID{tenantID}, scoped.lookups, "the instance is looked up in the caller's tenant")
			assert.Zero(t, outbox.listCalls, "no event is read for an instance that is not there")
		})
		t.Run(name+" asks for another tenant's instance", func(t *testing.T) {
			svc, instances, _, _, fake, _, _, _ := newInstanceServiceHarness()
			scoped := &tenantScopedInstances{fakeInstanceRepo: instances}
			outbox := &countingOutbox{fakeOutbox: fake}
			svc.Instances = scoped
			svc.Outbox = outbox
			owner, caller := uuid.New(), uuid.New()
			inst := &domain.Instance{ID: uuid.New(), TenantID: owner}
			instances.byID[inst.ID] = inst

			_, err := svc.ListEvents(context.Background(), caller, inst.ID, scope, port.Page{Limit: 10})

			require.ErrorIs(t, err, port.ErrInstanceNotFound)
			assert.Equal(t, []uuid.UUID{caller}, scoped.lookups)
			assert.Zero(t, outbox.listCalls)
		})
	}
}

func TestInstanceService_ListEvents_ALookupFailureIsNotReadAsNotFound(t *testing.T) {
	svc, instances, _, _, _, _, _, _ := newInstanceServiceHarness()
	instances.getErr = assert.AnError

	_, err := svc.ListEvents(context.Background(), uuid.New(), uuid.New(), port.ReadScope{IsAdmin: true}, port.Page{Limit: 10})

	require.Error(t, err)
	assert.NotErrorIs(t, err, port.ErrInstanceNotFound)
}

func TestInstanceService_ListEvents_ATaskCreatedEventLeavesConnectorInputsAndMappingOut(t *testing.T) {
	svc, instances, _, _, outbox, _, _, _ := newInstanceServiceHarness()
	tenantID := uuid.New()
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID}
	instances.byID[inst.ID] = inst
	stored, err := json.Marshal(map[string]any{
		"tenant_id": tenantID.String(),
		"data": map[string]any{
			"stage_type":      "serviceTask",
			"connector_type":  "http",
			"resolved_inputs": map[string]any{"api_key": "s3cret", "url": "https://example.test"},
			"output_mapping":  []map[string]any{{"name": "result", "path": "$.body"}},
		},
	})
	require.NoError(t, err)
	outbox.records = []*domain.OutboxEventRecord{{ID: uuid.New(), EventType: domain.EventWorkflowTaskCreated, Payload: stored}}

	res, err := svc.ListEvents(context.Background(), tenantID, inst.ID, port.ReadScope{IsAdmin: true}, port.Page{Limit: 10})

	require.NoError(t, err)
	require.Len(t, res.Items, 1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(res.Items[0].PayloadJSON, &payload))
	assert.NotContains(t, payload, "resolved_inputs")
	assert.NotContains(t, payload, "output_mapping")
	assert.Equal(t, "serviceTask", payload["stage_type"])
	assert.Equal(t, "http", payload["connector_type"])
	assert.NotContains(t, string(res.Items[0].PayloadJSON), "s3cret")
	assert.Contains(t, string(outbox.records[0].Payload), "resolved_inputs", "the stored event keeps the inputs the connector worker needs")
}
