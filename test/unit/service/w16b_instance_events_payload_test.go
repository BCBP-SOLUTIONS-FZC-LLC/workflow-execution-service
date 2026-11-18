package service_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type w16bEventLookup struct{ tenantID, instanceID uuid.UUID }

type w16bEventInstances struct {
	port.InstanceRepository
	inst    *domain.Instance
	lookups []w16bEventLookup
}

func (r *w16bEventInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.lookups = append(r.lookups, w16bEventLookup{tenantID, id})
	if r.inst == nil || r.inst.ID != id || r.inst.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return r.inst, nil
}

type w16bEventOutbox struct {
	port.OutboxRepository
	records []*domain.OutboxEventRecord
	reads   []w16bEventLookup
}

func (o *w16bEventOutbox) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, _ port.PageRequest) ([]*domain.OutboxEventRecord, *port.Cursor, error) {
	o.reads = append(o.reads, w16bEventLookup{tenantID, instanceID})
	return o.records, nil, nil
}

const w16bConnectorData = `{"task_id":"9d0a1a0e-3f2b-4b9a-8a52-2f6f3b7f1c10","resolved_inputs":{"api_key":"s3cret"},"output_mapping":[{"source":"id","target":"ref"}],"stage_type":"serviceTask"}`

func TestInstanceService_ListEvents_OnlyATaskCreatedEventIsRedactedOtherEventsAreReturnedAsStored(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID}
	cases := []struct {
		name      string
		eventType string
		data      string
		want      string
	}{
		{"a task-completed event keeps both fields", domain.EventWorkflowTaskCompleted, w16bConnectorData, w16bConnectorData},
		{"a task-claimed event keeps both fields", domain.EventWorkflowTaskClaimed, w16bConnectorData, w16bConnectorData},
		{"an instance-started event keeps both fields", domain.EventWorkflowInstanceStarted, w16bConnectorData, w16bConnectorData},
		{"a task-failed event is not redacted: data that is not an object is returned as stored", domain.EventWorkflowTaskFailed, `"resolved_inputs"`, `"resolved_inputs"`},
		{"a task-completed event is not redacted: data that is an array is returned as stored", domain.EventWorkflowTaskCompleted, `[{"resolved_inputs":1}]`, `[{"resolved_inputs":1}]`},
		{"a task-created event leaves the two fields out", domain.EventWorkflowTaskCreated, w16bConnectorData,
			`{"task_id":"9d0a1a0e-3f2b-4b9a-8a52-2f6f3b7f1c10","stage_type":"serviceTask"}`},
	}
	records := make([]*domain.OutboxEventRecord, len(cases))
	for i, tc := range cases {
		stored := `{"tenant_id":"` + tenantID.String() + `","data":` + tc.data + `}`
		records[i] = &domain.OutboxEventRecord{ID: uuid.New(), EventType: tc.eventType, Payload: json.RawMessage(stored), CreatedAt: time.Unix(int64(1000+i), 0)}
	}
	instances := &w16bEventInstances{inst: inst}
	outbox := &w16bEventOutbox{records: records}
	svc := &service.InstanceService{Instances: instances, Outbox: outbox}

	res, err := svc.ListEvents(context.Background(), tenantID, inst.ID, port.ReadScope{IsAdmin: true}, port.Page{Limit: 10})

	require.NoError(t, err)
	require.Len(t, res.Items, len(cases))
	for i, tc := range cases {
		assert.Equal(t, records[i].ID, res.Items[i].ID, tc.name)
		assert.JSONEq(t, tc.want, string(res.Items[i].PayloadJSON), tc.name)
	}
	assert.Equal(t, []w16bEventLookup{{tenantID, inst.ID}}, instances.lookups)
	assert.Equal(t, []w16bEventLookup{{tenantID, inst.ID}}, outbox.reads)
	for i, rec := range records {
		assert.Contains(t, string(rec.Payload), "resolved_inputs", "the stored event %d is not changed by the read", i)
	}
}
