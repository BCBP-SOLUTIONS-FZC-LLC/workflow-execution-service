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

type w18bEventsOutbox struct {
	port.OutboxRepository
	rows  []*domain.OutboxEventRecord
	reads []w18bLookup
}

func (o *w18bEventsOutbox) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, _ port.PageRequest) ([]*domain.OutboxEventRecord, *port.Cursor, error) {
	o.reads = append(o.reads, w18bLookup{tenantID, instanceID})
	return o.rows, nil, nil
}

type w18bWarnLogger struct {
	warnings []map[string]any
}

func (l *w18bWarnLogger) Debug(string, map[string]any) {}
func (l *w18bWarnLogger) Info(string, map[string]any)  {}
func (l *w18bWarnLogger) Error(string, map[string]any) {}
func (l *w18bWarnLogger) Warn(_ string, fields map[string]any) {
	l.warnings = append(l.warnings, fields)
}

func w18bEventsRig(rows []*domain.OutboxEventRecord) (*service.InstanceService, *w18bSignalInstances, *w18bEventsOutbox, *w18bWarnLogger, *domain.Instance) {
	tenantID := uuid.New()
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID}
	instances := &w18bSignalInstances{rows: []*domain.Instance{inst}}
	outbox := &w18bEventsOutbox{rows: rows}
	log := &w18bWarnLogger{}
	return &service.InstanceService{Instances: instances, Outbox: outbox, Log: log}, instances, outbox, log, inst
}

func w18bRecord(eventType, payload string) *domain.OutboxEventRecord {
	return &domain.OutboxEventRecord{ID: uuid.New(), EventType: eventType, Payload: json.RawMessage(payload), CreatedAt: time.Unix(1_700_000_000, 0).UTC()}
}

func TestInstanceService_ListEvents_TaskIdAndNodeKeyComeFromTheEnvelopeDataAndAreLeftOutWhenAbsentOrZero(t *testing.T) {
	t.Parallel()

	taskID := uuid.New()
	cases := []struct {
		name         string
		data         string
		wantTaskID   *uuid.UUID
		wantNodeKey  *string
		wantPayload  string
		eventType    string
		wantTypeName port.WorkflowEventType
	}{
		{
			name: "both present", data: `{"task_id":"` + taskID.String() + `","node_key":"finance/review","status":"READY"}`,
			wantTaskID: &taskID, wantNodeKey: ptrW18b("finance/review"),
			wantPayload: `{"task_id":"` + taskID.String() + `","node_key":"finance/review","status":"READY"}`,
			eventType:   domain.EventWorkflowTaskClaimed, wantTypeName: port.EventTaskClaimed,
		},
		{
			name: "an all-zero task id is left out and the node key stays", data: `{"task_id":"00000000-0000-0000-0000-000000000000","node_key":"ops/pack"}`,
			wantTaskID: nil, wantNodeKey: ptrW18b("ops/pack"),
			wantPayload: `{"task_id":"00000000-0000-0000-0000-000000000000","node_key":"ops/pack"}`,
			eventType:   domain.EventWorkflowTaskClaimed, wantTypeName: port.EventTaskClaimed,
		},
		{
			name: "an empty node key is left out and the task id stays", data: `{"task_id":"` + taskID.String() + `","node_key":""}`,
			wantTaskID: &taskID, wantNodeKey: nil,
			wantPayload: `{"task_id":"` + taskID.String() + `","node_key":""}`,
			eventType:   domain.EventWorkflowTaskClaimed, wantTypeName: port.EventTaskClaimed,
		},
		{
			name: "data without either field leaves both out", data: `{"reason":"x"}`,
			wantPayload: `{"reason":"x"}`,
			eventType:   domain.EventWorkflowInstanceStarted, wantTypeName: port.EventInstanceStarted,
		},
		{
			name: "data that is a string leaves both out and is returned as stored", data: `"plain"`,
			wantPayload: `"plain"`,
			eventType:   domain.EventWorkflowTaskClaimed, wantTypeName: port.EventTaskClaimed,
		},
		{
			name: "data that is an array leaves both out and is returned as stored", data: `[{"task_id":"` + taskID.String() + `"}]`,
			wantPayload: `[{"task_id":"` + taskID.String() + `"}]`,
			eventType:   domain.EventWorkflowTaskClaimed, wantTypeName: port.EventTaskClaimed,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := w18bRecord(tc.eventType, `{"tenant_id":"t","data":`+tc.data+`}`)
			svc, instances, outbox, log, inst := w18bEventsRig([]*domain.OutboxEventRecord{rec})

			res, err := svc.ListEvents(context.Background(), inst.TenantID, inst.ID, port.ReadScope{IsAdmin: true}, port.Page{Limit: 10})

			require.NoError(t, err)
			require.Len(t, res.Items, 1)
			got := res.Items[0]
			assert.Equal(t, rec.ID, got.ID)
			assert.Equal(t, tc.wantTypeName, got.EventType)
			assert.Equal(t, inst.ID, got.WorkflowInstanceID)
			assert.Equal(t, inst.TenantID, got.TenantID)
			assert.Equal(t, tc.wantTaskID, got.TaskID)
			assert.Equal(t, tc.wantNodeKey, got.NodeKey)
			assert.JSONEq(t, tc.wantPayload, string(got.PayloadJSON))
			assert.Empty(t, log.warnings, "a row with odd data is still rendered")
			assert.Equal(t, []w18bLookup{{inst.TenantID, inst.ID}}, instances.lookups)
			assert.Equal(t, []w18bLookup{{inst.TenantID, inst.ID}}, outbox.reads)
		})
	}
}

func ptrW18b[T any](v T) *T { return &v }

func TestInstanceService_ListEvents_ARowWhoseEnvelopeFieldsHaveTheWrongTypesIsSkippedAndTheRestAreReturned(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		payload string
	}{
		{"the actor is a number", `{"tenant_id":"t","actor":5,"data":{}}`},
		{"the tenant id is an object", `{"tenant_id":{"id":"t"},"data":{}}`},
		{"the whole envelope is a string", `"not an envelope"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			before := w18bRecord(domain.EventWorkflowInstanceStarted, `{"tenant_id":"t","data":{"n":1}}`)
			bad := w18bRecord(domain.EventWorkflowInstanceStarted, tc.payload)
			after := w18bRecord(domain.EventWorkflowInstanceStarted, `{"tenant_id":"t","data":{"n":2}}`)
			svc, _, outbox, log, inst := w18bEventsRig([]*domain.OutboxEventRecord{before, bad, after})

			res, err := svc.ListEvents(context.Background(), inst.TenantID, inst.ID, port.ReadScope{IsAdmin: true}, port.Page{Limit: 10})

			require.NoError(t, err, "a row that cannot be read is left out, not an error for the page")
			require.Len(t, res.Items, 2)
			assert.Equal(t, before.ID, res.Items[0].ID)
			assert.Equal(t, after.ID, res.Items[1].ID)
			assert.JSONEq(t, `{"n":1}`, string(res.Items[0].PayloadJSON))
			assert.JSONEq(t, `{"n":2}`, string(res.Items[1].PayloadJSON))
			require.Len(t, log.warnings, 1)
			assert.Equal(t, bad.ID, log.warnings[0]["event_id"], "the warning names the skipped row")
			assert.Equal(t, []w18bLookup{{inst.TenantID, inst.ID}}, outbox.reads)
		})
	}
}
