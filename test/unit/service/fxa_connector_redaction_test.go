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

var fxaNonObjectData = map[string]string{
	"an array of objects": `[{"resolved_inputs":{"api_key":"s3cret"},"output_mapping":[]}]`,
	"a string":            `"resolved_inputs s3cret"`,
	"a number":            `42`,
}

func TestTaskGet_ExtrasThatAreNotAJSONObjectAreNotShownToANonAdministrator(t *testing.T) {
	for name, extras := range fxaNonObjectData {
		t.Run(name, func(t *testing.T) {
			svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
			tenantID, assignee := uuid.New(), uuid.New()
			task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New(), NodeKey: "finance/send", RecordVersion: 1, ExtrasJSON: json.RawMessage(extras)}
			tasks.byID[task.ID] = task
			assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: assignee, IsActive: true})

			got, _, err := svc.Get(context.Background(), tenantID, task.ID, port.ReadScope{CallerUserID: assignee})

			require.NoError(t, err)
			assert.Empty(t, got.ExtrasJSON)
			assert.JSONEq(t, extras, string(task.ExtrasJSON), "the stored task is not changed")
		})
	}
}

func TestInstanceService_ListEvents_ATaskCreatedEventWithDataThatIsNotAJSONObjectShowsNoData(t *testing.T) {
	for name, data := range fxaNonObjectData {
		t.Run(name, func(t *testing.T) {
			svc, instances, _, _, outbox, _, _, _ := newInstanceServiceHarness()
			tenantID := uuid.New()
			inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID}
			instances.byID[inst.ID] = inst
			stored := `{"tenant_id":"` + tenantID.String() + `","data":` + data + `}`
			outbox.records = []*domain.OutboxEventRecord{{ID: uuid.New(), EventType: domain.EventWorkflowTaskCreated, Payload: json.RawMessage(stored)}}

			res, err := svc.ListEvents(context.Background(), tenantID, inst.ID, port.ReadScope{IsAdmin: true}, port.Page{Limit: 10})

			require.NoError(t, err)
			require.Len(t, res.Items, 1)
			assert.Empty(t, res.Items[0].PayloadJSON)
		})
	}
}
