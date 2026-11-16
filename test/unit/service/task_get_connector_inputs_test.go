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

func TestTaskGet_AConnectorTasksInputsAndOutputMappingAreForAdministratorsOnly(t *testing.T) {
	const extras = `{"resolved_inputs":{"to":"a@example.com"},"output_mapping":[{"source":"id","target":"mail_id"}],"note":"kept"}`
	tenantID, departmentID, assignee := uuid.New(), uuid.New(), uuid.New()

	cases := []struct {
		name      string
		scope     port.ReadScope
		wantInput bool
	}{
		{"administrator", port.ReadScope{IsAdmin: true}, true},
		{"member of the task's department", port.ReadScope{CallerUserID: uuid.New(), Departments: []port.DepartmentRole{{DepartmentID: departmentID, Role: "reviewer"}}}, false},
		{"assignee", port.ReadScope{CallerUserID: assignee}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
			task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New(), DepartmentID: departmentID, NodeKey: "finance/send", RecordVersion: 1, ExtrasJSON: json.RawMessage(extras)}
			tasks.byID[task.ID] = task
			assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: assignee, IsActive: true})

			got, _, err := svc.Get(context.Background(), tenantID, task.ID, tc.scope)

			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(got.ExtrasJSON, &fields))
			if tc.wantInput {
				assert.Contains(t, fields, "resolved_inputs")
				assert.Contains(t, fields, "output_mapping")
			} else {
				assert.NotContains(t, fields, "resolved_inputs")
				assert.NotContains(t, fields, "output_mapping")
			}
			assert.Contains(t, fields, "note", "other detail stays visible to every reader")
			assert.JSONEq(t, extras, string(task.ExtrasJSON), "the stored task is not changed")
		})
	}
}

func TestTaskGet_ATaskWithoutExtrasStaysWithoutExtrasForANonAdministrator(t *testing.T) {
	svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
	tenantID, assignee := uuid.New(), uuid.New()
	task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New(), NodeKey: "finance/review", RecordVersion: 1}
	tasks.byID[task.ID] = task
	assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: assignee, IsActive: true})

	got, _, err := svc.Get(context.Background(), tenantID, task.ID, port.ReadScope{CallerUserID: assignee})

	require.NoError(t, err)
	assert.Empty(t, got.ExtrasJSON)
}
