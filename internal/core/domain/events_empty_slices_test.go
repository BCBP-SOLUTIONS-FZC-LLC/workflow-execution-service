package domain

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPayloadBuilders_NilSlicesSerialiseAsEmptyArraysNeverNull(t *testing.T) {
	core, task := CommonCore{}, TaskScopedCore{}
	at := time.Date(2026, 4, 2, 10, 30, 0, 0, time.UTC)
	cases := []struct {
		name    string
		payload any
		fields  []string
	}{
		{"degraded", NewWorkflowInstanceDegradedPayload(core, uuid.Nil, nil), []string{"failed_branches"}},
		{"force routed", NewWorkflowInstanceForceRoutedPayload(core, uuid.Nil, nil, "n", ForceRouteDirectionBack), []string{"from_node_keys"}},
		{"task created", NewWorkflowTaskCreatedPayload(core, task, "review", nil, nil, nil, nil, nil), []string{"assignee_user_ids"}},
		{"task claimed", NewWorkflowTaskClaimedPayload(core, task, uuid.Nil), []string{"assignee_user_ids"}},
		{"task completed", NewWorkflowTaskCompletedPayload(core, task, uuid.Nil), []string{"assignee_user_ids"}},
		{"task deferred", NewWorkflowTaskDeferredPayload(core, task, "n", nil, nil), []string{"assignee_user_ids"}},
		{"task reassigned", NewWorkflowTaskReassignedPayload(core, task, nil, uuid.Nil, ReassignInitiatorAdmin, nil), []string{"assignee_user_ids"}},
		{"task superseded", NewWorkflowTaskSupersededPayload(core, task, uuid.Nil), []string{"assignee_user_ids"}},
		{"task failed", NewWorkflowTaskFailedPayload(core, task, "c", ""), []string{"assignee_user_ids"}},
		{"sla warning", NewWorkflowTaskSLAWarningPayload(core, task, at), []string{"assignee_user_ids"}},
		{"sla breached", NewWorkflowTaskSLABreachedPayload(core, task, at), []string{"assignee_user_ids"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.payload)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &fields))
			for _, f := range tc.fields {
				assert.JSONEq(t, `[]`, string(fields[f]), "%s", f)
			}
		})
	}
}
