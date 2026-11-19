package eventbus_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func w26bValidator(t *testing.T) *eventbus.SchemaValidator {
	t.Helper()
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	return validator
}

// w26bTaskCreatedPayload is a schema-valid WorkflowTaskCreated body as a generic map.
func w26bTaskCreatedPayload(t *testing.T) map[string]any {
	t.Helper()
	core := domain.CommonCore{WorkflowInstanceID: uuid.New(), BusinessKey: "TND-1", WorkflowVersionID: uuid.New()}
	task := domain.TaskScopedCore{TaskID: uuid.New(), NodeKey: "review", DepartmentID: uuid.New(), AssigneeUserIDs: []uuid.UUID{uuid.New()}}
	dueAt := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	raw, err := json.Marshal(domain.NewWorkflowTaskCreatedPayload(core, task, "review", &dueAt, nil, nil, nil, nil))
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))
	return m
}

func w26bMarshal(t *testing.T, m map[string]any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	return raw
}

func TestSchemaValidator_UnregisteredEventType_FailsClosedNamingTheType(t *testing.T) {
	validator := w26bValidator(t)
	cases := []struct {
		name      string
		eventType string
	}{
		{name: "a retired type", eventType: "workflow.task.message-sent"},
		{name: "the empty string", eventType: ""},
		{name: "a registered name in another case", eventType: "WorkflowTaskSLAWarning"},
		{name: "a registered name in lower case", eventType: "workflowtaskcreated"},
		{name: "a registered name with a trailing space", eventType: "WorkflowTaskCreated "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.Validate(context.Background(), tc.eventType, json.RawMessage(`{}`))

			require.Error(t, err)
			assert.Contains(t, err.Error(), `no schema registered for event type "`+tc.eventType+`"`)
		})
	}

	t.Run("the registered spelling is found", func(t *testing.T) {
		err := validator.Validate(context.Background(), domain.EventWorkflowTaskSLAWarning, json.RawMessage(`{}`))

		require.Error(t, err, "an empty body is not a valid warning")
		assert.NotContains(t, err.Error(), "no schema registered")
		assert.Contains(t, err.Error(), `event payload violates schema "WorkflowTaskSlaWarning"`)
	})
}

func TestSchemaValidator_PayloadIsNotJSON_FailsNamingTheTypeAndKeepsTheCause(t *testing.T) {
	validator := w26bValidator(t)
	cases := []struct {
		name    string
		payload json.RawMessage
	}{
		{name: "nil", payload: nil},
		{name: "empty", payload: json.RawMessage{}},
		{name: "truncated object", payload: json.RawMessage(`{"workflow_instance_id":"`)},
		{name: "plain text", payload: json.RawMessage(`not valid json`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.Validate(context.Background(), domain.EventWorkflowTaskCreated, tc.payload)

			require.Error(t, err)
			assert.Contains(t, err.Error(), `schema validation: unmarshal "WorkflowTaskCreated" payload: `)
			assert.NotContains(t, err.Error(), "violates schema", "the payload never reached the schema")
			var syntax *json.SyntaxError
			assert.ErrorAs(t, err, &syntax, "the decoder's error stays reachable")
		})
	}
}

func TestSchemaValidator_PayloadIsJSONThatBreaksTheSchema_FailsNamingTheSchema(t *testing.T) {
	validator := w26bValidator(t)
	require.NoError(t, validator.Validate(context.Background(), domain.EventWorkflowTaskCreated, w26bMarshal(t, w26bTaskCreatedPayload(t))),
		"the unmodified payload is the baseline for every case below")

	withField := func(field string, value any) json.RawMessage {
		m := w26bTaskCreatedPayload(t)
		m[field] = value
		return w26bMarshal(t, m)
	}
	withoutField := func(field string) json.RawMessage {
		m := w26bTaskCreatedPayload(t)
		delete(m, field)
		return w26bMarshal(t, m)
	}
	cases := []struct {
		name    string
		payload json.RawMessage
	}{
		{name: "null instead of an object", payload: json.RawMessage(`null`)},
		{name: "an array instead of an object", payload: json.RawMessage(`[]`)},
		{name: "a string instead of an object", payload: json.RawMessage(`"WorkflowTaskCreated"`)},
		{name: "a number instead of an object", payload: json.RawMessage(`42`)},
		{name: "an empty object", payload: json.RawMessage(`{}`)},
		{name: "a required field is missing", payload: withoutField("task_id")},
		{name: "a text field holds a number", payload: withField("node_key", 7)},
		{name: "an id field holds an object", payload: withField("task_id", map[string]any{"id": 1})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.Validate(context.Background(), domain.EventWorkflowTaskCreated, tc.payload)

			require.Error(t, err)
			assert.Contains(t, err.Error(), `event payload violates schema "WorkflowTaskCreated": `)
			assert.NotContains(t, err.Error(), "unmarshal", "the payload was valid JSON")
			assert.NotContains(t, err.Error(), "no schema registered")
		})
	}
}
