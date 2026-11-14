package eventbus_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

// vsxTaskCreatedPayload is a schema-valid WorkflowTaskCreated body as a generic map.
func vsxTaskCreatedPayload(t *testing.T) map[string]any {
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

func vsxMarshal(t *testing.T, m map[string]any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	return raw
}

func TestSchemaValidator_WorkflowTaskCreated_FormatViolations_RejectedNamingTheSchema(t *testing.T) {
	t.Parallel()

	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, validator.Validate(ctx, domain.EventWorkflowTaskCreated, vsxMarshal(t, vsxTaskCreatedPayload(t))),
		"the unmodified payload is the baseline for every case below")

	cases := []struct {
		name  string
		field string
		value any
	}{
		{"instance id is not a uuid", "workflow_instance_id", "not-a-uuid"},
		{"instance id is a number", "workflow_instance_id", 12345},
		{"due_at is a date without time", "due_at", "2026-03-01"},
		{"due_at has no timezone", "due_at", "2026-03-01T12:00:00"},
		{"due_at uses a space instead of T", "due_at", "2026-03-01 12:00:00Z"},
		{"due_at is free text", "due_at", "tomorrow"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := vsxTaskCreatedPayload(t)
			m[tc.field] = tc.value

			err := validator.Validate(ctx, domain.EventWorkflowTaskCreated, vsxMarshal(t, m))

			require.Error(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("event payload violates schema %q", domain.EventWorkflowTaskCreated))
		})
	}

	t.Run("RFC3339 due_at with an offset and a null due_at are accepted", func(t *testing.T) {
		t.Parallel()

		withOffset := vsxTaskCreatedPayload(t)
		withOffset["due_at"] = "2026-03-01T12:00:00+02:00"
		require.NoError(t, validator.Validate(ctx, domain.EventWorkflowTaskCreated, vsxMarshal(t, withOffset)))

		withNull := vsxTaskCreatedPayload(t)
		withNull["due_at"] = nil
		require.NoError(t, validator.Validate(ctx, domain.EventWorkflowTaskCreated, vsxMarshal(t, withNull)))
	})
}

func TestSchemaValidator_Validate_ConcurrentCallsOnOneValidator_EachGetsItsOwnVerdict(t *testing.T) {
	t.Parallel()

	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)

	const callers = 48
	payloads := make([]json.RawMessage, callers)
	wantValid := make([]bool, callers)
	for i := range payloads {
		m := vsxTaskCreatedPayload(t)
		switch i % 3 {
		case 0:
			wantValid[i] = true
		case 1:
			m["workflow_instance_id"] = "not-a-uuid"
		case 2:
			delete(m, "stage_type")
		}
		payloads[i] = vsxMarshal(t, m)
	}

	results := make([]error, callers)
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	for i := range callers {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			results[i] = validator.Validate(context.Background(), domain.EventWorkflowTaskCreated, payloads[i])
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()

	for i, err := range results {
		if wantValid[i] {
			assert.NoError(t, err, "caller %d sent a valid payload", i)
			continue
		}
		require.Error(t, err, "caller %d sent an invalid payload", i)
		assert.True(t, strings.Contains(err.Error(), "event payload violates schema"), "caller %d: %v", i, err)
	}
}
