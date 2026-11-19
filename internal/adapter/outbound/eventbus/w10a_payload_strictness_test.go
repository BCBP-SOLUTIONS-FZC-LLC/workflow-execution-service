package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

// w10aRefusedPaths lists where in the payload the validator found a fault, as "a/b" paths, one per leaf cause.
func w10aRefusedPaths(t *testing.T, err error) []string {
	t.Helper()
	var verr *jsonschema.ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("error %v is not a schema validation error", err)
	}
	var paths []string
	var walk func(e *jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(e.Causes) == 0 {
			paths = append(paths, strings.Join(e.InstanceLocation, "/"))
			return
		}
		for _, c := range e.Causes {
			walk(c)
		}
	}
	walk(verr)
	return paths
}

func w10aCore() domain.CommonCore {
	return domain.CommonCore{WorkflowInstanceID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-001", WorkflowVersionID: uuid.New()}
}

func w10aValidate(t *testing.T, eventType string, payload any) error {
	t.Helper()
	validator, err := NewSchemaValidator()
	if err != nil {
		t.Fatalf("NewSchemaValidator() error = %v", err)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return validator.Validate(context.Background(), eventType, raw)
}

func TestSchemaValidator_ListFieldsThatMarshalAsNull_AreRefused(t *testing.T) {
	cases := []struct {
		name      string
		eventType string
		payload   any
		wantNull  string
		wantPath  string
	}{
		{
			name:      "a created task with no human assignees",
			eventType: domain.EventWorkflowTaskCreated,
			payload: func() any {
				p := domain.NewWorkflowTaskCreatedPayload(w10aCore(),
					domain.TaskScopedCore{TaskID: uuid.New(), NodeKey: "finance/review", DepartmentID: uuid.New()},
					"userTask", nil, nil, nil, nil, nil)
				p.AssigneeUserIDs = nil
				return p
			}(),
			wantNull: `"assignee_user_ids":null`,
			wantPath: "assignee_user_ids",
		},
		{
			name:      "a force route with no source nodes",
			eventType: domain.EventWorkflowInstanceForceRouted,
			payload: func() any {
				p := domain.NewWorkflowInstanceForceRoutedPayload(w10aCore(), uuid.New(), nil, "finance/review",
					domain.ForceRouteDirectionForward)
				p.FromNodeKeys = nil
				return p
			}(),
			wantNull: `"from_node_keys":null`,
			wantPath: "from_node_keys",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.payload)
			if err != nil {
				t.Fatalf("marshal payload: %v", err)
			}
			if !strings.Contains(string(raw), tc.wantNull) {
				t.Fatalf("payload %s does not carry %s: the case no longer shows the null list", raw, tc.wantNull)
			}

			err = w10aValidate(t, tc.eventType, tc.payload)

			if err == nil {
				t.Fatalf("Validate() accepted %s, want a refusal of the null list", raw)
			}
			if paths := w10aRefusedPaths(t, err); len(paths) != 1 || paths[0] != tc.wantPath {
				t.Errorf("refused at %v, want only %q", paths, tc.wantPath)
			}
		})
	}
}

func TestSchemaValidator_ExtraPropertiesAreAcceptedAndBadFormatsAreRefused(t *testing.T) {
	startedBy := uuid.New()
	started := func(mutate func(map[string]any)) json.RawMessage {
		raw, err := json.Marshal(domain.NewWorkflowInstanceStartedPayload(w10aCore(), startedBy))
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		var fields map[string]any
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		mutate(fields)
		out, err := json.Marshal(fields)
		if err != nil {
			t.Fatalf("marshal mutated payload: %v", err)
		}
		return out
	}
	finished := func(completedAt string) json.RawMessage {
		raw, err := json.Marshal(domain.NewWorkflowInstanceFinishedPayload(w10aCore(), startedBy, time.Date(2026, 11, 18, 9, 30, 0, 0, time.UTC)))
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		var fields map[string]any
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		fields["completed_at"] = completedAt
		out, err := json.Marshal(fields)
		if err != nil {
			t.Fatalf("marshal mutated payload: %v", err)
		}
		return out
	}
	cases := []struct {
		name      string
		eventType string
		payload   json.RawMessage
		wantPath  string
	}{
		{"an unknown property is accepted", domain.EventWorkflowInstanceStarted, started(func(f map[string]any) { f["added_by_a_newer_producer"] = "x" }), ""},
		{"a well-formed timestamp is accepted", domain.EventWorkflowInstanceFinished, finished("2026-11-18T09:30:00Z"), ""},
		{"a uuid field with a bad string is refused", domain.EventWorkflowInstanceStarted, started(func(f map[string]any) { f["started_by_user_id"] = "not-a-uuid" }), "started_by_user_id"},
		{"a date-time field with a bad string is refused", domain.EventWorkflowInstanceFinished, finished("yesterday at noon"), "completed_at"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			validator, err := NewSchemaValidator()
			if err != nil {
				t.Fatalf("NewSchemaValidator() error = %v", err)
			}

			err = validator.Validate(context.Background(), tc.eventType, tc.payload)

			if tc.wantPath == "" {
				if err != nil {
					t.Fatalf("Validate() refused %s: %v", tc.payload, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() accepted %s, want a refusal at %q", tc.payload, tc.wantPath)
			}
			if paths := w10aRefusedPaths(t, err); len(paths) != 1 || paths[0] != tc.wantPath {
				t.Errorf("refused at %v, want only %q", paths, tc.wantPath)
			}
		})
	}
}
