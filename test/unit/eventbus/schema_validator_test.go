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

func testCore() domain.CommonCore {
	return domain.CommonCore{
		WorkflowInstanceID: uuid.New(),
		BusinessKey:        "TND-2026-04471",
		WorkflowVersionID:  uuid.New(),
	}
}

func testTaskCore() domain.TaskScopedCore {
	return domain.TaskScopedCore{
		TaskID:          uuid.New(),
		NodeKey:         "review_finance",
		DepartmentID:    uuid.New(),
		AssigneeUserIDs: []uuid.UUID{uuid.New()},
	}
}

type eventCase struct {
	name      string
	eventType string
	payload   func() any
	dropField string
}

var allEvents = []eventCase{
	{
		name:      "WorkflowInstanceStarted",
		eventType: domain.EventWorkflowInstanceStarted,
		payload:   func() any { return domain.NewWorkflowInstanceStartedPayload(testCore(), uuid.New()) },
		dropField: "started_by_user_id",
	},
	{
		name:      "WorkflowInstancePaused",
		eventType: domain.EventWorkflowInstancePaused,
		payload: func() any {
			return domain.NewWorkflowInstancePausedPayload(testCore(), uuid.New(), domain.InitiatorAdmin, nil)
		},
		dropField: "initiator",
	},
	{
		name:      "WorkflowInstanceResumed",
		eventType: domain.EventWorkflowInstanceResumed,
		payload: func() any {
			return domain.NewWorkflowInstanceResumedPayload(testCore(), uuid.New(), domain.InitiatorAdmin, nil)
		},
		dropField: "initiator",
	},
	{
		name:      "WorkflowInstanceCancelled",
		eventType: domain.EventWorkflowInstanceCancelled,
		payload: func() any {
			return domain.NewWorkflowInstanceCancelledPayload(testCore(), uuid.New(), uuid.New(), nil)
		},
		dropField: "actor_user_id",
	},
	{
		name:      "WorkflowInstanceTerminated",
		eventType: domain.EventWorkflowInstanceTerminated,
		payload: func() any {
			return domain.NewWorkflowInstanceTerminatedPayload(testCore(), uuid.New(), domain.TerminatedInitiatorAdmin, nil)
		},
		dropField: "initiator",
	},
	{
		name:      "WorkflowInstanceDegraded",
		eventType: domain.EventWorkflowInstanceDegraded,
		payload: func() any {
			return domain.NewWorkflowInstanceDegradedPayload(testCore(), []domain.FailedBranch{{DepartmentID: uuid.New(), LastNodeKey: "review_legal"}})
		},
		dropField: "failed_branches",
	},
	{
		name:      "WorkflowInstanceFailed",
		eventType: domain.EventWorkflowInstanceFailed,
		payload:   func() any { return domain.NewWorkflowInstanceFailedPayload(testCore(), "TemporalActivityError") },
		dropField: "error_class",
	},
	{
		name:      "WorkflowInstanceFinished",
		eventType: domain.EventWorkflowInstanceFinished,
		payload: func() any {
			return domain.NewWorkflowInstanceFinishedPayload(testCore(), uuid.New(), time.Now().UTC())
		},
		dropField: "completed_at",
	},
	{
		name:      "WorkflowInstanceForceRouted",
		eventType: domain.EventWorkflowInstanceForceRouted,
		payload: func() any {
			return domain.NewWorkflowInstanceForceRoutedPayload(testCore(), uuid.New(), []string{"review_finance"}, "review_legal", domain.ForceRouteDirectionForward)
		},
		dropField: "direction",
	},
	{
		name:      "WorkflowTaskCreated",
		eventType: domain.EventWorkflowTaskCreated,
		payload: func() any {
			return domain.NewWorkflowTaskCreatedPayload(testCore(), testTaskCore(), "review", nil, nil, nil, nil, nil)
		},
		dropField: "stage_type",
	},
	{
		name:      "WorkflowTaskClaimed",
		eventType: domain.EventWorkflowTaskClaimed,
		payload: func() any {
			return domain.NewWorkflowTaskClaimedPayload(testCore(), testTaskCore(), uuid.New())
		},
		dropField: "claimed_by_user_id",
	},
	{
		name:      "WorkflowTaskCompleted",
		eventType: domain.EventWorkflowTaskCompleted,
		payload: func() any {
			return domain.NewWorkflowTaskCompletedPayload(testCore(), testTaskCore(), uuid.New())
		},
		dropField: "completed_by_user_id",
	},
	{
		name:      "WorkflowTaskDeferred",
		eventType: domain.EventWorkflowTaskDeferred,
		payload: func() any {
			return domain.NewWorkflowTaskDeferredPayload(testCore(), testTaskCore(), "review_finance", nil, nil)
		},
		dropField: "deferred_to_node_key",
	},
	{
		name:      "WorkflowTaskReassigned",
		eventType: domain.EventWorkflowTaskReassigned,
		payload: func() any {
			return domain.NewWorkflowTaskReassignedPayload(testCore(), testTaskCore(), uuid.New(), uuid.New(), domain.ReassignInitiatorAdmin, nil)
		},
		dropField: "new_user_id",
	},
	{
		name:      "WorkflowTaskSuperseded",
		eventType: domain.EventWorkflowTaskSuperseded,
		payload: func() any {
			return domain.NewWorkflowTaskSupersededPayload(testCore(), testTaskCore(), uuid.New())
		},
		dropField: "actor_user_id",
	},
	{
		name:      "WorkflowTaskFailed",
		eventType: domain.EventWorkflowTaskFailed,
		payload: func() any {
			return domain.NewWorkflowTaskFailedPayload(testCore(), testTaskCore(), "instance_terminated")
		},
		dropField: "cascade_source",
	},
	{
		name:      "WorkflowTaskSlaWarning",
		eventType: domain.EventWorkflowTaskSLAWarning,
		payload: func() any {
			return domain.NewWorkflowTaskSLAWarningPayload(testCore(), testTaskCore(), time.Now().UTC())
		},
		dropField: "follow_up_at",
	},
	{
		name:      "WorkflowTaskSlaBreached",
		eventType: domain.EventWorkflowTaskSLABreached,
		payload: func() any {
			return domain.NewWorkflowTaskSLABreachedPayload(testCore(), testTaskCore(), time.Now().UTC())
		},
		dropField: "due_at",
	},
}

func TestSchemaValidator_AllEventTypes_AreRegistered(t *testing.T) {
	// Guards against an accidental resurrection of workflow.task.message-sent
	// or a dropped table entry - the catalogue is exactly 18 events.
	assert.Len(t, allEvents, 18)
}

func TestSchemaValidator_ValidPayload_Accepted(t *testing.T) {
	for _, tc := range allEvents {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.payload())
			require.NoError(t, err)

			validator, err := eventbus.NewSchemaValidator()
			require.NoError(t, err)

			err = validator.Validate(context.Background(), tc.eventType, raw)
			require.NoError(t, err)
		})
	}
}

func TestSchemaValidator_MissingRequiredField_Rejected(t *testing.T) {
	for _, tc := range allEvents {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.payload())
			require.NoError(t, err)

			var m map[string]any
			require.NoError(t, json.Unmarshal(raw, &m))
			delete(m, tc.dropField)
			broken, err := json.Marshal(m)
			require.NoError(t, err)

			validator, err := eventbus.NewSchemaValidator()
			require.NoError(t, err)

			err = validator.Validate(context.Background(), tc.eventType, broken)
			require.Error(t, err)
		})
	}
}

func TestSchemaValidator_UnknownEventType_FailsClosed(t *testing.T) {
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)

	err = validator.Validate(context.Background(), "workflow.task.message-sent", []byte(`{}`))
	require.Error(t, err, "fail-closed: an unregistered event type must be rejected")
}

func TestSchemaValidator_InvalidJSON_Rejected(t *testing.T) {
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)

	err = validator.Validate(context.Background(), domain.EventWorkflowInstanceStarted, []byte(`{not valid json`))
	require.Error(t, err)
}
