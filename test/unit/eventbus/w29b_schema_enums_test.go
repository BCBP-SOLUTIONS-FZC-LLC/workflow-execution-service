package eventbus_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/eventschema"
)

// w29bValidateWith marshals payload, sets field to value (nil value removes the key), and validates it.
func w29bValidateWith(t *testing.T, validator *eventbus.SchemaValidator, eventType string, payload any, field string, value any, remove bool) error {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))
	if remove {
		delete(m, field)
	} else {
		m[field] = value
	}
	changed, err := json.Marshal(m)
	require.NoError(t, err)
	return validator.Validate(context.Background(), eventType, changed)
}

func w29bNewValidator(t *testing.T) *eventbus.SchemaValidator {
	t.Helper()
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	return validator
}

func TestSchemaValidator_EnumFields_AcceptOnlyTheirOwnValues(t *testing.T) {
	cases := []struct {
		name      string
		eventType string
		payload   func() any
		field     string
		accepted  []string
		refused   []string
	}{
		{
			name: "paused initiator", eventType: domain.EventWorkflowInstancePaused, field: "initiator",
			payload: func() any {
				return domain.NewWorkflowInstancePausedPayload(testCore(), uuid.New(), domain.InitiatorAdmin, nil)
			},
			accepted: []string{"admin", "tenant_state", "safety_net", "ooo"},
			refused:  []string{"degraded_recovery", "Admin", ""},
		},
		{
			name: "resumed initiator", eventType: domain.EventWorkflowInstanceResumed, field: "initiator",
			payload: func() any {
				return domain.NewWorkflowInstanceResumedPayload(testCore(), uuid.New(), domain.InitiatorAdmin, nil)
			},
			accepted: []string{"admin", "tenant_state", "ooo", "degraded_recovery"},
			refused:  []string{"safety_net", "Admin", ""},
		},
		{
			name: "terminated initiator", eventType: domain.EventWorkflowInstanceTerminated, field: "initiator",
			payload: func() any {
				return domain.NewWorkflowInstanceTerminatedPayload(testCore(), uuid.New(), domain.TerminatedInitiatorAdmin, nil, nil)
			},
			accepted: []string{"admin", "tenant_state"},
			refused:  []string{"safety_net", "override", ""},
		},
		{
			name: "reassigned initiator", eventType: domain.EventWorkflowTaskReassigned, field: "initiator",
			payload: func() any {
				return domain.NewWorkflowTaskReassignedPayload(testCore(), testTaskCore(), nil, uuid.New(), domain.ReassignInitiatorAdmin, nil)
			},
			accepted: []string{"admin", "override", "delegation"},
			refused:  []string{"tenant_state", "safety_net", ""},
		},
		{
			name: "force-routed direction", eventType: domain.EventWorkflowInstanceForceRouted, field: "direction",
			payload: func() any {
				return domain.NewWorkflowInstanceForceRoutedPayload(testCore(), uuid.New(), []string{"review_finance"}, "review_legal", domain.ForceRouteDirectionForward)
			},
			accepted: []string{"forward", "back"},
			refused:  []string{"sideways", "Forward", ""},
		},
	}
	validator := w29bNewValidator(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, v := range tc.accepted {
				assert.NoError(t, w29bValidateWith(t, validator, tc.eventType, tc.payload(), tc.field, v, false), "%q is in the enum", v)
			}
			for _, v := range tc.refused {
				assert.Error(t, w29bValidateWith(t, validator, tc.eventType, tc.payload(), tc.field, v, false), "%q is outside the enum", v)
			}
			assert.Error(t, w29bValidateWith(t, validator, tc.eventType, tc.payload(), tc.field, 7, false), "a number is not an enum text")
		})
	}
}

func TestSchemaValidator_OptionalFields_MayBeOmittedOrNullButAreStillTyped(t *testing.T) {
	due, followUp := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC), time.Date(2026, 11, 19, 9, 0, 0, 0, time.UTC)
	connector, reason := "storage", "waiting for documents"
	actor, delegation, oldUser := uuid.New(), uuid.New(), uuid.New()
	cases := []struct {
		name      string
		eventType string
		payload   func() any
		field     string
		wrong     any
	}{
		{"created due_at", domain.EventWorkflowTaskCreated, func() any { return w29bCreated(&due, &followUp, &connector) }, "due_at", 5},
		{"created follow_up_at", domain.EventWorkflowTaskCreated, func() any { return w29bCreated(&due, &followUp, &connector) }, "follow_up_at", 5},
		{"created connector_type", domain.EventWorkflowTaskCreated, func() any { return w29bCreated(&due, &followUp, &connector) }, "connector_type", 5},
		{"created resolved_inputs", domain.EventWorkflowTaskCreated, func() any { return w29bCreated(&due, &followUp, &connector) }, "resolved_inputs", "text"},
		{"created output_mapping", domain.EventWorkflowTaskCreated, func() any { return w29bCreated(&due, &followUp, &connector) }, "output_mapping", "text"},
		{"deferred reason", domain.EventWorkflowTaskDeferred, func() any {
			return domain.NewWorkflowTaskDeferredPayload(testCore(), testTaskCore(), "review_finance", &reason, &due)
		}, "reason", 5},
		{"deferred due_at", domain.EventWorkflowTaskDeferred, func() any {
			return domain.NewWorkflowTaskDeferredPayload(testCore(), testTaskCore(), "review_finance", &reason, &due)
		}, "due_at", 5},
		{"reassigned delegation_id", domain.EventWorkflowTaskReassigned, func() any {
			return domain.NewWorkflowTaskReassignedPayload(testCore(), testTaskCore(), &oldUser, uuid.New(), domain.ReassignInitiatorDelegation, &delegation)
		}, "delegation_id", 5},
		{"terminated actor_user_id", domain.EventWorkflowInstanceTerminated, func() any {
			return domain.NewWorkflowInstanceTerminatedPayload(testCore(), uuid.New(), domain.TerminatedInitiatorAdmin, &actor, &reason)
		}, "actor_user_id", 5},
		{"terminated reason", domain.EventWorkflowInstanceTerminated, func() any {
			return domain.NewWorkflowInstanceTerminatedPayload(testCore(), uuid.New(), domain.TerminatedInitiatorAdmin, &actor, &reason)
		}, "reason", 5},
		{"cancelled reason", domain.EventWorkflowInstanceCancelled, func() any {
			return domain.NewWorkflowInstanceCancelledPayload(testCore(), uuid.New(), actor, &reason)
		}, "reason", 5},
		{"paused actor_user_id", domain.EventWorkflowInstancePaused, func() any {
			return domain.NewWorkflowInstancePausedPayload(testCore(), uuid.New(), domain.InitiatorAdmin, &actor)
		}, "actor_user_id", 5},
		{"resumed actor_user_id", domain.EventWorkflowInstanceResumed, func() any {
			return domain.NewWorkflowInstanceResumedPayload(testCore(), uuid.New(), domain.InitiatorAdmin, &actor)
		}, "actor_user_id", 5},
	}
	validator := w29bNewValidator(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.payload())
			require.NoError(t, err)
			require.Contains(t, string(raw), `"`+tc.field+`":`, "the baseline payload carries the field, so removing it changes something")
			require.NoError(t, w29bValidateWith(t, validator, tc.eventType, tc.payload(), "workflow_instance_id", uuid.NewString(), false), "the populated payload is valid")

			assert.NoError(t, w29bValidateWith(t, validator, tc.eventType, tc.payload(), tc.field, nil, true), "omitted")
			assert.NoError(t, w29bValidateWith(t, validator, tc.eventType, tc.payload(), tc.field, nil, false), "null")
			assert.Error(t, w29bValidateWith(t, validator, tc.eventType, tc.payload(), tc.field, tc.wrong, false), "a value of the wrong type")
		})
	}
}

func w29bCreated(due, followUp *time.Time, connector *string) domain.WorkflowTaskCreatedPayload {
	return domain.NewWorkflowTaskCreatedPayload(testCore(), testTaskCore(), "connector", due, followUp, connector,
		map[string]any{"bucket": "tenders"}, []dsl.IOVar{{Source: "url", Target: "document_url"}})
}

func TestSchemaValidator_Catalogue_MatchesTheOutboundEventTypesAndNamesEachPayloadType(t *testing.T) {
	schemas := map[string][]byte{
		domain.EventWorkflowInstanceStarted:     eventschema.WorkflowInstanceStarted,
		domain.EventWorkflowInstancePaused:      eventschema.WorkflowInstancePaused,
		domain.EventWorkflowInstanceResumed:     eventschema.WorkflowInstanceResumed,
		domain.EventWorkflowInstanceCancelled:   eventschema.WorkflowInstanceCancelled,
		domain.EventWorkflowInstanceTerminated:  eventschema.WorkflowInstanceTerminated,
		domain.EventWorkflowInstanceDegraded:    eventschema.WorkflowInstanceDegraded,
		domain.EventWorkflowInstanceFailed:      eventschema.WorkflowInstanceFailed,
		domain.EventWorkflowInstanceFinished:    eventschema.WorkflowInstanceFinished,
		domain.EventWorkflowInstanceForceRouted: eventschema.WorkflowInstanceForceRouted,
		domain.EventWorkflowTaskCreated:         eventschema.WorkflowTaskCreated,
		domain.EventWorkflowTaskClaimed:         eventschema.WorkflowTaskClaimed,
		domain.EventWorkflowTaskCompleted:       eventschema.WorkflowTaskCompleted,
		domain.EventWorkflowTaskDeferred:        eventschema.WorkflowTaskDeferred,
		domain.EventWorkflowTaskReassigned:      eventschema.WorkflowTaskReassigned,
		domain.EventWorkflowTaskSuperseded:      eventschema.WorkflowTaskSuperseded,
		domain.EventWorkflowTaskFailed:          eventschema.WorkflowTaskFailed,
		domain.EventWorkflowTaskSLAWarning:      eventschema.WorkflowTaskSLAWarning,
		domain.EventWorkflowTaskSLABreached:     eventschema.WorkflowTaskSLABreached,
	}
	var tested, embedded []string
	for _, tc := range allEvents {
		tested = append(tested, tc.eventType)
	}
	for eventType := range schemas {
		embedded = append(embedded, eventType)
	}
	assert.ElementsMatch(t, domain.OutboundEventTypes(), tested, "every outbound event type has a payload case in the validator tests, and no other does")
	assert.ElementsMatch(t, domain.OutboundEventTypes(), embedded, "every outbound event type has one embedded schema file")

	slaEvents := map[string]string{
		domain.EventWorkflowTaskSLAWarning:  "WorkflowTaskSlaWarningPayload",
		domain.EventWorkflowTaskSLABreached: "WorkflowTaskSlaBreachedPayload",
	}
	for _, tc := range allEvents {
		t.Run(tc.name, func(t *testing.T) {
			var schema struct {
				Title string `json:"title"`
			}
			require.NoError(t, json.Unmarshal(schemas[tc.eventType], &schema))
			goName := reflect.TypeOf(tc.payload()).Name()
			require.True(t, strings.HasSuffix(goName, "Payload"), goName)

			if want, isSLA := slaEvents[tc.eventType]; isSLA {
				assert.Equal(t, want, schema.Title, "the two SLA events spell Sla in the schema title")
				assert.NotEqual(t, goName, schema.Title)
				assert.Equal(t, strings.Replace(goName, "SLA", "Sla", 1), schema.Title)
				return
			}
			assert.Equal(t, goName, schema.Title, "the schema title is the Go payload type name")
		})
	}
}
