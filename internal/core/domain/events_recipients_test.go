package domain

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func recipientsOf(t *testing.T, payload any) []Recipient {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	require.Contains(t, fields, "recipients", "every dashboard event names its readers")
	var got []Recipient
	require.NoError(t, json.Unmarshal(fields["recipients"], &got))
	return got
}

func TestPayloadBuilders_TaskEventsNameTheAssigneesAndTheDepartment(t *testing.T) {
	core := CommonCore{}
	a1, a2, dept := uuid.New(), uuid.New(), uuid.New()
	task := TaskScopedCore{DepartmentID: dept, AssigneeUserIDs: []uuid.UUID{a1, a2}}
	at := time.Date(2026, 4, 2, 10, 30, 0, 0, time.UTC)
	want := []Recipient{
		{Kind: RecipientUser, ID: a1.String()},
		{Kind: RecipientUser, ID: a2.String()},
		{Kind: RecipientDepartment, ID: dept.String()},
	}
	for name, payload := range map[string]any{
		"created":    NewWorkflowTaskCreatedPayload(core, task, "review", nil, nil, nil, nil, nil),
		"claimed":    NewWorkflowTaskClaimedPayload(core, task, a1),
		"completed":  NewWorkflowTaskCompletedPayload(core, task, a1),
		"deferred":   NewWorkflowTaskDeferredPayload(core, task, "n", nil, nil),
		"superseded": NewWorkflowTaskSupersededPayload(core, task, uuid.Nil),
		"failed":     NewWorkflowTaskFailedPayload(core, task, "c", ""),
		"warning":    NewWorkflowTaskSLAWarningPayload(core, task, at),
		"breached":   NewWorkflowTaskSLABreachedPayload(core, task, at),
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, want, recipientsOf(t, payload))
		})
	}
}

func TestPayloadBuilders_AReassignmentNamesThePreviousAndNewAssigneeAndTheDepartment(t *testing.T) {
	oldUser, newUser, dept := uuid.New(), uuid.New(), uuid.New()
	task := TaskScopedCore{DepartmentID: dept, AssigneeUserIDs: []uuid.UUID{newUser}}

	withOld := NewWorkflowTaskReassignedPayload(CommonCore{}, task, &oldUser, newUser, ReassignInitiatorAdmin, nil)
	withoutOld := NewWorkflowTaskReassignedPayload(CommonCore{}, task, nil, newUser, ReassignInitiatorAdmin, nil)

	assert.Equal(t, []Recipient{
		{Kind: RecipientUser, ID: oldUser.String()},
		{Kind: RecipientUser, ID: newUser.String()},
		{Kind: RecipientDepartment, ID: dept.String()},
	}, recipientsOf(t, withOld))
	assert.Equal(t, []Recipient{
		{Kind: RecipientUser, ID: newUser.String()},
		{Kind: RecipientDepartment, ID: dept.String()},
	}, recipientsOf(t, withoutOld), "nobody held the task before, so only the new assignee is named")
}

func TestPayloadBuilders_InstanceEventsNameTheStarter(t *testing.T) {
	core, starter := CommonCore{}, uuid.New()
	actor := uuid.New()
	want := []Recipient{{Kind: RecipientUser, ID: starter.String()}}
	for name, payload := range map[string]any{
		"paused":     NewWorkflowInstancePausedPayload(core, starter, InitiatorAdmin, &actor),
		"resumed":    NewWorkflowInstanceResumedPayload(core, starter, InitiatorAdmin, &actor),
		"cancelled":  NewWorkflowInstanceCancelledPayload(core, starter, actor, nil),
		"terminated": NewWorkflowInstanceTerminatedPayload(core, starter, TerminatedInitiatorAdmin, &actor, nil),
		"degraded":   NewWorkflowInstanceDegradedPayload(core, starter, nil),
		"failed":     NewWorkflowInstanceFailedPayload(core, starter, "e"),
		"finished":   NewWorkflowInstanceFinishedPayload(core, starter, time.Now()),
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, want, recipientsOf(t, payload))
		})
	}
}

func TestPayloadBuilders_AnUnsetIDIsNeverNamedAndTheListIsNeverNull(t *testing.T) {
	core := CommonCore{}
	for name, payload := range map[string]any{
		"task with no assignee or department": NewWorkflowTaskCreatedPayload(core, TaskScopedCore{}, "review", nil, nil, nil, nil, nil),
		"reassignment with nil ids":           NewWorkflowTaskReassignedPayload(core, TaskScopedCore{}, &uuid.Nil, uuid.Nil, ReassignInitiatorAdmin, nil),
		"instance with no starter":            NewWorkflowInstanceFinishedPayload(core, uuid.Nil, time.Now()),
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &fields))
			assert.JSONEq(t, `[]`, string(fields["recipients"]))
		})
	}
}

func TestPayloadBuilders_EventsNoDashboardRendersNameNoReaders(t *testing.T) {
	for name, payload := range map[string]any{
		"started":      NewWorkflowInstanceStartedPayload(CommonCore{}, uuid.New()),
		"force routed": NewWorkflowInstanceForceRoutedPayload(CommonCore{}, uuid.New(), nil, "n", ForceRouteDirectionBack),
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			assert.NotContains(t, string(raw), `"recipients"`)
		})
	}
}
