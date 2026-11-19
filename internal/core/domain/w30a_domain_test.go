package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func w30aCore() CommonCore {
	return CommonCore{
		WorkflowInstanceID: uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		SubjectID:          "subject-1",
		BusinessKey:        "bk-1",
		WorkflowVersionID:  uuid.MustParse("22222222-2222-4222-8222-222222222222"),
	}
}

func w30aTask() TaskScopedCore {
	return TaskScopedCore{
		TaskID:          uuid.MustParse("33333333-3333-4333-8333-333333333333"),
		NodeKey:         "review",
		DepartmentID:    uuid.MustParse("44444444-4444-4444-8444-444444444444"),
		AssigneeUserIDs: []uuid.UUID{uuid.MustParse("55555555-5555-4555-8555-555555555555")},
	}
}

func w30aKeys(t *testing.T, payload any) map[string]json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	out := map[string]json.RawMessage{}
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

func w30aStrPtr(s string) *string { return &s }

func TestTenantTaskQueueName_FixedTenants_YieldExactQueueName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		tenant uuid.UUID
		want   string
	}{
		{"zero uuid", uuid.Nil, "wf-queue-00000000-0000-0000-0000-000000000000"},
		{"fixed uuid", uuid.MustParse("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"), "wf-queue-0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := TenantTaskQueueName(tc.tenant)
			assert.Equal(t, tc.want, got)
			assert.Len(t, got, 45)
		})
	}
}

func TestDeterministicAssignmentID_SameInputs_SameVersion5ID(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		taskID, user uuid.UUID
	}{
		{"random pair", uuid.New(), uuid.New()},
		{"nil pair", uuid.Nil, uuid.Nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first := DeterministicAssignmentID(tc.taskID, tc.user)
			second := DeterministicAssignmentID(tc.taskID, tc.user)
			assert.Equal(t, first, second)
			assert.NotEqual(t, uuid.Nil, first)
			assert.Equal(t, uuid.Version(5), first.Version())
			assert.Equal(t, uuid.RFC4122, first.Variant())
		})
	}
}

func TestDeterministicAssignmentID_DifferentInputs_DifferentIDs(t *testing.T) {
	t.Parallel()
	taskA, taskB := uuid.New(), uuid.New()
	userA, userB := uuid.New(), uuid.New()
	cases := []struct {
		name        string
		left, right uuid.UUID
	}{
		{"task and user swapped", DeterministicAssignmentID(taskA, userA), DeterministicAssignmentID(userA, taskA)},
		{"different user", DeterministicAssignmentID(taskA, userA), DeterministicAssignmentID(taskA, userB)},
		{"second task for one user", DeterministicAssignmentID(taskA, userA), DeterministicAssignmentID(taskB, userA)},
		{"nil pair against real pair", DeterministicAssignmentID(uuid.Nil, uuid.Nil), DeterministicAssignmentID(taskA, userA)},
		{"nil task against nil user", DeterministicAssignmentID(uuid.Nil, userA), DeterministicAssignmentID(userA, uuid.Nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.NotEqual(t, tc.left, tc.right)
		})
	}
}

func TestMessageName_AnyString_PassesThroughUnchanged(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", ""},
		{"lower case type", "workflowinstancestarted", "workflowinstancestarted"},
		{"created type", EventWorkflowTaskCreated, "WorkflowTaskCreated"},
		{"unknown", "NotAnEvent", "NotAnEvent"},
		{"padded", " WorkflowTaskCreated ", " WorkflowTaskCreated "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, MessageName(tc.input))
		})
	}
}

func TestOutboundEventTypes_Listing_18DistinctWireNamesInAFreshSlice(t *testing.T) {
	t.Parallel()
	wire := []string{
		"WorkflowInstanceStarted", "WorkflowInstancePaused", "WorkflowInstanceResumed", "WorkflowInstanceCancelled",
		"WorkflowInstanceTerminated", "WorkflowInstanceDegraded", "WorkflowInstanceFailed", "WorkflowInstanceFinished",
		"WorkflowInstanceForceRouted", "WorkflowTaskCreated", "WorkflowTaskClaimed", "WorkflowTaskCompleted",
		"WorkflowTaskDeferred", "WorkflowTaskReassigned", "WorkflowTaskSuperseded", "WorkflowTaskFailed",
		"WorkflowTaskSlaWarning", "WorkflowTaskSlaBreached",
	}
	got := OutboundEventTypes()
	assert.Len(t, got, 18)
	assert.ElementsMatch(t, wire, got)
	seen := map[string]bool{}
	for _, name := range got {
		assert.False(t, seen[name], "duplicate wire type %s", name)
		seen[name] = true
	}

	got[0] = "Mutated"
	again := OutboundEventTypes()
	assert.Equal(t, "WorkflowInstanceStarted", again[0])
}

func TestEventConstants_SLAEvents_CarryLowerCaseSlaWireStrings(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"warning", EventWorkflowTaskSLAWarning, "WorkflowTaskSlaWarning"},
		{"breached", EventWorkflowTaskSLABreached, "WorkflowTaskSlaBreached"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.got)
			assert.NotContains(t, tc.got, "SLA")
			assert.Equal(t, tc.want, MessageName(tc.got))
		})
	}
}

func TestInstanceLifecyclePayloads_ActorUserID_OmittedWhenNilEmittedWhenSet(t *testing.T) {
	t.Parallel()
	started := uuid.New()
	actor := uuid.MustParse("66666666-6666-4666-8666-666666666666")
	build := map[string]func(actor *uuid.UUID) any{
		"paused": func(a *uuid.UUID) any {
			return NewWorkflowInstancePausedPayload(w30aCore(), started, InitiatorAdmin, a)
		},
		"resumed": func(a *uuid.UUID) any {
			return NewWorkflowInstanceResumedPayload(w30aCore(), started, InitiatorAdmin, a)
		},
		"terminated": func(a *uuid.UUID) any {
			return NewWorkflowInstanceTerminatedPayload(w30aCore(), started, "admin", a, nil)
		},
	}
	for name, mk := range build {
		t.Run(name+" nil actor", func(t *testing.T) {
			t.Parallel()
			keys := w30aKeys(t, mk(nil))
			assert.NotContains(t, keys, "actor_user_id")
			assert.Contains(t, keys, "initiator")
		})
		t.Run(name+" set actor", func(t *testing.T) {
			t.Parallel()
			keys := w30aKeys(t, mk(&actor))
			require.Contains(t, keys, "actor_user_id")
			assert.JSONEq(t, `"66666666-6666-4666-8666-666666666666"`, string(keys["actor_user_id"]))
		})
	}
}

func TestWorkflowInstanceCancelledPayload_ActorAndReason_EmissionRules(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		actor      uuid.UUID
		reason     *string
		wantActor  string
		wantReason *string
	}{
		{"nil reason is omitted", uuid.MustParse("66666666-6666-4666-8666-666666666666"), nil, "66666666-6666-4666-8666-666666666666", nil},
		{"empty reason is emitted", uuid.MustParse("66666666-6666-4666-8666-666666666666"), w30aStrPtr(""), "66666666-6666-4666-8666-666666666666", w30aStrPtr("")},
		{"set reason", uuid.MustParse("77777777-7777-4777-8777-777777777777"), w30aStrPtr("duplicate"), "77777777-7777-4777-8777-777777777777", w30aStrPtr("duplicate")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			keys := w30aKeys(t, NewWorkflowInstanceCancelledPayload(w30aCore(), uuid.New(), tc.actor, tc.reason))
			require.Contains(t, keys, "actor_user_id")
			assert.JSONEq(t, fmt.Sprintf("%q", tc.wantActor), string(keys["actor_user_id"]))
			if tc.wantReason == nil {
				assert.NotContains(t, keys, "reason")
				return
			}
			require.Contains(t, keys, "reason")
			assert.JSONEq(t, fmt.Sprintf("%q", *tc.wantReason), string(keys["reason"]))
		})
	}
}

func TestWorkflowTaskCreatedPayload_OptionalFields_OmittedWhenEmptyStageTypeAlways(t *testing.T) {
	t.Parallel()
	due := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	follow := time.Date(2030, 1, 1, 3, 4, 5, 0, time.UTC)
	connector := "http"

	t.Run("bare payload omits every optional key", func(t *testing.T) {
		t.Parallel()
		keys := w30aKeys(t, NewWorkflowTaskCreatedPayload(w30aCore(), w30aTask(), "", nil, nil, nil, map[string]any{}, []dsl.IOVar{}))
		for _, omitted := range []string{"due_at", "follow_up_at", "connector_type", "resolved_inputs", "output_mapping"} {
			assert.NotContains(t, keys, omitted)
		}
		require.Contains(t, keys, "stage_type")
		assert.JSONEq(t, `""`, string(keys["stage_type"]))
	})
	t.Run("nil maps and slices are omitted too", func(t *testing.T) {
		t.Parallel()
		keys := w30aKeys(t, NewWorkflowTaskCreatedPayload(w30aCore(), w30aTask(), "human", nil, nil, nil, nil, nil))
		assert.NotContains(t, keys, "resolved_inputs")
		assert.NotContains(t, keys, "output_mapping")
		assert.JSONEq(t, `"human"`, string(keys["stage_type"]))
	})
	t.Run("set optional fields are emitted", func(t *testing.T) {
		t.Parallel()
		keys := w30aKeys(t, NewWorkflowTaskCreatedPayload(w30aCore(), w30aTask(), "connector", &due, &follow, &connector,
			map[string]any{"k": "v"}, []dsl.IOVar{{}}))
		assert.JSONEq(t, `"2030-01-02T03:04:05Z"`, string(keys["due_at"]))
		assert.JSONEq(t, `"2030-01-01T03:04:05Z"`, string(keys["follow_up_at"]))
		assert.JSONEq(t, `"http"`, string(keys["connector_type"]))
		assert.JSONEq(t, `{"k":"v"}`, string(keys["resolved_inputs"]))
		assert.Contains(t, keys, "output_mapping")
		assert.JSONEq(t, `"connector"`, string(keys["stage_type"]))
	})
}

func TestFailedPayloads_ErrorClassAndCascadeSource_EmissionRules(t *testing.T) {
	t.Parallel()
	t.Run("task error_class omitted when empty, cascade_source always", func(t *testing.T) {
		t.Parallel()
		keys := w30aKeys(t, NewWorkflowTaskFailedPayload(w30aCore(), w30aTask(), "", ""))
		assert.NotContains(t, keys, "error_class")
		require.Contains(t, keys, "cascade_source")
		assert.JSONEq(t, `""`, string(keys["cascade_source"]))
	})
	t.Run("task error_class and cascade_source emitted when set", func(t *testing.T) {
		t.Parallel()
		keys := w30aKeys(t, NewWorkflowTaskFailedPayload(w30aCore(), w30aTask(), "instance_terminate", "connector_error"))
		assert.JSONEq(t, `"connector_error"`, string(keys["error_class"]))
		assert.JSONEq(t, `"instance_terminate"`, string(keys["cascade_source"]))
	})
	t.Run("instance error_class emitted even when empty", func(t *testing.T) {
		t.Parallel()
		keys := w30aKeys(t, NewWorkflowInstanceFailedPayload(w30aCore(), uuid.New(), ""))
		require.Contains(t, keys, "error_class")
		assert.JSONEq(t, `""`, string(keys["error_class"]))
		assert.NotContains(t, keys, "cascade_source")
	})
	t.Run("instance error_class emitted when set", func(t *testing.T) {
		t.Parallel()
		keys := w30aKeys(t, NewWorkflowInstanceFailedPayload(w30aCore(), uuid.New(), "boom"))
		assert.JSONEq(t, `"boom"`, string(keys["error_class"]))
	})
}

func TestTaskEventPayloads_CommonAndTaskScopedCore_FlattenedToTopLevel(t *testing.T) {
	t.Parallel()
	user := uuid.New()
	cases := []struct {
		name    string
		payload any
	}{
		{"created", NewWorkflowTaskCreatedPayload(w30aCore(), w30aTask(), "human", nil, nil, nil, nil, nil)},
		{"claimed", NewWorkflowTaskClaimedPayload(w30aCore(), w30aTask(), user)},
		{"completed", NewWorkflowTaskCompletedPayload(w30aCore(), w30aTask(), user)},
		{"deferred", NewWorkflowTaskDeferredPayload(w30aCore(), w30aTask(), "next", nil, nil)},
		{"reassigned", NewWorkflowTaskReassignedPayload(w30aCore(), w30aTask(), nil, user, ReassignInitiatorAdmin, nil)},
		{"superseded", NewWorkflowTaskSupersededPayload(w30aCore(), w30aTask(), uuid.Nil)},
		{"failed", NewWorkflowTaskFailedPayload(w30aCore(), w30aTask(), "", "")},
		{"sla warning", NewWorkflowTaskSLAWarningPayload(w30aCore(), w30aTask(), time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))},
		{"sla breached", NewWorkflowTaskSLABreachedPayload(w30aCore(), w30aTask(), time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			keys := w30aKeys(t, tc.payload)
			for _, flat := range []string{
				"workflow_instance_id", "subject_id", "business_key", "workflow_version_id",
				"task_id", "node_key", "department_id", "assignee_user_ids",
			} {
				assert.Contains(t, keys, flat)
			}
			assert.NotContains(t, keys, "CommonCore")
			assert.NotContains(t, keys, "TaskScopedCore")
			assert.NotContains(t, keys, "common_core")
			assert.NotContains(t, keys, "task_scoped_core")
			assert.JSONEq(t, `"11111111-1111-4111-8111-111111111111"`, string(keys["workflow_instance_id"]))
			assert.JSONEq(t, `"33333333-3333-4333-8333-333333333333"`, string(keys["task_id"]))
			assert.JSONEq(t, `["55555555-5555-4555-8555-555555555555"]`, string(keys["assignee_user_ids"]))
		})
	}

	t.Run("completed payload round trips", func(t *testing.T) {
		t.Parallel()
		in := NewWorkflowTaskCompletedPayload(w30aCore(), w30aTask(), user)
		raw, err := json.Marshal(in)
		require.NoError(t, err)
		var out WorkflowTaskCompletedPayload
		require.NoError(t, json.Unmarshal(raw, &out))
		assert.Equal(t, in, out)
	})
}

func TestWorkflowTaskDeferredAndReassignedPayloads_OptionalKeys_EmissionRules(t *testing.T) {
	t.Parallel()
	due := time.Date(2030, 5, 6, 7, 8, 9, 0, time.UTC)
	delegation := uuid.MustParse("88888888-8888-4888-8888-888888888888")
	newUser := uuid.New()

	t.Run("deferred emits empty node key, omits nil reason and due_at", func(t *testing.T) {
		t.Parallel()
		keys := w30aKeys(t, NewWorkflowTaskDeferredPayload(w30aCore(), w30aTask(), "", nil, nil))
		require.Contains(t, keys, "deferred_to_node_key")
		assert.JSONEq(t, `""`, string(keys["deferred_to_node_key"]))
		assert.NotContains(t, keys, "reason")
		assert.NotContains(t, keys, "due_at")
	})
	t.Run("deferred emits a set reason and due_at", func(t *testing.T) {
		t.Parallel()
		keys := w30aKeys(t, NewWorkflowTaskDeferredPayload(w30aCore(), w30aTask(), "rework", w30aStrPtr("waiting on docs"), &due))
		assert.JSONEq(t, `"rework"`, string(keys["deferred_to_node_key"]))
		assert.JSONEq(t, `"waiting on docs"`, string(keys["reason"]))
		assert.JSONEq(t, `"2030-05-06T07:08:09Z"`, string(keys["due_at"]))
	})
	t.Run("deferred emits an empty reason", func(t *testing.T) {
		t.Parallel()
		keys := w30aKeys(t, NewWorkflowTaskDeferredPayload(w30aCore(), w30aTask(), "rework", w30aStrPtr(""), nil))
		require.Contains(t, keys, "reason")
		assert.JSONEq(t, `""`, string(keys["reason"]))
	})
	t.Run("reassigned omits nil delegation_id and old_user_id", func(t *testing.T) {
		t.Parallel()
		keys := w30aKeys(t, NewWorkflowTaskReassignedPayload(w30aCore(), w30aTask(), nil, newUser, ReassignInitiatorAdmin, nil))
		assert.NotContains(t, keys, "delegation_id")
		assert.NotContains(t, keys, "old_user_id")
		assert.JSONEq(t, fmt.Sprintf("%q", newUser.String()), string(keys["new_user_id"]))
	})
	t.Run("reassigned emits a set delegation_id", func(t *testing.T) {
		t.Parallel()
		keys := w30aKeys(t, NewWorkflowTaskReassignedPayload(w30aCore(), w30aTask(), nil, newUser, ReassignInitiatorDelegation, &delegation))
		assert.JSONEq(t, `"88888888-8888-4888-8888-888888888888"`, string(keys["delegation_id"]))
		assert.JSONEq(t, `"delegation"`, string(keys["initiator"]))
	})
}

func TestSentinelErrors_MatchingAndDistinctness(t *testing.T) {
	t.Parallel()
	sentinels := []struct {
		name string
		err  error
	}{
		{"ErrNotFound", ErrNotFound},
		{"ErrDuplicateBusinessKey", ErrDuplicateBusinessKey},
		{"ErrRecordVersionConflict", ErrRecordVersionConflict},
		{"ErrDuplicateActiveAssignment", ErrDuplicateActiveAssignment},
		{"ErrAlreadyExists", ErrAlreadyExists},
	}
	for i, tc := range sentinels {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.ErrorIs(t, fmt.Errorf("loading task: %w", tc.err), tc.err)
			assert.ErrorIs(t, fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", tc.err)), tc.err)
			assert.ErrorIs(t, errors.Join(errors.New("other"), tc.err), tc.err)
			assert.NotErrorIs(t, fmt.Errorf("%v", tc.err), tc.err, "%v flattens the chain")
			assert.NotEmpty(t, tc.err.Error())
			for j, other := range sentinels {
				if i == j {
					continue
				}
				assert.NotErrorIs(t, tc.err, other.err)
				assert.NotErrorIs(t, fmt.Errorf("wrapped: %w", tc.err), other.err)
			}
		})
	}
}

func TestEventConstants_InitiatorsAndDirections_HoldExactWireStrings(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"terminated admin", TerminatedInitiatorAdmin, "admin"},
		{"terminated tenant_state", TerminatedInitiatorTenantState, "tenant_state"},
		{"reassign admin", ReassignInitiatorAdmin, "admin"},
		{"reassign override", ReassignInitiatorOverride, "override"},
		{"reassign delegation", ReassignInitiatorDelegation, "delegation"},
		{"direction forward", ForceRouteDirectionForward, "forward"},
		{"direction back", ForceRouteDirectionBack, "back"},
		{"initiator admin", InitiatorAdmin, "admin"},
		{"initiator tenant_state", InitiatorTenantState, "tenant_state"},
		{"cascade source", CascadeSourceInstanceTerminate, "instance_terminate"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.got)
		})
	}
}

func TestValidRoleLevel_Inputs_AcceptsExactlyTheThreeLevels(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		input string
		want  bool
	}{
		{"preparator", "preparator", true},
		{"reviewer", "reviewer", true},
		{"approver", "approver", true},
		{"empty", "", false},
		{"upper case", "Reviewer", false},
		{"all caps", "APPROVER", false},
		{"padded", " reviewer", false},
		{"trailing space", "preparator ", false},
		{"unknown", "admin", false},
		{"plural", "reviewers", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ValidRoleLevel(tc.input))
		})
	}
}
