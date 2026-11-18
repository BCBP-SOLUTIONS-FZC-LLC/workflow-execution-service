package handler_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w21aInstanceKeys is every key a public instance summary carries when completed_at is not set.
var w21aInstanceKeys = []string{
	"business_key", "created_at", "current_node_keys", "id", "record_version", "saved_node_keys", "started_at",
	"started_by_user_id", "status", "subject_id", "temporal_workflow_id", "tenant_id", "updated_at",
	"workflow_id", "workflow_version_id",
}

var w21aTaskEntryKeys = []string{
	"assignee_count", "assignee_mode", "created_at", "department_id", "id", "node_key", "record_version",
	"status", "task_type", "tenant_id", "workflow_instance_id",
}

func w21aInstance(id uuid.UUID, startedAt time.Time) *domain.Instance {
	return &domain.Instance{
		ID: id, TenantID: testTenantID, WorkflowID: uuid.New(), WorkflowVersionID: uuid.New(),
		SubjectID: "subj-" + id.String()[:4], BusinessKey: "bk-" + id.String()[:4],
		TemporalWorkflowID: "tenant:" + testTenantID.String() + ":instance:" + id.String(), TemporalRunID: "run-secret",
		Status: domain.InstanceStatusRunning, CurrentNodeKeys: []string{"finance/Review"}, SavedNodeKeys: []string{"finance/Intake"},
		ContextJSON: json.RawMessage(`{"amount":10}`), OverrideMap: json.RawMessage(`{"finance/Review":"` + testUserID.String() + `"}`),
		TaskQueue: "tenant-queue-secret", StartedByUserID: testUserID, StartedAt: &startedAt, RecordVersion: 5,
		CreatedAt: startedAt.Add(-time.Minute), UpdatedAt: startedAt.Add(time.Minute),
	}
}

func TestW21aListInstances_StartedBoundsWithANumericOffset_AreAcceptedAsThatInstant(t *testing.T) {
	cases := []struct {
		name       string
		after      string
		before     string
		wantAfter  time.Time
		wantBefore time.Time
	}{
		{name: "positive offset", after: "2026-03-04T05:06:07+02:00", wantAfter: time.Date(2026, 3, 4, 3, 6, 7, 0, time.UTC)},
		{name: "negative offset with minutes", after: "2026-03-04T05:06:07-05:30", wantAfter: time.Date(2026, 3, 4, 10, 36, 7, 0, time.UTC)},
		{name: "offset on the upper bound", before: "2026-03-04T05:06:07+02:00", wantBefore: time.Date(2026, 3, 4, 3, 6, 7, 0, time.UTC)},
		{name: "offset on both bounds", after: "2026-03-01T00:00:00+01:00", before: "2026-03-04T00:00:00-01:00",
			wantAfter: time.Date(2026, 2, 28, 23, 0, 0, 0, time.UTC), wantBefore: time.Date(2026, 3, 4, 1, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w21aNewRig()
			query := url.Values{}
			if tc.after != "" {
				query.Set("started_after", tc.after)
			}
			if tc.before != "" {
				query.Set("started_before", tc.before)
			}

			w := rig.get("/api/v1/instances?"+query.Encode(), true, nil)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, rig.instances.lists, 1)
			got := rig.instances.lists[0]
			assert.Equal(t, testTenantID, got.tenantID)
			if tc.after == "" {
				assert.Nil(t, got.filter.StartedAfter)
			} else {
				require.NotNil(t, got.filter.StartedAfter)
				assert.True(t, tc.wantAfter.Equal(*got.filter.StartedAfter), "got %s want %s", got.filter.StartedAfter, tc.wantAfter)
			}
			if tc.before == "" {
				assert.Nil(t, got.filter.StartedBefore)
			} else {
				require.NotNil(t, got.filter.StartedBefore)
				assert.True(t, tc.wantBefore.Equal(*got.filter.StartedBefore), "got %s want %s", got.filter.StartedBefore, tc.wantBefore)
			}
		})
	}
}

func TestW21aListInstances_Items_CarryTheirStoredFieldsAndNothingInternal(t *testing.T) {
	rig := w21aNewRig()
	newer, older := uuid.New(), uuid.New()
	newerStart := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	olderStart := time.Date(2026, 3, 3, 1, 2, 3, 0, time.UTC)
	finished := newerStart.Add(time.Hour)
	first := w21aInstance(newer, newerStart)
	first.CompletedAt = &finished
	first.Status = domain.InstanceStatusCompleted
	rig.instances.rows = []*domain.Instance{first, w21aInstance(older, olderStart)}
	rig.instances.next = &port.Cursor{CreatedAt: olderStart, ID: older}

	w := rig.get("/api/v1/instances", true, nil)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	body := w21aObject(t, w.Body.Bytes())
	items := w21aItems(t, body)
	require.Len(t, items, 2)
	assert.Equal(t, newer.String(), w21aString(t, items[0]["id"]), "the order the repository gave is kept")
	assert.Equal(t, older.String(), w21aString(t, items[1]["id"]))
	assert.Equal(t, w21aWith(w21aInstanceKeys, "completed_at"), w21aKeys(items[0]), "a completed instance adds completed_at and no internal field")
	assert.Equal(t, w21aInstanceKeys, w21aKeys(items[1]), "completed_at is left out while it is not set; no run id, task queue or context")
	assert.True(t, newerStart.Equal(w21aTime(t, items[0]["started_at"])))
	assert.True(t, olderStart.Equal(w21aTime(t, items[1]["started_at"])), "started_at is the stored time")
	assert.True(t, finished.Equal(w21aTime(t, items[0]["completed_at"])))
	assert.Equal(t, "COMPLETED", w21aString(t, items[0]["status"]))
	assert.Equal(t, "RUNNING", w21aString(t, items[1]["status"]))
	assert.Equal(t, rig.instances.rows[1].BusinessKey, w21aString(t, items[1]["business_key"]))
	assert.Equal(t, rig.instances.rows[1].SubjectID, w21aString(t, items[1]["subject_id"]))
	assert.Equal(t, testTenantID.String(), w21aString(t, items[1]["tenant_id"]))
	assert.Equal(t, rig.instances.rows[1].WorkflowVersionID.String(), w21aString(t, items[1]["workflow_version_id"]))
	assert.EqualValues(t, 5, w21aNumber(t, items[1]["record_version"]))
	assert.JSONEq(t, `["finance/Review"]`, string(items[1]["current_node_keys"]))
	assert.JSONEq(t, `["finance/Intake"]`, string(items[1]["saved_node_keys"]))
	pos, err := port.DecodeCursor(w21aString(t, body["next_cursor"]))
	require.NoError(t, err)
	assert.Equal(t, older, pos.ID)
	require.Len(t, rig.instances.lists, 1)
	assert.Equal(t, testTenantID, rig.instances.lists[0].tenantID)
	assert.Nil(t, rig.instances.lists[0].filter.Scope, "an admin reads the whole tenant")
}

func TestW21aListInstances_UndecodableCursor_IsRefusedWithAFixedDetail(t *testing.T) {
	enc := func(s string) string { return base64.URLEncoding.EncodeToString([]byte(s)) }
	cases := []struct {
		name   string
		cursor string
	}{
		{"not base64", "%21%21%21"},
		{"base64 of text that is not JSON", enc("hello")},
		{"JSON without created_at", enc(`{"id":"` + uuid.NewString() + `"}`)},
		{"JSON without id", enc(`{"created_at":"2026-03-04T05:06:07Z"}`)},
		{"JSON with the nil id", enc(`{"created_at":"2026-03-04T05:06:07Z","id":"` + uuid.Nil.String() + `"}`)},
		{"JSON with a created_at that is not a time", enc(`{"created_at":"yesterday","id":"` + uuid.NewString() + `"}`)},
	}
	routes := []string{"/api/v1/instances", "/api/v1/instances/" + testInstID.String() + "/events"}
	for _, route := range routes {
		for _, tc := range cases {
			t.Run(route+" "+tc.name, func(t *testing.T) {
				rig := w21aNewRig()
				rig.instances.rows = []*domain.Instance{w21aInstance(testInstID, time.Now())}

				w := rig.get(route+"?cursor="+tc.cursor, true, nil)

				p := w21aDecodeProblem(t, w)
				require.Equal(t, http.StatusBadRequest, w.Code)
				assert.Equal(t, "BAD_REQUEST", p.Code)
				assert.Equal(t, w21aBadRequestType, p.Type)
				assert.Equal(t, "invalid cursor", p.Detail)
				assert.Empty(t, rig.instances.lists, "a refused request reads nothing")
				assert.Empty(t, rig.instances.gets)
				assert.Empty(t, rig.outbox.lists)
			})
		}
	}
}

func TestW21aListInstances_ADecodableCursor_ReachesTheRepositoryAsThePageStart(t *testing.T) {
	rig := w21aNewRig()
	at := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	id := uuid.New()

	w := rig.get("/api/v1/instances?cursor="+port.EncodeCursor(port.CursorPosition{CreatedAt: at, ID: id}), true, nil)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, rig.instances.lists, 1)
	require.NotNil(t, rig.instances.lists[0].page.After)
	assert.Equal(t, id, rig.instances.lists[0].page.After.ID)
	assert.True(t, at.Equal(rig.instances.lists[0].page.After.CreatedAt))
}

func TestW21aListInstances_StartedBoundNotRFC3339_IsRefusedNamingTheParameter(t *testing.T) {
	values := []string{"not-a-date", "2026-03-04", "2026-03-04T05:06:07", "1772600767"}
	for _, param := range []string{"started_after", "started_before"} {
		for _, value := range values {
			t.Run(param+"="+value, func(t *testing.T) {
				rig := w21aNewRig()

				w := rig.get("/api/v1/instances?"+param+"="+value, true, nil)

				p := w21aDecodeProblem(t, w)
				require.Equal(t, http.StatusBadRequest, w.Code)
				assert.Equal(t, "BAD_REQUEST", p.Code)
				assert.Equal(t, w21aBadRequestType, p.Type)
				assert.Equal(t, "invalid "+param+" query parameter", p.Detail)
				assert.Empty(t, rig.instances.lists, "a refused request reads nothing")
			})
		}
	}
}

func TestW21aGetInstance_Body_CarriesTheInstanceFieldsAndItsTasks(t *testing.T) {
	rig := w21aNewRig()
	startedAt := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	inst := w21aInstance(testInstID, startedAt)
	rig.instances.rows = []*domain.Instance{inst}
	taskA, taskB := uuid.New(), uuid.New()
	rig.tasks.rows = []*domain.Task{
		{ID: taskA, TenantID: testTenantID, WorkflowInstanceID: testInstID, NodeKey: "finance/Review", DepartmentID: uuid.New(), Status: domain.TaskStatusReady, RecordVersion: 2, AssigneeMode: "single"},
		{ID: taskB, TenantID: testTenantID, WorkflowInstanceID: testInstID, NodeKey: "finance/Approve", DepartmentID: uuid.New(), Status: domain.TaskStatusCompleted, RecordVersion: 4, AssigneeMode: "all"},
	}
	rig.assignments.counts = map[uuid.UUID]int{taskA: 1, taskB: 3}

	w := rig.get("/api/v1/instances/"+testInstID.String(), true, nil)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	body := w21aObject(t, w.Body.Bytes())
	assert.Equal(t, w21aWith(w21aInstanceKeys, "has_context", "override_map", "tasks"), w21aKeys(body), "the instance fields, then tasks, has_context and override_map")
	assert.Equal(t, testInstID.String(), w21aString(t, body["id"]))
	assert.Equal(t, testTenantID.String(), w21aString(t, body["tenant_id"]))
	assert.Equal(t, inst.WorkflowID.String(), w21aString(t, body["workflow_id"]))
	assert.Equal(t, inst.WorkflowVersionID.String(), w21aString(t, body["workflow_version_id"]))
	assert.Equal(t, inst.SubjectID, w21aString(t, body["subject_id"]))
	assert.Equal(t, inst.BusinessKey, w21aString(t, body["business_key"]))
	assert.Equal(t, inst.TemporalWorkflowID, w21aString(t, body["temporal_workflow_id"]))
	assert.Equal(t, "RUNNING", w21aString(t, body["status"]))
	assert.Equal(t, testUserID.String(), w21aString(t, body["started_by_user_id"]))
	assert.EqualValues(t, 5, w21aNumber(t, body["record_version"]))
	assert.True(t, startedAt.Equal(w21aTime(t, body["started_at"])), "started_at is the stored instant")
	assert.True(t, inst.CreatedAt.Equal(w21aTime(t, body["created_at"])))
	assert.True(t, inst.UpdatedAt.Equal(w21aTime(t, body["updated_at"])))
	assert.JSONEq(t, `["finance/Review"]`, string(body["current_node_keys"]))
	assert.JSONEq(t, `["finance/Intake"]`, string(body["saved_node_keys"]))
	assert.JSONEq(t, `true`, string(body["has_context"]))
	assert.JSONEq(t, `{"finance/Review":"`+testUserID.String()+`"}`, string(body["override_map"]))
	var tasks []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body["tasks"], &tasks))
	require.Len(t, tasks, 2)
	assert.Equal(t, taskA.String(), w21aString(t, tasks[0]["id"]))
	assert.Equal(t, taskB.String(), w21aString(t, tasks[1]["id"]))
	assert.Equal(t, []w21aLookup{{testTenantID, testInstID}}, rig.instances.gets)
	assert.Equal(t, []w21aLookup{{testTenantID, testInstID}}, rig.tasks.byInstance)
}

func TestW21aGetInstance_NeverExposesTheRunTheQueueOrTheContext(t *testing.T) {
	rig := w21aNewRig()
	rig.instances.rows = []*domain.Instance{w21aInstance(testInstID, time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC))}

	w := rig.get("/api/v1/instances/"+testInstID.String(), true, nil)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	body := w21aObject(t, w.Body.Bytes())
	for _, hidden := range []string{"temporal_run_id", "task_queue", "context_json"} {
		assert.NotContains(t, body, hidden)
	}
	assert.NotContains(t, w.Body.String(), "run-secret", "the stored run id is not in the body under any name")
	assert.NotContains(t, w.Body.String(), "tenant-queue-secret", "the stored queue is not in the body under any name")
	assert.NotContains(t, w.Body.String(), `"amount"`, "the stored context is not in the body under any name")
	assert.JSONEq(t, `true`, string(body["has_context"]), "only the fact that a context exists is shown")
}

func TestW21aGetInstance_NoTasks_AnswersAnEmptyTasksArray(t *testing.T) {
	rig := w21aNewRig()
	rig.instances.rows = []*domain.Instance{w21aInstance(testInstID, time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC))}

	w := rig.get("/api/v1/instances/"+testInstID.String(), true, nil)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	body := w21aObject(t, w.Body.Bytes())
	assert.JSONEq(t, `[]`, string(body["tasks"]), "no tasks is [] and not null")
	assert.Empty(t, rig.assignments.tallies, "no counts are read for an empty task list")
}

func TestW21aGetInstance_TaskEntries_CarryTheirFieldsAndOmitUnsetOptionalOnes(t *testing.T) {
	rig := w21aNewRig()
	rig.instances.rows = []*domain.Instance{w21aInstance(testInstID, time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC))}
	taskA, taskB, deferredFrom := uuid.New(), uuid.New(), uuid.New()
	dept := uuid.New()
	created := time.Date(2026, 3, 4, 6, 0, 0, 0, time.UTC)
	due, done := created.Add(24*time.Hour), created.Add(time.Hour)
	connector := "send-email"
	rig.tasks.rows = []*domain.Task{
		{ID: taskA, TenantID: testTenantID, WorkflowInstanceID: testInstID, NodeKey: "finance/Review", DepartmentID: dept, Status: domain.TaskStatusCompleted,
			RecordVersion: 7, AssigneeMode: "all", DueAt: &due, DeferredFromTaskID: &deferredFrom, CompletedAt: &done, CreatedAt: created},
		{ID: taskB, TenantID: testTenantID, WorkflowInstanceID: testInstID, NodeKey: "finance/Notify", DepartmentID: dept, Status: domain.TaskStatusReady,
			RecordVersion: 1, AssigneeMode: "single", ConnectorType: &connector, CreatedAt: created},
	}
	rig.assignments.counts = map[uuid.UUID]int{taskA: 3}

	w := rig.get("/api/v1/instances/"+testInstID.String(), true, nil)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var tasks []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(w21aObject(t, w.Body.Bytes())["tasks"], &tasks))
	require.Len(t, tasks, 2)
	full, bare := tasks[0], tasks[1]
	assert.Equal(t, w21aWith(w21aTaskEntryKeys, "completed_at", "deferred_from_task_id", "due_at"), w21aKeys(full))
	assert.Equal(t, w21aTaskEntryKeys, w21aKeys(bare), "due_at, deferred_from_task_id and completed_at are left out while unset")
	assert.Equal(t, taskA.String(), w21aString(t, full["id"]))
	assert.Equal(t, testInstID.String(), w21aString(t, full["workflow_instance_id"]))
	assert.Equal(t, testTenantID.String(), w21aString(t, full["tenant_id"]))
	assert.Equal(t, "finance/Review", w21aString(t, full["node_key"]))
	assert.Equal(t, "userTask", w21aString(t, full["task_type"]))
	assert.Equal(t, dept.String(), w21aString(t, full["department_id"]))
	assert.Equal(t, "COMPLETED", w21aString(t, full["status"]))
	assert.EqualValues(t, 7, w21aNumber(t, full["record_version"]))
	assert.Equal(t, "all", w21aString(t, full["assignee_mode"]))
	assert.EqualValues(t, 3, w21aNumber(t, full["assignee_count"]))
	assert.Equal(t, deferredFrom.String(), w21aString(t, full["deferred_from_task_id"]))
	assert.True(t, due.Equal(w21aTime(t, full["due_at"])))
	assert.True(t, done.Equal(w21aTime(t, full["completed_at"])))
	assert.True(t, created.Equal(w21aTime(t, full["created_at"])))
	assert.Equal(t, "serviceTask", w21aString(t, bare["task_type"]), "a connector-typed task is a serviceTask")
	assert.EqualValues(t, 0, w21aNumber(t, bare["assignee_count"]), "a task with no active assignee counts zero")
	require.Len(t, rig.assignments.tallies, 1)
	assert.Equal(t, testTenantID, rig.assignments.tallies[0].tenantID)
	assert.ElementsMatch(t, []uuid.UUID{taskA, taskB}, rig.assignments.tallies[0].taskIDs, "one count query covers every task of the page")
}

func TestW21aGetInstance_IdNotAUUID_IsRefusedNamingThePathParameter(t *testing.T) {
	cases := []string{"not-a-uuid", "123", "44444444-4444-4444-4444-44444444444", "44444444-4444-4444-4444-4444444444444"}
	for _, id := range cases {
		t.Run(id, func(t *testing.T) {
			rig := w21aNewRig()

			w := rig.get("/api/v1/instances/"+id, true, nil)

			p := w21aDecodeProblem(t, w)
			require.Equal(t, http.StatusBadRequest, w.Code)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w21aBadRequestType, p.Type)
			assert.Equal(t, "invalid id path parameter", p.Detail)
			assert.Equal(t, []w21aParam{{Name: "id", Reason: "is not valid"}}, p.InvalidParams)
			assert.Empty(t, rig.instances.gets, "a refused request reads nothing")
		})
	}
}

func TestW21aListInstanceEvents_Items_CarryTheirFieldsAndOmitUnsetOnes(t *testing.T) {
	rig := w21aNewRig()
	rig.instances.rows = []*domain.Instance{w21aInstance(testInstID, time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC))}
	taskID, actor, taskEvent, startEvent := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	later := time.Date(2026, 3, 4, 7, 0, 0, 0, time.UTC)
	earlier := later.Add(-time.Hour)
	rig.outbox.rows = []*domain.OutboxEventRecord{
		{ID: taskEvent, EventType: domain.EventWorkflowTaskCreated, CreatedAt: later,
			Payload: json.RawMessage(`{"tenant_id":"` + testTenantID.String() + `","actor":"` + actor.String() + `","data":{"task_id":"` + taskID.String() + `","node_key":"finance/Review","resolved_inputs":{"api_key":"connector-secret"}}}`)},
		{ID: startEvent, EventType: domain.EventWorkflowInstanceStarted, CreatedAt: earlier,
			Payload: json.RawMessage(`{"tenant_id":"` + testTenantID.String() + `","data":{"subject_id":"s"}}`)},
	}

	w := rig.get("/api/v1/instances/"+testInstID.String()+"/events", true, nil)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	body := w21aObject(t, w.Body.Bytes())
	assert.Equal(t, []string{"items"}, w21aKeys(body), "no next_cursor key when there is no next page")
	items := w21aItems(t, body)
	require.Len(t, items, 2)
	task, started := items[0], items[1]
	assert.Equal(t, taskEvent.String(), w21aString(t, task["id"]), "the order the repository gave is kept")
	assert.Equal(t, []string{"actor_user_id", "event_type", "id", "node_key", "occurred_at", "payload_json", "task_id", "tenant_id", "workflow_instance_id"}, w21aKeys(task))
	assert.Equal(t, []string{"event_type", "id", "occurred_at", "payload_json", "tenant_id", "workflow_instance_id"}, w21aKeys(started),
		"actor_user_id, task_id and node_key are left out while unset")
	assert.Equal(t, testTenantID.String(), w21aString(t, task["tenant_id"]))
	assert.Equal(t, testTenantID.String(), w21aString(t, started["tenant_id"]))
	assert.Equal(t, testInstID.String(), w21aString(t, task["workflow_instance_id"]))
	assert.Equal(t, "TASK_CREATED", w21aString(t, task["event_type"]))
	assert.Equal(t, "INSTANCE_STARTED", w21aString(t, started["event_type"]))
	assert.Equal(t, actor.String(), w21aString(t, task["actor_user_id"]))
	assert.Equal(t, taskID.String(), w21aString(t, task["task_id"]))
	assert.Equal(t, "finance/Review", w21aString(t, task["node_key"]))
	assert.True(t, later.Equal(w21aTime(t, task["occurred_at"])))
	assert.True(t, earlier.Equal(w21aTime(t, started["occurred_at"])))
	assert.JSONEq(t, `{"subject_id":"s"}`, string(started["payload_json"]))
	assert.JSONEq(t, `{"task_id":"`+taskID.String()+`","node_key":"finance/Review"}`, string(task["payload_json"]),
		"the task-created payload keeps its task id and node key and drops the connector-only inputs")
	assert.NotContains(t, string(task["payload_json"]), "resolved_inputs")
	assert.NotContains(t, w.Body.String(), "connector-secret")
	assert.Equal(t, []w21aLookup{{testTenantID, testInstID}}, rig.instances.gets)
	require.Len(t, rig.outbox.lists, 1)
	assert.Equal(t, testTenantID, rig.outbox.lists[0].tenantID)
	assert.Equal(t, testInstID, rig.outbox.lists[0].instanceID)
}

func TestW21aListInstanceEvents_ANextPage_AnswersTheCursorTheRepositoryGave(t *testing.T) {
	rig := w21aNewRig()
	rig.instances.rows = []*domain.Instance{w21aInstance(testInstID, time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC))}
	last := uuid.New()
	at := time.Date(2026, 3, 4, 7, 0, 0, 0, time.UTC)
	rig.outbox.rows = []*domain.OutboxEventRecord{{ID: last, EventType: domain.EventWorkflowInstanceStarted, CreatedAt: at, Payload: json.RawMessage(`{"data":{}}`)}}
	rig.outbox.next = &port.Cursor{CreatedAt: at, ID: last}

	w := rig.get("/api/v1/instances/"+testInstID.String()+"/events?limit=1", true, nil)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	pos, err := port.DecodeCursor(w21aString(t, w21aObject(t, w.Body.Bytes())["next_cursor"]))
	require.NoError(t, err)
	assert.Equal(t, last, pos.ID)
	assert.True(t, at.Equal(pos.CreatedAt))
	require.Len(t, rig.outbox.lists, 1)
	assert.Equal(t, 1, rig.outbox.lists[0].page.Limit)
}

func TestW21aListInstanceEvents_LimitNotAPositiveInteger_IsRefusedWithAFixedDetail(t *testing.T) {
	for _, limit := range []string{"0", "-1", "abc", "1.5", "99999999999999999999"} {
		t.Run("limit="+limit, func(t *testing.T) {
			rig := w21aNewRig()
			rig.instances.rows = []*domain.Instance{w21aInstance(testInstID, time.Now())}

			w := rig.get("/api/v1/instances/"+testInstID.String()+"/events?limit="+limit, true, nil)

			p := w21aDecodeProblem(t, w)
			require.Equal(t, http.StatusBadRequest, w.Code)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w21aBadRequestType, p.Type)
			assert.Equal(t, "limit must be a positive integer", p.Detail)
			assert.Empty(t, rig.instances.gets, "a refused request reads nothing")
			assert.Empty(t, rig.outbox.lists)
		})
	}
}
