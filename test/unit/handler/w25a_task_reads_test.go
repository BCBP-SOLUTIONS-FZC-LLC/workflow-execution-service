package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// A cursor the codec refuses is a 400 BAD_REQUEST with the detail "invalid cursor" and the service is not asked;
// a valid cursor reaches the service as the keyset position; an empty cursor= means the first page.
func TestListTasks_Cursor_RefusedWhenInvalidPassedOnWhenValidFirstPageWhenEmpty(t *testing.T) {
	position := port.CursorPosition{CreatedAt: time.Date(2026, 11, 18, 9, 30, 0, 0, time.UTC), ID: uuid.New()}
	cases := []struct {
		name       string
		query      string
		wantStatus int
		wantCursor *port.CursorPosition
	}{
		{name: "not base64", query: "cursor=" + url.QueryEscape("not-a-valid-cursor!!!"), wantStatus: http.StatusBadRequest},
		{name: "base64 of text that is not JSON", query: "cursor=bm90LWpzb24=", wantStatus: http.StatusBadRequest},
		{name: "JSON of the wrong shape", query: "cursor=eyJmb28iOiJiYXIifQ==", wantStatus: http.StatusBadRequest},
		{name: "a valid cursor", query: "cursor=" + url.QueryEscape(port.EncodeCursor(position)), wantStatus: http.StatusOK, wantCursor: &position},
		{name: "an empty cursor", query: "cursor=", wantStatus: http.StatusOK},
		{name: "no cursor", query: "limit=5", wantStatus: http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls []port.Page
			tasks := &fakeTaskService{
				list: func(_ context.Context, tenantID uuid.UUID, _ port.ReadScope, _ port.TaskFilter, page port.Page) (port.PageResult[*port.Task], error) {
					assert.Equal(t, testTenantID, tenantID)
					calls = append(calls, page)
					return port.PageResult[*port.Task]{}, nil
				},
			}
			router := newRouter(newHandler(tasks, &fakeEligibilityChecker{}))

			w := w25aServe(router, w25aRaw(http.MethodGet, "/api/v1/tasks?"+tc.query, "", ""))

			if tc.wantStatus == http.StatusBadRequest {
				p := w25aDecodeProblem(t, w, http.StatusBadRequest)
				assert.Equal(t, "BAD_REQUEST", p.Code)
				assert.Equal(t, "invalid cursor", p.Detail)
				assert.Empty(t, calls, "the service is not asked for a page")
				return
			}
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, calls, 1)
			if tc.wantCursor == nil {
				assert.Nil(t, calls[0].Cursor, "no keyset position means the first page")
				return
			}
			require.NotNil(t, calls[0].Cursor)
			assert.True(t, tc.wantCursor.CreatedAt.Equal(calls[0].Cursor.CreatedAt))
			assert.Equal(t, tc.wantCursor.ID, calls[0].Cursor.ID)
		})
	}
}

func w25aFullTask() *port.Task {
	due := time.Date(2026, 12, 1, 10, 0, 0, 0, time.UTC)
	completed := time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
	followUp := time.Date(2026, 11, 25, 8, 0, 0, 0, time.UTC)
	deferredFrom := uuid.New()
	connector := "storage"
	return &port.Task{
		ID: testTaskID, TenantID: testTenantID, WorkflowInstanceID: testInstID, NodeKey: "finance/review",
		TaskType: "userTask", DepartmentID: uuid.New(), Status: port.TaskStatusCompleted, RecordVersion: 7,
		AssigneeMode: "all", AssigneeCount: 3, ConnectorType: &connector, ExtrasJSON: json.RawMessage(`{"note":"x"}`),
		DeferredFromTaskID: &deferredFrom, DueAt: &due, FollowUpAt: &followUp,
		CreatedAt: time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC), UpdatedAt: time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC),
		CompletedAt: &completed,
	}
}

func w25aKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// A task is rendered with exactly its public fields on the list and on the answer to a claim: the connector
// type, the extras, the follow-up time, the update time and the visit count are never part of it.
func TestTaskResponses_ListItemAndClaimAnswer_CarryExactlyThePublicFields(t *testing.T) {
	task := w25aFullTask()
	wantKeys := []string{
		"assignee_count", "assignee_mode", "completed_at", "created_at", "deferred_from_task_id", "department_id",
		"due_at", "id", "node_key", "record_version", "status", "task_type", "tenant_id", "workflow_instance_id",
	}
	tasks := &fakeTaskService{
		list: func(context.Context, uuid.UUID, port.ReadScope, port.TaskFilter, port.Page) (port.PageResult[*port.Task], error) {
			return port.PageResult[*port.Task]{Items: []*port.Task{task}}, nil
		},
		claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) { return task, nil },
	}
	router := newRouter(newHandler(tasks, &fakeEligibilityChecker{}))

	list := w25aServe(router, w25aRaw(http.MethodGet, "/api/v1/tasks", "", ""))
	claim := w25aServe(router, w25aRaw(http.MethodPost, w25aClaimPath(), "application/json", w25aClaimBody))

	require.Equal(t, http.StatusOK, list.Code, list.Body.String())
	require.Equal(t, http.StatusAccepted, claim.Code, claim.Body.String())
	var listBody struct {
		Items []map[string]any `json:"items"`
	}
	decodeJSON(t, list.Body, &listBody)
	require.Len(t, listBody.Items, 1)
	var claimBody map[string]any
	decodeJSON(t, claim.Body, &claimBody)
	for name, got := range map[string]map[string]any{"list item": listBody.Items[0], "claim answer": claimBody} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, wantKeys, w25aKeys(got))
			assert.Equal(t, task.ID.String(), got["id"])
			assert.Equal(t, task.WorkflowInstanceID.String(), got["workflow_instance_id"])
			assert.Equal(t, testTenantID.String(), got["tenant_id"])
			assert.Equal(t, "finance/review", got["node_key"])
			assert.Equal(t, "userTask", got["task_type"])
			assert.Equal(t, task.DepartmentID.String(), got["department_id"])
			assert.Equal(t, "COMPLETED", got["status"])
			assert.EqualValues(t, 7, got["record_version"])
			assert.Equal(t, "all", got["assignee_mode"])
			assert.EqualValues(t, 3, got["assignee_count"])
			assert.Equal(t, "2026-12-01T10:00:00Z", got["due_at"])
			assert.Equal(t, task.DeferredFromTaskID.String(), got["deferred_from_task_id"])
			assert.Equal(t, "2026-11-01T08:00:00Z", got["created_at"])
			assert.Equal(t, "2026-11-20T08:00:00Z", got["completed_at"])
		})
	}
}

// The read scope is built from the caller's user id, the admin status and the x-departments header: entries
// that are blank, have no colon or carry an id that is not a UUID are dropped; the valid ones keep their order.
func TestTaskReads_Scope_DropsMalformedDepartmentEntries(t *testing.T) {
	deptA, deptB := uuid.New(), uuid.New()
	cases := []struct {
		name   string
		header string
		roles  string
		want   []port.DepartmentRole
		admin  bool
	}{
		{name: "valid pairs with spaces trimmed", header: " " + deptA.String() + ":approver , " + deptB.String() + ":reviewer ",
			want: []port.DepartmentRole{{DepartmentID: deptA, Role: "approver"}, {DepartmentID: deptB, Role: "reviewer"}}},
		{name: "blank entries are dropped", header: deptA.String() + ":approver,, ,", want: []port.DepartmentRole{{DepartmentID: deptA, Role: "approver"}}},
		{name: "an entry without a colon is dropped", header: deptA.String() + ",nocolon," + deptB.String() + ":reviewer",
			want: []port.DepartmentRole{{DepartmentID: deptB, Role: "reviewer"}}},
		{name: "an entry whose id is not a UUID is dropped", header: "not-a-uuid:approver," + deptA.String() + ":approver",
			want: []port.DepartmentRole{{DepartmentID: deptA, Role: "approver"}}},
		{name: "only malformed entries leave no departments", header: "x," + ":approver,not-a-uuid:r", want: nil},
		{name: "an absent header leaves no departments", header: "", want: nil},
		{name: "an admin role sets the admin flag", header: deptA.String() + ":approver", roles: "tenant_admin", admin: true,
			want: []port.DepartmentRole{{DepartmentID: deptA, Role: "approver"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var listScope, getScope port.ReadScope
			tasks := &fakeTaskService{
				list: func(_ context.Context, _ uuid.UUID, scope port.ReadScope, _ port.TaskFilter, _ port.Page) (port.PageResult[*port.Task], error) {
					listScope = scope
					return port.PageResult[*port.Task]{}, nil
				},
				get: func(_ context.Context, _, _ uuid.UUID, scope port.ReadScope) (*port.Task, []*port.TaskAssignment, error) {
					getScope = scope
					return w25aFullTask(), nil, nil
				},
			}
			router := newRouter(newHandler(tasks, &fakeEligibilityChecker{}))
			for _, target := range []string{"/api/v1/tasks", "/api/v1/tasks/" + testTaskID.String()} {
				r := w25aRaw(http.MethodGet, target, "", "")
				if tc.header != "" {
					r.Header.Set("x-departments", tc.header)
				}
				if tc.roles != "" {
					r.Header.Set("x-tenant-roles", tc.roles)
				}
				w := w25aServe(router, r)
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			}

			for _, got := range []port.ReadScope{listScope, getScope} {
				assert.Equal(t, testUserID, got.CallerUserID)
				assert.Equal(t, tc.admin, got.IsAdmin)
				if len(tc.want) == 0 {
					assert.Empty(t, got.Departments)
				} else {
					assert.Equal(t, tc.want, got.Departments)
				}
			}
		})
	}
}
