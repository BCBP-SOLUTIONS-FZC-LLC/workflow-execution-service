package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type hhbProblem struct {
	Type          string `json:"type"`
	Code          string `json:"code"`
	Detail        string `json:"detail"`
	InvalidParams []struct {
		Name   string `json:"name"`
		Reason string `json:"reason"`
	} `json:"invalid_params"`
}

func hhbDecodeProblem(t *testing.T, w *httptest.ResponseRecorder) hhbProblem {
	t.Helper()
	var p hhbProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	return p
}

const hhbBadRequestType = "https://errors.bcbp.io/execution/bad-request"

func TestListTasks_MalformedDepartmentsHeaderEntries_AreDroppedAndValidOnesScope(t *testing.T) {
	validDept := uuid.New()
	laterDept := uuid.New()
	header := strings.Join([]string{
		"no-colon-at-all",
		"not-a-uuid:approver",
		":approver",
		"",
		"  ",
		validDept.String() + ":approver",
		"  " + laterDept.String() + ":reviewer  ",
	}, ",")

	var gotTenant uuid.UUID
	var gotScope port.ReadScope
	fake := &fakeTaskService{
		list: func(_ context.Context, tenantID uuid.UUID, scope port.ReadScope, _ port.TaskFilter, _ port.Page) (port.PageResult[*port.Task], error) {
			gotTenant, gotScope = tenantID, scope
			return port.PageResult[*port.Task]{}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	r := req(http.MethodGet, "/api/v1/tasks", nil)
	r.Header.Set("x-departments", header)
	w := do(router, r)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, testTenantID, gotTenant)
	assert.Equal(t, testUserID, gotScope.CallerUserID)
	assert.False(t, gotScope.IsAdmin)
	assert.Equal(t, []port.DepartmentRole{
		{DepartmentID: validDept, Role: "approver"},
		{DepartmentID: laterDept, Role: "reviewer"},
	}, gotScope.Departments, "only the well-formed pairs may reach the service")
}

func TestListTasks_OnlyMalformedDepartmentsHeader_YieldsNoDepartments(t *testing.T) {
	called := false
	var gotScope port.ReadScope
	fake := &fakeTaskService{
		list: func(_ context.Context, _ uuid.UUID, scope port.ReadScope, _ port.TaskFilter, _ port.Page) (port.PageResult[*port.Task], error) {
			called, gotScope = true, scope
			return port.PageResult[*port.Task]{}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	r := req(http.MethodGet, "/api/v1/tasks", nil)
	r.Header.Set("x-departments", "garbage,not-a-uuid:approver")
	w := do(router, r)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.True(t, called)
	assert.Empty(t, gotScope.Departments)
}

func TestListTasks_InvalidFilterAndInvalidLimit_NamesTheFilter(t *testing.T) {
	cases := []struct {
		name   string
		filter string
		param  string
	}{
		{"instance_id", "instance_id=not-a-uuid", "instance_id"},
		{"department_id", "department_id=not-a-uuid", "department_id"},
		{"assignee_user_id", "assignee_user_id=not-a-uuid", "assignee_user_id"},
		{"due_before", "due_before=tomorrow", "due_before"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			fake := &fakeTaskService{
				list: func(context.Context, uuid.UUID, port.ReadScope, port.TaskFilter, port.Page) (port.PageResult[*port.Task], error) {
					called = true
					return port.PageResult[*port.Task]{}, nil
				},
			}
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, req(http.MethodGet, "/api/v1/tasks?limit=0&"+tc.filter, nil))

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			p := hhbDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, hhbBadRequestType, p.Type)
			assert.Equal(t, "invalid "+tc.param+" query parameter", p.Detail)
			assert.NotContains(t, p.Detail, "limit")
			assert.False(t, called)
		})
	}
}

func TestListTasks_AllFiltersEmpty_AreTreatedAsAbsent(t *testing.T) {
	called := false
	var gotFilter port.TaskFilter
	var gotPage port.Page
	fake := &fakeTaskService{
		list: func(_ context.Context, _ uuid.UUID, _ port.ReadScope, filter port.TaskFilter, page port.Page) (port.PageResult[*port.Task], error) {
			called, gotFilter, gotPage = true, filter, page
			return port.PageResult[*port.Task]{}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks?status=&instance_id=&department_id=&assignee_user_id=&due_before=", nil))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.True(t, called)
	assert.Nil(t, gotFilter.Status)
	assert.Nil(t, gotFilter.WorkflowInstanceID)
	assert.Nil(t, gotFilter.DepartmentID)
	assert.Nil(t, gotFilter.AssigneeUserID)
	assert.Nil(t, gotFilter.DueBefore)
	assert.Nil(t, gotPage.Cursor)

	var body struct {
		Items []json.RawMessage `json:"items"`
	}
	require.NoError(t, json.NewDecoder(bytes.NewReader(w.Body.Bytes())).Decode(&body))
	assert.NotNil(t, body.Items, "an empty page is an empty list, not a missing field")
}
