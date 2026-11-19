package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type w20aTaskLookup struct{ tenantID, taskID uuid.UUID }

type w20aTaskRepo struct {
	port.TaskRepository
	row     *domain.Task
	lookups []w20aTaskLookup
}

func (r *w20aTaskRepo) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.lookups = append(r.lookups, w20aTaskLookup{tenantID, id})
	if r.row.TenantID != tenantID || r.row.ID != id {
		return nil, domain.ErrNotFound
	}
	return r.row, nil
}

type w20aAssignmentRepo struct {
	port.TaskAssignmentRepository
	rows    []*domain.TaskAssignment
	lookups []w20aTaskLookup
}

func (r *w20aAssignmentRepo) ListActiveByTask(_ context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.lookups = append(r.lookups, w20aTaskLookup{tenantID, taskID})
	return r.rows, nil
}

// The real task service sits behind the real handler: the task belongs to one department and is assigned to
// someone else, so only the caller's x-departments header can grant access.
func TestW20aGetTask_DepartmentsHeaderThatDoesNotMatch_IsRefusedWith403(t *testing.T) {
	taskDept, otherDept := uuid.New(), uuid.New()
	cases := []struct {
		name        string
		header      string
		sendHeader  bool
		wantStatus  int
		wantCode    string
		wantGranted bool
	}{
		{name: "header absent", wantStatus: http.StatusForbidden, wantCode: "NOT_AUTHORIZED_FOR_RESOURCE"},
		{name: "header empty", sendHeader: true, header: "", wantStatus: http.StatusForbidden, wantCode: "NOT_AUTHORIZED_FOR_RESOURCE"},
		{name: "header only whitespace and commas", sendHeader: true, header: " , ,", wantStatus: http.StatusForbidden, wantCode: "NOT_AUTHORIZED_FOR_RESOURCE"},
		{name: "entry without a role", sendHeader: true, header: taskDept.String(), wantStatus: http.StatusForbidden, wantCode: "NOT_AUTHORIZED_FOR_RESOURCE"},
		{name: "entry whose id is not a uuid", sendHeader: true, header: "not-a-uuid:approver", wantStatus: http.StatusForbidden, wantCode: "NOT_AUTHORIZED_FOR_RESOURCE"},
		{name: "entry with an empty id", sendHeader: true, header: ":approver", wantStatus: http.StatusForbidden, wantCode: "NOT_AUTHORIZED_FOR_RESOURCE"},
		{name: "only other departments", sendHeader: true, header: otherDept.String() + ":approver," + uuid.NewString() + ":reviewer", wantStatus: http.StatusForbidden, wantCode: "NOT_AUTHORIZED_FOR_RESOURCE"},
		{name: "the task's department with a valid entry", sendHeader: true, header: taskDept.String() + ":approver", wantStatus: http.StatusOK, wantGranted: true},
		{name: "malformed entries around the task's department", sendHeader: true, header: "garbage, " + taskDept.String() + ":approver ,:x", wantStatus: http.StatusOK, wantGranted: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := &w20aTaskRepo{row: &domain.Task{
				ID: testTaskID, TenantID: testTenantID, WorkflowInstanceID: testInstID, DepartmentID: taskDept,
				NodeKey: "finance/Review", Status: domain.TaskStatusReady, RecordVersion: 2,
			}}
			assignments := &w20aAssignmentRepo{rows: []*domain.TaskAssignment{{ID: uuid.New(), TenantID: testTenantID, TaskID: testTaskID, UserID: uuid.New(), IsActive: true}}}
			svc := &service.TaskService{Tasks: tasks, Assignments: assignments}
			router := newRouter(handler.New(handler.Services{Tasks: svc}))
			r := req(http.MethodGet, "/api/v1/tasks/"+testTaskID.String(), nil)
			if tc.sendHeader {
				r.Header.Set("x-departments", tc.header)
			}

			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)

			require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
			assert.Equal(t, []w20aTaskLookup{{testTenantID, testTaskID}}, tasks.lookups)
			assert.Equal(t, []w20aTaskLookup{{testTenantID, testTaskID}}, assignments.lookups)
			if tc.wantGranted {
				var got struct {
					ID           uuid.UUID `json:"id"`
					DepartmentID uuid.UUID `json:"department_id"`
				}
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got), w.Body.String())
				assert.Equal(t, testTaskID, got.ID)
				assert.Equal(t, taskDept, got.DepartmentID)
				return
			}
			p := w20aDecode(t, w)
			assert.Equal(t, tc.wantCode, p.Code)
			assert.NotContains(t, w.Body.String(), taskDept.String(), "a refused read must not leak the task")
		})
	}
}
