package handler_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
)

type recordingUserTaskPauser struct {
	calls      int
	tenantID   uuid.UUID
	userID     uuid.UUID
	department *uuid.UUID
}

func (p *recordingUserTaskPauser) PauseUserTasks(_ context.Context, tenantID, userID uuid.UUID, departmentID *uuid.UUID) error {
	p.calls++
	p.tenantID, p.userID, p.department = tenantID, userID, departmentID
	return nil
}

func TestPauseUserTasks_OptionalDepartment(t *testing.T) {
	userID := uuid.New()
	deptID := uuid.New()
	path := "/api/v1/internal/users/" + userID.String() + "/pause-tasks"

	cases := []struct {
		name       string
		body       map[string]any
		wantStatus int
		wantCalled bool
		wantDept   *uuid.UUID
	}{
		{"department given is passed on", map[string]any{"tenant_id": testTenantID, "department_id": deptID}, http.StatusNoContent, true, &deptID},
		{"department absent means every department", map[string]any{"tenant_id": testTenantID}, http.StatusNoContent, true, nil},
		{"department null means every department", map[string]any{"tenant_id": testTenantID, "department_id": nil}, http.StatusNoContent, true, nil},
		{"department that is not a uuid is refused", map[string]any{"tenant_id": testTenantID, "department_id": "nope"}, http.StatusBadRequest, false, nil},
		{"department of the wrong type is refused", map[string]any{"tenant_id": testTenantID, "department_id": 7}, http.StatusBadRequest, false, nil},
		{"empty department is refused, not read as every department", map[string]any{"tenant_id": testTenantID, "department_id": ""}, http.StatusBadRequest, false, nil},
		{"the all-zero department is refused", map[string]any{"tenant_id": testTenantID, "department_id": uuid.Nil}, http.StatusBadRequest, false, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			pauser := &recordingUserTaskPauser{}
			h := handler.New(handler.Services{UserTaskPauser: pauser})
			r := gin.New()
			r.POST("/api/v1/internal/users/:user_id/pause-tasks", h.PauseUserTasks)

			w := do(r, internalReq(http.MethodPost, path, tc.body))

			require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
			if !tc.wantCalled {
				assert.Zero(t, pauser.calls, "a refused request must not pause anything")
				assert.Contains(t, w.Body.String(), handler.CodeBadRequest)
				return
			}
			assert.Equal(t, 1, pauser.calls)
			assert.Equal(t, userID, pauser.userID)
			assert.Equal(t, testTenantID, pauser.tenantID)
			assert.Equal(t, tc.wantDept, pauser.department)
		})
	}
}
