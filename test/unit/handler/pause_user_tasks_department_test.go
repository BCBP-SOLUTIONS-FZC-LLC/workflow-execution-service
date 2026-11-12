package handler_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type recordingUserTaskPauser struct {
	err        error
	calls      int
	tenantID   uuid.UUID
	userID     uuid.UUID
	department *uuid.UUID
}

func (p *recordingUserTaskPauser) PauseUserTasks(_ context.Context, tenantID, userID uuid.UUID, departmentID *uuid.UUID) error {
	p.calls++
	p.tenantID, p.userID, p.department = tenantID, userID, departmentID
	return p.err
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

// A pause that could not reach every instance must answer a 5xx, because the
// Definition Service retries on a 5xx and treats a 2xx or 4xx as done. A joined
// cause that happens to wrap a not-found error must not turn into a 404.
func TestPauseUserTasks_IncompletePauseAnswers503(t *testing.T) {
	userID := uuid.New()
	path := "/api/v1/internal/users/" + userID.String() + "/pause-tasks"
	cause := fmt.Errorf("%w: %w", port.ErrUserPauseIncomplete,
		errors.Join(fmt.Errorf("cannot read task: %w", port.ErrTaskNotFound), errors.New("dial tcp 10.0.0.7:7233: connection refused")))

	gin.SetMode(gin.TestMode)
	h := handler.New(handler.Services{UserTaskPauser: &recordingUserTaskPauser{err: cause}})
	r := gin.New()
	r.POST("/api/v1/internal/users/:user_id/pause-tasks", h.PauseUserTasks)

	w := do(r, internalReq(http.MethodPost, path, map[string]any{"tenant_id": testTenantID}))

	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), handler.CodeUpstreamUnavailable)
	assert.NotContains(t, w.Body.String(), "10.0.0.7", "the cause stays in the log, not the body")
}

const handlerCodeUpstream = handler.CodeUpstreamUnavailable

func doPause(t *testing.T, pauser *recordingUserTaskPauser) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	h := handler.New(handler.Services{UserTaskPauser: pauser})
	r := gin.New()
	r.POST("/api/v1/internal/users/:user_id/pause-tasks", h.PauseUserTasks)
	return do(r, internalReq(http.MethodPost, "/api/v1/internal/users/"+uuid.NewString()+"/pause-tasks", map[string]any{"tenant_id": testTenantID}))
}
