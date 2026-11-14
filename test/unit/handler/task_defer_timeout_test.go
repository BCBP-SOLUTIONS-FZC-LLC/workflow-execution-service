package handler_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// The defer route hands the caller's ids to the service and answers a deadline
// error from the workflow update as 503 with the "request timed out" detail
// (the code alone is already covered through the get route).
func TestDeferTask_WorkflowUpdateRunningPastTheDeadlineIsServiceUnavailable(t *testing.T) {
	var gotTenant, gotTask, gotUser uuid.UUID
	fake := &fakeTaskService{
		deferTask: func(_ context.Context, tenantID, taskID, userID uuid.UUID, _ string, _ int64) (*port.Task, error) {
			gotTenant, gotTask, gotUser = tenantID, taskID, userID
			return nil, fmt.Errorf("update stage-defer: %w", context.DeadlineExceeded)
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/defer", map[string]any{"record_version": 1}))

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	var body problemBody
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "UPSTREAM_UNAVAILABLE", body.Code)
	assert.Equal(t, "request timed out", body.Detail)
	assert.Equal(t, testTenantID, gotTenant)
	assert.Equal(t, testTaskID, gotTask)
	assert.Equal(t, testUserID, gotUser)
}
