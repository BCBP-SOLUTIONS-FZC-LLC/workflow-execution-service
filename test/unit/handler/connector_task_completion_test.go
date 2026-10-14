package handler_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// fakeConnectorTaskService is a hand-rolled fake mirroring this package's
// own func-field-per-method convention.
type fakeConnectorTaskService struct {
	complete     func(context.Context, uuid.UUID, uuid.UUID, map[string]any) error
	fail         func(context.Context, uuid.UUID, uuid.UUID, string, map[string]any) error
	completeCall struct {
		called   bool
		tenantID uuid.UUID
		taskID   uuid.UUID
		output   map[string]any
	}
	failCall struct {
		called     bool
		tenantID   uuid.UUID
		taskID     uuid.UUID
		errorClass string
		output     map[string]any
	}
}

func (f *fakeConnectorTaskService) Complete(ctx context.Context, tenantID, taskID uuid.UUID, output map[string]any) error {
	f.completeCall.called = true
	f.completeCall.tenantID = tenantID
	f.completeCall.taskID = taskID
	f.completeCall.output = output
	if f.complete != nil {
		return f.complete(ctx, tenantID, taskID, output)
	}
	return nil
}

func (f *fakeConnectorTaskService) Fail(ctx context.Context, tenantID, taskID uuid.UUID, errorClass string, output map[string]any) error {
	f.failCall.called = true
	f.failCall.tenantID = tenantID
	f.failCall.taskID = taskID
	f.failCall.errorClass = errorClass
	f.failCall.output = output
	if f.fail != nil {
		return f.fail(ctx, tenantID, taskID, errorClass, output)
	}
	return nil
}

func TestCompleteConnectorTask_Success(t *testing.T) {
	fake := &fakeConnectorTaskService{}
	router := newInternalRouter(newConnectorTaskHandler(fake))

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/connector-tasks/"+testTaskID.String()+"/complete", map[string]any{
		"tenant_id": testTenantID.String(),
		"output":    map[string]any{"docUrl": "s3://x"},
	}))

	require.Equal(t, http.StatusNoContent, w.Code)
	assert.True(t, fake.completeCall.called)
	assert.Equal(t, testTenantID, fake.completeCall.tenantID)
	assert.Equal(t, testTaskID, fake.completeCall.taskID)
	assert.Equal(t, "s3://x", fake.completeCall.output["docUrl"])
}

func TestCompleteConnectorTask_MissingTenantID_Returns400(t *testing.T) {
	fake := &fakeConnectorTaskService{}
	router := newInternalRouter(newConnectorTaskHandler(fake))

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/connector-tasks/"+testTaskID.String()+"/complete", map[string]any{
		"output": map[string]any{},
	}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.False(t, fake.completeCall.called)
}

func TestCompleteConnectorTask_NotConnectorTyped_Returns409(t *testing.T) {
	fake := &fakeConnectorTaskService{
		complete: func(context.Context, uuid.UUID, uuid.UUID, map[string]any) error {
			return port.ErrTaskNotConnectorTyped
		},
	}
	router := newInternalRouter(newConnectorTaskHandler(fake))

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/connector-tasks/"+testTaskID.String()+"/complete", map[string]any{
		"tenant_id": testTenantID.String(),
	}))

	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestCompleteConnectorTask_TaskNotFound_Returns404(t *testing.T) {
	fake := &fakeConnectorTaskService{
		complete: func(context.Context, uuid.UUID, uuid.UUID, map[string]any) error {
			return port.ErrTaskNotFound
		},
	}
	router := newInternalRouter(newConnectorTaskHandler(fake))

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/connector-tasks/"+testTaskID.String()+"/complete", map[string]any{
		"tenant_id": testTenantID.String(),
	}))

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestFailConnectorTask_Success(t *testing.T) {
	fake := &fakeConnectorTaskService{}
	router := newInternalRouter(newConnectorTaskHandler(fake))

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/connector-tasks/"+testTaskID.String()+"/fail", map[string]any{
		"tenant_id":   testTenantID.String(),
		"error_class": "upstream_error",
		"output":      map[string]any{"deliveryOutcome": "unknown"},
	}))

	require.Equal(t, http.StatusNoContent, w.Code)
	assert.True(t, fake.failCall.called)
	assert.Equal(t, "upstream_error", fake.failCall.errorClass)
	assert.Equal(t, map[string]any{"deliveryOutcome": "unknown"}, fake.failCall.output)
}

func TestFailConnectorTask_MissingErrorClass_Returns400(t *testing.T) {
	fake := &fakeConnectorTaskService{}
	router := newInternalRouter(newConnectorTaskHandler(fake))

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/connector-tasks/"+testTaskID.String()+"/fail", map[string]any{
		"tenant_id": testTenantID.String(),
	}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.False(t, fake.failCall.called)
}

func TestFailConnectorTask_InvalidTaskIDParam_Returns400(t *testing.T) {
	fake := &fakeConnectorTaskService{}
	router := newInternalRouter(newConnectorTaskHandler(fake))

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/connector-tasks/not-a-uuid/fail", map[string]any{
		"tenant_id":   testTenantID.String(),
		"error_class": "timeout",
	}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.False(t, fake.failCall.called)
}

func TestFailConnectorTask_ServiceError_Returns404(t *testing.T) {
	fake := &fakeConnectorTaskService{
		fail: func(context.Context, uuid.UUID, uuid.UUID, string, map[string]any) error {
			return port.ErrTaskNotFound
		},
	}
	router := newInternalRouter(newConnectorTaskHandler(fake))

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/connector-tasks/"+testTaskID.String()+"/fail", map[string]any{
		"tenant_id":   testTenantID.String(),
		"error_class": "timeout",
	}))

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestCompleteConnectorTask_InvalidTaskIDParam_Returns400(t *testing.T) {
	fake := &fakeConnectorTaskService{}
	router := newInternalRouter(newConnectorTaskHandler(fake))

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/connector-tasks/not-a-uuid/complete", map[string]any{
		"tenant_id": testTenantID.String(),
	}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.False(t, fake.completeCall.called)
}

// The acting principal is the tenant's automation principal, resolved by the
// service from IAM; a caller never names one, so a caller cannot name a wrong one.
func TestCompleteConnectorTask_RequestCarriesNoActor(t *testing.T) {
	fake := &fakeConnectorTaskService{}
	router := newInternalRouter(newConnectorTaskHandler(fake))

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/connector-tasks/"+testTaskID.String()+"/complete", map[string]any{
		"tenant_id": testTenantID.String(),
		"output":    map[string]any{"docUrl": "s3://x"},
	}))

	require.Equal(t, http.StatusNoContent, w.Code)
	assert.True(t, fake.completeCall.called)
}

func TestFailConnectorTask_RequestCarriesNoActor(t *testing.T) {
	fake := &fakeConnectorTaskService{}
	router := newInternalRouter(newConnectorTaskHandler(fake))

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/connector-tasks/"+testTaskID.String()+"/fail", map[string]any{
		"tenant_id":   testTenantID.String(),
		"error_class": "upstream_timeout",
	}))

	require.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, "upstream_timeout", fake.failCall.errorClass)
}

func TestCompleteConnectorTask_NoAutomationPrincipal_Returns409(t *testing.T) {
	fake := &fakeConnectorTaskService{
		complete: func(context.Context, uuid.UUID, uuid.UUID, map[string]any) error {
			return port.ErrNoAutomationPrincipal
		},
	}
	router := newInternalRouter(newConnectorTaskHandler(fake))

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/connector-tasks/"+testTaskID.String()+"/complete", map[string]any{
		"tenant_id": testTenantID.String(),
	}))

	assert.Equal(t, http.StatusConflict, w.Code)
}
