package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func TestTaskSignals_ObserveDurationPerOperation(t *testing.T) {
	const name = "workflow_execution_service_task_signal_duration_seconds"
	fake := &fakeTaskService{
		claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			return newTestTask(), nil
		},
		complete: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, json.RawMessage, int64) (*port.Task, error) {
			return newTestTask(), nil
		},
		deferTask: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) (*port.Task, error) {
			return newTestTask(), nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))
	base := "/api/v1/tasks/" + testTaskID.String()

	for _, op := range []string{"claim", "complete", "defer"} {
		before := obstest.Sum(t, name, map[string]string{"operation": op})
		w := do(router, req(http.MethodPost, base+"/"+op, map[string]any{"record_version": 4}))
		require.Equal(t, http.StatusAccepted, w.Code, op)
		assert.Equal(t, before+1, obstest.Sum(t, name, map[string]string{"operation": op}), "%s adds one observation", op)
	}
}

func TestTaskSignals_FailedRequestsAreNotObserved(t *testing.T) {
	const name = "workflow_execution_service_task_signal_duration_seconds"
	fake := &fakeTaskService{
		claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			return nil, port.ErrTaskNotFound
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))
	before := obstest.Sum(t, name, map[string]string{"operation": "claim"})

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/claim", map[string]any{"record_version": 4}))
	require.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, before, obstest.Sum(t, name, map[string]string{"operation": "claim"}), "the SLO covers requests answered 202")
}

func TestStartInstance_ObservesStartDuration(t *testing.T) {
	const name = "workflow_execution_service_instance_start_duration_seconds"
	fake := &fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
			return &port.Instance{ID: uuid.New()}, nil
		},
	}
	router := newRouter(newInstanceHandler(fake))
	before := obstest.Sum(t, name, nil)

	w := do(router, req(http.MethodPost, instancesPath(), map[string]any{
		"subject_id": "SUBJ-1", "business_key": "TND-001", "workflow_version_id": uuid.New(),
	}))
	require.Equal(t, http.StatusCreated, w.Code)
	assert.Equal(t, before+1, obstest.Sum(t, name, nil))
}

func TestUpstreamFailures_AreCountedByDependency(t *testing.T) {
	const name = "workflow_execution_service_upstream_dependency_errors_total"
	cases := []struct {
		err        error
		dependency string
	}{
		{port.ErrDefinitionUnavailable, "definition_service"},
		{port.ErrDefinitionRejected, "definition_service"},
		{adapterhttp.ErrUpstreamUnavailable, "iam"},
	}
	for _, tc := range cases {
		fake := &fakeInstanceService{
			start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) { return nil, tc.err },
		}
		router := newRouter(newInstanceHandler(fake))
		before := obstest.Sum(t, name, map[string]string{"dependency": tc.dependency})

		w := do(router, req(http.MethodPost, instancesPath(), map[string]any{
			"subject_id": "SUBJ-1", "business_key": "TND-001", "workflow_version_id": uuid.New(),
		}))
		require.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Equal(t, before+1, obstest.Sum(t, name, map[string]string{"dependency": tc.dependency}), "%v", tc.err)
	}
}

func TestRequestTimeout_IsNotCountedAsAnUpstreamFailure(t *testing.T) {
	const name = "workflow_execution_service_upstream_dependency_errors_total"
	fake := &fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
			return nil, context.DeadlineExceeded
		},
	}
	router := newRouter(newInstanceHandler(fake))
	before := obstest.Sum(t, name, nil)

	w := do(router, req(http.MethodPost, instancesPath(), map[string]any{
		"subject_id": "SUBJ-1", "business_key": "TND-001", "workflow_version_id": uuid.New(),
	}))
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, before, obstest.Sum(t, name, nil))
}
