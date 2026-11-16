package handler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type startfixProblem struct {
	Type   string `json:"type"`
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

const startfixSecretCause = "rpc error: code = Unavailable desc = connection error: dial tcp 10.1.2.3:7233"

func startfixStartWith(t *testing.T, err error) (int, startfixProblem, string) {
	t.Helper()
	fake := &fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) { return nil, err },
	}
	w := do(newRouter(newInstanceHandler(fake)), req(http.MethodPost, instancesPath(), map[string]any{
		"subject_id": "SUBJ-1", "business_key": "TND-001", "workflow_version_id": uuid.New(),
	}))
	var p startfixProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	return w.Code, p, w.Body.String()
}

func TestStartInstance_AClusterFailureAnswers503WithAFixedDetail(t *testing.T) {
	cause := fmt.Errorf("start workflow: %w: %s", port.ErrTemporalUnavailable, startfixSecretCause)

	status, p, body := startfixStartWith(t, cause)

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
	assert.NotEmpty(t, p.Detail)
	assert.NotContains(t, body, "10.1.2.3", "the error text of the failed call stays out of the body")
	assert.NotContains(t, body, "rpc error")
	assert.NotContains(t, p.Type, "temporal", "the problem type does not name the workflow engine")
}

func TestStartInstance_AVersionTheDefinitionServiceDoesNotKnowAnswers404(t *testing.T) {
	status, p, body := startfixStartWith(t, port.ErrVersionNotFound)

	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "VERSION_NOT_FOUND", p.Code)
	assert.Equal(t, "https://errors.bcbp.io/execution/version-not-found", p.Type)
	assert.NotContains(t, body, "tenant")
}

func TestPauseInstance_ASignalFailureAnswers503WithAFixedDetail(t *testing.T) {
	fake := &fakeInstanceService{
		pause: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
			return fmt.Errorf("signal workflow: %w: %s", port.ErrTemporalUnavailable, startfixSecretCause)
		},
	}
	w := do(newRouter(newInstanceHandler(fake)), asAdmin(req(http.MethodPost, instancePath(testInstID)+"/pause", map[string]any{
		"reason": "maintenance", "record_version": 1,
	})))

	var p startfixProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
	assert.NotContains(t, w.Body.String(), "10.1.2.3")
	assert.NotContains(t, p.Type, "temporal")
}

func TestMapErr_ADeadlineStillAnswersRequestTimedOut(t *testing.T) {
	cause := fmt.Errorf("start workflow: %w: %w", port.ErrTemporalUnavailable, context.DeadlineExceeded)

	status, p, _ := startfixStartWith(t, cause)

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Equal(t, "request timed out", p.Detail)
}
