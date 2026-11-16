package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type catalogueProblem struct {
	Type  string `json:"type"`
	Title string `json:"title"`
	Code  string `json:"code"`
}

func TestStartInstance_ASuspendedTenantAnswersItsOwnProblemTypeAndTitle(t *testing.T) {
	fake := &fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
			return nil, port.ErrTenantNotActive
		},
	}

	w := do(newRouter(newInstanceHandler(fake)), req(http.MethodPost, instancesPath(), map[string]any{
		"subject_id": "SUBJ-1", "business_key": "TND-001", "workflow_version_id": uuid.New(),
	}))

	require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
	var p catalogueProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p))
	assert.Equal(t, "TENANT_NOT_ACTIVE", p.Code)
	assert.Equal(t, "https://errors.bcbp.io/execution/tenant-not-active", p.Type)
	assert.Equal(t, "Tenant Not Active", p.Title)
}

func TestClaimTask_ANonAssigneeProblemTypeUsesTheKebabCaseCode(t *testing.T) {
	fake := &fakeTaskService{
		claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			return nil, port.ErrNotAssignee
		},
	}

	w := do(newRouter(newHandler(fake, &fakeEligibilityChecker{})), req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/claim", map[string]any{"record_version": 1}))

	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	var p catalogueProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p))
	assert.Equal(t, "NOT_ASSIGNEE", p.Code)
	assert.Equal(t, "https://errors.bcbp.io/execution/not-assignee", p.Type)
}
