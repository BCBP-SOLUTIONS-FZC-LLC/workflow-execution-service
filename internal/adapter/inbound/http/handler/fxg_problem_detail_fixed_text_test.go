package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const fxgSecretShaped = "frontend 10.9.8.7:7233 channel instance.force_forward:9f1 token=s3cr3t-value"

// Every refusal that names a sentinel answers the sentinel's own text; the wrapped cause is not part of the body.
func TestMapErr_WrappedRefusalAnswersTheSentinelText(t *testing.T) {
	cases := []struct {
		sentinel error
		status   int
		code     ErrCode
	}{
		{port.ErrTaskNotFound, http.StatusNotFound, CodeTaskNotFound},
		{port.ErrNotAssignee, http.StatusForbidden, CodeNotAssignee},
		{port.ErrNotAuthorizedForRead, http.StatusForbidden, CodeNotAuthorizedForResource},
		{port.ErrRecordVersionConflict, http.StatusConflict, CodeRecordVersionConflict},
		{port.ErrTaskAlreadyClaimed, http.StatusConflict, CodeTaskAlreadyClaimed},
		{port.ErrClaimNotApplicable, http.StatusConflict, CodeClaimNotApplicable},
		{port.ErrInvalidTaskState, http.StatusConflict, CodeInvalidTaskState},
		{port.ErrNodeAlreadyResolved, http.StatusConflict, CodeNodeAlreadyResolved},
		{port.ErrTenantMismatch, http.StatusForbidden, CodeTenantMismatch},
		{port.ErrIdempotencyKeyReplay, http.StatusConflict, CodeIdempotencyReplay},
		{port.ErrOverrideNoOp, http.StatusBadRequest, CodeOverrideNoOp},
		{port.ErrAnswerVariablesInvalid, http.StatusBadRequest, CodeBadRequest},
		{port.ErrInstanceNotFound, http.StatusNotFound, CodeInstanceNotFound},
		{port.ErrWaitNotFound, http.StatusNotFound, CodeWaitNotFound},
		{port.ErrNotWaiting, http.StatusConflict, CodeNotWaiting},
		{port.ErrDuplicateBusinessKey, http.StatusConflict, CodeDuplicateBusinessKey},
		{port.ErrTenantNotActive, http.StatusConflict, CodeTenantNotActive},
		{port.ErrVersionNotPublished, http.StatusConflict, CodeVersionNotPublished},
		{port.ErrVersionInvalid, http.StatusConflict, CodeVersionInvalid},
		{port.ErrVersionNotFound, http.StatusNotFound, CodeVersionNotFound},
		{port.ErrInstanceAlreadyTerminal, http.StatusConflict, CodeInstanceAlreadyTerminal},
		{port.ErrInvalidInstanceState, http.StatusConflict, CodeInvalidInstanceState},
		{port.ErrWorkflowNotFound, http.StatusConflict, CodeInvalidInstanceState},
		{port.ErrForceBackNoSavedBranch, http.StatusConflict, CodeForceBackNoSavedBranch},
		{port.ErrAssigneeIneligible, http.StatusUnprocessableEntity, CodeAssigneeIneligible},
		{port.ErrTaskNotHumanActionable, http.StatusConflict, CodeTaskNotHumanActionable},
		{port.ErrTaskNotConnectorTyped, http.StatusConflict, CodeTaskNotConnectorTyped},
		{port.ErrNoAutomationPrincipal, http.StatusConflict, CodeNoAutomationPrincipal},
	}
	for _, tc := range cases {
		t.Run(tc.sentinel.Error(), func(t *testing.T) {
			wrapped := fmt.Errorf("signal workflow: %w: %w", tc.sentinel, errors.New(fxgSecretShaped))

			status, code, detail := mapErr(wrapped)

			assert.Equal(t, tc.status, status)
			assert.Equal(t, tc.code, code)
			assert.Equal(t, tc.sentinel.Error(), detail)
			assert.NotContains(t, detail, "10.9.8.7")
			assert.NotContains(t, detail, "s3cr3t-value")
		})
	}
}

// These two refusals are wrapped with text the service builds from the request (why the target was refused, which
// node key is invalid), so the caller gets that text to act on.
func TestMapErr_RefusalsWithActionableText_KeepTheirWrappedDetail(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   ErrCode
	}{
		{fmt.Errorf("%w: node %q is not in the plan", port.ErrTargetNodeNotFound, "finance/Approve"), http.StatusConflict, CodeTargetNodeNotFound},
		{fmt.Errorf("%w: unknown node key %q", port.ErrOverrideMapInvalid, "finance/Missing"), http.StatusUnprocessableEntity, CodeOverrideMapInvalid},
	}
	for _, tc := range cases {
		t.Run(string(tc.code), func(t *testing.T) {
			status, code, detail := mapErr(tc.err)

			assert.Equal(t, tc.status, status)
			assert.Equal(t, tc.code, code)
			assert.Equal(t, tc.err.Error(), detail)
		})
	}
}

// The typed ineligible-assignee error keeps its own fixed text and node list.
func TestMapErr_IneligibleAssigneeKeepsItsOwnFixedText(t *testing.T) {
	typed := &port.AssigneeIneligibleError{Nodes: []string{"finance/Approve"}}

	status, code, detail := mapErr(fmt.Errorf("start: %w", typed))

	assert.Equal(t, http.StatusUnprocessableEntity, status)
	assert.Equal(t, CodeAssigneeIneligible, code)
	assert.Equal(t, typed.Error(), detail)
}

// The body carries the sentinel text only; the cause stays on the request for the access log.
func TestErrResponse_WrappedRefusalKeepsTheCauseOutOfTheBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/instances/x/force-forward", nil)
	cause := errors.New(fxgSecretShaped)

	errResponse(c, fmt.Errorf("signal workflow %q: %w: %w", "instance.force_forward:9f1", port.ErrWorkflowNotFound, cause))

	require.Equal(t, http.StatusConflict, rec.Code)
	var body problemDetails
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, port.ErrWorkflowNotFound.Error(), body.Detail)
	assert.NotContains(t, rec.Body.String(), "instance.force_forward")
	assert.NotContains(t, rec.Body.String(), "s3cr3t-value")
	require.Len(t, c.Errors, 1)
	assert.ErrorIs(t, c.Errors[0].Err, cause)
}
