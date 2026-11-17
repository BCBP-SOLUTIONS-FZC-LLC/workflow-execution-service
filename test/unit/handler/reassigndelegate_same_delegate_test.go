package handler_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestReassignDelegate_NewDelegateIsTheOldDelegate_Returns400NamingNewDelegateID(t *testing.T) {
	var calls []port.ReassignDelegateInput
	router := newInternalRouter(handler.New(handler.Services{WorkflowClient: hrdRecordingReassignClient(&calls)}))
	same := uuid.New()
	body := `{"tenant_id":"` + uuid.NewString() + `","old_delegate_id":"` + same.String() + `","new_delegate_id":"` + same.String() + `"}`

	w := hrdInternalPost(router, hrdReassignDelegatePath, "application/json", body)

	p := hrdDecodeProblem(t, w, http.StatusBadRequest)
	assert.Equal(t, "BAD_REQUEST", p.Code)
	require.Len(t, p.InvalidParams, 1)
	assert.Equal(t, "new_delegate_id", p.InvalidParams[0].Name)
	assert.Empty(t, calls, "a move from a user to themselves must not reach the workflow client")
}

func TestReassignDelegate_DifferentDelegates_ReachTheWorkflowClient(t *testing.T) {
	var calls []port.ReassignDelegateInput
	router := newInternalRouter(handler.New(handler.Services{WorkflowClient: hrdRecordingReassignClient(&calls)}))
	body := `{"tenant_id":"` + uuid.NewString() + `","old_delegate_id":"` + uuid.NewString() + `","new_delegate_id":"` + uuid.NewString() + `"}`

	w := hrdInternalPost(router, hrdReassignDelegatePath, "application/json", body)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Len(t, calls, 1)
}
