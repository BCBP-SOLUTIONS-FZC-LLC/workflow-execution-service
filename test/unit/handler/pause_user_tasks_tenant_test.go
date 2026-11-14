package handler_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
)

func TestPauseUserTasks_AllZeroTenantID_Returns400(t *testing.T) {
	pauser := &hhaUserTaskPauser{}
	router := hhaInternalRouter(handler.New(handler.Services{UserTaskPauser: pauser}))

	w := hhaPostJSON(router, "/api/v1/internal/users/"+uuid.NewString()+"/pause-tasks",
		`{"tenant_id":"00000000-0000-0000-0000-000000000000"}`)

	p := hhaDecodeProblem(t, w, http.StatusBadRequest)
	assert.Equal(t, "BAD_REQUEST", p.Code)
	require.Len(t, p.InvalidParams, 1)
	assert.Equal(t, "tenant_id", p.InvalidParams[0].Name)
	assert.Equal(t, "is required", p.InvalidParams[0].Reason)
	hhaAssertNoInternalText(t, w)
	assert.Empty(t, pauser.calls, "an all-zero tenant must not pause anyone's tasks")
}
