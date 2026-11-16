package handler_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type hrdAnswerCall struct {
	tenantID, instanceID uuid.UUID
	key                  string
}

func TestFireOutsideBoundary_DeadlineDuringUpstreamCall_Returns503(t *testing.T) {
	var calls []hrdAnswerCall
	router := newRouter(handler.New(handler.Services{Instances: &fakeInstanceService{
		answerOutsideWait: func(_ context.Context, tenantID, instanceID uuid.UUID, _ port.ReadScope, key string, _ map[string]any) error {
			calls = append(calls, hrdAnswerCall{tenantID: tenantID, instanceID: instanceID, key: key})
			return fmt.Errorf("query workflow state: %w", context.DeadlineExceeded)
		},
	}}))

	w := do(router, hrdPublicRequest(http.MethodPost, firePath("Prepare", "Boundary_Withdraw"), "", false))

	p := hrdDecodeProblem(t, w, http.StatusServiceUnavailable)
	assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
	assert.Equal(t, "request timed out", p.Detail)
	assert.NotContains(t, w.Body.String(), "query workflow state", "the upstream wording stays out of the body")
	require.Len(t, calls, 1)
	assert.Equal(t, testTenantID, calls[0].tenantID)
	assert.Equal(t, testInstID, calls[0].instanceID)
	assert.Equal(t, "Prepare#Boundary_Withdraw", calls[0].key)
}
