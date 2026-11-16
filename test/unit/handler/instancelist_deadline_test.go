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

func TestListInstances_QueryDeadlineExceeded_Returns503(t *testing.T) {
	var gotTenants []uuid.UUID
	router := newRouter(handler.New(handler.Services{Instances: &fakeInstanceService{
		list: func(_ context.Context, tenantID uuid.UUID, _ port.ReadScope, _ port.InstanceFilter, _ port.Page) (port.PageResult[*port.Instance], error) {
			gotTenants = append(gotTenants, tenantID)
			return port.PageResult[*port.Instance]{}, fmt.Errorf("list instances: %w", context.DeadlineExceeded)
		},
	}}))

	w := do(router, hrdPublicRequest(http.MethodGet, instancesPath(), "", false))

	p := hrdDecodeProblem(t, w, http.StatusServiceUnavailable)
	assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
	assert.Equal(t, "request timed out", p.Detail)
	assert.Contains(t, w.Body.String(), "/temporal-unavailable", "the problem type names the unavailable upstream")
	assert.NotContains(t, w.Body.String(), "list instances", "the wrapped error text stays out of the body")
	require.Equal(t, []uuid.UUID{testTenantID}, gotTenants)
}
