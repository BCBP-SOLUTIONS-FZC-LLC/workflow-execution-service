package handler_test

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestListInstances_AnUnknownStatusFilterIsRefusedBeforeTheQuery(t *testing.T) {
	for _, status := range []string{"BOGUS", "running", " RUNNING", "RUNNING ", "READY"} {
		t.Run(status, func(t *testing.T) {
			calls := 0
			fake := &fakeInstanceService{
				list: func(context.Context, uuid.UUID, port.ReadScope, port.InstanceFilter, port.Page) (port.PageResult[*port.Instance], error) {
					calls++
					return port.PageResult[*port.Instance]{}, nil
				},
			}
			router := newRouter(newInstanceHandler(fake))

			w := do(router, req(http.MethodGet, "/api/v1/instances?status="+url.QueryEscape(status), nil))

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			p := hhbDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, "status", p.InvalidParams[0].Name)
			assert.Zero(t, calls, "a refused filter must not reach the service")
		})
	}
}

func TestListInstances_EveryInstanceStatusIsAcceptedAsAFilter(t *testing.T) {
	for _, status := range []port.InstanceStatus{
		port.InstanceStatusRunning, port.InstanceStatusPaused, port.InstanceStatusCompleted,
		port.InstanceStatusTerminated, port.InstanceStatusFailed, port.InstanceStatusDegraded,
	} {
		t.Run(string(status), func(t *testing.T) {
			var got *port.InstanceStatus
			fake := &fakeInstanceService{
				list: func(_ context.Context, _ uuid.UUID, _ port.ReadScope, filter port.InstanceFilter, _ port.Page) (port.PageResult[*port.Instance], error) {
					got = filter.Status
					return port.PageResult[*port.Instance]{}, nil
				},
			}
			router := newRouter(newInstanceHandler(fake))

			w := do(router, req(http.MethodGet, "/api/v1/instances?status="+string(status), nil))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.NotNil(t, got)
			assert.Equal(t, status, *got)
		})
	}
}
