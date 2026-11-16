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

func TestListTasks_AnUnknownStatusFilterIsRefusedBeforeTheQuery(t *testing.T) {
	for _, status := range []string{"BOGUS", "ready", " READY", "READY ", "IN-PROGRESS"} {
		t.Run(status, func(t *testing.T) {
			calls := 0
			fake := &fakeTaskService{
				list: func(context.Context, uuid.UUID, port.ReadScope, port.TaskFilter, port.Page) (port.PageResult[*port.Task], error) {
					calls++
					return port.PageResult[*port.Task]{}, nil
				},
			}
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, req(http.MethodGet, "/api/v1/tasks?status="+url.QueryEscape(status), nil))

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			p := hhbDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, "status", p.InvalidParams[0].Name)
			assert.Zero(t, calls, "a refused filter must not reach the service")
		})
	}
}

func TestListTasks_EveryTaskStatusIsAcceptedAsAFilter(t *testing.T) {
	for _, status := range []port.TaskStatus{
		port.TaskStatusReady, port.TaskStatusInProgress, port.TaskStatusCompleted,
		port.TaskStatusDeferred, port.TaskStatusFailed, port.TaskStatusSuperseded,
	} {
		t.Run(string(status), func(t *testing.T) {
			var got *port.TaskStatus
			fake := &fakeTaskService{
				list: func(_ context.Context, _ uuid.UUID, _ port.ReadScope, filter port.TaskFilter, _ port.Page) (port.PageResult[*port.Task], error) {
					got = filter.Status
					return port.PageResult[*port.Task]{}, nil
				},
			}
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, req(http.MethodGet, "/api/v1/tasks?status="+string(status), nil))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.NotNil(t, got)
			assert.Equal(t, status, *got)
		})
	}
}
