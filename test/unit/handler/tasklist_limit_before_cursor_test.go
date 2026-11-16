package handler_test

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestListTasks_LimitAndCursorBothInvalid_ReportsTheLimit(t *testing.T) {
	cases := []struct {
		name, query, wantDetail string
	}{
		{"zero limit with a bad cursor", "limit=0&cursor=" + url.QueryEscape("!!!bad"), "limit must be a positive integer"},
		{"valid limit with a bad cursor", "limit=5&cursor=" + url.QueryEscape("!!!bad"), "invalid cursor"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			fake := &fakeTaskService{
				list: func(context.Context, uuid.UUID, port.ReadScope, port.TaskFilter, port.Page) (port.PageResult[*port.Task], error) {
					called = true
					return port.PageResult[*port.Task]{}, nil
				},
			}
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, req(http.MethodGet, "/api/v1/tasks?"+tc.query, nil))

			p := hrdDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, tc.wantDetail, p.Detail)
			assert.Empty(t, p.InvalidParams)
			assert.False(t, called, "a refused query must not reach the service")
		})
	}
}
