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

func hhbListEventsRecording(gotPage *port.Page, called *bool) *fakeInstanceService {
	return &fakeInstanceService{
		listEvents: func(_ context.Context, _, _ uuid.UUID, _ port.ReadScope, page port.Page) (port.PageResult[*port.WorkflowEvent], error) {
			*called = true
			*gotPage = page
			return port.PageResult[*port.WorkflowEvent]{}, nil
		},
	}
}

func TestListInstanceEvents_LimitAboveTheMaximum_IsClampedToOneHundred(t *testing.T) {
	for _, limit := range []string{"101", "100000"} {
		t.Run("limit="+limit, func(t *testing.T) {
			var gotPage port.Page
			called := false
			var gotInstance uuid.UUID
			fake := &fakeInstanceService{
				listEvents: func(_ context.Context, tenantID, instanceID uuid.UUID, _ port.ReadScope, page port.Page) (port.PageResult[*port.WorkflowEvent], error) {
					called, gotPage, gotInstance = true, page, instanceID
					assert.Equal(t, testTenantID, tenantID)
					return port.PageResult[*port.WorkflowEvent]{}, nil
				},
			}
			router := newRouter(newInstanceHandler(fake))

			w := do(router, req(http.MethodGet, instanceEventsPath(testInstID)+"?limit="+limit, nil))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.True(t, called)
			assert.Equal(t, testInstID, gotInstance)
			assert.Equal(t, 100, gotPage.Limit)
		})
	}
}

func TestListInstanceEvents_InvalidCursor_Returns400(t *testing.T) {
	for name, cursor := range map[string]string{
		"not base64":              "not-a-cursor",
		"base64 but not JSON":     "bm90LWpzb24=",
		"valid JSON, wrong shape": "eyJmb28iOiJiYXIifQ==",
	} {
		t.Run(name, func(t *testing.T) {
			var gotPage port.Page
			called := false
			router := newRouter(newInstanceHandler(hhbListEventsRecording(&gotPage, &called)))

			w := do(router, req(http.MethodGet, instanceEventsPath(testInstID)+"?cursor="+url.QueryEscape(cursor), nil))

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			p := hhbDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, hhbBadRequestType, p.Type)
			assert.Equal(t, "invalid cursor", p.Detail)
			assert.False(t, called)
		})
	}
}

func TestListInstanceEvents_InstanceIDNotAUUID_Returns400(t *testing.T) {
	var gotPage port.Page
	called := false
	router := newRouter(newInstanceHandler(hhbListEventsRecording(&gotPage, &called)))

	w := do(router, req(http.MethodGet, "/api/v1/instances/not-a-uuid/events", nil))

	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	p := hhbDecodeProblem(t, w)
	assert.Equal(t, "BAD_REQUEST", p.Code)
	assert.Equal(t, hhbBadRequestType, p.Type)
	assert.Equal(t, "invalid id path parameter", p.Detail)
	assert.False(t, called)
}
