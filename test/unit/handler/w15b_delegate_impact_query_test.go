package handler_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const w15bBadRequestType = "https://errors.bcbp.io/execution/bad-request"

type w15bInvalidParam struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

type w15bProblem struct {
	Type          string             `json:"type"`
	Title         string             `json:"title"`
	Status        int                `json:"status"`
	Detail        string             `json:"detail"`
	Code          string             `json:"code"`
	InvalidParams []w15bInvalidParam `json:"invalid_params"`
}

// w15bDecodeProblem fails the test unless the response is a problem+json body with the given status.
func w15bDecodeProblem(t *testing.T, w *httptest.ResponseRecorder, status int) w15bProblem {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
	var p w15bProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	assert.Equal(t, status, p.Status)
	return p
}

func w15bInternalRouter(h *handler.Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	rg := r.Group("/api/v1/internal")
	rg.GET("/workflows/delegate-impact", h.DelegateImpact)
	rg.GET("/workflows/:workflow_id/active-instances", h.ActiveInstances)
	rg.POST("/users/:user_id/pause-tasks", h.PauseUserTasks)
	rg.POST("/events/delegation", h.HandleDelegationEvents)
	return r
}

func w15bServe(router *gin.Engine, method, target, contentType, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

// w15bImpactClient records every delegate-impact call it receives.
type w15bImpactClient struct {
	calls []port.DelegateImpactInput
}

func (f *w15bImpactClient) ReassignDelegate(context.Context, port.ReassignDelegateInput) (int, error) {
	return 0, nil
}

func (f *w15bImpactClient) CancelByDelegate(context.Context, port.CancelByDelegateInput) (int, error) {
	return 0, nil
}

func (f *w15bImpactClient) DelegateImpact(_ context.Context, in port.DelegateImpactInput) (port.DelegateImpactResult, error) {
	f.calls = append(f.calls, in)
	return port.DelegateImpactResult{}, nil
}

var _ port.WorkflowClient = (*w15bImpactClient)(nil)

func w15bCursor(raw string) string { return base64.URLEncoding.EncodeToString([]byte(raw)) }

func TestDelegateImpact_RefusedQuery_Returns400WithTheProblemBody(t *testing.T) {
	t.Parallel()

	tenantID, delegateID := uuid.New().String(), uuid.New().String()
	valid := func() url.Values {
		return url.Values{"tenant_id": {tenantID}, "delegate_user_id": {delegateID}}
	}
	with := func(key string, value *string) url.Values {
		q := valid()
		if value == nil {
			q.Del(key)
		} else {
			q.Set(key, *value)
		}
		return q
	}
	str := func(s string) *string { return &s }

	cases := []struct {
		name       string
		query      url.Values
		wantDetail string
	}{
		{"tenant_id absent", with("tenant_id", nil), "invalid tenant_id query parameter"},
		{"tenant_id empty", with("tenant_id", str("")), "invalid tenant_id query parameter"},
		{"tenant_id not a UUID", with("tenant_id", str("tenant-one")), "invalid tenant_id query parameter"},
		{"delegate_user_id absent", with("delegate_user_id", nil), "invalid delegate_user_id query parameter"},
		{"delegate_user_id empty", with("delegate_user_id", str("")), "invalid delegate_user_id query parameter"},
		{"delegate_user_id not a UUID", with("delegate_user_id", str("12345")), "invalid delegate_user_id query parameter"},
		{"delegation_id not a UUID", with("delegation_id", str("not-a-uuid")), "invalid delegation_id query parameter"},
		{"delegation_id truncated UUID", with("delegation_id", str(uuid.New().String()[:35])), "invalid delegation_id query parameter"},
		{"limit zero", with("limit", str("0")), "limit must be a positive integer"},
		{"limit negative", with("limit", str("-5")), "limit must be a positive integer"},
		{"limit not a number", with("limit", str("abc")), "limit must be a positive integer"},
		{"limit fractional", with("limit", str("1.5")), "limit must be a positive integer"},
		{"limit overflows an integer", with("limit", str("99999999999999999999")), "limit must be a positive integer"},
		{"cursor not base64url", with("cursor", str("%%%")), "invalid cursor"},
		{"cursor in the standard alphabet", with("cursor", str(base64.StdEncoding.EncodeToString([]byte{0xfb, 0xff, 0xfe}))), "invalid cursor"},
		{"cursor not JSON", with("cursor", str(w15bCursor("not json"))), "invalid cursor"},
		{"cursor without created_at", with("cursor", str(w15bCursor(`{"id":"`+uuid.New().String()+`"}`))), "invalid cursor"},
		{"cursor with a nil id", with("cursor", str(w15bCursor(`{"created_at":"2024-01-01T00:00:00Z","id":"`+uuid.Nil.String()+`"}`))), "invalid cursor"},
		{"cursor without an id", with("cursor", str(w15bCursor(`{"created_at":"2024-01-01T00:00:00Z"}`))), "invalid cursor"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := &w15bImpactClient{}
			router := w15bInternalRouter(handler.New(handler.Services{WorkflowClient: client}))

			w := w15bServe(router, http.MethodGet, "/api/v1/internal/workflows/delegate-impact?"+tc.query.Encode(), "", "")

			p := w15bDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w15bBadRequestType, p.Type)
			assert.Equal(t, tc.wantDetail, p.Detail)
			assert.Empty(t, client.calls, "a refused query must not reach the workflow client")
		})
	}
}

func TestDelegateImpact_ValidQuery_ReachesTheClientWithItsParameters(t *testing.T) {
	t.Parallel()

	tenantID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New()
	position := port.CursorPosition{CreatedAt: time.Date(2030, 5, 6, 7, 8, 9, 0, time.UTC), ID: uuid.New()}
	client := &w15bImpactClient{}
	router := w15bInternalRouter(handler.New(handler.Services{WorkflowClient: client}))
	q := url.Values{
		"tenant_id": {tenantID.String()}, "delegate_user_id": {delegateID.String()}, "delegation_id": {delegationID.String()},
		"limit": {"1"}, "cursor": {port.EncodeCursor(position)},
	}

	w := w15bServe(router, http.MethodGet, "/api/v1/internal/workflows/delegate-impact?"+q.Encode(), "", "")

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, client.calls, 1)
	got := client.calls[0]
	assert.Equal(t, tenantID, got.TenantID)
	assert.Equal(t, delegateID, got.DelegateUserID)
	require.NotNil(t, got.DelegationID)
	assert.Equal(t, delegationID, *got.DelegationID)
	assert.Equal(t, 1, got.Page.Limit)
	require.NotNil(t, got.Page.Cursor)
	assert.True(t, position.CreatedAt.Equal(got.Page.Cursor.CreatedAt))
	assert.Equal(t, position.ID, got.Page.Cursor.ID)
}
