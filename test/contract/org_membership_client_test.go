package contract_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type delegateClient struct {
	impact    port.DelegateImpactResult
	reassign  port.ReassignDelegateInput
	cancelled port.CancelByDelegateInput
}

func (d *delegateClient) ReassignDelegate(_ context.Context, in port.ReassignDelegateInput) (int, error) {
	d.reassign = in
	return 1, nil
}

func (d *delegateClient) CancelByDelegate(_ context.Context, in port.CancelByDelegateInput) (int, error) {
	d.cancelled = in
	return 1, nil
}

func (d *delegateClient) DelegateImpact(context.Context, port.DelegateImpactInput) (port.DelegateImpactResult, error) {
	return d.impact, nil
}

func delegateRouter(d *delegateClient) http.Handler {
	gin.SetMode(gin.TestMode)
	return httpadapter.NewRouter(httpadapter.RouterConfig{
		GinConfig: gincommon.Config{ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: "workflow", Environment: "test"}, AppEnv: "test", InternalAPIToken: "internal-secret",
		Handler: handler.New(handler.Services{WorkflowClient: d}),
		DB:      upDB{}, Cache: upPinger{}, Temporal: upPinger{},
	}).Handler()
}

func orgMembershipRequest(method, target string, body map[string]any, token string) *http.Request {
	var req *http.Request
	if body == nil {
		req = httptest.NewRequest(method, target, nil)
	} else {
		raw, _ := json.Marshal(body)
		req = httptest.NewRequest(method, target, strings.NewReader(string(raw)))
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("x-tenant-id", "t")
	req.Header.Set("x-user-id", "iam-system")
	req.Header.Set("x-tenant-roles", "iam-system")
	if token != "" {
		req.Header.Set("x-internal-token", token)
	}
	return req
}

func TestOrgMembershipClient_DelegateImpact(t *testing.T) {
	tenantID, delegate, delegationID := uuid.New(), uuid.New(), uuid.New()
	workflowID := uuid.New()
	d := &delegateClient{impact: port.DelegateImpactResult{
		ActiveWorkflows: 1, ActiveAssignments: 2,
		WorkflowIDs: port.PageResult[uuid.UUID]{Items: []uuid.UUID{workflowID}},
	}}
	router := delegateRouter(d)
	query := url.Values{
		"tenant_id":        {tenantID.String()},
		"delegate_user_id": {delegate.String()},
		"delegation_id":    {delegationID.String()},
	}
	target := "/api/v1/internal/workflows/delegate-impact?" + query.Encode()

	t.Run("the removal gate reads the fields it names", func(t *testing.T) {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, orgMembershipRequest(http.MethodGet, target, nil, "internal-secret"))
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())

		var body struct {
			ActiveWorkflows int         `json:"active_workflows"`
			WorkflowIDs     []uuid.UUID `json:"workflow_ids"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, 1, body.ActiveWorkflows)
		assert.Equal(t, []uuid.UUID{workflowID}, body.WorkflowIDs)
	})

	t.Run("a call without the internal token is refused", func(t *testing.T) {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, orgMembershipRequest(http.MethodGet, target, nil, ""))
		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})
}

func TestOrgMembershipClient_ReassignAndCancel(t *testing.T) {
	tenantID, oldDelegate, newDelegate, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	d := &delegateClient{}
	router := delegateRouter(d)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, orgMembershipRequest(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", map[string]any{
		"tenant_id": tenantID, "old_delegate_id": oldDelegate, "new_delegate_id": newDelegate, "delegation_id": delegationID,
	}, "internal-secret"))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, oldDelegate, d.reassign.OldDelegateID)
	assert.Equal(t, newDelegate, d.reassign.NewDelegateID)
	require.NotNil(t, d.reassign.DelegationID)
	assert.Equal(t, delegationID, *d.reassign.DelegationID)

	w = httptest.NewRecorder()
	router.ServeHTTP(w, orgMembershipRequest(http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate", map[string]any{
		"tenant_id": tenantID, "delegate_user_id": oldDelegate, "delegation_id": delegationID,
	}, "internal-secret"))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, oldDelegate, d.cancelled.DelegateUserID)
}
