package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	inboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	w25aTenant = "11111111-1111-4111-8111-111111111111"
	w25aUser   = "22222222-2222-4222-8222-222222222222"
)

type w25aServiceCall struct {
	op       string
	tenantID uuid.UUID
	userID   uuid.UUID
	id       uuid.UUID
	detail   string
}

// w25aInstances records every instance call; any method it does not implement panics on use.
type w25aInstances struct {
	port.InstanceService
	mu              sync.Mutex
	calls           []w25aServiceCall
	forwardVersions []int64
}

func (f *w25aInstances) add(c w25aServiceCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, c)
}

func (f *w25aInstances) recorded() []w25aServiceCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]w25aServiceCall(nil), f.calls...)
}

func (f *w25aInstances) Start(_ context.Context, in port.StartInstanceInput) (*port.Instance, error) {
	f.add(w25aServiceCall{"start", in.TenantID, in.StartedByUserID, in.WorkflowVersionID, in.BusinessKey})
	now := time.Date(2026, 11, 18, 8, 0, 0, 0, time.UTC)
	return &port.Instance{ID: uuid.New(), TenantID: in.TenantID, Status: port.InstanceStatusRunning, StartedAt: &now, CreatedAt: now, UpdatedAt: now, RecordVersion: 1}, nil
}

func (f *w25aInstances) List(_ context.Context, tenantID uuid.UUID, scope port.ReadScope, _ port.InstanceFilter, _ port.Page) (port.PageResult[*port.Instance], error) {
	f.add(w25aServiceCall{op: "list", tenantID: tenantID, userID: scope.CallerUserID})
	return port.PageResult[*port.Instance]{}, nil
}

func (f *w25aInstances) Terminate(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string) error {
	f.add(w25aServiceCall{"terminate", tenantID, actorUserID, instanceID, reason})
	return nil
}

func (f *w25aInstances) ForceForward(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, target string, recordVersion int64) error {
	f.add(w25aServiceCall{"force-forward", tenantID, actorUserID, instanceID, target})
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forwardVersions = append(f.forwardVersions, recordVersion)
	return nil
}

type w25aHealthy struct{}

func (w25aHealthy) Ping(context.Context) error { return nil }
func (w25aHealthy) Health(context.Context) inboundhttp.DBHealth {
	return inboundhttp.DBHealth{Healthy: true}
}

func w25aNewRouter(t *testing.T, instances port.InstanceService) http.Handler {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := inboundhttp.NewRouter(inboundhttp.RouterConfig{
		GinConfig: gincommon.Config{
			ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: "workflow", Environment: "test",
		},
		AppEnv:   "test",
		Handler:  handler.New(handler.Services{Instances: instances}),
		DB:       w25aHealthy{},
		Cache:    w25aHealthy{},
		Temporal: w25aHealthy{},
	})
	return router.Handler()
}

func w25aSend(h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func w25aGatewayBody(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out), w.Body.String())
	return out
}

// On the start, list, terminate and force-forward routes a tenant or user id that is not a UUID is refused with the
// gateway's 401 body, the same one a request with no identity gets, and the service is never called; the same
// request with valid ids does reach the service.
func TestInstanceRoutes_IdentityThatIsNotAUUID_AnswersTheGatewayBodyOf401(t *testing.T) {
	instanceID := uuid.MustParse("44444444-4444-4444-8444-444444444444")
	versionID := uuid.MustParse("55555555-5555-4555-8555-555555555555")
	routes := []struct {
		name     string
		method   string
		path     string
		body     string
		admin    bool
		wantCall w25aServiceCall
		wantCode int
	}{
		{
			name: "start an instance", method: http.MethodPost, path: "/api/v1/instances",
			body:     `{"subject_id":"SUBJ-1","business_key":"TND-001","workflow_version_id":"` + versionID.String() + `"}`,
			wantCall: w25aServiceCall{"start", uuid.MustParse(w25aTenant), uuid.MustParse(w25aUser), versionID, "TND-001"}, wantCode: http.StatusCreated,
		},
		{
			name: "list instances", method: http.MethodGet, path: "/api/v1/instances",
			wantCall: w25aServiceCall{op: "list", tenantID: uuid.MustParse(w25aTenant), userID: uuid.MustParse(w25aUser)}, wantCode: http.StatusOK,
		},
		{
			name: "terminate an instance", method: http.MethodPost, path: "/api/v1/instances/" + instanceID.String() + "/terminate",
			body: `{"reason":"duplicate"}`, admin: true,
			wantCall: w25aServiceCall{"terminate", uuid.MustParse(w25aTenant), uuid.MustParse(w25aUser), instanceID, "duplicate"}, wantCode: http.StatusOK,
		},
		{
			name: "force-forward an instance", method: http.MethodPost, path: "/api/v1/instances/" + instanceID.String() + "/force-forward",
			body: `{"target_node_key":"dept/n","record_version":2}`, admin: true,
			wantCall: w25aServiceCall{"force-forward", uuid.MustParse(w25aTenant), uuid.MustParse(w25aUser), instanceID, "dept/n"}, wantCode: http.StatusAccepted,
		},
	}
	badIdentities := []struct {
		name    string
		headers map[string]string
	}{
		{"tenant id is not a uuid", map[string]string{"x-tenant-id": "not-a-uuid", "x-user-id": w25aUser}},
		{"user id is not a uuid", map[string]string{"x-tenant-id": w25aTenant, "x-user-id": "not-a-uuid"}},
		{"both ids are not uuids", map[string]string{"x-tenant-id": "tenant-1", "x-user-id": "user-1"}},
	}
	for _, rt := range routes {
		t.Run(rt.name, func(t *testing.T) {
			svc := &w25aInstances{}
			h := w25aNewRouter(t, svc)
			withRole := func(headers map[string]string) map[string]string {
				out := map[string]string{}
				for k, v := range headers {
					out[k] = v
				}
				if rt.admin {
					out["x-tenant-roles"] = "tenant_admin"
				}
				return out
			}
			missing := w25aSend(h, rt.method, rt.path, rt.body, withRole(nil))
			require.Equal(t, http.StatusUnauthorized, missing.Code, missing.Body.String())
			want := w25aGatewayBody(t, missing)

			for _, bad := range badIdentities {
				w := w25aSend(h, rt.method, rt.path, rt.body, withRole(bad.headers))

				require.Equal(t, http.StatusUnauthorized, w.Code, bad.name+": "+w.Body.String())
				got := w25aGatewayBody(t, w)
				assert.Equal(t, "missing or invalid authentication headers", got["error"], bad.name)
				assert.EqualValues(t, http.StatusUnauthorized, got["status"], bad.name)
				assert.Equal(t, want["error"], got["error"], bad.name)
				assert.Equal(t, want["status"], got["status"], bad.name)
				assert.Equal(t, missing.Header().Get("Content-Type"), w.Header().Get("Content-Type"), bad.name)
				assert.NotEmpty(t, got["request_id"], bad.name)
				assert.NotEmpty(t, got["trace_id"], bad.name)
				assert.Empty(t, svc.recorded(), bad.name+": a refused identity must not reach the service")
			}

			ok := w25aSend(h, rt.method, rt.path, rt.body, withRole(map[string]string{"x-tenant-id": w25aTenant, "x-user-id": w25aUser}))

			require.Equal(t, rt.wantCode, ok.Code, ok.Body.String())
			assert.Equal(t, []w25aServiceCall{rt.wantCall}, svc.recorded(), "the same request with valid ids reaches the service")
			svc.mu.Lock()
			versions := append([]int64(nil), svc.forwardVersions...)
			svc.mu.Unlock()
			if rt.wantCall.op == "force-forward" {
				assert.Equal(t, []int64{2}, versions, "the record_version of the body reaches the service")
			} else {
				assert.Empty(t, versions)
			}
		})
	}
}
