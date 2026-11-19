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

// The rig serves the production router around recording fakes and a map-backed cache.

const (
	w26bTenant     = "11111111-1111-4111-8111-111111111111"
	w26bUser       = "22222222-2222-4222-8222-222222222222"
	w26bTask       = "33333333-3333-4333-8333-333333333333"
	w26bInstance   = "44444444-4444-4444-8444-444444444444"
	w26bNewUser    = "66666666-6666-4666-8666-666666666666"
	w26bReplayType = "https://errors.bcbp.io/execution/idempotency-key-replay"
)

type w26bCall struct {
	op       string
	tenantID uuid.UUID
	userID   uuid.UUID
	id       uuid.UUID
	detail   string
	version  int64
}

type w26bRecorder struct {
	mu    sync.Mutex
	calls []w26bCall
}

func (r *w26bRecorder) add(c w26bCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, c)
}

func (r *w26bRecorder) recorded() []w26bCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]w26bCall(nil), r.calls...)
}

type w26bTasks struct {
	port.TaskService
	rec *w26bRecorder
}

func (f *w26bTasks) Get(_ context.Context, tenantID, taskID uuid.UUID, scope port.ReadScope) (*port.Task, []*port.TaskAssignment, error) {
	f.rec.add(w26bCall{op: "get task", tenantID: tenantID, userID: scope.CallerUserID, id: taskID})
	return &port.Task{ID: taskID, TenantID: tenantID, Status: port.TaskStatusReady}, nil, nil
}

func (f *w26bTasks) Claim(_ context.Context, tenantID, taskID, userID uuid.UUID, recordVersion int64) (*port.Task, error) {
	f.rec.add(w26bCall{op: "claim task", tenantID: tenantID, userID: userID, id: taskID, version: recordVersion})
	return &port.Task{ID: taskID, TenantID: tenantID, Status: port.TaskStatusInProgress}, nil
}

func (f *w26bTasks) Defer(_ context.Context, tenantID, taskID, userID uuid.UUID, reason string, recordVersion int64) (*port.Task, error) {
	f.rec.add(w26bCall{op: "defer task", tenantID: tenantID, userID: userID, id: taskID, detail: reason, version: recordVersion})
	return &port.Task{ID: taskID, TenantID: tenantID, Status: port.TaskStatusDeferred}, nil
}

func (f *w26bTasks) OverrideAssignee(_ context.Context, in port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
	f.rec.add(w26bCall{op: "override assignee", tenantID: in.TenantID, userID: in.ActorUserID, id: in.InstanceID, detail: in.NodeKey + "|" + in.NewUserID.String(), version: in.RecordVersion})
	return &port.AssigneeOverride{WorkflowInstanceID: in.InstanceID, NodeKey: in.NodeKey, NewUserID: in.NewUserID, ActorUserID: in.ActorUserID, RecordVersion: in.RecordVersion + 1}, nil
}

type w26bInstances struct {
	port.InstanceService
	rec *w26bRecorder
}

func (f *w26bInstances) ForceForward(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, target string, recordVersion int64) error {
	f.rec.add(w26bCall{op: "force-forward", tenantID: tenantID, userID: actorUserID, id: instanceID, detail: target, version: recordVersion})
	return nil
}

type w26bCache struct {
	mu   sync.Mutex
	data map[string]string
}

func (c *w26bCache) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data[key], nil
}

func (c *w26bCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = value
	return nil
}

func (c *w26bCache) Del(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		delete(c.data, k)
	}
	return nil
}

func (c *w26bCache) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.data[key]; exists {
		return false, nil
	}
	c.data[key] = value
	return true, nil
}

func (c *w26bCache) Ping(context.Context) error { return nil }

type w26bHealthy struct{}

func (w26bHealthy) Ping(context.Context) error { return nil }
func (w26bHealthy) Health(context.Context) inboundhttp.DBHealth {
	return inboundhttp.DBHealth{Healthy: true}
}

type w26bRig struct {
	rec     *w26bRecorder
	handler http.Handler
}

func w26bNewRig(t *testing.T) *w26bRig {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rig := &w26bRig{rec: &w26bRecorder{}}
	router := inboundhttp.NewRouter(inboundhttp.RouterConfig{
		GinConfig: gincommon.Config{
			ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: "workflow", Environment: "test",
		},
		AppEnv: "test",
		Handler: handler.New(handler.Services{
			Tasks:          &w26bTasks{rec: rig.rec},
			Instances:      &w26bInstances{rec: rig.rec},
			Cache:          &w26bCache{data: map[string]string{}},
			IdempotencyTTL: time.Hour,
		}),
		DB:       w26bHealthy{},
		Cache:    w26bHealthy{},
		Temporal: w26bHealthy{},
	})
	rig.handler = router.Handler()
	return rig
}

func (r *w26bRig) send(method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	hr := httptest.NewRequest(method, path, strings.NewReader(body))
	hr.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		hr.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.handler.ServeHTTP(w, hr)
	return w
}

func w26bJSON(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out), w.Body.String())
	return out
}

func w26bHeaders(admin bool) map[string]string {
	h := map[string]string{"x-tenant-id": w26bTenant, "x-user-id": w26bUser}
	if admin {
		h["x-tenant-roles"] = "tenant_admin"
	}
	return h
}

// A tenant or user id that is not a UUID is refused on the task read, claim, defer and node-override routes with the
// gateway's 401 body, the same one a request with no identity gets, and the service is never called; the same
// request with valid ids does reach the service with the caller's ids.
func TestTaskRoutes_IdentityThatIsNotAUUID_AnswersTheGatewayBodyOf401(t *testing.T) {
	tenant, user := uuid.MustParse(w26bTenant), uuid.MustParse(w26bUser)
	task, inst := uuid.MustParse(w26bTask), uuid.MustParse(w26bInstance)
	routes := []struct {
		name       string
		method     string
		path       string
		body       string
		admin      bool
		wantStatus int
		wantCall   w26bCall
	}{
		{
			name: "get a task", method: http.MethodGet, path: "/api/v1/tasks/" + w26bTask, wantStatus: http.StatusOK,
			wantCall: w26bCall{op: "get task", tenantID: tenant, userID: user, id: task},
		},
		{
			name: "claim a task", method: http.MethodPost, path: "/api/v1/tasks/" + w26bTask + "/claim",
			body: `{"record_version":3}`, wantStatus: http.StatusAccepted,
			wantCall: w26bCall{op: "claim task", tenantID: tenant, userID: user, id: task, version: 3},
		},
		{
			name: "defer a task", method: http.MethodPost, path: "/api/v1/tasks/" + w26bTask + "/defer",
			body: `{"reason":"waiting for a quote","record_version":3}`, wantStatus: http.StatusOK,
			wantCall: w26bCall{op: "defer task", tenantID: tenant, userID: user, id: task, detail: "waiting for a quote", version: 3},
		},
		{
			name: "override a node assignee", method: http.MethodPost, path: "/api/v1/instances/" + w26bInstance + "/nodes/finance-review/override",
			body: `{"new_user_id":"` + w26bNewUser + `","record_version":2}`, admin: true, wantStatus: http.StatusOK,
			wantCall: w26bCall{op: "override assignee", tenantID: tenant, userID: user, id: inst, detail: "finance-review|" + w26bNewUser, version: 2},
		},
	}
	badIdentities := []struct {
		name    string
		headers map[string]string
	}{
		{"tenant id is not a uuid", map[string]string{"x-tenant-id": "not-a-uuid", "x-user-id": w26bUser}},
		{"user id is not a uuid", map[string]string{"x-tenant-id": w26bTenant, "x-user-id": "not-a-uuid"}},
		{"both ids are not uuids", map[string]string{"x-tenant-id": "tenant-1", "x-user-id": "user-1"}},
	}
	for _, rt := range routes {
		t.Run(rt.name, func(t *testing.T) {
			rig := w26bNewRig(t)
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
			missing := rig.send(rt.method, rt.path, rt.body, withRole(nil))
			require.Equal(t, http.StatusUnauthorized, missing.Code, missing.Body.String())
			want := w26bJSON(t, missing)

			for _, bad := range badIdentities {
				w := rig.send(rt.method, rt.path, rt.body, withRole(bad.headers))

				require.Equal(t, http.StatusUnauthorized, w.Code, bad.name+": "+w.Body.String())
				got := w26bJSON(t, w)
				assert.Equal(t, "missing or invalid authentication headers", got["error"], bad.name)
				assert.EqualValues(t, http.StatusUnauthorized, got["status"], bad.name)
				assert.Equal(t, want["error"], got["error"], bad.name)
				assert.Equal(t, want["status"], got["status"], bad.name)
				assert.Equal(t, missing.Header().Get("Content-Type"), w.Header().Get("Content-Type"), bad.name)
				assert.NotEmpty(t, got["request_id"], bad.name)
				assert.NotEmpty(t, got["trace_id"], bad.name)
				assert.Empty(t, rig.rec.recorded(), bad.name+": a refused identity must not reach the service")
			}

			ok := rig.send(rt.method, rt.path, rt.body, withRole(w26bHeaders(false)))

			require.Equal(t, rt.wantStatus, ok.Code, ok.Body.String())
			assert.Equal(t, []w26bCall{rt.wantCall}, rig.rec.recorded(), "the same request with valid ids reaches the service")
		})
	}
}

// The force-forward route is wrapped by the idempotency middleware in the production router: a repeat of the same
// key and body is answered from the stored answer, a reused key with another body is refused with 409, and in both
// cases only the first request signals the workflow.
func TestForceForwardRoute_IdempotencyKey_ReplayAnswersFromStoreAndAnotherBodyIsRefused(t *testing.T) {
	inst := uuid.MustParse(w26bInstance)
	path := "/api/v1/instances/" + w26bInstance + "/force-forward"
	firstBody := `{"target_node_key":"finance/review","record_version":2}`
	otherBody := `{"target_node_key":"finance/approve","record_version":2}`
	keyed := func(key string) map[string]string {
		h := w26bHeaders(true)
		h["Idempotency-Key"] = key
		return h
	}
	firstCall := w26bCall{op: "force-forward", tenantID: uuid.MustParse(w26bTenant), userID: uuid.MustParse(w26bUser), id: inst, detail: "finance/review", version: 2}
	rig := w26bNewRig(t)

	first := rig.send(http.MethodPost, path, firstBody, keyed("w26b-key-1"))
	require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
	require.Equal(t, []w26bCall{firstCall}, rig.rec.recorded())

	replay := rig.send(http.MethodPost, path, firstBody, keyed("w26b-key-1"))

	assert.Equal(t, http.StatusAccepted, replay.Code, replay.Body.String())
	assert.Equal(t, first.Body.String(), replay.Body.String(), "a replay answers with the stored body")
	assert.Equal(t, []w26bCall{firstCall}, rig.rec.recorded(), "a replay does not signal again")

	changed := rig.send(http.MethodPost, path, otherBody, keyed("w26b-key-1"))

	require.Equal(t, http.StatusConflict, changed.Code, changed.Body.String())
	body := w26bJSON(t, changed)
	assert.Equal(t, "IDEMPOTENCY_KEY_REPLAY", body["code"])
	assert.Equal(t, w26bReplayType, body["type"])
	assert.EqualValues(t, http.StatusConflict, body["status"])
	assert.Equal(t, []w26bCall{firstCall}, rig.rec.recorded(), "the changed request must not reach the service")

	fresh := rig.send(http.MethodPost, path, otherBody, keyed("w26b-key-2"))

	require.Equal(t, http.StatusAccepted, fresh.Code, fresh.Body.String())
	secondCall := firstCall
	secondCall.detail = "finance/approve"
	assert.Equal(t, []w26bCall{firstCall, secondCall}, rig.rec.recorded(), "the same body under a new key is a new request")
}
