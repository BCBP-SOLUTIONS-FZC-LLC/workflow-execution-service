package http_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	inboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	w20aTenantID  = "11111111-1111-4111-8111-111111111111"
	w20aUserID    = "22222222-2222-4222-8222-222222222222"
	w20aInstance  = "44444444-4444-4444-8444-444444444444"
	w20aTask      = "33333333-3333-4333-8333-333333333333"
	w20aKeyHeader = "Idempotency-Key"
	w20aWaitLimit = 5 * time.Second
)

type w20aCall struct {
	op       string
	tenantID uuid.UUID
	userID   uuid.UUID
	id       uuid.UUID
}

type w20aLogEntry struct{ level, msg string }

type w20aLogger struct {
	mu      sync.Mutex
	entries []w20aLogEntry
}

func (l *w20aLogger) add(level, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, w20aLogEntry{level, msg})
}

func (l *w20aLogger) Debug(msg string, _ map[string]interface{}) { l.add("debug", msg) }
func (l *w20aLogger) Info(msg string, _ map[string]interface{})  { l.add("info", msg) }
func (l *w20aLogger) Warn(msg string, _ map[string]interface{})  { l.add("warn", msg) }
func (l *w20aLogger) Error(msg string, _ map[string]interface{}) { l.add("error", msg) }

func (l *w20aLogger) warnsWithPrefix(prefix string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.entries {
		if e.level == "warn" && strings.HasPrefix(e.msg, prefix) {
			n++
		}
	}
	return n
}

// w20aCache is a map-backed cache; SetNX reports whether the key was free, as the real store does.
type w20aCache struct {
	mu      sync.Mutex
	data    map[string]string
	nxErr   error
	setNXes int
	sets    int
}

func (c *w20aCache) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data[key], nil
}

func (c *w20aCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sets++
	c.data[key] = value
	return nil
}

func (c *w20aCache) Del(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		delete(c.data, k)
	}
	return nil
}

func (c *w20aCache) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setNXes++
	if c.nxErr != nil {
		return false, c.nxErr
	}
	if _, taken := c.data[key]; taken {
		return false, nil
	}
	c.data[key] = value
	return true, nil
}

func (c *w20aCache) Ping(context.Context) error { return nil }

func (c *w20aCache) touched() (claims, sets, stored int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setNXes, c.sets, len(c.data)
}

type w20aHealth struct{}

func (w20aHealth) Ping(context.Context) error { return nil }

func (w20aHealth) Health(context.Context) inboundhttp.DBHealth {
	return inboundhttp.DBHealth{Healthy: true}
}

// w20aGate holds a service call open so a second request can arrive while the first is still running.
type w20aGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *w20aGate) open() { g.once.Do(func() { close(g.release) }) }

type w20aRig struct {
	mu      sync.Mutex
	calls   []w20aCall
	details []w20aDetail
	gate    *w20aGate
	cache   *w20aCache
	log     *w20aLogger
	handler http.Handler
}

// record notes a service call. Only the first call waits on the gate, so a second call that leaks past the
// idempotency wrapper fails the test at once instead of hanging it.
func (r *w20aRig) record(c w20aCall) {
	r.mu.Lock()
	r.calls = append(r.calls, c)
	first := len(r.calls) == 1
	gate := r.gate
	r.mu.Unlock()
	if gate == nil || !first {
		return
	}
	gate.entered <- struct{}{}
	<-gate.release
}

// w20aDetail holds the arguments that decide what a call does beyond the caller's ids.
type w20aDetail struct {
	waitKey   string
	variables map[string]any
	newUserID uuid.UUID
}

func (r *w20aRig) recordDetail(d w20aDetail) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.details = append(r.details, d)
}

func (r *w20aRig) recordedDetails() []w20aDetail {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]w20aDetail(nil), r.details...)
}

func (r *w20aRig) recorded() []w20aCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]w20aCall(nil), r.calls...)
}

type w20aTasks struct {
	port.TaskService
	rig *w20aRig
}

func (f *w20aTasks) Complete(_ context.Context, tenantID, taskID, userID uuid.UUID, _ json.RawMessage, _ int64) (*port.Task, error) {
	f.rig.record(w20aCall{"complete task", tenantID, userID, taskID})
	return &port.Task{ID: taskID, TenantID: tenantID, Status: port.TaskStatusInProgress}, nil
}

func (f *w20aTasks) Defer(_ context.Context, tenantID, taskID, userID uuid.UUID, _ string, _ int64) (*port.Task, error) {
	f.rig.record(w20aCall{"defer task", tenantID, userID, taskID})
	return &port.Task{ID: taskID, TenantID: tenantID, Status: port.TaskStatusDeferred}, nil
}

func (f *w20aTasks) Reassign(_ context.Context, tenantID, taskID, actorUserID, newUserID uuid.UUID, _ int64) (*port.Task, error) {
	f.rig.recordDetail(w20aDetail{newUserID: newUserID})
	f.rig.record(w20aCall{"reassign task", tenantID, actorUserID, taskID})
	return &port.Task{ID: taskID, TenantID: tenantID, Status: port.TaskStatusReady}, nil
}

type w20aInstances struct {
	port.InstanceService
	rig *w20aRig
}

func (f *w20aInstances) Start(_ context.Context, in port.StartInstanceInput) (*port.Instance, error) {
	f.rig.record(w20aCall{"start instance", in.TenantID, in.StartedByUserID, in.WorkflowVersionID})
	return &port.Instance{ID: uuid.MustParse(w20aInstance), TenantID: in.TenantID}, nil
}

func (f *w20aInstances) AnswerOutsideWait(_ context.Context, tenantID, instanceID uuid.UUID, scope port.ReadScope, key string, variables map[string]any) error {
	f.rig.recordDetail(w20aDetail{waitKey: key, variables: variables})
	f.rig.record(w20aCall{"answer outside wait", tenantID, scope.CallerUserID, instanceID})
	return nil
}

type w20aWorkflows struct {
	port.WorkflowClient
	rig *w20aRig
}

func (f *w20aWorkflows) CancelByDelegate(_ context.Context, in port.CancelByDelegateInput) (int, error) {
	f.rig.record(w20aCall{"cancel by delegate", in.TenantID, uuid.Nil, in.DelegateUserID})
	return 3, nil
}

func (f *w20aWorkflows) DelegateImpact(_ context.Context, in port.DelegateImpactInput) (port.DelegateImpactResult, error) {
	f.rig.record(w20aCall{"delegate impact", in.TenantID, uuid.Nil, in.DelegateUserID})
	return port.DelegateImpactResult{ActiveWorkflows: 2}, nil
}

// w20aNewRig builds the production router around recording fakes and a map-backed cache.
func w20aNewRig(t *testing.T) *w20aRig {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rig := &w20aRig{cache: &w20aCache{data: map[string]string{}}, log: &w20aLogger{}}
	router := inboundhttp.NewRouter(inboundhttp.RouterConfig{
		GinConfig: gincommon.Config{
			ServiceName:  "workflow-execution-service",
			BuildVersion: "test",
			Domain:       "workflow",
			Environment:  "test",
			Logger:       rig.log,
		},
		AppEnv: "test",
		Handler: handler.New(handler.Services{
			Tasks:          &w20aTasks{rig: rig},
			Instances:      &w20aInstances{rig: rig},
			WorkflowClient: &w20aWorkflows{rig: rig},
			Cache:          rig.cache,
			Log:            rig.log,
		}),
		DB:       w20aHealth{},
		Cache:    w20aHealth{},
		Temporal: w20aHealth{},
	})
	rig.handler = router.Handler()
	return rig
}

type w20aRequest struct {
	method  string
	path    string
	body    string
	headers map[string]string
}

func (r *w20aRig) send(req w20aRequest) *httptest.ResponseRecorder {
	hr := httptest.NewRequest(req.method, req.path, strings.NewReader(req.body))
	hr.Header.Set("Content-Type", "application/json")
	for k, v := range req.headers {
		hr.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.handler.ServeHTTP(w, hr)
	return w
}

func w20aUserHeaders() map[string]string {
	return map[string]string{"x-tenant-id": w20aTenantID, "x-user-id": w20aUserID}
}

func w20aAdminHeaders() map[string]string {
	return map[string]string{"x-tenant-id": w20aTenantID, "x-user-id": w20aUserID, "x-tenant-roles": "tenant_admin"}
}

func w20aSystemHeaders() map[string]string {
	return map[string]string{"x-user-id": w20aUserID, "x-tenant-roles": "iam-system"}
}

func w20aWithKey(headers map[string]string, key string) map[string]string {
	out := map[string]string{w20aKeyHeader: key}
	for k, v := range headers {
		out[k] = v
	}
	return out
}

func w20aJSONBody(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	raw, err := io.ReadAll(w.Result().Body)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(raw, &out), "body is not JSON: %s", string(raw))
	return out
}
