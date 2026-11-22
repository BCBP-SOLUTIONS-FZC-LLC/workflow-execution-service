package http_test

import (
	"context"
	"encoding/json"
	"fmt"
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
	w27aTenant   = "11111111-1111-4111-8111-111111111111"
	w27aUser     = "22222222-2222-4222-8222-222222222222"
	w27aTask     = "33333333-3333-4333-8333-333333333333"
	w27aInstance = "44444444-4444-4444-8444-444444444444"
	w27aWorkflow = "55555555-5555-4555-8555-555555555555"
	w27aDelegate = "66666666-6666-4666-8666-666666666666"
	w27aOther    = "77777777-7777-4777-8777-777777777777"
)

// w27aCall is one service call a handler made: what it was and the ids it carried.
type w27aCall struct {
	op     string
	tenant uuid.UUID
	user   uuid.UUID
	id     uuid.UUID
}

type w27aLog struct{}

func (w27aLog) Debug(string, map[string]interface{}) {}
func (w27aLog) Info(string, map[string]interface{})  {}
func (w27aLog) Warn(string, map[string]interface{})  {}
func (w27aLog) Error(string, map[string]interface{}) {}

// w27aCache is a map-backed cache; SetNX reports whether the key was free, as the real store does.
type w27aCache struct {
	mu   sync.Mutex
	data map[string]string
	nx   int
}

func (c *w27aCache) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data[key], nil
}

func (c *w27aCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = value
	return nil
}

func (c *w27aCache) Del(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		delete(c.data, k)
	}
	return nil
}

func (c *w27aCache) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nx++
	if _, taken := c.data[key]; taken {
		return false, nil
	}
	c.data[key] = value
	return true, nil
}

func (c *w27aCache) Ping(context.Context) error { return nil }

func (c *w27aCache) claims() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nx
}

// w27aProbeOrder records which dependency probe ran, in order.
type w27aProbeOrder struct {
	mu    sync.Mutex
	steps []string
}

func (o *w27aProbeOrder) add(step string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.steps = append(o.steps, step)
}

func (o *w27aProbeOrder) seen() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.steps...)
}

type w27aDB struct {
	order  *w27aProbeOrder
	health inboundhttp.DBHealth
}

func (d *w27aDB) Health(context.Context) inboundhttp.DBHealth {
	d.order.add("db")
	return d.health
}

type w27aPinger struct {
	order *w27aProbeOrder
	name  string
	err   error
}

func (p *w27aPinger) Ping(context.Context) error {
	p.order.add(p.name)
	return p.err
}

type w27aRig struct {
	mu       sync.Mutex
	calls    []w27aCall
	cache    *w27aCache
	order    *w27aProbeOrder
	db       *w27aDB
	cachePg  *w27aPinger
	temporal *w27aPinger
	shutdown bool
	handler  http.Handler
}

func (r *w27aRig) record(c w27aCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, c)
}

func (r *w27aRig) recorded() []w27aCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]w27aCall(nil), r.calls...)
}

func (r *w27aRig) setShuttingDown(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shutdown = v
}

func (r *w27aRig) isShuttingDown() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.shutdown
}

type w27aTasks struct {
	port.TaskService
	rig *w27aRig
}

// Each fake folds the arguments that decide what the call does (version, reason, target, new user) into the
// recorded op, so a test that compares the whole call also pins them.
func (f *w27aTasks) Claim(_ context.Context, tenantID, taskID, userID uuid.UUID, recordVersion int64) (*port.Task, error) {
	f.rig.record(w27aCall{fmt.Sprintf("claim task version=%d", recordVersion), tenantID, userID, taskID})
	return &port.Task{ID: taskID, TenantID: tenantID, Status: port.TaskStatusInProgress}, nil
}

func (f *w27aTasks) OverrideAssignee(_ context.Context, in port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
	f.rig.record(w27aCall{fmt.Sprintf("override assignee node=%s new_user=%s version=%d reason=%q", in.NodeKey, in.NewUserID, in.RecordVersion, in.Reason), in.TenantID, in.ActorUserID, in.InstanceID})
	return &port.AssigneeOverride{WorkflowInstanceID: in.InstanceID, NodeKey: in.NodeKey, NewUserID: in.NewUserID, ActorUserID: in.ActorUserID, RecordVersion: in.RecordVersion}, nil
}

func (f *w27aTasks) ActiveByUser(_ context.Context, tenantID, userID uuid.UUID, _ port.Page) (port.PageResult[*port.ActiveUserTask], error) {
	f.rig.record(w27aCall{"active by user", tenantID, uuid.Nil, userID})
	return port.PageResult[*port.ActiveUserTask]{}, nil
}

type w27aInstances struct {
	port.InstanceService
	rig *w27aRig
}

func (f *w27aInstances) Pause(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error {
	f.rig.record(w27aCall{fmt.Sprintf("pause instance reason=%q version=%d", reason, recordVersion), tenantID, actorUserID, instanceID})
	return nil
}

func (f *w27aInstances) Resume(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, recordVersion int64) error {
	f.rig.record(w27aCall{fmt.Sprintf("resume instance version=%d", recordVersion), tenantID, actorUserID, instanceID})
	return nil
}

func (f *w27aInstances) Cancel(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error {
	f.rig.record(w27aCall{fmt.Sprintf("cancel instance reason=%q version=%d", reason, recordVersion), tenantID, actorUserID, instanceID})
	return nil
}

func (f *w27aInstances) Terminate(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string) error {
	f.rig.record(w27aCall{fmt.Sprintf("terminate instance reason=%q", reason), tenantID, actorUserID, instanceID})
	return nil
}

func (f *w27aInstances) AnswerOutsideWait(_ context.Context, tenantID, instanceID uuid.UUID, scope port.ReadScope, key string, _ map[string]any) error {
	f.rig.record(w27aCall{"answer outside wait " + key, tenantID, scope.CallerUserID, instanceID})
	return nil
}

func (f *w27aInstances) ForceBack(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, targetNodeKey string, recordVersion int64) error {
	f.rig.record(w27aCall{fmt.Sprintf("force back instance target=%q version=%d", targetNodeKey, recordVersion), tenantID, actorUserID, instanceID})
	return nil
}

type w27aWorkflows struct {
	port.WorkflowClient
	rig *w27aRig
}

func (f *w27aWorkflows) ReassignDelegate(_ context.Context, in port.ReassignDelegateInput) (int, error) {
	f.rig.record(w27aCall{"reassign delegate", in.TenantID, in.NewDelegateID, in.OldDelegateID})
	return 4, nil
}

func (f *w27aWorkflows) CancelByDelegate(_ context.Context, in port.CancelByDelegateInput) (int, error) {
	f.rig.record(w27aCall{"cancel by delegate", in.TenantID, uuid.Nil, in.DelegateUserID})
	return 3, nil
}

func (f *w27aWorkflows) DelegateImpact(_ context.Context, in port.DelegateImpactInput) (port.DelegateImpactResult, error) {
	f.rig.record(w27aCall{"delegate impact", in.TenantID, uuid.Nil, in.DelegateUserID})
	return port.DelegateImpactResult{ActiveWorkflows: 2, ActiveAssignments: 5}, nil
}

type w27aGuard struct {
	rig *w27aRig
}

func (f *w27aGuard) CheckActiveInstances(_ context.Context, tenantID, workflowID uuid.UUID) (bool, int32, error) {
	f.rig.record(w27aCall{"check active instances", tenantID, uuid.Nil, workflowID})
	return true, 7, nil
}

// w27aNewRig builds the production router around recording fakes. Every probe and service call is recorded.
func w27aNewRig(t *testing.T, appEnv string) *w27aRig {
	t.Helper()
	gin.SetMode(gin.TestMode)
	order := &w27aProbeOrder{}
	rig := &w27aRig{
		cache:    &w27aCache{data: map[string]string{}},
		order:    order,
		db:       &w27aDB{order: order, health: inboundhttp.DBHealth{Healthy: true}},
		cachePg:  &w27aPinger{order: order, name: "cache"},
		temporal: &w27aPinger{order: order, name: "temporal"},
	}
	router := inboundhttp.NewRouter(inboundhttp.RouterConfig{
		GinConfig: gincommon.Config{
			ServiceName:  "workflow-execution-service",
			BuildVersion: "test",
			Domain:       "workflow",
			Environment:  "test",
			Logger:       w27aLog{},
		},
		AppEnv: appEnv,
		Handler: handler.New(handler.Services{
			Tasks:          &w27aTasks{rig: rig},
			Instances:      &w27aInstances{rig: rig},
			WorkflowClient: &w27aWorkflows{rig: rig},
			ArchiveGuard:   &w27aGuard{rig: rig},
			Cache:          rig.cache,
			Log:            w27aLog{},
		}),
		DB:           rig.db,
		Cache:        rig.cachePg,
		Temporal:     rig.temporal,
		ShuttingDown: rig.isShuttingDown,
	})
	rig.handler = router.Handler()
	return rig
}

type w27aRequest struct {
	method  string
	path    string
	body    string
	headers map[string]string
}

func (r *w27aRig) send(req w27aRequest) *httptest.ResponseRecorder {
	hr := httptest.NewRequest(req.method, req.path, strings.NewReader(req.body))
	hr.Header.Set("Content-Type", "application/json")
	for k, v := range req.headers {
		hr.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.handler.ServeHTTP(w, hr)
	return w
}

func w27aPublicHeaders() map[string]string {
	return map[string]string{"x-tenant-id": w27aTenant, "x-user-id": w27aUser}
}

func w27aAdminHeaders() map[string]string {
	return map[string]string{"x-tenant-id": w27aTenant, "x-user-id": w27aUser, "x-tenant-roles": "tenant_admin"}
}

func w27aSystemHeaders() map[string]string {
	return map[string]string{"x-user-id": w27aUser, "x-tenant-roles": "iam-system"}
}

func w27aBody(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	raw, err := io.ReadAll(w.Result().Body)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(raw, &out), "body is not JSON: %s", string(raw))
	return out
}
