package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

var (
	testTenantID = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	testUserID   = uuid.MustParse("22222222-2222-2222-2222-222222222222")
	testTaskID   = uuid.MustParse("33333333-3333-3333-3333-333333333333")
	testInstID   = uuid.MustParse("44444444-4444-4444-4444-444444444444")
)

// fakeTaskService is a hand-rolled fake (repo convention: no mockgen at the
// handler layer): one optional func field per port.TaskService method,
// falling back to a zero-value default when unset.
type fakeTaskService struct {
	list             func(context.Context, uuid.UUID, port.ReadScope, port.TaskFilter, port.Page) (port.PageResult[*port.Task], error)
	get              func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope) (*port.Task, []*port.TaskAssignment, error)
	getByNode        func(context.Context, uuid.UUID, uuid.UUID, string) (*port.Task, error)
	claim            func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error)
	complete         func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, json.RawMessage, int64) (*port.Task, error)
	deferTask        func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) (*port.Task, error)
	reassign         func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error)
	overrideAssignee func(context.Context, port.AssigneeOverrideInput) (*port.AssigneeOverride, error)
	signalReassign   func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, int64) error
	activeByUser     func(context.Context, uuid.UUID, uuid.UUID, port.Page) (port.PageResult[*port.ActiveUserTask], error)
}

func (f *fakeTaskService) List(ctx context.Context, tenantID uuid.UUID, scope port.ReadScope, filter port.TaskFilter, page port.Page) (port.PageResult[*port.Task], error) {
	if f.list != nil {
		return f.list(ctx, tenantID, scope, filter, page)
	}
	return port.PageResult[*port.Task]{}, nil
}

func (f *fakeTaskService) Get(ctx context.Context, tenantID, taskID uuid.UUID, scope port.ReadScope) (*port.Task, []*port.TaskAssignment, error) {
	if f.get != nil {
		return f.get(ctx, tenantID, taskID, scope)
	}
	return nil, nil, nil
}

func (f *fakeTaskService) GetByNode(ctx context.Context, tenantID, instanceID uuid.UUID, nodeKey string) (*port.Task, error) {
	if f.getByNode != nil {
		return f.getByNode(ctx, tenantID, instanceID, nodeKey)
	}
	return nil, nil
}

func (f *fakeTaskService) Claim(ctx context.Context, tenantID, taskID, userID uuid.UUID, recordVersion int64) (*port.Task, error) {
	if f.claim != nil {
		return f.claim(ctx, tenantID, taskID, userID, recordVersion)
	}
	return nil, nil
}

func (f *fakeTaskService) Complete(ctx context.Context, tenantID, taskID, userID uuid.UUID, resultJSON json.RawMessage, recordVersion int64) (*port.Task, error) {
	if f.complete != nil {
		return f.complete(ctx, tenantID, taskID, userID, resultJSON, recordVersion)
	}
	return nil, nil
}

func (f *fakeTaskService) Defer(ctx context.Context, tenantID, taskID, userID uuid.UUID, reason string, recordVersion int64) (*port.Task, error) {
	if f.deferTask != nil {
		return f.deferTask(ctx, tenantID, taskID, userID, reason, recordVersion)
	}
	return nil, nil
}

func (f *fakeTaskService) Reassign(ctx context.Context, tenantID, taskID, actorUserID, newUserID uuid.UUID, recordVersion int64) (*port.Task, error) {
	if f.reassign != nil {
		return f.reassign(ctx, tenantID, taskID, actorUserID, newUserID, recordVersion)
	}
	return nil, nil
}

func (f *fakeTaskService) OverrideAssignee(ctx context.Context, in port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
	if f.overrideAssignee != nil {
		return f.overrideAssignee(ctx, in)
	}
	return nil, nil
}

func (f *fakeTaskService) SignalReassign(ctx context.Context, tenantID, taskID, actorUserID, previousUserID, newUserID uuid.UUID, recordVersion int64) error {
	if f.signalReassign != nil {
		return f.signalReassign(ctx, tenantID, taskID, actorUserID, previousUserID, newUserID, recordVersion)
	}
	return nil
}

func (f *fakeTaskService) ActiveByUser(ctx context.Context, tenantID, userID uuid.UUID, page port.Page) (port.PageResult[*port.ActiveUserTask], error) {
	if f.activeByUser != nil {
		return f.activeByUser(ctx, tenantID, userID, page)
	}
	return port.PageResult[*port.ActiveUserTask]{}, nil
}

var _ port.TaskService = (*fakeTaskService)(nil)

// fakeEligibilityChecker is the equivalent hand-rolled fake for
// port.EligibilityChecker.
type fakeEligibilityChecker struct {
	check func(context.Context, uuid.UUID, uuid.UUID, string, uuid.UUID) (bool, error)
}

func (f *fakeEligibilityChecker) CheckEligibility(ctx context.Context, newUserID, departmentID uuid.UUID, requiredLevel string, actorID uuid.UUID) (bool, error) {
	if f.check != nil {
		return f.check(ctx, newUserID, departmentID, requiredLevel, actorID)
	}
	return true, nil
}

var _ port.EligibilityChecker = (*fakeEligibilityChecker)(nil)

type fakeWorkflowClient struct {
	reassignDelegate func(context.Context, port.ReassignDelegateInput) (int, error)
	cancelByDelegate func(context.Context, port.CancelByDelegateInput) (int, error)
	delegateImpact   func(context.Context, port.DelegateImpactInput) (port.DelegateImpactResult, error)
}

func (f *fakeWorkflowClient) ReassignDelegate(ctx context.Context, in port.ReassignDelegateInput) (int, error) {
	if f.reassignDelegate != nil {
		return f.reassignDelegate(ctx, in)
	}
	return 0, nil
}

func (f *fakeWorkflowClient) CancelByDelegate(ctx context.Context, in port.CancelByDelegateInput) (int, error) {
	if f.cancelByDelegate != nil {
		return f.cancelByDelegate(ctx, in)
	}
	return 0, nil
}

func (f *fakeWorkflowClient) DelegateImpact(ctx context.Context, in port.DelegateImpactInput) (port.DelegateImpactResult, error) {
	if f.delegateImpact != nil {
		return f.delegateImpact(ctx, in)
	}
	return port.DelegateImpactResult{}, nil
}

var _ port.WorkflowClient = (*fakeWorkflowClient)(nil)

// fakeCacheStore is the hand-rolled fake for port.CacheStore, backed by an
// in-memory map — used by the idempotency wrapper's tests. getErr, when set,
// forces Get to return an error (e.g. a transient cache-unavailable case).
type fakeCacheStore struct {
	data   map[string]string
	getErr error
}

func newFakeCacheStore() *fakeCacheStore {
	return &fakeCacheStore{data: map[string]string{}}
}

func (f *fakeCacheStore) Get(_ context.Context, key string) (string, error) {
	if f.getErr != nil {
		return "", f.getErr
	}
	return f.data[key], nil
}

func (f *fakeCacheStore) Set(_ context.Context, key string, value string, _ time.Duration) error {
	f.data[key] = value
	return nil
}

func (f *fakeCacheStore) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		delete(f.data, k)
	}
	return nil
}

func (f *fakeCacheStore) SetNX(_ context.Context, key string, value string, _ time.Duration) (bool, error) {
	if _, exists := f.data[key]; exists {
		return false, nil
	}
	f.data[key] = value
	return true, nil
}

func (f *fakeCacheStore) Ping(_ context.Context) error { return nil }

var _ port.CacheStore = (*fakeCacheStore)(nil)

func newHandler(tasks *fakeTaskService, eligibility *fakeEligibilityChecker) *handler.Handler {
	return handler.New(handler.Services{Tasks: tasks, Eligibility: eligibility})
}

func newDelegateHandler(wc *fakeWorkflowClient) *handler.Handler {
	return handler.New(handler.Services{WorkflowClient: wc})
}

func newDelegateHandlerWithCache(wc *fakeWorkflowClient, cache *fakeCacheStore) *handler.Handler {
	return handler.New(handler.Services{WorkflowClient: wc, Cache: cache, IdempotencyTTL: time.Hour})
}

func newRouter(h *handler.Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	for _, mw := range gincommon.ProtectedMiddlewares(gincommon.Config{}) {
		r.Use(mw)
	}
	rg := r.Group("/api/v1")
	handler.RegisterRoutes(rg, h)
	return r
}

// newInternalRouter builds a BARE router (no gincommon.ProtectedMiddlewares,
// no middleware.RequireInternalToken) for the /internal/workflows/* routes —
// these callers carry no gateway identity, and the token guard is a
// separate middleware-package concern tested on its own.
func newInternalRouter(h *handler.Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	handler.RegisterInternalRoutes(r.Group("/api/v1/internal"), h)
	return r
}

// internalReq is like req() but WITHOUT gateway identity headers — these
// routes ignore x-tenant-id/x-user-id entirely.
func internalReq(method, path string, body any) *http.Request {
	var r io.Reader
	if body != nil {
		r = jsonBody(body)
	}
	httpReq := httptest.NewRequest(method, path, r)
	httpReq.Header.Set("Content-Type", "application/json")
	return httpReq
}

// req builds a request with the standard gateway identity headers. Extra
// headers (x-tenant-roles, x-departments) can be layered on by the caller
// for scope-specific tests.
func req(method, path string, body any) *http.Request {
	var r io.Reader
	if body != nil {
		r = jsonBody(body)
	}
	httpReq := httptest.NewRequest(method, path, r)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-tenant-id", testTenantID.String())
	httpReq.Header.Set("x-user-id", testUserID.String())
	return httpReq
}

func do(router *gin.Engine, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func jsonBody(v any) io.Reader {
	b, _ := json.Marshal(v)
	return bytes.NewReader(b)
}

func decodeJSON(t *testing.T, body *bytes.Buffer, v any) {
	t.Helper()
	if err := json.Unmarshal(body.Bytes(), v); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
}
