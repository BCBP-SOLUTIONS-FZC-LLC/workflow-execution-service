package handler_test

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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	w17bMembershipConsumer = "membership-execution"
	w17bConnectorConsumer  = "connector-stream-execution"
)

var w17bEventTime = time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)

type w17bProblem struct {
	Type          string `json:"type"`
	Title         string `json:"title"`
	Status        int    `json:"status"`
	Detail        string `json:"detail"`
	Instance      string `json:"instance"`
	Code          string `json:"code"`
	InvalidParams []struct {
		Name   string `json:"name"`
		Reason string `json:"reason"`
	} `json:"invalid_params"`
}

// w17bDecodeProblem fails unless the answer is a problem body with the given status.
func w17bDecodeProblem(t *testing.T, w *httptest.ResponseRecorder, status int) w17bProblem {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
	var p w17bProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	assert.Equal(t, status, p.Status)
	return p
}

// w17bInboxCall is one inbox read or write: the event id and the consumer name it was given.
type w17bInboxCall struct {
	EventID  uuid.UUID
	Consumer string
}

// w17bInbox is a processed-event repository that records every call and can fail either side.
type w17bInbox struct {
	mu        sync.Mutex
	isErr     error
	markErr   error
	done      map[w17bInboxCall]bool
	checked   []w17bInboxCall
	marked    []w17bInboxCall
	markCtxOK []bool
	onCheck   func(context.Context)
	order     *w17bOrder
}

func newW17bInbox() *w17bInbox { return &w17bInbox{done: map[w17bInboxCall]bool{}} }

func (f *w17bInbox) IsProcessed(ctx context.Context, eventID uuid.UUID, consumer string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := w17bInboxCall{EventID: eventID, Consumer: consumer}
	f.checked = append(f.checked, call)
	if f.onCheck != nil {
		f.onCheck(ctx)
	}
	if f.isErr != nil {
		return false, f.isErr
	}
	return f.done[call], nil
}

func (f *w17bInbox) MarkProcessed(ctx context.Context, eventID uuid.UUID, consumer string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := w17bInboxCall{EventID: eventID, Consumer: consumer}
	f.marked = append(f.marked, call)
	f.markCtxOK = append(f.markCtxOK, ctx.Err() == nil)
	f.order.add("mark")
	if f.markErr != nil {
		return f.markErr
	}
	f.done[call] = true
	return nil
}

func (f *w17bInbox) Prune(context.Context, time.Duration) (int64, error) { return 0, nil }

var _ port.ProcessedEventRepository = (*w17bInbox)(nil)

// w17bOrder records the order in which the fakes were used.
type w17bOrder struct {
	mu     sync.Mutex
	events []string
}

func (o *w17bOrder) add(name string) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, name)
}

// w17bRecencyCall is one recency read or write: the method, the scope key and the event time.
type w17bRecencyCall struct {
	Method string
	Scope  string
	At     time.Time
}

// w17bRecency is a recency guard that records every call and can fail either read.
type w17bRecency struct {
	last           map[string]time.Time
	calls          []w17bRecencyCall
	locked         []string
	shouldApplyErr error
	unlessOlderErr error
}

func newW17bRecency() *w17bRecency { return &w17bRecency{last: map[string]time.Time{}} }

func (f *w17bRecency) ShouldApply(_ context.Context, scope string, at time.Time) (bool, error) {
	f.calls = append(f.calls, w17bRecencyCall{"ShouldApply", scope, at})
	if f.shouldApplyErr != nil {
		return false, f.shouldApplyErr
	}
	last, ok := f.last[scope]
	return !ok || at.After(last), nil
}

func (f *w17bRecency) ShouldApplyUnlessOlder(_ context.Context, scope string, at time.Time) (bool, error) {
	f.calls = append(f.calls, w17bRecencyCall{"ShouldApplyUnlessOlder", scope, at})
	if f.unlessOlderErr != nil {
		return false, f.unlessOlderErr
	}
	last, ok := f.last[scope]
	return !ok || !at.Before(last), nil
}

func (f *w17bRecency) CheckAndCommit(_ context.Context, scope string, at time.Time) (bool, error) {
	f.calls = append(f.calls, w17bRecencyCall{"CheckAndCommit", scope, at})
	last, ok := f.last[scope]
	if ok && !at.After(last) {
		return false, nil
	}
	f.last[scope] = at
	return true, nil
}

func (f *w17bRecency) Commit(_ context.Context, scope string, at time.Time) error {
	f.calls = append(f.calls, w17bRecencyCall{"Commit", scope, at})
	if last, ok := f.last[scope]; !ok || at.After(last) {
		f.last[scope] = at
	}
	return nil
}

func (f *w17bRecency) WithLock(ctx context.Context, scope string, fn func(context.Context) error) error {
	f.locked = append(f.locked, scope)
	return fn(ctx)
}

func (f *w17bRecency) methods() []string {
	out := make([]string, 0, len(f.calls))
	for _, c := range f.calls {
		out = append(out, c.Method)
	}
	return out
}

var _ port.RecencyGuard = (*w17bRecency)(nil)

// w17bLifecycle is a tenant lifecycle reconciler that records each input and the tenant setting its context carried.
type w17bLifecycle struct {
	inputs     []port.TenantLifecycleInput
	ctxTenants []string
	err        error
	onApply    func(context.Context, port.TenantLifecycleInput)
}

func (f *w17bLifecycle) Apply(ctx context.Context, in port.TenantLifecycleInput) error {
	f.inputs = append(f.inputs, in)
	f.ctxTenants = append(f.ctxTenants, gucTenant(ctx))
	if f.onApply != nil {
		f.onApply(ctx, in)
	}
	return f.err
}

var _ port.TenantLifecycleReconciler = (*w17bLifecycle)(nil)

type w17bLogLine struct {
	Message string
	Fields  map[string]any
}

// w17bLog records warnings and errors.
type w17bLog struct {
	mu    sync.Mutex
	warns []w17bLogLine
	errs  []w17bLogLine
}

func (l *w17bLog) Debug(string, map[string]any) {}
func (l *w17bLog) Info(string, map[string]any)  {}

func (l *w17bLog) Warn(msg string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, w17bLogLine{msg, fields})
}

func (l *w17bLog) Error(msg string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errs = append(l.errs, w17bLogLine{msg, fields})
}

var _ port.Logger = (*w17bLog)(nil)

func (l *w17bLog) warnWith(eventID uuid.UUID, contains string) (w17bLogLine, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.warns {
		if id, ok := line.Fields["event_id"].(uuid.UUID); ok && id == eventID && strings.Contains(line.Message, contains) {
			return line, true
		}
	}
	return w17bLogLine{}, false
}

// w17bPublisher records each stream entry and the tenant setting and cancellation state its context carried.
type w17bPublisher struct {
	events     []port.ConnectorTaskCreatedEvent
	ctxTenants []string
	ctxLive    []bool
	err        error
	order      *w17bOrder
}

func (p *w17bPublisher) PublishTaskCreated(ctx context.Context, e port.ConnectorTaskCreatedEvent) error {
	p.events = append(p.events, e)
	p.ctxTenants = append(p.ctxTenants, gucTenant(ctx))
	p.ctxLive = append(p.ctxLive, ctx.Err() == nil)
	p.order.add("publish")
	return p.err
}

var _ port.ConnectorEventPublisher = (*w17bPublisher)(nil)

// w17bPrincipals is a principal resolver that records the tenant it was asked about.
type w17bPrincipals struct {
	principal  port.AutomationPrincipal
	err        error
	tenants    []uuid.UUID
	ctxTenants []string
	ctxLive    []bool
	onCall     func()
}

func (p *w17bPrincipals) AutomationPrincipal(ctx context.Context, tenantID uuid.UUID) (port.AutomationPrincipal, error) {
	p.tenants = append(p.tenants, tenantID)
	p.ctxTenants = append(p.ctxTenants, gucTenant(ctx))
	if p.onCall != nil {
		p.onCall()
	}
	p.ctxLive = append(p.ctxLive, ctx.Err() == nil)
	return p.principal, p.err
}

var _ port.AutomationPrincipalResolver = (*w17bPrincipals)(nil)

// w17bGate is a dispatch gate that records the tenant and task it was asked about.
type w17bGate struct {
	tenants    []uuid.UUID
	tasks      []uuid.UUID
	ctxTenants []string
}

func (g *w17bGate) Dispatchable(ctx context.Context, tenantID, taskID uuid.UUID) (bool, error) {
	g.tenants = append(g.tenants, tenantID)
	g.tasks = append(g.tasks, taskID)
	g.ctxTenants = append(g.ctxTenants, gucTenant(ctx))
	return true, nil
}

var _ port.ConnectorDispatchGate = (*w17bGate)(nil)

// w17bTenantRig is a handler wired with recording fakes for the tenant events route.
type w17bTenantRig struct {
	inbox     *w17bInbox
	recency   *w17bRecency
	lifecycle *w17bLifecycle
	log       *w17bLog
	router    *gin.Engine
}

func newW17bTenantRig() *w17bTenantRig {
	rig := &w17bTenantRig{inbox: newW17bInbox(), recency: newW17bRecency(), lifecycle: &w17bLifecycle{}, log: &w17bLog{}}
	rig.router = newInternalRouter(handler.New(handler.Services{
		ProcessedEvents: rig.inbox,
		Recency:         rig.recency,
		TenantLifecycle: rig.lifecycle,
		Log:             rig.log,
	}))
	return rig
}

func w17bPost(router *gin.Engine, path string, body map[string]any) *httptest.ResponseRecorder {
	return do(router, internalReq(http.MethodPost, path, body))
}

const (
	w17bTenantPath       = "/api/v1/internal/events/tenant"
	w17bWorkflowTaskPath = "/api/v1/internal/events/workflow-task"
)

// w17bTenantStatePayload is a complete TenantStateChanged payload for the tenant.
func w17bTenantStatePayload(tenantID uuid.UUID, status string, changedAt time.Time) map[string]any {
	return map[string]any{
		"tenant_id": tenantID.String(), "status": status, "previous_status": "active",
		"plan": "pro", "previous_plan": "pro", "changed_at": changedAt.Format(time.RFC3339), "cause": "non_payment",
	}
}
