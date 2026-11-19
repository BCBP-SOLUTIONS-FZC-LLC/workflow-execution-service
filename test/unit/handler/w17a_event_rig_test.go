package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

const (
	w17aDelegationRoute  = "/api/v1/internal/events/delegation"
	w17aUserProfileRoute = "/api/v1/internal/events/user-profile"
	w17aTenantRoute      = "/api/v1/internal/events/tenant"
	w17aIngestMetric     = "workflow_execution_service_internal_events_ingest_total"
	w17aInternalType     = "https://errors.bcbp.io/execution/internal-error"
	w17aBadRequestType   = "https://errors.bcbp.io/execution/bad-request"
)

type w17aProblem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
	Code   string `json:"code"`
}

func w17aDecodeProblem(t *testing.T, w *httptest.ResponseRecorder) w17aProblem {
	t.Helper()
	require.Equal(t, "application/problem+json", w.Header().Get("Content-Type"), w.Body.String())
	var p w17aProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	return p
}

func w17aGUCTenant(ctx context.Context) string {
	g, _ := pgcommon.GUCSetFromContext(ctx)
	return g.TenantID
}

type w17aLogEntry struct {
	level  string
	msg    string
	fields map[string]any
}

type w17aLogger struct{ entries []w17aLogEntry }

func (l *w17aLogger) add(level, msg string, fields map[string]any) {
	l.entries = append(l.entries, w17aLogEntry{level: level, msg: msg, fields: fields})
}

func (l *w17aLogger) Debug(msg string, fields map[string]any) { l.add("debug", msg, fields) }
func (l *w17aLogger) Info(msg string, fields map[string]any)  { l.add("info", msg, fields) }
func (l *w17aLogger) Warn(msg string, fields map[string]any)  { l.add("warn", msg, fields) }
func (l *w17aLogger) Error(msg string, fields map[string]any) { l.add("error", msg, fields) }

func (l *w17aLogger) at(level string) []w17aLogEntry {
	var out []w17aLogEntry
	for _, e := range l.entries {
		if e.level == level {
			out = append(out, e)
		}
	}
	return out
}

var _ port.Logger = (*w17aLogger)(nil)

type w17aInboxKey struct {
	eventID  uuid.UUID
	consumer string
}

// w17aInbox is the processed-event store: it records every lookup and every mark,
// and can fail either one.
type w17aInbox struct {
	lookupErr error
	markErr   error
	seen      map[w17aInboxKey]bool
	lookups   []w17aInboxKey
	marks     []w17aInboxKey
}

func (f *w17aInbox) IsProcessed(_ context.Context, eventID uuid.UUID, consumer string) (bool, error) {
	key := w17aInboxKey{eventID: eventID, consumer: consumer}
	f.lookups = append(f.lookups, key)
	if f.lookupErr != nil {
		return false, f.lookupErr
	}
	return f.seen[key], nil
}

func (f *w17aInbox) MarkProcessed(_ context.Context, eventID uuid.UUID, consumer string) error {
	key := w17aInboxKey{eventID: eventID, consumer: consumer}
	f.marks = append(f.marks, key)
	if f.markErr != nil {
		return f.markErr
	}
	f.seen[key] = true
	return nil
}

func (f *w17aInbox) Process(ctx context.Context, env events.Envelope[json.RawMessage], consumer string, apply func(context.Context) error) error {
	return processThrough(ctx, f, env, consumer, apply)
}

func (f *w17aInbox) Prune(context.Context, time.Duration) (int64, error) { return 0, nil }

var _ port.ProcessedEventRepository = (*w17aInbox)(nil)

type w17aRecencyCall struct {
	scopeKey string
	at       time.Time
}

// w17aRecency runs the locked function directly and records every guard call. A guard read
// that has an error set fails with it; the lock itself never fails.
type w17aRecency struct {
	shouldApplyErr error
	unlessOlderErr error
	last           map[string]time.Time
	locks          []string
	shouldApply    []w17aRecencyCall
	unlessOlder    []w17aRecencyCall
	commits        []w17aRecencyCall
}

func (f *w17aRecency) WithLock(ctx context.Context, scopeKey string, fn func(context.Context) error) error {
	f.locks = append(f.locks, scopeKey)
	return fn(ctx)
}

func (f *w17aRecency) ShouldApply(_ context.Context, scopeKey string, at time.Time) (bool, error) {
	f.shouldApply = append(f.shouldApply, w17aRecencyCall{scopeKey: scopeKey, at: at})
	if f.shouldApplyErr != nil {
		return false, f.shouldApplyErr
	}
	last, ok := f.last[scopeKey]
	return !ok || at.After(last), nil
}

func (f *w17aRecency) ShouldApplyUnlessOlder(_ context.Context, scopeKey string, at time.Time) (bool, error) {
	f.unlessOlder = append(f.unlessOlder, w17aRecencyCall{scopeKey: scopeKey, at: at})
	if f.unlessOlderErr != nil {
		return false, f.unlessOlderErr
	}
	last, ok := f.last[scopeKey]
	return !ok || !at.Before(last), nil
}

func (f *w17aRecency) CheckAndCommit(_ context.Context, scopeKey string, at time.Time) (bool, error) {
	last, ok := f.last[scopeKey]
	if ok && !at.After(last) {
		return false, nil
	}
	f.last[scopeKey] = at
	return true, nil
}

func (f *w17aRecency) Commit(_ context.Context, scopeKey string, at time.Time) error {
	f.commits = append(f.commits, w17aRecencyCall{scopeKey: scopeKey, at: at})
	f.last[scopeKey] = at
	return nil
}

var _ port.RecencyGuard = (*w17aRecency)(nil)

type w17aDelegationCall struct {
	rerouted     bool
	tenantID     uuid.UUID
	delegationID uuid.UUID
	delegatorID  uuid.UUID
	delegateID   uuid.UUID
	scope        string
	scopeID      string
	startsAt     string
	emittedAt    string
	endsAt       string
	endedReason  string
	gucTenant    string
}

// w17aTime is how the recorded times are written, so a test names them as text.
func w17aTime(at time.Time) string { return at.UTC().Format(time.RFC3339Nano) }

// w17aDelegation records each reroute or reversal with the tenant setting on its context.
// results are consumed one per call; a call beyond them succeeds.
type w17aDelegation struct {
	calls   []w17aDelegationCall
	results []error
}

func (f *w17aDelegation) record(call w17aDelegationCall) error {
	f.calls = append(f.calls, call)
	if len(f.calls) <= len(f.results) {
		return f.results[len(f.calls)-1]
	}
	return nil
}

func (f *w17aDelegation) Reroute(ctx context.Context, in port.DelegationRerouteInput) error {
	call := w17aDelegationCall{
		rerouted: true, tenantID: in.TenantID, delegationID: in.DelegationID, delegatorID: in.DelegatorID,
		delegateID: in.DelegateID, scope: in.Scope, gucTenant: w17aGUCTenant(ctx),
		startsAt: w17aTime(in.StartsAt), emittedAt: w17aTime(in.EmittedAt),
	}
	if in.ScopeID != nil {
		call.scopeID = *in.ScopeID
	}
	if in.EndsAt != nil {
		call.endsAt = w17aTime(*in.EndsAt)
	}
	return f.record(call)
}

func (f *w17aDelegation) Reverse(ctx context.Context, in port.DelegationReversalInput) error {
	return f.record(w17aDelegationCall{
		tenantID: in.TenantID, delegationID: in.DelegationID, delegatorID: in.DelegatorID,
		delegateID: in.DelegateID, endedReason: in.EndedReason, gucTenant: w17aGUCTenant(ctx),
	})
}

var _ port.DelegationReconciler = (*w17aDelegation)(nil)

type w17aVacateCall struct {
	tenantID  uuid.UUID
	userID    uuid.UUID
	gucTenant string
}

// w17aAssignments is the assignment store behind the real user safety-net reconciler: only the
// tenant-wide vacate is implemented. results are consumed one per call.
type w17aAssignments struct {
	port.TaskAssignmentRepository
	calls   []w17aVacateCall
	results []error
}

func (f *w17aAssignments) VacateAllActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	f.calls = append(f.calls, w17aVacateCall{tenantID: tenantID, userID: userID, gucTenant: w17aGUCTenant(ctx)})
	if len(f.calls) <= len(f.results) {
		return nil, f.results[len(f.calls)-1]
	}
	return nil, nil
}

type w17aAvailabilityCall struct {
	tenantID  uuid.UUID
	userID    uuid.UUID
	status    string
	gucTenant string
}

type w17aAvailability struct {
	err   error
	calls []w17aAvailabilityCall
}

func (f *w17aAvailability) Apply(ctx context.Context, in port.UserAvailabilityInput) error {
	f.calls = append(f.calls, w17aAvailabilityCall{tenantID: in.TenantID, userID: in.UserID, status: in.Status, gucTenant: w17aGUCTenant(ctx)})
	return f.err
}

var _ port.OOOAvailabilityReconciler = (*w17aAvailability)(nil)

type w17aTenantCall struct {
	tenantID  uuid.UUID
	status    string
	changedAt time.Time
	gucTenant string
}

type w17aTenantLifecycle struct {
	err   error
	calls []w17aTenantCall
}

func (f *w17aTenantLifecycle) Apply(ctx context.Context, in port.TenantLifecycleInput) error {
	f.calls = append(f.calls, w17aTenantCall{tenantID: in.TenantID, status: in.Status, changedAt: in.ChangedAt, gucTenant: w17aGUCTenant(ctx)})
	return f.err
}

var _ port.TenantLifecycleReconciler = (*w17aTenantLifecycle)(nil)

// w17aRig is the handler wired to recording stores. The user safety net is the real
// reconciler over w17aAssignments.
type w17aRig struct {
	inbox        *w17aInbox
	recency      *w17aRecency
	delegation   *w17aDelegation
	assignments  *w17aAssignments
	availability *w17aAvailability
	tenant       *w17aTenantLifecycle
	log          *w17aLogger
	router       *gin.Engine
}

func w17aNewRig() *w17aRig { return w17aNewRigWithTenant(nil) }

// w17aNewRigWithTenant wires the given tenant reconciler instead of the recording one when it
// is not nil.
func w17aNewRigWithTenant(tenant port.TenantLifecycleReconciler) *w17aRig {
	rig := &w17aRig{
		inbox:        &w17aInbox{seen: map[w17aInboxKey]bool{}},
		recency:      &w17aRecency{last: map[string]time.Time{}},
		delegation:   &w17aDelegation{},
		assignments:  &w17aAssignments{},
		availability: &w17aAvailability{},
		tenant:       &w17aTenantLifecycle{},
		log:          &w17aLogger{},
	}
	var lifecycle port.TenantLifecycleReconciler = rig.tenant
	if tenant != nil {
		lifecycle = tenant
	}
	h := handler.New(handler.Services{
		ProcessedEvents: rig.inbox,
		Recency:         rig.recency,
		Delegation:      rig.delegation,
		TenantLifecycle: lifecycle,
		UserSafetyNet:   &service.UserSafetyNetReconciler{Assignments: rig.assignments},
		OOOAvailability: rig.availability,
		Log:             rig.log,
	})
	rig.router = newInternalRouter(h)
	return rig
}

// post sends body as the request body of a JSON POST to route.
func (r *w17aRig) post(route, body string) *httptest.ResponseRecorder {
	return r.postWithType(route, "application/json", body)
}

func (r *w17aRig) postWithType(route, contentType, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, route, strings.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()
	r.router.ServeHTTP(w, req)
	return w
}

// postEnvelope sends the envelope as JSON.
func (r *w17aRig) postEnvelope(route string, env map[string]any) *httptest.ResponseRecorder {
	b, err := json.Marshal(env)
	if err != nil {
		panic(err)
	}
	return r.post(route, string(b))
}

func w17aMarshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// untouched reports whether no reconciler, guard or inbox write ran.
func (r *w17aRig) untouched() bool {
	return len(r.delegation.calls) == 0 && len(r.assignments.calls) == 0 && len(r.availability.calls) == 0 &&
		len(r.tenant.calls) == 0 && len(r.recency.locks) == 0 && len(r.inbox.marks) == 0
}

const (
	w17aScopeID  = "5b1f0c1e-3c7d-4e0a-9d52-0a6f2f1b7c11"
	w17aStartsAt = "2030-01-02T03:04:05Z"
	w17aEndsAt   = "2030-02-03T04:05:06Z"
)

func w17aStartedData(delegationID, delegatorID, delegateID uuid.UUID) map[string]any {
	return map[string]any{
		"delegation_id": delegationID, "delegator_id": delegatorID, "delegate_id": delegateID,
		"scope": "department", "scope_id": w17aScopeID, "starts_at": w17aStartsAt, "ends_at": w17aEndsAt,
	}
}

func w17aEndedData(delegationID, delegatorID, delegateID uuid.UUID) map[string]any {
	return map[string]any{
		"delegation_id": delegationID, "delegator_id": delegatorID, "delegate_id": delegateID, "ended_reason": "expired",
	}
}

func w17aUserDeletedData(userID uuid.UUID) map[string]any {
	return map[string]any{"user_id": userID.String(), "deleted_at": "2030-01-02T03:04:05Z"}
}
