//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	temporaladapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// reconcilerFixture starts a fresh instance (singleTaskPlan) and polls its
// task to READY — every inbound-reconciler test below drives one of the
// three internal-event routes against this same starting point.
type reconcilerFixture struct {
	admin      *apiClient
	assignee   *apiClient
	pool       *pgcommon.Pool
	tenantID   uuid.UUID
	assigneeID uuid.UUID
	instanceID uuid.UUID
	task       taskSummary
}

func newReconcilerFixture(t *testing.T) *reconcilerFixture {
	t.Helper()
	pool := fixtures.NewTestPool(t)
	sdk := fixtures.NewTestTemporalServer(t)

	tenantID := uuid.New()
	assigneeUserID := uuid.New()
	versionID := uuid.New()

	definitions := &fakeDefinitionClient{
		versionID: versionID,
		workflow: &port.CompiledWorkflow{
			WorkflowID: uuid.New(), VersionID: versionID, Status: "PUBLISHED", IsValid: true,
			CompiledPlanJSON: string(mustMarshal(t, singleTaskPlan(assigneeUserID))),
		},
	}

	validator, err := eventbus.NewSchemaValidator()
	if err != nil {
		t.Fatalf("new schema validator: %v", err)
	}
	deps := &temporaladapter.Deps{
		Instances: postgres.NewInstanceRepo(pool), Tasks: postgres.NewTaskRepo(pool), Assignments: postgres.NewTaskAssignmentRepo(pool),
		Outbox: postgres.NewOutboxRepo(pool), Transactor: postgres.NewTransactor(pool), Validator: validator, Definitions: definitions,
	}
	startWorker(t, sdk, deps, "wf-e2e-test-queue")

	router := buildRouter(t, pool, sdk, definitions)
	srv := httptest.NewServer(router.Handler())
	t.Cleanup(srv.Close)

	admin := &apiClient{t: t, http: srv.Client(), baseURL: srv.URL, tenantID: tenantID, userID: assigneeUserID, tenantRoles: "tenant_admin"}
	assignee := &apiClient{t: t, http: srv.Client(), baseURL: srv.URL, tenantID: tenantID, userID: assigneeUserID}

	var startResp struct {
		ID uuid.UUID `json:"id"`
	}
	resp := admin.do(http.MethodPost, "/api/v1/instances", map[string]any{
		"business_key":        "e2e-reconciler-" + uuid.NewString(),
		"workflow_version_id": versionID,
	}, &startResp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /instances status = %d, want 201", resp.StatusCode)
	}

	detail := pollInstance(t, admin, startResp.ID, func(d instanceDetail) bool {
		return len(d.Tasks) > 0 && d.Tasks[0].Status == "READY"
	})
	return &reconcilerFixture{
		admin: admin, assignee: assignee, pool: pool, tenantID: tenantID, assigneeID: assigneeUserID,
		instanceID: startResp.ID, task: detail.Tasks[0],
	}
}

func postInternalEvent(t *testing.T, client *apiClient, route, eventType string, tenantID uuid.UUID, at time.Time, data map[string]any) *http.Response {
	t.Helper()
	body := map[string]any{
		"id": uuid.NewString(), "type": eventType, "tenant_id": tenantID.String(),
		"time": at.Format(time.RFC3339), "data": data,
	}
	return client.do(http.MethodPost, route, body, nil)
}

// TestE2E_UserDeletedVacatesAssignment drives POST
// /api/v1/internal/events/user-profile{UserDeleted} against a real Temporal
// server: UserSafetyNetReconciler.VacateAssignments must vacate the deleted
// user's assignment without tearing down the instance itself, and the task
// must become uncompletable by that (now-vacated) user afterward.
func TestE2E_UserDeletedVacatesAssignment(t *testing.T) {
	f := newReconcilerFixture(t)

	resp := postInternalEvent(t, f.admin, "/api/v1/internal/events/user-profile", "UserDeleted", f.tenantID, time.Now(), map[string]any{
		"user_id": f.assigneeID.String(), "deleted_at": time.Now().Format(time.RFC3339),
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST .../events/user-profile (UserDeleted) status = %d, want 200", resp.StatusCode)
	}

	// VacateAssignments is synchronous (a direct DB write, no signal round
	// trip) — the vacate has already committed by the time the handler
	// responds, so no poll is needed before probing it.
	resp = f.assignee.do(http.MethodPost, "/api/v1/tasks/"+f.task.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"approved"}`),
		"record_version": f.task.RecordVersion,
	}, nil)
	if resp.StatusCode == http.StatusAccepted {
		t.Errorf("deleted user could still complete the task; want rejected (assignment must be vacated)")
	}

	detail := pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool { return true })
	if detail.Status != "RUNNING" {
		t.Errorf("instance status = %q, want RUNNING (UserDeleted is a per-assignment safety net, not an instance-wide action)", detail.Status)
	}
}

// TestE2E_UserAvailabilityChangedPausesAndResumes drives POST
// /api/v1/internal/events/user-profile{UserAvailabilityChanged} — status=ooo
// must pause the user's RUNNING instances via a real SignalWorkflow call,
// status=available must resume them.
func TestE2E_UserAvailabilityChangedPausesAndResumes(t *testing.T) {
	f := newReconcilerFixture(t)

	t0 := time.Now()
	resp := postInternalEvent(t, f.admin, "/api/v1/internal/events/user-profile", "UserAvailabilityChanged", f.tenantID, t0, map[string]any{
		"user_id": f.assigneeID.String(), "status": "ooo",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST .../events/user-profile (ooo) status = %d, want 200", resp.StatusCode)
	}
	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "PAUSED"
	})

	// RecencyGuard is a strict `<=`-skip — the resume event's own timestamp
	// must be strictly later than the pause event's.
	resp = postInternalEvent(t, f.admin, "/api/v1/internal/events/user-profile", "UserAvailabilityChanged", f.tenantID, t0.Add(time.Second), map[string]any{
		"user_id": f.assigneeID.String(), "status": "available",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST .../events/user-profile (available) status = %d, want 200", resp.StatusCode)
	}
	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "RUNNING"
	})
}

// TestE2E_TenantStateChangedSuspendResume drives POST
// /api/v1/internal/events/tenant{TenantStateChanged}: a non-active status
// pauses every RUNNING instance tenant-wide, and status=active resumes the
// ones it paused.
func TestE2E_TenantStateChangedSuspendResume(t *testing.T) {
	f := newReconcilerFixture(t)

	t0 := time.Now()
	resp := postInternalEvent(t, f.admin, "/api/v1/internal/events/tenant", "TenantStateChanged", f.tenantID, t0, map[string]any{
		"tenant_id": f.tenantID.String(), "status": "suspended", "previous_status": "active",
		"plan": "starter", "previous_plan": "starter", "changed_at": t0.Format(time.RFC3339), "cause": "TenantSuspended",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST .../events/tenant (suspended) status = %d, want 200", resp.StatusCode)
	}
	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "PAUSED"
	})

	t1 := t0.Add(time.Second)
	resp = postInternalEvent(t, f.admin, "/api/v1/internal/events/tenant", "TenantStateChanged", f.tenantID, t1, map[string]any{
		"tenant_id": f.tenantID.String(), "status": "active", "previous_status": "suspended",
		"plan": "starter", "previous_plan": "starter", "changed_at": t1.Format(time.RFC3339), "cause": "TenantReactivated",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST .../events/tenant (active) status = %d, want 200", resp.StatusCode)
	}
	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "RUNNING"
	})
}

// TestE2E_TenantStateChangedOffboardTerminates drives the offboard path:
// status=offboarded must genuinely TerminateWorkflow the instance (not just
// flip a status flag) and record a workflow.instance.terminated event.
func TestE2E_TenantStateChangedOffboardTerminates(t *testing.T) {
	f := newReconcilerFixture(t)

	t0 := time.Now()
	resp := postInternalEvent(t, f.admin, "/api/v1/internal/events/tenant", "TenantStateChanged", f.tenantID, t0, map[string]any{
		"tenant_id": f.tenantID.String(), "status": "offboarded", "previous_status": "active",
		"plan": "starter", "previous_plan": "starter", "changed_at": t0.Format(time.RFC3339), "cause": "TenantOffboarded",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST .../events/tenant (offboarded) status = %d, want 200", resp.StatusCode)
	}

	detail := pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "TERMINATED"
	})
	if len(detail.Tasks) != 1 || detail.Tasks[0].Status != "FAILED" {
		t.Errorf("task status = %+v, want the single task FAILED (cascade)", detail.Tasks)
	}

	var events workflowEventsPage
	resp = f.admin.do(http.MethodGet, "/api/v1/instances/"+f.instanceID.String()+"/events", nil, &events)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET .../events status = %d, want 200", resp.StatusCode)
	}
	if len(events.Items) == 0 || events.Items[0].EventType != "INSTANCE_TERMINATED" {
		t.Errorf("newest event = %+v, want INSTANCE_TERMINATED", events.Items)
	}
}

// TestE2E_UserAvailabilityChangedConcurrentOutOfOrder drives LLD §6.3's own
// documented concern: IAM's standard (non-FIFO) delivery means two
// conflicting events for the same scope key can arrive and process
// concurrently, out of timestamp order. Fires "available" (the later
// timestamp) and "ooo" (the earlier one) truly concurrently — RecencyGuard's
// WithLock (a Postgres session-level advisory lock) serializes them, but
// only ShouldApply's own `<=`-skip decides which one's effect survives.
// Either processing order must land on the same final state: the earlier
// "ooo" must never win just because it happened to be processed second.
func TestE2E_UserAvailabilityChangedConcurrentOutOfOrder(t *testing.T) {
	f := newReconcilerFixture(t)

	t0 := time.Now()
	t1 := t0.Add(time.Second)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		postInternalEvent(t, f.admin, "/api/v1/internal/events/user-profile", "UserAvailabilityChanged", f.tenantID, t1, map[string]any{
			"user_id": f.assigneeID.String(), "status": "available",
		})
	}()
	go func() {
		defer wg.Done()
		postInternalEvent(t, f.admin, "/api/v1/internal/events/user-profile", "UserAvailabilityChanged", f.tenantID, t0, map[string]any{
			"user_id": f.assigneeID.String(), "status": "ooo",
		})
	}()
	wg.Wait()

	// RUNNING is correct under both possible processing orders: "ooo" first
	// then "available" (available's later timestamp legitimately resumes
	// what ooo just paused — a real, if transient, pause the poll below
	// waits out), or "available" first then "ooo" (ooo's earlier timestamp
	// is rejected as stale against available's already-committed recency
	// value, so the instance is never paused at all). Either signal's
	// effect lands asynchronously (the HTTP call only waits for
	// SignalWorkflow's own send to be acknowledged, not for the workflow to
	// react to it), so this polls for RUNNING rather than checking once.
	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "RUNNING"
	})
	// Settle-check: confirm it doesn't flip back to PAUSED shortly after
	// (would indicate the two events' effects applied in the wrong order).
	time.Sleep(500 * time.Millisecond)
	detail := pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool { return true })
	if detail.Status != "RUNNING" {
		t.Errorf("instance status = %q shortly after settling, want RUNNING to hold", detail.Status)
	}
}

// countProcessedEvents returns how many processed_event rows exist for
// consumer, optionally narrowed to a single event id.
func countProcessedEvents(t *testing.T, pool *pgcommon.Pool, consumer string, eventID *uuid.UUID) int {
	t.Helper()
	var n int
	query := `SELECT count(*) FROM processed_event WHERE consumer = $1`
	args := []any{consumer}
	if eventID != nil {
		query += ` AND event_id = $2`
		args = append(args, *eventID)
	}
	if err := pool.WithConn(context.Background(), func(ctx context.Context, c *pgxpool.Conn) error {
		return c.QueryRow(ctx, query, args...).Scan(&n)
	}); err != nil {
		t.Fatalf("count processed_event: %v", err)
	}
	return n
}

// TestE2E_DuplicateInternalEventDeliveryIsDeduped closes LLD §7.2's dedup
// half. SNS delivers at-least-once, so every inbound handler can be invoked
// twice with the same envelope; processed_event + RecordIfNew is this
// service's own guarantee that the second invocation does nothing.
//
// The existing coverage stops one layer short of this on both sides:
// test/unit/handler drives the handler against a fake processedEvents repo,
// and test/integration/postgres exercises the real repo with no handler
// above it. Nothing composed the two, so nothing proved the dedup row is
// actually durable across two separate HTTP requests.
//
// The final delivery with a fresh event id is the control: without it, a
// completely dead RecordIfNew path would satisfy the "still one row"
// assertion just as well as a working one.
func TestE2E_DuplicateInternalEventDeliveryIsDeduped(t *testing.T) {
	f := newReconcilerFixture(t)

	const route = "/api/v1/internal/events/user-profile"
	eventID := uuid.New()
	body := map[string]any{
		"id": eventID.String(), "type": "UserDeleted", "tenant_id": f.tenantID.String(),
		"time": time.Now().Format(time.RFC3339),
		"data": map[string]any{
			"user_id": f.assigneeID.String(), "deleted_at": time.Now().Format(time.RFC3339),
		},
	}

	if resp := f.admin.do(http.MethodPost, route, body, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("first delivery status = %d, want 200", resp.StatusCode)
	}
	if n := countProcessedEvents(t, f.pool, "user-execution", &eventID); n != 1 {
		t.Fatalf("processed_event rows for the delivered event = %d, want 1 — the dedup row was never written, so every redelivery would re-run the handler", n)
	}

	// Byte-identical redelivery, exactly as SQS would repeat it.
	if resp := f.admin.do(http.MethodPost, route, body, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("redelivery status = %d, want 200 — a duplicate must be acknowledged, not rejected", resp.StatusCode)
	}
	if n := countProcessedEvents(t, f.pool, "user-execution", &eventID); n != 1 {
		t.Errorf("processed_event rows after redelivery = %d, want 1", n)
	}

	// Control: a distinct event id must still be recorded, proving the
	// assertions above measure dedup rather than a write path that never runs.
	body["id"] = uuid.NewString()
	if resp := f.admin.do(http.MethodPost, route, body, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("distinct-id delivery status = %d, want 200", resp.StatusCode)
	}
	if n := countProcessedEvents(t, f.pool, "user-execution", nil); n != 2 {
		t.Errorf("processed_event rows for consumer user-execution = %d, want 2 (one per distinct event id)", n)
	}
}

// TestE2E_TenantStateChangedOutOfOrderSuspendResume is item #26 against a
// real Temporal server. Both events are serialised by RecencyGuard, but the
// pause's effect on workflow_instance.status is written asynchronously by
// the workflow's own activity. The resume sweep used to select instances
// with a WHERE status = 'PAUSED' filter, so an instance whose row had not
// caught up was not skipped — it was never selected at all, no resume was
// sent, and no further event would ever arrive. Stuck PAUSED forever.
//
// The sweep now selects every non-terminal instance and signals
// unconditionally, leaving signalPreconditions — which runs on the
// workflow's own goroutine and is never stale — as the only gate.
func TestE2E_TenantStateChangedOutOfOrderSuspendResume(t *testing.T) {
	f := newReconcilerFixture(t)

	t0 := time.Now()
	suspend := map[string]any{
		"tenant_id": f.tenantID.String(), "status": "suspended", "previous_status": "active",
		"plan": "starter", "previous_plan": "starter", "changed_at": t0.Format(time.RFC3339), "cause": "TenantSuspended",
	}
	resp := postInternalEvent(t, f.admin, "/api/v1/internal/events/tenant", "TenantStateChanged", f.tenantID, t0, suspend)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST .../events/tenant (suspended) status = %d, want 200", resp.StatusCode)
	}

	// Deliberately no poll for PAUSED: reactivation arrives while the pause
	// is still in flight, which is exactly the race being covered.
	t1 := t0.Add(time.Second)
	reactivate := map[string]any{
		"tenant_id": f.tenantID.String(), "status": "active", "previous_status": "suspended",
		"plan": "starter", "previous_plan": "starter", "changed_at": t1.Format(time.RFC3339), "cause": "TenantReactivated",
	}
	resp = postInternalEvent(t, f.admin, "/api/v1/internal/events/tenant", "TenantStateChanged", f.tenantID, t1, reactivate)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST .../events/tenant (active) status = %d, want 200", resp.StatusCode)
	}

	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "RUNNING"
	})
	// Settle-check: a resume that was never sent would leave the instance
	// PAUSED permanently, and nothing later would rescue it.
	time.Sleep(500 * time.Millisecond)
	detail := pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool { return true })
	if detail.Status != "RUNNING" {
		t.Errorf("instance status = %q after settling, want RUNNING", detail.Status)
	}
}

// TestE2E_TenantReactivationDoesNotResumeAnOOOPause covers the other half of
// #26: LLD §6.2 specifies resume as initiator-filtered, but no initiator
// column exists to filter on — and adding one would reintroduce exactly the
// stale-read this service just removed. The pause initiator is held in
// interpreter state instead, so the filter is enforced at signal validation.
//
// Without it, a tenant suspend/reactivate cycle silently resumes an
// unrelated OOO pause (and any deliberate admin pause), putting work back in
// front of a user who is still out of office.
func TestE2E_TenantReactivationDoesNotResumeAnOOOPause(t *testing.T) {
	f := newReconcilerFixture(t)

	t0 := time.Now()
	resp := postInternalEvent(t, f.admin, "/api/v1/internal/events/user-profile", "UserAvailabilityChanged", f.tenantID, t0, map[string]any{
		"user_id": f.assigneeID.String(), "status": "ooo",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST .../events/user-profile (ooo) status = %d, want 200", resp.StatusCode)
	}
	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "PAUSED"
	})

	for i, state := range []string{"suspended", "active"} {
		at := t0.Add(time.Duration(i+1) * time.Second)
		previous := "active"
		if state == "active" {
			previous = "suspended"
		}
		resp = postInternalEvent(t, f.admin, "/api/v1/internal/events/tenant", "TenantStateChanged", f.tenantID, at, map[string]any{
			"tenant_id": f.tenantID.String(), "status": state, "previous_status": previous,
			"plan": "starter", "previous_plan": "starter", "changed_at": at.Format(time.RFC3339),
			"cause": "TenantStateChanged",
		})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST .../events/tenant (%s) status = %d, want 200", state, resp.StatusCode)
		}
	}

	// The tenant is active again, but the pause belongs to the OOO
	// reconciler — only an "ooo" (or admin) resume may lift it.
	time.Sleep(time.Second)
	detail := pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool { return true })
	if detail.Status != "PAUSED" {
		t.Errorf("instance status = %q after tenant reactivation, want PAUSED — tenant state must not resume an OOO pause", detail.Status)
	}

	// The user coming back does lift it, proving the instance is not wedged.
	resp = postInternalEvent(t, f.admin, "/api/v1/internal/events/user-profile", "UserAvailabilityChanged", f.tenantID, t0.Add(4*time.Second), map[string]any{
		"user_id": f.assigneeID.String(), "status": "available",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST .../events/user-profile (available) status = %d, want 200", resp.StatusCode)
	}
	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "RUNNING"
	})
}
