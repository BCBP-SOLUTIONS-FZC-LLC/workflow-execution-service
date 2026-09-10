//go:build e2e

package e2e_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

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
		admin: admin, assignee: assignee, tenantID: tenantID, assigneeID: assigneeUserID,
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
