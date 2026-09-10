//go:build e2e

package e2e_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	temporaladapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// lifecycleFixture wires the same real Postgres+Temporal+HTTP stack every
// e2e test in this package uses, one instance started and its single task
// polled to READY — the common starting point pause/resume/cancel/terminate
// each build on.
type lifecycleFixture struct {
	admin         *apiClient
	instanceID    uuid.UUID
	recordVersion int64
	task          taskSummary
}

func newLifecycleFixture(t *testing.T) *lifecycleFixture {
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

	var startResp struct {
		ID uuid.UUID `json:"id"`
	}
	resp := admin.do(http.MethodPost, "/api/v1/instances", map[string]any{
		"business_key":        "e2e-lifecycle-" + uuid.NewString(),
		"workflow_version_id": versionID,
	}, &startResp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /instances status = %d, want 201", resp.StatusCode)
	}

	detail := pollInstance(t, admin, startResp.ID, func(d instanceDetail) bool {
		return len(d.Tasks) > 0 && d.Tasks[0].Status == "READY"
	})
	return &lifecycleFixture{admin: admin, instanceID: startResp.ID, recordVersion: detail.RecordVersion, task: detail.Tasks[0]}
}

// TestE2E_InstancePauseResume drives pause -> resume against a real Temporal
// server: the task itself must stay untouched (still READY, not cancelled)
// while paused, and a pause on an already-PAUSED instance must be rejected
// (409, not silently idempotent) — PauseInstance's real contract, not an
// assumed one.
func TestE2E_InstancePauseResume(t *testing.T) {
	f := newLifecycleFixture(t)

	resp := f.admin.do(http.MethodPost, "/api/v1/instances/"+f.instanceID.String()+"/pause", map[string]any{
		"reason": "waiting on customer", "record_version": f.recordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST .../pause status = %d, want 202", resp.StatusCode)
	}

	detail := pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "PAUSED"
	})
	if len(detail.Tasks) != 1 || detail.Tasks[0].Status != "READY" {
		t.Errorf("task status = %+v, want the single task still READY while paused", detail.Tasks)
	}

	// Pausing an already-PAUSED instance is rejected, not a no-op — assert
	// the specific INVALID_INSTANCE_STATE code, not just any 409 (a stale
	// record_version would also 409, for an unrelated reason).
	var problem struct {
		Code string `json:"code"`
	}
	resp = f.admin.do(http.MethodPost, "/api/v1/instances/"+f.instanceID.String()+"/pause", map[string]any{
		"reason": "again", "record_version": detail.RecordVersion,
	}, &problem)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("POST .../pause (already paused) status = %d, want 409", resp.StatusCode)
	}
	if problem.Code != "INVALID_INSTANCE_STATE" {
		t.Errorf("POST .../pause (already paused) code = %q, want INVALID_INSTANCE_STATE", problem.Code)
	}

	resp = f.admin.do(http.MethodPost, "/api/v1/instances/"+f.instanceID.String()+"/resume", map[string]any{
		"record_version": detail.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST .../resume status = %d, want 202", resp.StatusCode)
	}
	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "RUNNING"
	})

	resp = f.admin.do(http.MethodPost, "/api/v1/tasks/"+f.task.ID.String()+"/complete", map[string]any{
		"result_json":    map[string]any{"decision": "approved"},
		"record_version": f.task.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete status = %d, want 202", resp.StatusCode)
	}
	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "COMPLETED"
	})
}

// TestE2E_InstanceCancel drives POST /instances/:id/cancel: unlike Terminate,
// Cancel is signal-driven — the workflow itself, via CancelInstanceActivity,
// owns the terminal write and the task-fail cascade, not the HTTP handler
// synchronously.
func TestE2E_InstanceCancel(t *testing.T) {
	f := newLifecycleFixture(t)

	resp := f.admin.do(http.MethodPost, "/api/v1/instances/"+f.instanceID.String()+"/cancel", map[string]any{
		"reason": "duplicate submission", "record_version": f.recordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST .../cancel status = %d, want 202 (signal-driven, not synchronous)", resp.StatusCode)
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
	if len(events.Items) == 0 || events.Items[0].EventType != "INSTANCE_CANCELLED" {
		t.Errorf("newest event = %+v, want INSTANCE_CANCELLED (not INSTANCE_TERMINATED — that's Terminate's own event type)", events.Items)
	}
}

// TestE2E_InstanceTerminate drives POST /instances/:id/terminate: unlike
// Cancel, Terminate carries no record_version (a direct DB-write-then-
// TerminateWorkflow call, not signal-validated — LLD §3.1) and responds 200,
// not 202.
func TestE2E_InstanceTerminate(t *testing.T) {
	f := newLifecycleFixture(t)

	resp := f.admin.do(http.MethodPost, "/api/v1/instances/"+f.instanceID.String()+"/terminate", map[string]any{
		"reason": "tenant offboarded",
	}, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST .../terminate status = %d, want 200 (synchronous, unlike the signal-driven lifecycle actions)", resp.StatusCode)
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
