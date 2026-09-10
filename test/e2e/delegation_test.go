//go:build e2e

package e2e_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	temporaladapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// TestE2E_DelegationStartedThenEnded drives POST
// /api/v1/internal/events/delegation against a real Temporal server:
// DelegationStarted must reroute the delegator's active assignment to the
// delegate (vacating the original), and the paired DelegationEnded must
// reverse it back — DelegationReconciler.Reverse only restores an eligible
// original assignee, which the fake eligibility checker (always eligible)
// satisfies here.
func TestE2E_DelegationStartedThenEnded(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	sdk := fixtures.NewTestTemporalServer(t)

	tenantID := uuid.New()
	delegatorID := uuid.New()
	delegateID := uuid.New()
	versionID := uuid.New()
	delegationID := uuid.New()

	definitions := &fakeDefinitionClient{
		versionID: versionID,
		workflow: &port.CompiledWorkflow{
			WorkflowID: uuid.New(), VersionID: versionID, Status: "PUBLISHED", IsValid: true,
			CompiledPlanJSON: string(mustMarshal(t, singleTaskPlan(delegatorID))),
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

	admin := &apiClient{t: t, http: srv.Client(), baseURL: srv.URL, tenantID: tenantID, userID: delegatorID, tenantRoles: "tenant_admin"}
	delegator := &apiClient{t: t, http: srv.Client(), baseURL: srv.URL, tenantID: tenantID, userID: delegatorID}
	delegate := &apiClient{t: t, http: srv.Client(), baseURL: srv.URL, tenantID: tenantID, userID: delegateID}

	var startResp struct {
		ID uuid.UUID `json:"id"`
	}
	resp := admin.do(http.MethodPost, "/api/v1/instances", map[string]any{
		"business_key":        "e2e-delegation-" + uuid.NewString(),
		"workflow_version_id": versionID,
	}, &startResp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /instances status = %d, want 201", resp.StatusCode)
	}
	instanceID := startResp.ID

	detail := pollInstance(t, admin, instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) > 0 && d.Tasks[0].Status == "READY"
	})
	task := detail.Tasks[0]

	t0 := time.Now()
	resp = postInternalEvent(t, admin, "/api/v1/internal/events/delegation", "DelegationStarted", tenantID, t0, map[string]any{
		"delegation_id": delegationID.String(), "delegator_id": delegatorID.String(), "delegate_id": delegateID.String(),
		"scope": "all", "starts_at": t0.Format(time.RFC3339),
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST .../events/delegation (DelegationStarted) status = %d, want 200", resp.StatusCode)
	}

	// Reroute is synchronous (a direct DB transaction, no signal round trip
	// needed to observe the reassignment) — the delegate should be able to
	// complete the task, the delegator should not.
	resp = delegator.do(http.MethodPost, "/api/v1/tasks/"+task.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"approved"}`),
		"record_version": task.RecordVersion,
	}, nil)
	if resp.StatusCode == http.StatusAccepted {
		t.Errorf("delegator could still complete the task after DelegationStarted; want rejected")
	}

	var afterReroute taskSummary
	resp = admin.do(http.MethodGet, "/api/v1/tasks/"+task.ID.String(), nil, &afterReroute)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /tasks/:id status = %d, want 200", resp.StatusCode)
	}

	// End the delegation before completing the task, so the reversal has
	// something to restore rather than racing the task's own completion.
	t1 := t0.Add(time.Second)
	resp = postInternalEvent(t, admin, "/api/v1/internal/events/delegation", "DelegationEnded", tenantID, t1, map[string]any{
		"delegation_id": delegationID.String(), "delegator_id": delegatorID.String(), "delegate_id": delegateID.String(),
		"ended_reason": "expired",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST .../events/delegation (DelegationEnded) status = %d, want 200", resp.StatusCode)
	}

	var afterReversal taskSummary
	resp = admin.do(http.MethodGet, "/api/v1/tasks/"+task.ID.String(), nil, &afterReversal)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /tasks/:id status = %d, want 200", resp.StatusCode)
	}

	// The delegate must no longer be able to complete it (reversed away)...
	resp = delegate.do(http.MethodPost, "/api/v1/tasks/"+task.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"approved"}`),
		"record_version": afterReversal.RecordVersion,
	}, nil)
	if resp.StatusCode == http.StatusAccepted {
		t.Errorf("delegate could still complete the task after DelegationEnded; want rejected")
	}

	// ...but the original delegator, now restored, can.
	var finalDetail taskSummary
	resp = admin.do(http.MethodGet, "/api/v1/tasks/"+task.ID.String(), nil, &finalDetail)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /tasks/:id status = %d, want 200", resp.StatusCode)
	}
	resp = delegator.do(http.MethodPost, "/api/v1/tasks/"+task.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"approved"}`),
		"record_version": finalDetail.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete (restored delegator) status = %d, want 202 (DelegationEnded must restore the original assignee)", resp.StatusCode)
	}

	pollInstance(t, admin, instanceID, func(d instanceDetail) bool {
		return d.Status == "COMPLETED"
	})
}
