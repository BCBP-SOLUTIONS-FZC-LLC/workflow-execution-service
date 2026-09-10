//go:build e2e

package e2e_test

import (
	"encoding/json"
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

type workflowEventSummary struct {
	EventType          string    `json:"event_type"`
	WorkflowInstanceID uuid.UUID `json:"workflow_instance_id"`
}

type workflowEventsPage struct {
	Items []workflowEventSummary `json:"items"`
}

// TestE2E_InstantiateCompleteProjectsEvents drives the same instantiate ->
// dispatch -> complete round trip as TestE2E_InstantiateDispatchComplete, but
// asserts on the one HTTP-visible "status projected to events" surface this
// service owns: GET /instances/:id/events (InstanceService.ListEvents,
// reading the outbox_events table this service itself writes to on every
// transition). No e2e test exercised this endpoint at all before — the
// existing round-trip test only asserts instance/task status, never that
// the audit trail those transitions are supposed to produce is actually
// correct.
func TestE2E_InstantiateCompleteProjectsEvents(t *testing.T) {
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
		"business_key":        "e2e-events-" + uuid.NewString(),
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

	resp = assignee.do(http.MethodPost, "/api/v1/tasks/"+task.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"approved"}`),
		"record_version": task.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete status = %d, want 202", resp.StatusCode)
	}

	pollInstance(t, admin, instanceID, func(d instanceDetail) bool {
		return d.Status == "COMPLETED"
	})

	var events workflowEventsPage
	resp = admin.do(http.MethodGet, "/api/v1/instances/"+instanceID.String()+"/events", nil, &events)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /instances/:id/events status = %d, want 200", resp.StatusCode)
	}

	// Newest first (ListOutboxEventsByInstance's own ORDER BY created_at
	// DESC) — the reverse of the order these transitions actually happened
	// in.
	wantTypes := []string{"INSTANCE_COMPLETED", "TASK_COMPLETED", "TASK_CREATED", "INSTANCE_STARTED"}
	if len(events.Items) != len(wantTypes) {
		t.Fatalf("GET /instances/:id/events returned %d items, want %d; got %+v", len(events.Items), len(wantTypes), events.Items)
	}
	for i, want := range wantTypes {
		got := events.Items[i]
		if got.EventType != want {
			t.Errorf("events.Items[%d].EventType = %q, want %q (full list: %+v)", i, got.EventType, want, events.Items)
		}
		if got.WorkflowInstanceID != instanceID {
			t.Errorf("events.Items[%d].WorkflowInstanceID = %v, want %v", i, got.WorkflowInstanceID, instanceID)
		}
	}
}
