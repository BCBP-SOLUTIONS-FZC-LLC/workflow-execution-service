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

// TestE2E_TaskDefer drives POST /tasks/:id/defer against a real Temporal
// server and Postgres — the interpreter's own stage-defer signal handler
// (internal/workflow/signals.go's handleStageDefer) previously had no way to
// resolve the deferred task/assignment's real IDs (it stood in with the
// dept/stage NodeKey string, which DeferTaskActivity's real implementation
// can't uuid.Parse), a gap only a real-Activities e2e run like this one
// actually exercises.
func TestE2E_TaskDefer(t *testing.T) {
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
		"business_key":        "e2e-defer-" + uuid.NewString(),
		"workflow_version_id": versionID,
	}, &startResp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /instances status = %d, want 201", resp.StatusCode)
	}
	instanceID := startResp.ID

	detail := pollInstance(t, admin, instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) > 0 && d.Tasks[0].Status == "READY"
	})
	originalTask := detail.Tasks[0]

	resp = assignee.do(http.MethodPost, "/api/v1/tasks/"+originalTask.ID.String()+"/defer", map[string]any{
		"reason":         "need more info",
		"record_version": originalTask.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/defer status = %d, want 202", resp.StatusCode)
	}

	detail = pollInstance(t, admin, instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) == 2
	})

	var original, regressed *taskSummary
	for i := range detail.Tasks {
		task := &detail.Tasks[i]
		if task.ID == originalTask.ID {
			original = task
		} else {
			regressed = task
		}
	}
	if original == nil || regressed == nil {
		t.Fatalf("expected exactly the original task plus one new one, got %+v", detail.Tasks)
	}
	if original.Status != "DEFERRED" {
		t.Errorf("original task status = %q, want DEFERRED", original.Status)
	}
	if regressed.Status != "READY" {
		t.Errorf("regression task status = %q, want READY", regressed.Status)
	}
	if regressed.DeferredFromTaskID == nil || *regressed.DeferredFromTaskID != originalTask.ID {
		t.Errorf("regression task DeferredFromTaskID = %v, want %v", regressed.DeferredFromTaskID, originalTask.ID)
	}

	resp = assignee.do(http.MethodPost, "/api/v1/tasks/"+regressed.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"approved"}`),
		"record_version": regressed.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete (regression task) status = %d, want 202", resp.StatusCode)
	}

	pollInstance(t, admin, instanceID, func(d instanceDetail) bool {
		return d.Status == "COMPLETED"
	})
}
