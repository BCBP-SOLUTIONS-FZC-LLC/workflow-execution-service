//go:build e2e

package e2e_test

import (
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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// outsidePlan is Prepare, then a receive task only an outside party's answer
// completes, then Award.
func outsidePlan(assigneeUserID uuid.UUID) *dsl.CompiledCollaboration {
	human := func(nodeID string) dsl.StageDef {
		return dsl.StageDef{Type: "approve", NodeID: nodeID, Role: "reviewer", DefaultAssignees: []string{assigneeUserID.String()}}
	}
	return &dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans: []*dsl.CompiledPlan{{
			Name:      "main",
			TaskQueue: "wf-e2e-test-queue",
			Departments: []dsl.DepartmentDef{{
				ID: "Ops", Label: "Ops", IAMDepartmentID: uuid.New().String(),
				Stages: []dsl.StageDef{
					human("Prepare"),
					{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
					human("Award"),
				},
			}},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"Ops"}}}},
		}},
	}
}

// TestE2E_OutsideAnswer runs a plan with an outside wait through a real
// Temporal server, Postgres and the HTTP API: the answer is refused while the
// instance has not reached the wait, then accepted once it waits, and the
// instance goes on to its next task and completes.
func TestE2E_OutsideAnswer(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	sdk := fixtures.NewTestTemporalServer(t)

	tenantID, userID, versionID := uuid.New(), uuid.New(), uuid.New()
	definitions := &fakeDefinitionClient{
		versionID: versionID,
		workflow: &port.CompiledWorkflow{
			WorkflowID: uuid.New(), VersionID: versionID, Status: "PUBLISHED", IsValid: true,
			CompiledPlanJSON: string(mustMarshal(t, outsidePlan(userID))),
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
	srv := httptest.NewServer(buildRouter(t, pool, sdk, definitions).Handler())
	t.Cleanup(srv.Close)

	admin := &apiClient{t: t, http: srv.Client(), baseURL: srv.URL, tenantID: tenantID, userID: userID, tenantRoles: "tenant_admin"}
	assignee := &apiClient{t: t, http: srv.Client(), baseURL: srv.URL, tenantID: tenantID, userID: userID}

	var startResp struct {
		ID uuid.UUID `json:"id"`
	}
	resp := admin.do(http.MethodPost, "/api/v1/instances", map[string]any{
		"subject_id": "e2e-subject-" + uuid.NewString(), "business_key": "e2e-outside-" + uuid.NewString(), "workflow_version_id": versionID,
	}, &startResp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /instances status = %d, want 201", resp.StatusCode)
	}
	answerPath := "/api/v1/instances/" + startResp.ID.String() + "/nodes/Ops%2FReceive_Bid/complete"
	answer := map[string]any{"variables": map[string]any{"bid": "42"}}

	prepare := pollInstance(t, admin, startResp.ID, func(d instanceDetail) bool { return taskAt(d, "Ops/Prepare", "READY") != nil })
	if got := admin.do(http.MethodPost, answerPath, answer, nil).StatusCode; got != http.StatusConflict {
		t.Fatalf("an answer before the instance waits: status = %d, want 409 NOT_WAITING", got)
	}
	completeTask(t, assignee, *taskAt(prepare, "Ops/Prepare", "READY"))

	deadline := time.Now().Add(20 * time.Second)
	for {
		got := admin.do(http.MethodPost, answerPath, answer, nil).StatusCode
		if got == http.StatusAccepted {
			break
		}
		if got != http.StatusConflict || time.Now().After(deadline) {
			t.Fatalf("answer to the outside wait: status = %d, want 202 once it waits", got)
		}
		time.Sleep(100 * time.Millisecond)
	}

	award := pollInstance(t, admin, startResp.ID, func(d instanceDetail) bool { return taskAt(d, "Ops/Award", "READY") != nil })
	completeTask(t, assignee, *taskAt(award, "Ops/Award", "READY"))
	pollInstance(t, admin, startResp.ID, func(d instanceDetail) bool { return d.Status == "COMPLETED" })
}

func taskAt(d instanceDetail, nodeKey, status string) *taskSummary {
	for i := range d.Tasks {
		if d.Tasks[i].NodeKey == nodeKey && d.Tasks[i].Status == status {
			return &d.Tasks[i]
		}
	}
	return nil
}

func completeTask(t *testing.T, c *apiClient, task taskSummary) {
	t.Helper()
	resp := c.do(http.MethodPost, "/api/v1/tasks/"+task.ID.String()+"/complete", map[string]any{
		"result_json": map[string]any{}, "record_version": task.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete status = %d, want 202", resp.StatusCode)
	}
}
