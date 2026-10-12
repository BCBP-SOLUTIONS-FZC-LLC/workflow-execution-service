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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl/dsltest"
)

// TestE2E_LibraryCalls runs workflow-models' golden, exactly as Definition
// publishes it, through a real Temporal server, Postgres and the HTTP API:
// one module called from two lanes, calling a nested module. override_map
// names the person for every module task by its call-scoped node key; the
// four tasks run one after another and the instance completes.
func TestE2E_LibraryCalls(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	sdk := fixtures.NewTestTemporalServer(t)

	tenantID, userID, versionID := uuid.New(), uuid.New(), uuid.New()
	definitions := &fakeDefinitionClient{
		versionID: versionID,
		workflow: &port.CompiledWorkflow{
			WorkflowID: uuid.New(), VersionID: versionID, Status: "PUBLISHED", IsValid: true,
			CompiledPlanJSON: string(dsltest.LibraryCalls()),
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
	// Start routes to the queue the published plan names.
	var golden struct {
		Plans []struct {
			TaskQueue string `json:"task_queue"`
		} `json:"plans"`
	}
	if err := json.Unmarshal(dsltest.LibraryCalls(), &golden); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	startWorker(t, sdk, deps, golden.Plans[0].TaskQueue)
	srv := httptest.NewServer(buildRouter(t, pool, sdk, definitions).Handler())
	t.Cleanup(srv.Close)

	admin := &apiClient{t: t, http: srv.Client(), baseURL: srv.URL, tenantID: tenantID, userID: userID, tenantRoles: "tenant_admin"}
	assignee := &apiClient{t: t, http: srv.Client(), baseURL: srv.URL, tenantID: tenantID, userID: userID}

	overrides := map[string]string{}
	for _, key := range []string{
		"CA_Eng::Process_Review/Review_Task",
		"CA_Eng::Review_Check::Process_Check/Check_Task",
		"CA_Ops::Process_Review/Review_Task",
		"CA_Ops::Review_Check::Process_Check/Check_Task",
	} {
		overrides[key] = userID.String()
	}
	var startResp struct {
		ID uuid.UUID `json:"id"`
	}
	resp := admin.do(http.MethodPost, "/api/v1/instances", map[string]any{
		"subject_id":          "e2e-subject-" + uuid.NewString(),
		"business_key":        "e2e-" + uuid.NewString(),
		"workflow_version_id": versionID,
		"override_map":        overrides,
	}, &startResp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /instances status = %d, want 201", resp.StatusCode)
	}

	done := map[uuid.UUID]bool{}
	for step := range 4 {
		detail := pollInstance(t, admin, startResp.ID, func(d instanceDetail) bool {
			for _, task := range d.Tasks {
				if task.Status == "READY" && !done[task.ID] {
					return true
				}
			}
			return false
		})
		for _, task := range detail.Tasks {
			if task.Status != "READY" || done[task.ID] {
				continue
			}
			resp = assignee.do(http.MethodPost, "/api/v1/tasks/"+task.ID.String()+"/complete", map[string]any{
				"result_json": json.RawMessage(`{}`), "record_version": task.RecordVersion,
			}, nil)
			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("task %d: POST /tasks/:id/complete status = %d, want 202", step+1, resp.StatusCode)
			}
			done[task.ID] = true
			break
		}
	}

	final := pollInstance(t, admin, startResp.ID, func(d instanceDetail) bool { return d.Status == "COMPLETED" })
	if len(final.Tasks) != 4 {
		t.Errorf("instance ran %d tasks, want the four module tasks: %+v", len(final.Tasks), final.Tasks)
	}
}
