//go:build e2e

package e2e_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	temporaladapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl/dsltest"
)

// TestE2E_FlowOrder runs workflow-models' FlowOrder golden, exactly as
// Definition publishes it, through a real Temporal server, Postgres and the
// HTTP API. Approve decides "review", so the module called on that branch
// runs; the tasks run in the order the flows give, and the instance
// completes.
func TestE2E_FlowOrder(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	sdk := fixtures.NewTestTemporalServer(t)

	tenantID, userID, versionID := uuid.New(), uuid.New(), uuid.New()
	definitions := &fakeDefinitionClient{
		versionID: versionID,
		workflow: &port.CompiledWorkflow{
			WorkflowID: uuid.New(), VersionID: versionID, Status: "PUBLISHED", IsValid: true,
			CompiledPlanJSON: string(dsltest.FlowOrder()),
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
	var golden struct {
		Plans []struct {
			TaskQueue string `json:"task_queue"`
		} `json:"plans"`
	}
	if err := json.Unmarshal(dsltest.FlowOrder(), &golden); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	startWorker(t, sdk, deps, golden.Plans[0].TaskQueue)
	srv := httptest.NewServer(buildRouter(t, pool, sdk, definitions).Handler())
	t.Cleanup(srv.Close)

	admin := &apiClient{t: t, http: srv.Client(), baseURL: srv.URL, tenantID: tenantID, userID: userID, tenantRoles: "tenant_admin"}
	assignee := &apiClient{t: t, http: srv.Client(), baseURL: srv.URL, tenantID: tenantID, userID: userID}

	want := []string{
		"Engineering/Draft", "Ops/Check", "Engineering~2/Approve",
		"CA_Review::Process_Review/Review_Task", "CA_Review::Review_Check::Process_Check/Check_Task",
		"Ops~3/Pack", "Ops~4/Bill", "Engineering~3/Close",
	}
	overrides := map[string]string{"Ops~2/Fix": userID.String()}
	for _, key := range want {
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

	var ran []string
	for range want {
		detail := pollInstance(t, admin, startResp.ID, func(d instanceDetail) bool {
			return slices.ContainsFunc(d.Tasks, func(task taskSummary) bool { return task.Status == "READY" && !slices.Contains(ran, task.NodeKey) })
		})
		i := slices.IndexFunc(detail.Tasks, func(task taskSummary) bool { return task.Status == "READY" && !slices.Contains(ran, task.NodeKey) })
		task := detail.Tasks[i]
		result := `{}`
		if task.NodeKey == "Engineering~2/Approve" {
			result = `{"decision":"review"}`
		}
		resp = assignee.do(http.MethodPost, "/api/v1/tasks/"+task.ID.String()+"/complete", map[string]any{
			"result_json": json.RawMessage(result), "record_version": task.RecordVersion,
		}, nil)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("%s: POST /tasks/:id/complete status = %d, want 202", task.NodeKey, resp.StatusCode)
		}
		ran = append(ran, task.NodeKey)
	}

	pollInstance(t, admin, startResp.ID, func(d instanceDetail) bool { return d.Status == "COMPLETED" })
	// The two parallel branches' tasks are ready together, in either order.
	slices.Sort(ran[5:7])
	if !slices.Equal(ran, want) {
		t.Errorf("tasks ran in order %v, want %v", ran, want)
	}
}
