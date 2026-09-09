//go:build e2e

package e2e_test

import (
	"bytes"
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

// apiClient carries one caller's identity against one base URL — every
// /api/v1 route needs the same three headers, so this wraps them once
// instead of threading them through every call site.
type apiClient struct {
	t                *testing.T
	http             *http.Client
	baseURL          string
	tenantID, userID uuid.UUID
	tenantRoles      string
}

func (c *apiClient) do(method, path string, body, out any) *http.Response {
	c.t.Helper()
	var reader *bytes.Reader
	if body != nil {
		reader = bytes.NewReader(mustMarshalAny(c.t, body))
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, c.baseURL+path, reader)
	if err != nil {
		c.t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-tenant-id", c.tenantID.String())
	req.Header.Set("x-user-id", c.userID.String())
	if c.tenantRoles != "" {
		req.Header.Set("x-tenant-roles", c.tenantRoles)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	c.t.Cleanup(func() { _ = resp.Body.Close() })
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			c.t.Fatalf("decode response body: %v", err)
		}
	}
	return resp
}

func mustMarshalAny(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

type taskSummary struct {
	ID            uuid.UUID `json:"id"`
	Status        string    `json:"status"`
	RecordVersion int64     `json:"record_version"`
}

type instanceDetail struct {
	Status string        `json:"status"`
	Tasks  []taskSummary `json:"tasks"`
}

// pollInstance polls GET /instances/:id until ready reports true or
// pollTimeout elapses.
func pollInstance(t *testing.T, c *apiClient, instanceID uuid.UUID, ready func(instanceDetail) bool) instanceDetail {
	t.Helper()
	deadline := time.Now().Add(pollTimeout)
	var detail instanceDetail
	for {
		resp := c.do(http.MethodGet, "/api/v1/instances/"+instanceID.String(), nil, &detail)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /instances/:id status = %d, want 200", resp.StatusCode)
		}
		if ready(detail) {
			return detail
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out polling instance %s; last status = %q, tasks = %+v", instanceID, detail.Status, detail.Tasks)
		}
		time.Sleep(pollInterval)
	}
}

// TestE2E_InstantiateDispatchComplete is this repo's first real end-to-end
// round trip: a live Temporal server, a live Postgres database, and the
// actual HTTP router — no mocked persistence, no in-memory workflow
// simulator. It covers one department/one task, not every workflow shape.
func TestE2E_InstantiateDispatchComplete(t *testing.T) {
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
		"business_key":        "e2e-" + uuid.NewString(),
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
}
