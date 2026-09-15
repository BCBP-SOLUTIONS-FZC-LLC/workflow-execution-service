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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// parallelPlan is a Parallel gateway with two one-stage departments — the
// smallest realistic shape for testing that the instance only proceeds once
// BOTH branches complete, not on the first one, regardless of order.
func parallelPlan(assigneeUserID uuid.UUID) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans: []*dsl.CompiledPlan{{
			Name:      "main",
			TaskQueue: "wf-e2e-test-queue",
			Departments: []dsl.DepartmentDef{
				{
					ID: "warehouse", Label: "Warehouse", IAMDepartmentID: uuid.New().String(),
					Stages: []dsl.StageDef{{Type: "approve", NodeID: "pack", Role: "reviewer", DefaultAssignees: []string{assigneeUserID.String()}}},
				},
				{
					ID: "billing", Label: "Billing", IAMDepartmentID: uuid.New().String(),
					Stages: []dsl.StageDef{{Type: "approve", NodeID: "charge", Role: "reviewer", DefaultAssignees: []string{assigneeUserID.String()}}},
				},
			},
			Execution: dsl.ExecutionPlan{
				Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
					{DeptID: "warehouse", Steps: []dsl.ExecutionStep{{Sequential: []string{"warehouse"}}}},
					{DeptID: "billing", Steps: []dsl.ExecutionStep{{Sequential: []string{"billing"}}}},
				}}},
			},
		}},
	}
}

// runParallelBranchTest instantiates parallelPlan and completes its two
// tasks in the order given by completeFirst/completeSecond (identified by
// department NodeID suffix — "warehouse/pack" or "billing/charge") — the
// instance must only reach COMPLETED after the second one, never after the
// first alone.
func runParallelBranchTest(t *testing.T, firstDept, secondDept string) {
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
			CompiledPlanJSON: string(mustMarshal(t, parallelPlan(assigneeUserID))),
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
		"business_key":        "e2e-parallel-" + uuid.NewString(),
		"workflow_version_id": versionID,
	}, &startResp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /instances status = %d, want 201", resp.StatusCode)
	}
	instanceID := startResp.ID

	detail := pollInstance(t, admin, instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) == 2 && d.Tasks[0].Status == "READY" && d.Tasks[1].Status == "READY"
	})

	// The response doesn't carry NodeKey, but the department task queue
	// registration order is deterministic (warehouse then billing) — GET
	// /tasks lets us resolve each task's real node_key to tell them apart.
	warehouseTask, billingTask := resolveByNodeKey(t, admin, instanceID, detail, "warehouse/pack", "billing/charge")

	completeByDept := func(dept string) {
		var task taskSummary
		switch dept {
		case "warehouse/pack":
			task = warehouseTask
		case "billing/charge":
			task = billingTask
		}
		resp := assignee.do(http.MethodPost, "/api/v1/tasks/"+task.ID.String()+"/complete", map[string]any{
			"result_json":    json.RawMessage(`{}`),
			"record_version": task.RecordVersion,
		}, nil)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("POST /tasks/:id/complete (%s) status = %d, want 202", dept, resp.StatusCode)
		}
	}

	completeByDept(firstDept)

	// After only one branch completes, the instance must still be RUNNING —
	// never COMPLETED on the first branch alone.
	mid := pollInstance(t, admin, instanceID, func(d instanceDetail) bool { return true })
	if mid.Status != "RUNNING" {
		t.Fatalf("instance status after only %s completed = %q, want RUNNING (Parallel must wait for both branches)", firstDept, mid.Status)
	}

	completeByDept(secondDept)
	pollInstance(t, admin, instanceID, func(d instanceDetail) bool {
		return d.Status == "COMPLETED"
	})
}

// resolveByNodeKey fetches each task's own node_key via GET /tasks/:id (not
// carried by instanceDetail) to identify which of the two Parallel branch
// tasks is which, regardless of listing order.
func resolveByNodeKey(t *testing.T, admin *apiClient, instanceID uuid.UUID, detail instanceDetail, wantA, wantB string) (a, b taskSummary) {
	t.Helper()
	type taskWithNode struct {
		ID      uuid.UUID `json:"id"`
		NodeKey string    `json:"node_key"`
	}
	for _, task := range detail.Tasks {
		var full taskWithNode
		resp := admin.do(http.MethodGet, "/api/v1/tasks/"+task.ID.String(), nil, &full)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /tasks/:id status = %d, want 200", resp.StatusCode)
		}
		switch full.NodeKey {
		case wantA:
			a = task
		case wantB:
			b = task
		}
	}
	if a.ID == uuid.Nil || b.ID == uuid.Nil {
		t.Fatalf("expected tasks at %q and %q, got instance detail %+v", wantA, wantB, detail.Tasks)
	}
	return a, b
}

// TestE2E_ParallelBranchCompletionOrder_WarehouseFirst and its _BillingFirst
// sibling below drive both completion orders — the review's finding #13 (the
// lastResultJSON race) was specifically about two branches completing at the
// same virtual-time instant; against live Temporal this asserts the same
// wait-for-both invariant regardless of which one lands first for real.
func TestE2E_ParallelBranchCompletionOrder_WarehouseFirst(t *testing.T) {
	runParallelBranchTest(t, "warehouse/pack", "billing/charge")
}

func TestE2E_ParallelBranchCompletionOrder_BillingFirst(t *testing.T) {
	runParallelBranchTest(t, "billing/charge", "warehouse/pack")
}

// exclusiveRevertPlan is a single department ("gate") whose one stage gates
// an Exclusive step: a forward branch (decision == "approved") to a second
// department ("approved"), and a back-edge revert branch (decision ==
// "rework") that regresses to the SAME gate stage — a real revisit, forcing
// a second workflow_task row at gate/review and exercising the visit-keyed
// stale-signal fix (item 0) against a live Temporal server, not just the
// simulated-clock tier.
func exclusiveRevertPlan(assigneeUserID uuid.UUID) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans: []*dsl.CompiledPlan{{
			Name:      "main",
			TaskQueue: "wf-e2e-test-queue",
			Departments: []dsl.DepartmentDef{
				{
					ID: "gate", Label: "Gate", IAMDepartmentID: uuid.New().String(),
					Stages: []dsl.StageDef{{Type: "approve", NodeID: "review", Role: "reviewer", DefaultAssignees: []string{assigneeUserID.String()}}},
				},
				{
					ID: "approved", Label: "Approved", IAMDepartmentID: uuid.New().String(),
					Stages: []dsl.StageDef{{Type: "approve", NodeID: "finish", Role: "reviewer", DefaultAssignees: []string{assigneeUserID.String()}}},
				},
			},
			Execution: dsl.ExecutionPlan{
				// Mirrors the real compiler's own shape for "single forward
				// branch + reverts" (definition_service's
				// handleSingleForwardWithReverts): the forward path is a
				// SEPARATE, later Steps entry, appended by continuing
				// compile-time traversal from the forward target, and the
				// forward branch therefore names no target of its own —
				// matching it just advances the plan to that step. The gate
				// is re-entered after every revert resolves, so a second
				// "rework" sends the work back a second time instead of
				// falling through to the forward path unread.
				Steps: []dsl.ExecutionStep{
					{Sequential: []string{"gate"}},
					{Exclusive: []dsl.ExclusiveBranch{
						{ConditionExpression: `decision == "approved"`},
						{RevertToDept: "gate", RevertToNodeID: "review", ConditionExpression: `decision == "rework"`},
					}},
					{Sequential: []string{"approved"}},
				},
			},
		}},
	}
}

// exclusiveRevertFixture starts one exclusiveRevertPlan instance against a
// real Temporal server and a real database.
type exclusiveRevertFixture struct {
	admin      *apiClient
	assignee   *apiClient
	instanceID uuid.UUID
}

func newExclusiveRevertFixture(t *testing.T) *exclusiveRevertFixture {
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
			CompiledPlanJSON: string(mustMarshal(t, exclusiveRevertPlan(assigneeUserID))),
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
		"business_key":        "e2e-exclusive-" + uuid.NewString(),
		"workflow_version_id": versionID,
	}, &startResp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /instances status = %d, want 201", resp.StatusCode)
	}
	return &exclusiveRevertFixture{admin: admin, assignee: assignee, instanceID: startResp.ID}
}

// completeGateVisit completes the one READY task and waits for the instance
// to settle at wantTasks total tasks, returning the detail at that point.
func completeGateVisit(t *testing.T, f *exclusiveRevertFixture, task taskSummary, decision string, wantTasks int) instanceDetail {
	t.Helper()
	resp := f.assignee.do(http.MethodPost, "/api/v1/tasks/"+task.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"` + decision + `"}`),
		"record_version": task.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete (%s) status = %d, want 202", decision, resp.StatusCode)
	}
	return pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) == wantTasks
	})
}

// newTaskSince returns the one task in detail that is not already in seen.
func newTaskSince(t *testing.T, detail instanceDetail, seen map[uuid.UUID]bool) taskSummary {
	t.Helper()
	for _, task := range detail.Tasks {
		if !seen[task.ID] {
			seen[task.ID] = true
			return task
		}
	}
	t.Fatalf("no new task appeared; got %+v", detail.Tasks)
	return taskSummary{}
}

// TestE2E_ExclusiveGatewaySecondRejectionRevertsAgain is item #25 against a
// real Temporal server: a rework loop must honour every rejection, not just
// the first. Before the gate was re-entered after a revert resolved, the
// second "rework" was never read — the instance advanced to the forward
// department as though it had been approved, which on a tender workflow is
// silent wrong-path execution rather than a missing convenience.
func TestE2E_ExclusiveGatewaySecondRejectionRevertsAgain(t *testing.T) {
	f := newExclusiveRevertFixture(t)

	detail := pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) > 0 && d.Tasks[0].Status == "READY"
	})
	seen := map[uuid.UUID]bool{}
	visit := newTaskSince(t, detail, seen)

	// Two rejections in a row: each must produce another gate/review visit.
	for i := 2; i <= 3; i++ {
		detail = completeGateVisit(t, f, visit, "rework", i)
		visit = newTaskSince(t, detail, seen)
	}

	// Only now, on the third visit, does an approval release the work.
	detail = completeGateVisit(t, f, visit, "approved", 4)
	forward := newTaskSince(t, detail, seen)

	resp := f.assignee.do(http.MethodPost, "/api/v1/tasks/"+forward.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{}`),
		"record_version": forward.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete (forward) status = %d, want 202", resp.StatusCode)
	}
	detail = pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "COMPLETED"
	})
	// Four tasks exactly: three gate visits and one forward department. A
	// fifth would mean the forward department ran twice — once from the
	// gateway branch and once from the continuation step.
	if len(detail.Tasks) != 4 {
		t.Errorf("instance ended with %d tasks, want 4 (gate ×3 + the forward department once): %+v", len(detail.Tasks), detail.Tasks)
	}
}

// TestE2E_ExclusiveGatewayRevertThenForward drives the revert branch first
// (a real revisit of "gate/review"), then the forward branch on the second
// visit, against a real Temporal server.
func TestE2E_ExclusiveGatewayRevertThenForward(t *testing.T) {
	f := newExclusiveRevertFixture(t)
	admin, assignee, instanceID := f.admin, f.assignee, f.instanceID

	detail := pollInstance(t, admin, instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) > 0 && d.Tasks[0].Status == "READY"
	})
	firstVisit := detail.Tasks[0]

	// decision=rework selects the revert branch — must regress to a fresh
	// gate/review task, not reuse or wipe the original.
	resp := assignee.do(http.MethodPost, "/api/v1/tasks/"+firstVisit.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"rework"}`),
		"record_version": firstVisit.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete (rework) status = %d, want 202", resp.StatusCode)
	}

	detail = pollInstance(t, admin, instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) == 2
	})
	var secondVisit taskSummary
	for _, task := range detail.Tasks {
		if task.ID != firstVisit.ID {
			secondVisit = task
		}
	}
	if secondVisit.ID == uuid.Nil {
		t.Fatalf("expected a distinct second gate/review task after the revert, got %+v", detail.Tasks)
	}

	// decision=approved on the revisit selects the forward branch.
	resp = assignee.do(http.MethodPost, "/api/v1/tasks/"+secondVisit.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"approved"}`),
		"record_version": secondVisit.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete (approved) status = %d, want 202", resp.StatusCode)
	}

	detail = pollInstance(t, admin, instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) == 3
	})
	var finalStage taskSummary
	for _, task := range detail.Tasks {
		if task.ID != firstVisit.ID && task.ID != secondVisit.ID {
			finalStage = task
		}
	}
	if finalStage.ID == uuid.Nil {
		t.Fatalf("expected the 'approved' department's task after the forward branch, got %+v", detail.Tasks)
	}
	resp = assignee.do(http.MethodPost, "/api/v1/tasks/"+finalStage.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{}`),
		"record_version": finalStage.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete (final) status = %d, want 202", resp.StatusCode)
	}
	pollInstance(t, admin, instanceID, func(d instanceDetail) bool {
		return d.Status == "COMPLETED"
	})
}
