//go:build e2e

package e2e_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	temporaladapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// twoStagePlan is a two-department sequential collaboration — the smallest
// shape that gives force-forward/force-back a real second node to route
// between (singleTaskPlan has only one).
func twoStagePlan(assigneeUserID uuid.UUID) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans: []*dsl.CompiledPlan{{
			Name:      "main",
			TaskQueue: "wf-e2e-test-queue",
			Departments: []dsl.DepartmentDef{
				{
					ID: "sales", Label: "Sales", IAMDepartmentID: uuid.New().String(),
					Stages: []dsl.StageDef{{Type: "approve", NodeID: "review", Role: "reviewer", DefaultAssignees: []string{assigneeUserID.String()}}},
				},
				{
					ID: "shipping", Label: "Shipping", IAMDepartmentID: uuid.New().String(),
					Stages: []dsl.StageDef{{Type: "approve", NodeID: "prep", Role: "reviewer", DefaultAssignees: []string{assigneeUserID.String()}}},
				},
			},
			Execution: dsl.ExecutionPlan{
				Steps: []dsl.ExecutionStep{{Sequential: []string{"sales", "shipping"}}},
			},
		}},
	}
}

// adminOverrideFixture starts a fresh two-stage instance and polls its first
// task to READY — the common starting point every test below builds on.
type adminOverrideFixture struct {
	admin        *apiClient
	assignee     *apiClient
	assigneeID   uuid.UUID
	instanceID   uuid.UUID
	firstTask    taskSummary
	instanceVers int64
}

func newAdminOverrideFixture(t *testing.T) *adminOverrideFixture {
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
			CompiledPlanJSON: string(mustMarshal(t, twoStagePlan(assigneeUserID))),
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
		"business_key":        "e2e-override-" + uuid.NewString(),
		"workflow_version_id": versionID,
	}, &startResp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /instances status = %d, want 201", resp.StatusCode)
	}

	detail := pollInstance(t, admin, startResp.ID, func(d instanceDetail) bool {
		return len(d.Tasks) > 0 && d.Tasks[0].Status == "READY"
	})
	return &adminOverrideFixture{
		admin: admin, assignee: assignee, assigneeID: assigneeUserID,
		instanceID: startResp.ID, firstTask: detail.Tasks[0], instanceVers: detail.RecordVersion,
	}
}

// TestE2E_InstanceForceForward drives POST /instances/:id/force-forward
// against a real Temporal server, bypassing task 1 (still pending, never
// completed) straight to task 2.
//
// Task 1 must end SUPERSEDED, per LLD §3.4's READY/IN_PROGRESS -> SUPERSEDED
// row: a force-forward-bypassed task is neither a failure nor the assignee's
// own send-back, so it gets its own terminal status rather than being left
// open and claimable forever. The base (non-Parallel) path resolves the
// bypassed node from the live pending map the same way the Parallel/DEGRADED
// path always has.
func TestE2E_InstanceForceForward(t *testing.T) {
	f := newAdminOverrideFixture(t)

	resp := f.admin.do(http.MethodPost, "/api/v1/instances/"+f.instanceID.String()+"/force-forward", map[string]any{
		"target_node_key": "shipping/prep", "record_version": f.instanceVers,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST .../force-forward status = %d, want 202", resp.StatusCode)
	}

	detail := pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) == 2
	})
	var first, second *taskSummary
	for i := range detail.Tasks {
		if detail.Tasks[i].ID == f.firstTask.ID {
			first = &detail.Tasks[i]
		} else {
			second = &detail.Tasks[i]
		}
	}
	if first == nil || second == nil {
		t.Fatalf("expected the original task plus a new one, got %+v", detail.Tasks)
	}
	if first.Status != "SUPERSEDED" {
		t.Errorf("task 1 status = %q, want SUPERSEDED (bypassed by force-forward, LLD §3.4)", first.Status)
	}
	if second.Status != "READY" {
		t.Errorf("task 2 status = %q, want READY", second.Status)
	}

	// The workflow itself must have genuinely moved on regardless: task 2
	// completes the instance without task 1 ever needing to.
	resp = f.assignee.do(http.MethodPost, "/api/v1/tasks/"+second.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{}`),
		"record_version": second.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete (task 2) status = %d, want 202", resp.StatusCode)
	}
	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "COMPLETED"
	})
}

// TestE2E_InstanceForceBack drives task 1 to completion, then force-back
// from stage 2 — task 1 must be regressed and re-run (a genuine second
// visit, exercising the deterministic-task-ID + stale-signal fix together),
// and the plan must still continue on to stage 2 again afterward.
func TestE2E_InstanceForceBack(t *testing.T) {
	f := newAdminOverrideFixture(t)

	resp := f.assignee.do(http.MethodPost, "/api/v1/tasks/"+f.firstTask.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"approved"}`),
		"record_version": f.firstTask.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete status = %d, want 202", resp.StatusCode)
	}

	detail := pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) == 2
	})
	// Find stage 2's task (the one that isn't the original).
	var stage2 taskSummary
	for _, task := range detail.Tasks {
		if task.ID != f.firstTask.ID {
			stage2 = task
		}
	}
	if stage2.ID == uuid.Nil {
		t.Fatalf("expected stage 2's task to exist after stage 1 completed, got %+v", detail.Tasks)
	}
	seen := map[uuid.UUID]bool{f.firstTask.ID: true, stage2.ID: true}

	resp = f.admin.do(http.MethodPost, "/api/v1/instances/"+f.instanceID.String()+"/force-back", map[string]any{
		"record_version": detail.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST .../force-back status = %d, want 202", resp.StatusCode)
	}

	// Stage 1 must be regressed: a new task at "sales/review", distinct from
	// the original (deterministic-task-ID's VisitCount-derived ID). Poll on
	// the 3rd task row appearing, which is the signal that the regression
	// actually happened.
	detail = pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) == 3
	})
	var regressed taskSummary
	for _, task := range detail.Tasks {
		if !seen[task.ID] {
			regressed = task
		}
	}
	if regressed.ID == uuid.Nil {
		t.Fatalf("expected a new (3rd) task row after force-back, got %+v", detail.Tasks)
	}
	seen[regressed.ID] = true

	// Stage 2's original attempt was abandoned by force-back's cancelRun, so
	// it must be closed out SUPERSEDED for the same reason a force-forward-
	// bypassed task is — not left open and claimable alongside its own
	// replacement.
	for _, task := range detail.Tasks {
		if task.ID == stage2.ID && task.Status != "SUPERSEDED" {
			t.Errorf("stage 2's abandoned task status = %q, want SUPERSEDED (regressed by force-back)", task.Status)
		}
	}

	resp = f.assignee.do(http.MethodPost, "/api/v1/tasks/"+regressed.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"approved"}`),
		"record_version": regressed.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete (regressed stage 1) status = %d, want 202", resp.StatusCode)
	}

	// The plan must reach stage 2 again (a genuinely new, 4th task row) and
	// be completable to COMPLETED.
	detail = pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) == 4
	})
	var finalStage taskSummary
	for _, task := range detail.Tasks {
		if !seen[task.ID] {
			finalStage = task
		}
	}
	if finalStage.ID == uuid.Nil {
		t.Fatalf("expected a new (4th) task row once stage 1 re-completed, got %+v", detail.Tasks)
	}
	resp = f.assignee.do(http.MethodPost, "/api/v1/tasks/"+finalStage.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{}`),
		"record_version": finalStage.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete (final stage) status = %d, want 202", resp.StatusCode)
	}
	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return d.Status == "COMPLETED"
	})
}

// TestE2E_TaskReassign drives POST /tasks/:id/reassign: the new assignee must
// be able to claim/complete the task afterward, and the original assignee's
// assignment must be vacated.
func TestE2E_TaskReassign(t *testing.T) {
	f := newAdminOverrideFixture(t)
	newUserID := uuid.New()

	resp := f.admin.do(http.MethodPost, "/api/v1/tasks/"+f.firstTask.ID.String()+"/reassign", map[string]any{
		"new_user_id": newUserID, "record_version": f.firstTask.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/reassign status = %d, want 202", resp.StatusCode)
	}

	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) > 0 && d.Tasks[0].RecordVersion > f.firstTask.RecordVersion
	})

	var taskDetail taskSummary
	resp = f.admin.do(http.MethodGet, "/api/v1/tasks/"+f.firstTask.ID.String(), nil, &taskDetail)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /tasks/:id status = %d, want 200", resp.StatusCode)
	}

	// The original assignee must no longer be able to act on it — reassign
	// vacated their assignment (checked before the new assignee completes
	// it, since /complete is the only real probe for single-mode tasks and
	// a terminal task would trivially reject anyone afterward).
	resp = f.assignee.do(http.MethodPost, "/api/v1/tasks/"+f.firstTask.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"approved"}`),
		"record_version": taskDetail.RecordVersion,
	}, nil)
	if resp.StatusCode == http.StatusAccepted {
		t.Errorf("original assignee could still complete the reassigned task; want rejected")
	}

	newAssignee := &apiClient{t: t, http: f.admin.http, baseURL: f.admin.baseURL, tenantID: f.admin.tenantID, userID: newUserID}
	resp = newAssignee.do(http.MethodPost, "/api/v1/tasks/"+f.firstTask.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"approved"}`),
		"record_version": taskDetail.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete (reassigned) status = %d, want 202 (new assignee must be able to complete it)", resp.StatusCode)
	}
}

// TestE2E_ReassignConcurrentRace drives finding #3 of the 2026-09-07 review:
// two reassign calls racing with the same starting record_version. Neither
// HTTP call is rejected synchronously (both signal instance-reassign before
// either commits — task_service.go's Reassign only pre-checks against a
// stale snapshot); the real guard is ReassignAssignmentActivity's own
// BumpRecordVersion CAS (assignment.go), processed one at a time by the
// workflow's single-threaded interpreter. Exactly one of the two candidate
// assignees must end up the sole active assignee — never both, never
// neither.
func TestE2E_ReassignConcurrentRace(t *testing.T) {
	f := newAdminOverrideFixture(t)
	candidateA := uuid.New()
	candidateB := uuid.New()

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		resp := f.admin.do(http.MethodPost, "/api/v1/tasks/"+f.firstTask.ID.String()+"/reassign", map[string]any{
			"new_user_id": candidateA, "record_version": f.firstTask.RecordVersion,
		}, nil)
		statuses[0] = resp.StatusCode
	}()
	go func() {
		defer wg.Done()
		resp := f.admin.do(http.MethodPost, "/api/v1/tasks/"+f.firstTask.ID.String()+"/reassign", map[string]any{
			"new_user_id": candidateB, "record_version": f.firstTask.RecordVersion,
		}, nil)
		statuses[1] = resp.StatusCode
	}()
	wg.Wait()
	if statuses[0] != http.StatusAccepted || statuses[1] != http.StatusAccepted {
		t.Fatalf("both concurrent reassign calls should be synchronously accepted (202); got %v", statuses)
	}

	// Poll until the task's record_version has moved at least once, then
	// give the loser's (rejected, non-retryable) activity attempt a moment
	// to settle before inspecting the final assignment set.
	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) > 0 && d.Tasks[0].RecordVersion > f.firstTask.RecordVersion
	})

	winnerIsA := &apiClient{t: t, http: f.admin.http, baseURL: f.admin.baseURL, tenantID: f.admin.tenantID, userID: candidateA}
	winnerIsB := &apiClient{t: t, http: f.admin.http, baseURL: f.admin.baseURL, tenantID: f.admin.tenantID, userID: candidateB}

	// singleTaskPlan-style tasks are AssigneeMode "single" — /claim only
	// applies to "all" mode (TaskService.Claim's own ErrClaimNotApplicable
	// gate), so /complete is the real probe for "is this user the active
	// assignee." Try A first: if A is the winner this actually finishes the
	// task (and the instance, a one-department plan) — fine, since this is
	// the last assertion in the test either way.
	aCanComplete := f.assigneeCanComplete(winnerIsA, f.firstTask.ID)
	bCanComplete := f.assigneeCanComplete(winnerIsB, f.firstTask.ID)
	if aCanComplete == bCanComplete {
		t.Fatalf("exactly one candidate must be the active assignee; A completable=%v, B completable=%v (both or neither means a corrupted double-assignment)", aCanComplete, bCanComplete)
	}
}

// assigneeCanComplete probes whether client is the task's current active
// assignee by actually trying to complete it, fetching the task's current
// record_version fresh each time (the loser's attempt is expected to fail,
// either as ErrNotAssignee or a stale-version/already-terminal conflict —
// either way, "not completable" is the correct signal).
func (f *adminOverrideFixture) assigneeCanComplete(client *apiClient, taskID uuid.UUID) bool {
	var detail taskSummary
	resp := f.admin.do(http.MethodGet, "/api/v1/tasks/"+taskID.String(), nil, &detail)
	if resp.StatusCode != http.StatusOK {
		return false
	}
	resp = client.do(http.MethodPost, "/api/v1/tasks/"+taskID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"approved"}`),
		"record_version": detail.RecordVersion,
	}, nil)
	return resp.StatusCode == http.StatusAccepted
}

// TestE2E_NodeOverride drives POST /instances/:id/nodes/:node/override — a
// real e2e run here caught a genuine routing bug: a compiled NodeKey is
// always "<deptID>/<stageID>" (dsl's stageNodeKey), but gin's default
// routing 404s on a literal "/" inside a single :node path segment (it
// either splits into an extra segment if unescaped, or gets decoded back to
// "/" before ever reaching the router tree if sent as a plain %2F without
// UseRawPath). Fixed in router.go by setting UseRawPath — this test is what
// actually exercises that fix; callers must send the node key's "/"
// percent-encoded (%2F), which is why this path is built by hand below
// rather than through apiClient's usual raw string concatenation.
func TestE2E_NodeOverride(t *testing.T) {
	f := newAdminOverrideFixture(t)
	newUserID := uuid.New()

	overridePath := "/api/v1/instances/" + f.instanceID.String() + "/nodes/sales%2Freview/override"
	resp := f.admin.do(http.MethodPost, overridePath, map[string]any{
		"new_user_id": newUserID, "reason": "OOO", "record_version": f.firstTask.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST .../nodes/sales%%2Freview/override status = %d, want 200 (persist-then-signal, same synchronous shape as Terminate)", resp.StatusCode)
	}

	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool {
		return len(d.Tasks) > 0 && d.Tasks[0].RecordVersion > f.firstTask.RecordVersion
	})

	var taskDetail taskSummary
	resp = f.admin.do(http.MethodGet, "/api/v1/tasks/"+f.firstTask.ID.String(), nil, &taskDetail)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /tasks/:id status = %d, want 200", resp.StatusCode)
	}

	newAssignee := &apiClient{t: t, http: f.admin.http, baseURL: f.admin.baseURL, tenantID: f.admin.tenantID, userID: newUserID}
	resp = newAssignee.do(http.MethodPost, "/api/v1/tasks/"+f.firstTask.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"approved"}`),
		"record_version": taskDetail.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /tasks/:id/complete (overridden assignee) status = %d, want 202", resp.StatusCode)
	}
}
