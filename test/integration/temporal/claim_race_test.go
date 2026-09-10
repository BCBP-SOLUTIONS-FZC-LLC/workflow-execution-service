//go:build integration

package temporal_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	temporaladapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// multiAssigneeTaskPlan compiles to AssigneeMode "all" — CreateTaskActivity
// (task.go) derives that from len(stage.DefaultAssignees) > 1 — the shape
// claim lead/non-lead rejection needs.
func multiAssigneeTaskPlan(userA, userB uuid.UUID, taskQueue string) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans: []*dsl.CompiledPlan{{
			Name:      "main",
			TaskQueue: taskQueue,
			Departments: []dsl.DepartmentDef{{
				ID: "sales", Label: "Sales", IAMDepartmentID: uuid.New().String(),
				Stages: []dsl.StageDef{{
					Type: "approve", NodeID: "review", Role: "reviewer",
					DefaultAssignees: []string{userA.String(), userB.String()},
				}},
			}},
			Execution: dsl.ExecutionPlan{
				Steps: []dsl.ExecutionStep{{Sequential: []string{"sales"}}},
			},
		}},
	}
}

// TestClaim_SecondLeadAttemptRejectedAgainstLiveWorkflow drives a real
// multi-assignee task through a real Temporal execution: the first Claim
// must succeed (becomes lead), the second — a genuinely concurrent DB-level
// race, not a fake — must be rejected with ErrTaskAlreadyClaimed.
func TestClaim_SecondLeadAttemptRejectedAgainstLiveWorkflow(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	sdk := fixtures.NewTestTemporalServer(t)

	tenantID := uuid.New()
	userA, userB := uuid.New(), uuid.New()
	versionID := uuid.New()
	queueName := "wf-claim-race-test-queue"

	definitions := &fakeDefinitionClient{
		versionID: versionID,
		workflow: &port.CompiledWorkflow{
			WorkflowID: uuid.New(), VersionID: versionID, Status: "PUBLISHED", IsValid: true,
			CompiledPlanJSON: mustMarshalPlan(t, multiAssigneeTaskPlan(userA, userB, queueName)),
		},
	}

	validator, err := eventbus.NewSchemaValidator()
	if err != nil {
		t.Fatalf("new schema validator: %v", err)
	}
	instances := postgres.NewInstanceRepo(pool)
	tasks := postgres.NewTaskRepo(pool)
	assignments := postgres.NewTaskAssignmentRepo(pool)
	outboxRepo := postgres.NewOutboxRepo(pool)
	transactor := postgres.NewTransactor(pool)
	temporal := temporalclient.New(sdk)

	deps := &temporaladapter.Deps{
		Instances: instances, Tasks: tasks, Assignments: assignments,
		Outbox: outboxRepo, Transactor: transactor, Validator: validator, Definitions: definitions,
	}
	registerRealWorker(t, sdk, deps, queueName)

	instanceService := &service.InstanceService{
		Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outboxRepo,
		Transactor: transactor, Temporal: temporal, Definitions: definitions, Eligibility: fakeEligibilityChecker{},
		Validator: validator,
	}
	taskService := &service.TaskService{
		Instances: instances, Tasks: tasks, Assignments: assignments,
		Temporal: temporal, Eligibility: fakeEligibilityChecker{}, Definitions: definitions,
	}

	inst, err := instanceService.Start(context.Background(), port.StartInstanceInput{
		TenantID: tenantID, WorkflowVersionID: versionID, BusinessKey: "claim-race-" + uuid.NewString(),
		StartedByUserID: userA,
	})
	if err != nil {
		t.Fatalf("InstanceService.Start: %v", err)
	}

	var taskID uuid.UUID
	deadline := time.Now().Add(30 * time.Second)
	for {
		rows, _, err := tasks.ListByInstance(context.Background(), tenantID, inst.ID, port.PageRequest{Limit: 10})
		if err != nil {
			t.Fatalf("ListByInstance: %v", err)
		}
		if len(rows) > 0 && string(rows[0].Status) == "READY" {
			taskID = rows[0].ID
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the task to reach READY")
		}
		time.Sleep(250 * time.Millisecond)
	}

	// A genuine race: both claims fire concurrently against the same
	// starting record_version — exactly the "second gets rejected against a
	// live workflow execution" scenario item 9.3 calls for, not a sequential
	// call with a deliberately stale version.
	var wg sync.WaitGroup
	results := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, results[0] = taskService.Claim(context.Background(), tenantID, taskID, userA, 1)
	}()
	go func() {
		defer wg.Done()
		_, results[1] = taskService.Claim(context.Background(), tenantID, taskID, userB, 1)
	}()
	wg.Wait()

	successes, rejections := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case err == port.ErrTaskAlreadyClaimed || err == port.ErrRecordVersionConflict:
			rejections++
		default:
			t.Fatalf("unexpected Claim error: %v", err)
		}
	}
	if successes != 1 || rejections != 1 {
		t.Fatalf("got %d successes and %d rejections, want exactly 1 of each (results=%v) — a corrupted double-claim or a double-rejection both indicate the lead CAS isn't race-safe", successes, rejections, results)
	}
}
