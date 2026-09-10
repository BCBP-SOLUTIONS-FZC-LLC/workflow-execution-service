//go:build integration

// Package temporal_test exercises real multi-worker/multi-queue Temporal
// mechanics that neither the simulated-clock test/workflow tier nor the
// HTTP-driven test/e2e tier reaches: dynamic per-tenant queue registration,
// claim races against a live workflow execution, and DEGRADED/respawn
// against a real server's actual history. No HTTP layer — instantiate and
// drive instances via the real service layer directly, register real
// workers via a real client.Client, matching
// test/integration/postgres/outbox_lifecycle_test.go's non-HTTP style.
package temporal_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	temporaladapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

type fakeDefinitionClient struct {
	versionID uuid.UUID
	workflow  *port.CompiledWorkflow
}

func (f *fakeDefinitionClient) GetCompiledWorkflow(_ context.Context, _, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	if versionID != f.versionID {
		return nil, errNoFixture
	}
	return f.workflow, nil
}

var errNoFixture = fixtureError("no compiled workflow fixture for this version")

type fixtureError string

func (e fixtureError) Error() string { return string(e) }

type fakeEligibilityChecker struct{}

func (fakeEligibilityChecker) CheckEligibility(context.Context, uuid.UUID, uuid.UUID, string, uuid.UUID) (bool, error) {
	return true, nil
}

func (fakeEligibilityChecker) CheckEligibilityBatch(_ context.Context, reqs []port.EligibilityCheckRequest, _ uuid.UUID) ([]port.EligibilityResult, error) {
	results := make([]port.EligibilityResult, len(reqs))
	for i := range reqs {
		results[i] = port.EligibilityResult{Eligible: true}
	}
	return results, nil
}

// singleTaskPlan mirrors test/e2e's own fixture of the same name — the
// smallest realistic compiled collaboration, parameterized by TaskQueue so
// this tier can target an isolated queue.
func singleTaskPlan(assigneeUserID uuid.UUID, taskQueue string) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans: []*dsl.CompiledPlan{{
			Name:      "main",
			TaskQueue: taskQueue,
			Departments: []dsl.DepartmentDef{{
				ID:              "sales",
				Label:           "Sales",
				IAMDepartmentID: uuid.New().String(),
				Stages: []dsl.StageDef{{
					Type: "approve", NodeID: "review", Role: "reviewer",
					DefaultAssignees: []string{assigneeUserID.String()},
				}},
			}},
			Execution: dsl.ExecutionPlan{
				Steps: []dsl.ExecutionStep{{Sequential: []string{"sales"}}},
			},
		}},
	}
}

// registerRealWorker registers the real workflow interpreter and every real
// Activity against queueName, mirroring cmd/worker's own registration list
// (and test/e2e/helpers_test.go's own copy of it).
func registerRealWorker(t *testing.T, sdk client.Client, deps *temporaladapter.Deps, queueName string) worker.Worker {
	t.Helper()
	w := newWorkerBuilder(sdk, queueName)
	w.RegisterActivityWithOptions(deps.CreateTask, activity.RegisterOptions{Name: port.ActivityCreateTask})
	registerCommonActivities(w, deps)
	if err := w.Start(); err != nil {
		t.Fatalf("start worker on %q: %v", queueName, err)
	}
	t.Cleanup(w.Stop)
	return w
}

// newWorkerBuilder constructs a worker.Worker with the real workflow
// interpreter registered but no Activities yet — callers add
// ActivityCreateTask themselves (real or, for degraded_respawn_test.go's
// deliberate failure injection, wrapped) plus registerCommonActivities for
// everything else.
func newWorkerBuilder(sdk client.Client, queueName string) worker.Worker {
	w := worker.New(sdk, queueName, worker.Options{})
	w.RegisterWorkflowWithOptions(wfengine.Execute, sdkworkflow.RegisterOptions{Name: port.WorkflowTypeExecute})
	return w
}

// registerCommonActivities registers every real Activity except
// ActivityCreateTask, which callers register themselves (see
// newWorkerBuilder's own doc comment).
func registerCommonActivities(w worker.Worker, deps *temporaladapter.Deps) {
	w.RegisterActivityWithOptions(deps.GetCompiledPlan, activity.RegisterOptions{Name: port.ActivityGetCompiledPlan})
	w.RegisterActivityWithOptions(deps.UpdateInstanceNodes, activity.RegisterOptions{Name: port.ActivityUpdateInstanceNodes})
	w.RegisterActivityWithOptions(deps.ClaimAssignment, activity.RegisterOptions{Name: port.ActivityClaimAssignment})
	w.RegisterActivityWithOptions(deps.CompleteAssignment, activity.RegisterOptions{Name: port.ActivityCompleteAssignment})
	w.RegisterActivityWithOptions(deps.DeferTask, activity.RegisterOptions{Name: port.ActivityDeferTask})
	w.RegisterActivityWithOptions(deps.UpdateInstanceStatus, activity.RegisterOptions{Name: port.ActivityUpdateInstanceStatus})
	w.RegisterActivityWithOptions(deps.RecordForceRoute, activity.RegisterOptions{Name: port.ActivityRecordForceRoute})
	w.RegisterActivityWithOptions(deps.RecordSLAWarning, activity.RegisterOptions{Name: port.ActivityRecordSLAWarning})
	w.RegisterActivityWithOptions(deps.RecordSLABreach, activity.RegisterOptions{Name: port.ActivityRecordSLABreach})
	w.RegisterActivityWithOptions(deps.PauseInstance, activity.RegisterOptions{Name: port.ActivityPauseInstance})
	w.RegisterActivityWithOptions(deps.ResumeInstance, activity.RegisterOptions{Name: port.ActivityResumeInstance})
	w.RegisterActivityWithOptions(deps.CancelInstance, activity.RegisterOptions{Name: port.ActivityCancelInstance})
	w.RegisterActivityWithOptions(deps.ReassignAssignment, activity.RegisterOptions{Name: port.ActivityReassignAssignment})
	w.RegisterActivityWithOptions(deps.UpdateTaskStatus, activity.RegisterOptions{Name: port.ActivityUpdateTaskStatus})
}

// TestQueueTopology_DynamicRegistrationPicksUpIsolatedQueue mirrors
// cmd/worker/topology.go's own pollQueueTopologyOnce loop (a local copy, the
// same non-importable-private-function precedent
// cmd/connector-worker/e2e_test.go already sets for its own binary) against
// a real active_task_queues row: an instance started on an isolated queue
// with no worker yet listening must sit un-dispatched until the topology
// poll starts one, then proceed normally.
func TestQueueTopology_DynamicRegistrationPicksUpIsolatedQueue(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	sdk := fixtures.NewTestTemporalServer(t)

	tenantID := uuid.New()
	assigneeUserID := uuid.New()
	versionID := uuid.New()
	isolatedQueue := "wf-queue-" + tenantID.String()

	definitions := &fakeDefinitionClient{
		versionID: versionID,
		workflow: &port.CompiledWorkflow{
			WorkflowID: uuid.New(), VersionID: versionID, Status: "PUBLISHED", IsValid: true,
			CompiledPlanJSON: mustMarshalPlan(t, singleTaskPlan(assigneeUserID, isolatedQueue)),
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
	queues := postgres.NewActiveTaskQueueRepo(pool)
	temporal := temporalclient.New(sdk)

	deps := &temporaladapter.Deps{
		Instances: instances, Tasks: tasks, Assignments: assignments,
		Outbox: outboxRepo, Transactor: transactor, Validator: validator, Definitions: definitions,
	}

	// The default worker never listens on the isolated queue — only the
	// topology poll below should ever start one there.
	registerRealWorker(t, sdk, deps, "wf-default-test-queue")

	instanceService := &service.InstanceService{
		Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outboxRepo,
		Transactor: transactor, Temporal: temporal, Definitions: definitions, Eligibility: fakeEligibilityChecker{},
		Validator: validator,
	}

	inst, err := instanceService.Start(context.Background(), port.StartInstanceInput{
		TenantID: tenantID, WorkflowVersionID: versionID, BusinessKey: "topology-" + uuid.NewString(),
		StartedByUserID: assigneeUserID,
	})
	if err != nil {
		t.Fatalf("InstanceService.Start: %v", err)
	}

	// No worker on the isolated queue yet — the instance must not progress.
	time.Sleep(500 * time.Millisecond)
	rows, _, err := tasks.ListByInstance(context.Background(), tenantID, inst.ID, port.PageRequest{Limit: 10})
	if err != nil {
		t.Fatalf("ListByInstance: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("tasks = %+v, want none yet — no worker is listening on the isolated queue", rows)
	}

	// Seed active_task_queues and run one topology-poll cycle.
	if _, err := queues.Register(context.Background(), tenantID, isolatedQueue); err != nil {
		t.Fatalf("Register isolated queue: %v", err)
	}
	started := pollAndStartOnce(t, sdk, deps, queues, "wf-default-test-queue", map[string]bool{"wf-default-test-queue": true})
	if !started[isolatedQueue] {
		t.Fatalf("topology poll did not start a worker for %q", isolatedQueue)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		rows, _, err := tasks.ListByInstance(context.Background(), tenantID, inst.ID, port.PageRequest{Limit: 10})
		if err != nil {
			t.Fatalf("ListByInstance: %v", err)
		}
		if len(rows) > 0 && string(rows[0].Status) == "READY" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the isolated-queue worker to dispatch the task; rows=%+v", rows)
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Deregister the queue and poll again — additive-only design
	// (known_issues.md): the already-started worker keeps running, and no
	// second worker gets started for the same queue name.
	if err := queues.Deregister(context.Background(), isolatedQueue); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	startedAgain := pollAndStartOnce(t, sdk, deps, queues, "wf-default-test-queue", map[string]bool{"wf-default-test-queue": true, isolatedQueue: true})
	if len(startedAgain) != 0 {
		t.Errorf("second poll cycle started %v, want none (additive-only, and the row is gone)", startedAgain)
	}
}

// pollAndStartOnce is a local, minimal mirror of cmd/worker/topology.go's
// pollQueueTopologyOnce (that function is unexported in package main and not
// importable here) — lists active queues, starts a real worker for any not
// already in registered, and returns the set it started this call.
func pollAndStartOnce(t *testing.T, sdk client.Client, deps *temporaladapter.Deps, queues port.ActiveTaskQueueRepository, defaultQueue string, registered map[string]bool) map[string]bool {
	t.Helper()
	active, err := queues.ListActive(context.Background())
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	started := map[string]bool{}
	for _, q := range active {
		if q.QueueName == defaultQueue || registered[q.QueueName] {
			continue
		}
		registerRealWorker(t, sdk, deps, q.QueueName)
		registered[q.QueueName] = true
		started[q.QueueName] = true
	}
	return started
}

func mustMarshalPlan(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}
