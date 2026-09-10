//go:build e2e

// Package e2e_test exercises the real instantiate -> dispatch -> complete
// round trip through live Temporal, Postgres, and the actual HTTP router —
// every other integration tier in this repo fakes at least one of those
// three. Definition Service and the eligibility service are the only fakes
// here, since neither is this repo's own to stand up.
package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	temporaladapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// fakeDefinitionClient serves one hardcoded published plan, keyed by
// versionID — the only outbound dependency this test can't stand up for
// real (Definition Service is a separate repo).
type fakeDefinitionClient struct {
	versionID uuid.UUID
	workflow  *port.CompiledWorkflow
}

func (f *fakeDefinitionClient) GetCompiledWorkflow(_ context.Context, _, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	if versionID != f.versionID {
		return nil, fmt.Errorf("no compiled workflow fixture for version %s", versionID)
	}
	return f.workflow, nil
}

// fakeEligibilityChecker always returns eligible — this test only asserts
// the instantiate/dispatch/complete mechanics, not eligibility policy.
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

// singleTaskPlan is the smallest realistic compiled collaboration: one pool,
// one department, one human-actionable stage — assigneeUserID is wired in
// via the department's own DefaultAssignees, so no OverrideMap is needed at
// instantiation time.
func singleTaskPlan(assigneeUserID uuid.UUID) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans: []*dsl.CompiledPlan{{
			Name:      "main",
			TaskQueue: "wf-e2e-test-queue",
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

// startWorker registers the real workflow interpreter and every real
// Activity body against queueName, mirroring cmd/worker's own registration
// list exactly.
func startWorker(t *testing.T, sdk client.Client, deps *temporaladapter.Deps, queueName string) {
	t.Helper()
	w := worker.New(sdk, queueName, worker.Options{})
	w.RegisterWorkflowWithOptions(wfengine.Execute, sdkworkflow.RegisterOptions{Name: port.WorkflowTypeExecute})
	w.RegisterActivityWithOptions(deps.GetCompiledPlan, activity.RegisterOptions{Name: port.ActivityGetCompiledPlan})
	w.RegisterActivityWithOptions(deps.CreateTask, activity.RegisterOptions{Name: port.ActivityCreateTask})
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
	if err := w.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(w.Stop)
}

// alwaysHealthy satisfies both httpadapter.Pinger and httpadapter.DBPinger —
// this test never exercises /readyz, but the router needs non-nil pingers
// to construct.
type alwaysHealthy struct{}

func (alwaysHealthy) Ping(context.Context) error { return nil }
func (alwaysHealthy) Health(context.Context) httpadapter.DBHealth {
	return httpadapter.DBHealth{Healthy: true}
}

// buildRouter wires every service the test's own flow (start, get, complete)
// touches against a real Postgres pool and a real Temporal client, with
// Definition Service and eligibility faked.
func buildRouter(t *testing.T, pool *pgcommon.Pool, sdk client.Client, definitions *fakeDefinitionClient) *httpadapter.Router {
	t.Helper()

	instances := postgres.NewInstanceRepo(pool)
	tasks := postgres.NewTaskRepo(pool)
	assignments := postgres.NewTaskAssignmentRepo(pool)
	outboxRepo := postgres.NewOutboxRepo(pool)
	overrides := postgres.NewAssigneeOverrideRepo(pool)
	processedEvents := postgres.NewProcessedEventRepo(pool)
	recency := postgres.NewRecencyGuardRepo(pool)
	queues := postgres.NewActiveTaskQueueRepo(pool)
	transactor := postgres.NewTransactor(pool)

	validator, err := eventbus.NewSchemaValidator()
	if err != nil {
		t.Fatalf("new schema validator: %v", err)
	}
	temporal := temporalclient.New(sdk)
	eligibility := fakeEligibilityChecker{}

	instanceService := &service.InstanceService{
		Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outboxRepo,
		Transactor: transactor, Temporal: temporal, Definitions: definitions, Eligibility: eligibility,
		Validator: validator,
	}
	taskService := &service.TaskService{
		Instances: instances, Tasks: tasks, Assignments: assignments, Overrides: overrides,
		Temporal: temporal, Eligibility: eligibility, Definitions: definitions,
	}
	tenantLifecycleReconciler := &service.TenantLifecycleReconciler{
		Instances: instances, Tasks: tasks, Assignments: assignments, Queues: queues, Outbox: outboxRepo,
		Transactor: transactor, Temporal: temporal, Validator: validator,
	}
	userSafetyNetReconciler := &service.UserSafetyNetReconciler{Assignments: assignments}
	oooAvailabilityReconciler := &service.OOOAvailabilityReconciler{
		Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal,
	}
	delegationReconciler := &service.DelegationReconciler{
		Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outboxRepo,
		Transactor: transactor, Temporal: temporal, Definitions: definitions, Eligibility: eligibility,
		Validator: validator,
	}

	h := handler.New(handler.Services{
		Tasks: taskService, Instances: instanceService, Eligibility: eligibility,
		ProcessedEvents: processedEvents, Recency: recency,
		TenantLifecycle: tenantLifecycleReconciler, UserSafetyNet: userSafetyNetReconciler, OOOAvailability: oooAvailabilityReconciler,
		Delegation: delegationReconciler,
	})
	return httpadapter.NewRouter(httpadapter.RouterConfig{
		GinConfig: gincommon.Config{ServiceName: "execution-service-e2e-test", BuildVersion: "test"},
		AppEnv:    "dev",
		Handler:   h,
		DB:        alwaysHealthy{}, Cache: alwaysHealthy{}, Temporal: alwaysHealthy{},
	})
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

const pollTimeout = 60 * time.Second
const pollInterval = 250 * time.Millisecond
