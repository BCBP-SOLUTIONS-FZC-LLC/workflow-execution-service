//go:build integration

package temporal_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	temporaladapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// adminSignalWire mirrors internal/workflow's unexported adminSignal payload
// field-for-field, the same test-local-mirror convention
// test/workflow/helpers_test.go already uses.
type adminSignalWire struct {
	AdminUserID   string
	Reason        string
	Initiator     string
	TargetDeptID  string
	TargetNodeKey string
	RecordVersion int64
}

// parallelPlanTwoDepts is a Parallel gateway with two one-stage departments,
// parameterized by TaskQueue.
func parallelPlanTwoDepts(assigneeUserID uuid.UUID, taskQueue string) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans: []*dsl.CompiledPlan{{
			Name:      "main",
			TaskQueue: taskQueue,
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

// degradedFixture is the common state every DEGRADED-tier test below builds
// on: a real Parallel-branch instance, warehouse completed, billing's
// CreateTask deliberately failed once (the one injected failure point —
// see setupDegradedInstance's own doc comment), instance confirmed DEGRADED.
type degradedFixture struct {
	tenantID        uuid.UUID
	assigneeUserID  uuid.UUID
	inst            *port.Instance
	instances       port.InstanceRepository
	tasks           port.TaskRepository
	taskService     *service.TaskService
	temporalClient  port.TemporalClient
	billingAttempts *atomic.Int32
}

// setupDegradedInstance deliberately wraps only ActivityCreateTask (the
// first call for "billing/charge" returns a real non-retryable Temporal
// ApplicationError, every later call — a respawn included — goes to the
// real deps.CreateTask unchanged); every other Activity, the workflow
// interpreter, and the Temporal server are fully real. That's the one
// injected failure point needed to force DEGRADED deterministically —
// everything downstream (the DEGRADED transition, force-back, respawn, real
// Temporal history/replay) is exercised for real, which is this integration
// tier's actual point (LLD §7.2 tests #4/#5); CreateTaskActivity's own
// correctness against real Postgres is already covered elsewhere
// (test/integration/postgres).
func setupDegradedInstance(t *testing.T) *degradedFixture {
	t.Helper()
	pool := fixtures.NewTestPool(t)
	sdk := fixtures.NewTestTemporalServer(t)

	tenantID := uuid.New()
	assigneeUserID := uuid.New()
	versionID := uuid.New()
	queueName := "wf-degraded-test-queue"

	definitions := &fakeDefinitionClient{
		versionID: versionID,
		workflow: &port.CompiledWorkflow{
			WorkflowID: uuid.New(), VersionID: versionID, Status: "PUBLISHED", IsValid: true,
			CompiledPlanJSON: mustMarshalPlan(t, parallelPlanTwoDepts(assigneeUserID, queueName)),
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
	temporalClient := temporalclient.New(sdk)

	deps := &temporaladapter.Deps{
		Instances: instances, Tasks: tasks, Assignments: assignments,
		Outbox: outboxRepo, Transactor: transactor, Validator: validator, Definitions: definitions,
	}

	var billingAttempts atomic.Int32
	wrappedCreateTask := func(ctx context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		if in.NodeKey == "billing/charge" && billingAttempts.Add(1) == 1 {
			return port.CreateTaskOutput{}, temporal.NewApplicationError("simulated downstream failure", "ValidationError")
		}
		return deps.CreateTask(ctx, in)
	}

	w := newWorkerBuilder(sdk, queueName)
	w.RegisterActivityWithOptions(wrappedCreateTask, activity.RegisterOptions{Name: port.ActivityCreateTask})
	registerCommonActivities(w, deps)
	if err := w.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(w.Stop)

	instanceService := &service.InstanceService{
		Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outboxRepo,
		Transactor: transactor, Temporal: temporalClient, Definitions: definitions, Eligibility: fakeEligibilityChecker{},
		Validator: validator,
	}
	taskService := &service.TaskService{
		Instances: instances, Tasks: tasks, Assignments: assignments,
		Temporal: temporalClient, Eligibility: fakeEligibilityChecker{}, Definitions: definitions,
	}

	inst, err := instanceService.Start(context.Background(), port.StartInstanceInput{
		TenantID: tenantID, WorkflowVersionID: versionID, BusinessKey: "degraded-" + uuid.NewString(),
		StartedByUserID: assigneeUserID,
	})
	if err != nil {
		t.Fatalf("InstanceService.Start: %v", err)
	}

	// runParallel (degraded.go) only calls enterDegraded once EVERY branch
	// has settled — billing settles (failed) almost immediately, but
	// warehouse's own branch is still blocked on its pending human task, so
	// the instance stays RUNNING until warehouse also settles. Complete
	// warehouse first; DEGRADED only becomes observable once that happens.
	var warehouseTask *domain.Task
	deadline := time.Now().Add(30 * time.Second)
	for {
		rows, _, err := tasks.ListByInstance(context.Background(), tenantID, inst.ID, port.PageRequest{Limit: 10})
		if err != nil {
			t.Fatalf("ListByInstance: %v", err)
		}
		for _, r := range rows {
			if r.NodeKey == "warehouse/pack" && r.Status == domain.TaskStatusReady {
				warehouseTask = r
			}
		}
		if warehouseTask != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for warehouse/pack to reach READY; rows=%+v", rows)
		}
		time.Sleep(250 * time.Millisecond)
	}

	if _, err := taskService.Complete(context.Background(), tenantID, warehouseTask.ID, assigneeUserID, []byte(`{}`), warehouseTask.RecordVersion); err != nil {
		t.Fatalf("complete warehouse task: %v", err)
	}

	waitForInstanceStatus(t, instances, tenantID, inst.ID, domain.InstanceStatusDegraded)

	return &degradedFixture{
		tenantID: tenantID, assigneeUserID: assigneeUserID, inst: inst,
		instances: instances, tasks: tasks, taskService: taskService,
		temporalClient: temporalClient, billingAttempts: &billingAttempts,
	}
}

// TestDegraded_ParallelBranchFailsRespawnsAndCompletes drives a real
// force-back respawn of the failed branch through to instance completion —
// LLD §7.2 test #4.
func TestDegraded_ParallelBranchFailsRespawnsAndCompletes(t *testing.T) {
	f := setupDegradedInstance(t)

	inst2, err := f.instances.GetByID(context.Background(), f.tenantID, f.inst.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}

	// Force-back respawns the failed billing branch — this time CreateTask
	// succeeds (billingAttempts > 1).
	if err := f.temporalClient.SignalWorkflow(context.Background(), f.inst.TemporalWorkflowID, f.inst.ID, port.SignalInstanceForceBack, adminSignalWire{
		AdminUserID: f.assigneeUserID.String(), TargetDeptID: "billing", RecordVersion: inst2.RecordVersion,
	}); err != nil {
		t.Fatalf("signal instance-force-back: %v", err)
	}

	var billingTask *domain.Task
	deadline := time.Now().Add(30 * time.Second)
	for {
		rows, _, err := f.tasks.ListByInstance(context.Background(), f.tenantID, f.inst.ID, port.PageRequest{Limit: 10})
		if err != nil {
			t.Fatalf("ListByInstance: %v", err)
		}
		for _, r := range rows {
			if r.NodeKey == "billing/charge" && r.Status == domain.TaskStatusReady {
				billingTask = r
			}
		}
		if billingTask != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the respawned billing/charge task; rows=%+v", rows)
		}
		time.Sleep(250 * time.Millisecond)
	}

	// The instance stays DEGRADED until the respawned branch itself settles
	// (enterDegraded's own loop condition, degraded.go) — a re-created task
	// existing isn't enough on its own; confirm it explicitly before moving
	// on, rather than assuming.
	inst3, err := f.instances.GetByID(context.Background(), f.tenantID, f.inst.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if inst3.Status != domain.InstanceStatusDegraded {
		t.Fatalf("instance status right after respawn = %v, want still DEGRADED (only settles once the respawned branch completes)", inst3.Status)
	}

	if _, err := f.taskService.Complete(context.Background(), f.tenantID, billingTask.ID, f.assigneeUserID, []byte(`{}`), billingTask.RecordVersion); err != nil {
		t.Fatalf("complete respawned billing task: %v", err)
	}
	waitForInstanceStatus(t, f.instances, f.tenantID, f.inst.ID, domain.InstanceStatusCompleted)

	if got := f.billingAttempts.Load(); got != 2 {
		t.Errorf("billing CreateTaskActivity attempts = %d, want 2 (initial failure + respawn)", got)
	}
}

// TestDegraded_RejectsTenantStatePauseSignal drives LLD §7.2 test #5: a
// DEGRADED instance is never pausable — instance-pause requires RUNNING
// (signalPreconditions, signals.go), and runSignalRouter validates every
// admin signal BEFORE it ever reaches enterDegraded's own Selector (which
// registers no case for instance-pause at all). Sends the exact signal
// TenantLifecycleReconciler would for a tenant-suspend sweep, directly via
// SignalWorkflow (no HTTP layer, matching this tier's own design) — the
// instance must remain DEGRADED, never PAUSED.
func TestDegraded_RejectsTenantStatePauseSignal(t *testing.T) {
	f := setupDegradedInstance(t)

	if err := f.temporalClient.SignalWorkflow(context.Background(), f.inst.TemporalWorkflowID, f.inst.ID, port.SignalInstancePause, adminSignalWire{
		AdminUserID: f.assigneeUserID.String(), Initiator: "tenant_state", RecordVersion: 1,
	}); err != nil {
		t.Fatalf("signal instance-pause: %v", err)
	}

	// Give the signal a moment to actually reach the workflow and be
	// processed (or, correctly, dropped) before asserting it had no effect.
	time.Sleep(2 * time.Second)
	inst2, err := f.instances.GetByID(context.Background(), f.tenantID, f.inst.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if inst2.Status != domain.InstanceStatusDegraded {
		t.Fatalf("instance status after instance-pause while DEGRADED = %v, want still DEGRADED (the signal must be rejected, not silently applied)", inst2.Status)
	}
}

func waitForInstanceStatus(t *testing.T, instances port.InstanceRepository, tenantID, instanceID uuid.UUID, want domain.InstanceStatus) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		inst, err := instances.GetByID(context.Background(), tenantID, instanceID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		if inst.Status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for instance status %v; last status = %v", want, inst.Status)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
