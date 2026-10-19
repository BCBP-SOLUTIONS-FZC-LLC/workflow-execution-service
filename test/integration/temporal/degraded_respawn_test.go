//go:build integration

package temporal_test

import (
	"context"
	"encoding/json"
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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
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
	outbox          port.OutboxRepository
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
		Queues: postgres.NewActiveTaskQueueRepo(pool), Validator: validator,
	}
	taskService := &service.TaskService{
		Instances: instances, Tasks: tasks, Assignments: assignments,
		Temporal: temporalClient, Eligibility: fakeEligibilityChecker{}, Definitions: definitions,
	}

	inst, err := instanceService.Start(context.Background(), port.StartInstanceInput{
		TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "subj-" + uuid.NewString(), BusinessKey: "degraded-" + uuid.NewString(),
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
		temporalClient: temporalClient, billingAttempts: &billingAttempts, outbox: outboxRepo,
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

// TestDegraded_RecordsTenantStatePauseSignal drives LLD §7.2 test #5: a
// tenant-suspend pause reaching a DEGRADED instance is recorded, not
// rejected. The stored status stays DEGRADED (there is no degraded-and-paused
// status) and nothing is written for the pause until the park ends; when a
// force-back respawn then settles the failed branch the park ends in a
// paused event carrying the tenant_state initiator, never a degraded-recovery
// resume.
func TestDegraded_RecordsTenantStatePauseSignal(t *testing.T) {
	f := setupDegradedInstance(t)

	if err := f.temporalClient.SignalWorkflow(context.Background(), f.inst.TemporalWorkflowID, f.inst.ID, port.SignalInstancePause, adminSignalWire{
		Initiator: "tenant_state", RecordVersion: 1,
	}); err != nil {
		t.Fatalf("signal instance-pause: %v", err)
	}

	time.Sleep(2 * time.Second)
	inst2, err := f.instances.GetByID(context.Background(), f.tenantID, f.inst.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if inst2.Status != domain.InstanceStatusDegraded {
		t.Fatalf("instance status after instance-pause while DEGRADED = %v, want still DEGRADED", inst2.Status)
	}
	if got := outboxInitiators(t, f, domain.EventWorkflowInstancePaused); len(got) != 0 {
		t.Fatalf("a paused event was written while the park was still open: %v", got)
	}

	forceBackAndCompleteBilling(t, f)

	waitForOutboxInitiator(t, f, domain.EventWorkflowInstancePaused, "tenant_state")
	if got := outboxInitiators(t, f, domain.EventWorkflowInstanceResumed); len(got) != 0 {
		t.Fatalf("the park ended with a resume %v although a pause was held", got)
	}
}

// TestDegraded_TenantReactivationBeforeTheParkEndsLeavesNoPause is the other
// half of test #5: the reactivation that follows the suspension clears the
// held pause, so the park ends with the ordinary degraded-recovery resume and
// no paused event.
func TestDegraded_TenantReactivationBeforeTheParkEndsLeavesNoPause(t *testing.T) {
	f := setupDegradedInstance(t)

	for _, name := range []string{port.SignalInstancePause, port.SignalInstanceResume} {
		if err := f.temporalClient.SignalWorkflow(context.Background(), f.inst.TemporalWorkflowID, f.inst.ID, name, adminSignalWire{
			Initiator: "tenant_state", RecordVersion: 1,
		}); err != nil {
			t.Fatalf("signal %s: %v", name, err)
		}
	}

	forceBackAndCompleteBilling(t, f)

	waitForOutboxInitiator(t, f, domain.EventWorkflowInstanceResumed, "degraded_recovery")
	if got := outboxInitiators(t, f, domain.EventWorkflowInstancePaused); len(got) != 0 {
		t.Fatalf("a paused event was written although the pause was lifted: %v", got)
	}
}

func forceBackAndCompleteBilling(t *testing.T, f *degradedFixture) {
	t.Helper()
	inst, err := f.instances.GetByID(context.Background(), f.tenantID, f.inst.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if err := f.temporalClient.SignalWorkflow(context.Background(), f.inst.TemporalWorkflowID, f.inst.ID, port.SignalInstanceForceBack, adminSignalWire{
		AdminUserID: f.assigneeUserID.String(), TargetDeptID: "billing", RecordVersion: inst.RecordVersion,
	}); err != nil {
		t.Fatalf("signal instance-force-back: %v", err)
	}

	var billingTask *domain.Task
	deadline := time.Now().Add(30 * time.Second)
	for billingTask == nil {
		rows, _, err := f.tasks.ListByInstance(context.Background(), f.tenantID, f.inst.ID, port.PageRequest{Limit: 10})
		if err != nil {
			t.Fatalf("ListByInstance: %v", err)
		}
		for _, r := range rows {
			if r.NodeKey == "billing/charge" && r.Status == domain.TaskStatusReady {
				billingTask = r
			}
		}
		if billingTask == nil {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for the respawned billing/charge task; rows=%+v", rows)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	if _, err := f.taskService.Complete(context.Background(), f.tenantID, billingTask.ID, f.assigneeUserID, []byte(`{}`), billingTask.RecordVersion); err != nil {
		t.Fatalf("complete respawned billing task: %v", err)
	}
}

func outboxInitiators(t *testing.T, f *degradedFixture, eventType string) []string {
	t.Helper()
	var out []string
	var after *port.Cursor
	for {
		rows, next, err := f.outbox.ListByInstance(context.Background(), f.tenantID, f.inst.ID, port.PageRequest{After: after, Limit: 100})
		if err != nil {
			t.Fatalf("ListByInstance outbox: %v", err)
		}
		for _, r := range rows {
			if r.EventType != eventType {
				continue
			}
			var envelope struct {
				Data struct {
					Initiator string `json:"initiator"`
				} `json:"data"`
			}
			if err := json.Unmarshal(r.Payload, &envelope); err != nil {
				t.Fatalf("decode %s envelope: %v", eventType, err)
			}
			out = append(out, envelope.Data.Initiator)
		}
		if next == nil {
			return out
		}
		after = next
	}
}

func waitForOutboxInitiator(t *testing.T, f *degradedFixture, eventType, initiator string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		for _, got := range outboxInitiators(t, f, eventType) {
			if got == initiator {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for a %s event with initiator %q; have %v, events %v", eventType, initiator, outboxInitiators(t, f, eventType), outboxEventTypes(t, f))
		}
		time.Sleep(250 * time.Millisecond)
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

func outboxEventTypes(t *testing.T, f *degradedFixture) []string {
	t.Helper()
	rows, _, err := f.outbox.ListByInstance(context.Background(), f.tenantID, f.inst.ID, port.PageRequest{Limit: 100})
	if err != nil {
		t.Fatalf("ListByInstance outbox: %v", err)
	}
	types := make([]string, len(rows))
	for i, r := range rows {
		types[i] = r.EventType
	}
	return types
}
