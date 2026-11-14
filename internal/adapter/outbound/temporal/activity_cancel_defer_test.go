package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestCancelInstance_TasksAlreadyClosed_AreLeftUntouchedAndEmitNoEvent(t *testing.T) {
	world := wkactNewWorld()
	deps, tx := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	open := world.addTask(tenant, inst, "a/open", domain.TaskStatusReady)
	closed := map[domain.TaskStatus]*domain.Task{
		domain.TaskStatusCompleted:  world.addTask(tenant, inst, "b/completed", domain.TaskStatusCompleted),
		domain.TaskStatusDeferred:   world.addTask(tenant, inst, "c/deferred", domain.TaskStatusDeferred),
		domain.TaskStatusFailed:     world.addTask(tenant, inst, "d/failed", domain.TaskStatusFailed),
		domain.TaskStatusSuperseded: world.addTask(tenant, inst, "e/superseded", domain.TaskStatusSuperseded),
	}
	held := map[domain.TaskStatus]*domain.TaskAssignment{}
	for status, task := range closed {
		task.RecordVersion = 4
		held[status] = world.addAssignment(tenant, task, true)
	}

	err := deps.CancelInstance(context.Background(), port.CancelInstanceInput{
		InstanceID: inst.ID.String(), TenantID: tenant.String(), AdminUserID: uuid.NewString(),
	})

	require.NoError(t, err)
	assert.Equal(t, 1, tx.committed)
	assert.Equal(t, domain.InstanceStatusTerminated, world.instances[inst.ID].Status)
	assert.Equal(t, domain.TaskStatusFailed, world.tasks[open.ID].Status, "the open task is failed by the cancel")
	for status, task := range closed {
		assert.Equal(t, status, world.tasks[task.ID].Status, "%s task keeps its status", status)
		assert.Equal(t, int64(4), world.tasks[task.ID].RecordVersion, "%s task keeps its record version", status)
		assert.True(t, world.assignments[held[status].ID].IsActive, "%s task keeps its assignment", status)
	}
	for _, c := range world.callsTo("Tasks.UpdateStatus") {
		assert.Equal(t, open.ID, c.ID, "only the open task is written")
	}
	failed := world.eventsOfType(domain.EventWorkflowTaskFailed)
	require.Len(t, failed, 1, "a failed event only for the open task")
	assert.JSONEq(t, `"`+open.ID.String()+`"`, string(wkactPayloadFields(t, failed[0])["task_id"]))
	assert.Len(t, world.eventsOfType(domain.EventWorkflowInstanceCancelled), 1)
	assert.Len(t, world.outbox, 2)
	world.requireAllCallsForTenant(t, tenant)
}

type wkactDeferFixture struct {
	deps       *outboundtemporal.Deps
	world      *wkactWorld
	tx         *wkactTransactor
	tenant     uuid.UUID
	task       *domain.Task
	assignment *domain.TaskAssignment
	input      port.DeferTaskInput
	run        func() error
}

func wkactNewDeferFixture(t *testing.T, validatorErr error) *wkactDeferFixture {
	t.Helper()
	world := wkactNewWorld()
	deps, tx := wkactNewDeps(t, world)
	deps.Validator = wkactValidator{real: deps.Validator.(wkactValidator).real, err: validatorErr}
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	task := world.addTask(tenant, inst, "sales/review", domain.TaskStatusInProgress)
	assignment := world.addAssignment(tenant, task, true)
	f := &wkactDeferFixture{deps: deps, world: world, tx: tx, tenant: tenant, task: task, assignment: assignment}
	f.input = port.DeferTaskInput{
		TaskID: task.ID.String(), TenantID: tenant.String(), UserID: assignment.UserID.String(),
		AssignmentID: assignment.ID.String(), Reason: "waiting for documents", DeferredToNodeKey: "sales/later",
	}
	f.run = func() error { return deps.DeferTask(context.Background(), f.input) }
	return f
}

func (f *wkactDeferFixture) requireRolledBack(t *testing.T) {
	t.Helper()
	assert.Equal(t, 1, f.tx.rolledBack)
	assert.Equal(t, 0, f.tx.committed)
	assert.Equal(t, domain.TaskStatusInProgress, f.world.tasks[f.task.ID].Status, "task stays active")
	assert.Equal(t, int64(1), f.world.tasks[f.task.ID].RecordVersion)
	got := f.world.assignments[f.assignment.ID]
	assert.True(t, got.IsActive, "assignment stays open")
	assert.Nil(t, got.CompletedAt)
	assert.Empty(t, f.world.outbox, "no event without its status change")
}

// requireRetryCommits clears every injected failure and runs the same input
// again, as Temporal does after the failed attempt.
func (f *wkactDeferFixture) requireRetryCommits(t *testing.T) {
	t.Helper()
	clear(f.world.failures)
	f.deps.Validator = wkactNewValidator(t)

	require.NoError(t, f.run())

	assert.Equal(t, 1, f.tx.committed)
	assert.Equal(t, domain.TaskStatusDeferred, f.world.tasks[f.task.ID].Status)
	assignment := f.world.assignments[f.assignment.ID]
	assert.NotNil(t, assignment.CompletedAt, "the deferring assignment is completed")
	assert.False(t, assignment.IsActive)
	assert.Len(t, f.world.eventsOfType(domain.EventWorkflowTaskDeferred), 1, "exactly one deferred event after the retry")
	require.NotEmpty(t, f.world.callsTo("Assignments.Complete"))
	for _, c := range f.world.callsTo("Assignments.Complete") {
		assert.Equal(t, int64(2), c.Version, "the assignment is completed at the task version the status change produced")
	}
}

func TestDeferTask_EventNotEnqueued_ReturnsTheErrorAndRollsTheTransactionBack(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		outboxErr    error
		validatorErr error
		want         string
	}{
		{name: "outbox enqueue fails", outboxErr: errWkactBoom, want: "enqueue " + domain.EventWorkflowTaskDeferred},
		{name: "envelope refused by the validator", validatorErr: errWkactBoom, want: "build " + domain.EventWorkflowTaskDeferred + " envelope"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := wkactNewDeferFixture(t, tc.validatorErr)
			if tc.outboxErr != nil {
				f.world.failures["Outbox.Enqueue"] = tc.outboxErr
			}

			err := f.run()

			require.ErrorIs(t, err, errWkactBoom)
			assert.ErrorContains(t, err, tc.want)
			f.requireRolledBack(t)
			f.requireRetryCommits(t)
			f.world.requireAllCallsForTenant(t, f.tenant)
		})
	}
}

func TestDeferTask_InstanceUnreadableForEventCore_ReturnsTheErrorAndRollsBackStatusAndAssignment(t *testing.T) {
	f := wkactNewDeferFixture(t, nil)
	f.world.failures["Instances.GetByID"] = errWkactBoom

	err := f.run()

	require.ErrorIs(t, err, errWkactBoom)
	assert.ErrorContains(t, err, "get instance for event core")
	f.requireRolledBack(t)
	require.Len(t, f.world.callsTo("Instances.GetByID"), 1)
	f.requireRetryCommits(t)
	f.world.requireAllCallsForTenant(t, f.tenant)
}

func TestCancelInstance_AlreadyTerminated_RerunChangesNothingAndEmitsNoSecondEvent(t *testing.T) {
	world := wkactNewWorld()
	deps, tx := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	world.addTask(tenant, inst, "a/open", domain.TaskStatusReady)
	in := port.CancelInstanceInput{InstanceID: inst.ID.String(), TenantID: tenant.String(), AdminUserID: uuid.NewString()}

	require.NoError(t, deps.CancelInstance(context.Background(), in))
	eventsAfterFirst := len(world.outbox)
	writesAfterFirst := len(world.statusWrites)
	taskWritesAfterFirst := len(world.callsTo("Tasks.UpdateStatus"))
	require.NoError(t, deps.CancelInstance(context.Background(), in))

	assert.Equal(t, 2, tx.committed)
	assert.Len(t, world.outbox, eventsAfterFirst, "the rerun enqueues nothing")
	assert.Len(t, world.eventsOfType(domain.EventWorkflowInstanceCancelled), 1)
	assert.Len(t, world.statusWrites, writesAfterFirst)
	assert.Len(t, world.callsTo("Tasks.UpdateStatus"), taskWritesAfterFirst)
	assert.Equal(t, domain.InstanceStatusTerminated, world.instances[inst.ID].Status)
	world.requireAllCallsForTenant(t, tenant)
}

func TestCompleteAssignment_WritesTheAssignmentAtTheTaskVersionItRead(t *testing.T) {
	world := wkactNewWorld()
	deps, _ := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	task := world.addTask(tenant, inst, "sales/review", domain.TaskStatusInProgress)
	task.RecordVersion = 4
	assignment := world.addAssignment(tenant, task, true)

	out, err := deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		TaskID: task.ID.String(), TenantID: tenant.String(), UserID: assignment.UserID.String(), ResultJSON: `{"ok":true}`, RecordVersion: 1,
	})

	require.NoError(t, err)
	assert.True(t, out.AllDone)
	completes := world.callsTo("Assignments.Complete")
	require.Len(t, completes, 1)
	assert.Equal(t, wkactCall{Op: "Assignments.Complete", Tenant: tenant, ID: assignment.ID, Version: 4}, completes[0],
		"the task version read in the transaction, not the stale input version")
	assert.JSONEq(t, `{"ok":true}`, string(world.assignments[assignment.ID].ResultJSON))
	world.requireAllCallsForTenant(t, tenant)
}
