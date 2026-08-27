package temporal_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestRecordForceRoute_SupersedesBypassedTaskAndEnqueuesBoth(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	bypassed := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, NodeKey: "sales/review", Status: domain.TaskStatusReady, RecordVersion: 1}
	unrelated := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, NodeKey: "sales/other", Status: domain.TaskStatusReady, RecordVersion: 1}
	tasks := newFakeTaskRepo(bypassed, unrelated)
	activeAssignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: bypassed.ID, UserID: uuid.New()}
	assignments := newFakeAssignmentRepo(activeAssignment)
	outbox := &fakeOutbox{}
	deps := &outboundtemporal.Deps{
		Instances: newFakeInstanceRepo(inst), Tasks: tasks, Assignments: assignments,
		Outbox: outbox, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.RecordForceRoute(context.Background(), port.RecordForceRouteInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(),
		OldNodeKeys: []domain.NodeKey{"sales/review"}, TargetNodeID: "sales/approve", AdminUserID: uuid.New().String(),
	})
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusSuperseded, bypassed.Status)
	assert.Equal(t, domain.TaskStatusReady, unrelated.Status, "only the bypassed node's task is superseded")
	assert.False(t, activeAssignment.IsActive)

	require.Len(t, outbox.enqueued, 2)
	assert.Equal(t, domain.EventWorkflowTaskSuperseded, outbox.enqueued[0].Type)
	assert.Equal(t, domain.EventWorkflowInstanceForceRouted, outbox.enqueued[1].Type)
}

// TestRecordForceRoute_GetInstanceError forces RecordForceRoute's
// post-supersede Instances.GetByID to fail (the instance is absent from the
// repo) — the "get instance" wrap branch, distinct from supersedeBypassedTasks'
// own error paths exercised below.
func TestRecordForceRoute_GetInstanceError(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	deps := &outboundtemporal.Deps{
		Instances: newFakeInstanceRepo(), Tasks: newFakeTaskRepo(), Assignments: newFakeAssignmentRepo(),
		Outbox: &fakeOutbox{}, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.RecordForceRoute(context.Background(), port.RecordForceRouteInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(),
		OldNodeKeys: []domain.NodeKey{"sales/review"}, TargetNodeID: "sales/approve", AdminUserID: uuid.New().String(),
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "get instance")
}

// TestSupersedeBypassedTasks_ListError forces the task-listing call inside
// supersedeBypassedTasks to fail.
func TestSupersedeBypassedTasks_ListError(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	tasks := newFakeTaskRepo()
	tasks.listErr = errBoom
	deps := &outboundtemporal.Deps{
		Instances: newFakeInstanceRepo(inst), Tasks: tasks, Assignments: newFakeAssignmentRepo(),
		Outbox: &fakeOutbox{}, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.RecordForceRoute(context.Background(), port.RecordForceRouteInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(),
		OldNodeKeys: []domain.NodeKey{"sales/review"}, TargetNodeID: "sales/approve", AdminUserID: uuid.New().String(),
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "list tasks by instance")
}

// TestSupersedeBypassedTasks_Paginates forces a second ListByInstance page —
// the bypassed node's tasks span two pages, and both must end up SUPERSEDED,
// not just the first page's.
func TestSupersedeBypassedTasks_Paginates(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	page1Task := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, NodeKey: "sales/review", Status: domain.TaskStatusReady, RecordVersion: 1}
	page2Task := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, NodeKey: "sales/review", Status: domain.TaskStatusReady, RecordVersion: 1}
	tasks := newFakeTaskRepo(page1Task, page2Task)
	tasks.pages = [][]*domain.Task{{page1Task}, {page2Task}}
	outbox := &fakeOutbox{}
	deps := &outboundtemporal.Deps{
		Instances: newFakeInstanceRepo(inst), Tasks: tasks, Assignments: newFakeAssignmentRepo(),
		Outbox: outbox, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.RecordForceRoute(context.Background(), port.RecordForceRouteInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(),
		OldNodeKeys: []domain.NodeKey{"sales/review"}, TargetNodeID: "sales/approve", AdminUserID: uuid.New().String(),
	})
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusSuperseded, page1Task.Status, "page 1's bypassed task must be superseded")
	assert.Equal(t, domain.TaskStatusSuperseded, page2Task.Status, "page 2's bypassed task must be superseded too")
	assert.Equal(t, 2, tasks.pageCalls, "the pagination loop must have followed the cursor to a second page")
}

// TestSupersedeTask_UpdateStatusError forces the Tasks.UpdateStatus call
// inside supersedeTask to fail.
func TestSupersedeTask_UpdateStatusError(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	bypassed := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, NodeKey: "sales/review", Status: domain.TaskStatusReady, RecordVersion: 1}
	tasks := newFakeTaskRepo(bypassed)
	tasks.updateStatusErr = errBoom
	deps := &outboundtemporal.Deps{
		Instances: newFakeInstanceRepo(inst), Tasks: tasks, Assignments: newFakeAssignmentRepo(),
		Outbox: &fakeOutbox{}, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.RecordForceRoute(context.Background(), port.RecordForceRouteInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(),
		OldNodeKeys: []domain.NodeKey{"sales/review"}, TargetNodeID: "sales/approve", AdminUserID: uuid.New().String(),
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "supersede task")
}

// TestSupersedeTask_ListActiveAssignmentsError forces
// Assignments.ListActiveByTask inside supersedeTask to fail.
func TestSupersedeTask_ListActiveAssignmentsError(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	bypassed := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, NodeKey: "sales/review", Status: domain.TaskStatusReady, RecordVersion: 1}
	tasks := newFakeTaskRepo(bypassed)
	assignments := newFakeAssignmentRepo()
	assignments.listActiveErr = errBoom
	deps := &outboundtemporal.Deps{
		Instances: newFakeInstanceRepo(inst), Tasks: tasks, Assignments: assignments,
		Outbox: &fakeOutbox{}, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.RecordForceRoute(context.Background(), port.RecordForceRouteInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(),
		OldNodeKeys: []domain.NodeKey{"sales/review"}, TargetNodeID: "sales/approve", AdminUserID: uuid.New().String(),
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "list active assignments")
}

// TestSupersedeTask_VacateError forces Assignments.Vacate inside
// supersedeTask to fail.
func TestSupersedeTask_VacateError(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	bypassed := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, NodeKey: "sales/review", Status: domain.TaskStatusReady, RecordVersion: 1}
	tasks := newFakeTaskRepo(bypassed)
	activeAssignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: bypassed.ID, UserID: uuid.New()}
	assignments := newFakeAssignmentRepo(activeAssignment)
	assignments.vacateErr = errBoom
	deps := &outboundtemporal.Deps{
		Instances: newFakeInstanceRepo(inst), Tasks: tasks, Assignments: assignments,
		Outbox: &fakeOutbox{}, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.RecordForceRoute(context.Background(), port.RecordForceRouteInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(),
		OldNodeKeys: []domain.NodeKey{"sales/review"}, TargetNodeID: "sales/approve", AdminUserID: uuid.New().String(),
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "vacate assignment")
}

func TestRecordSLAWarning_EnqueuesAuditEventOnly(t *testing.T) {
	taskID, instanceID := uuid.New(), uuid.New()
	followUp := time.Now().UTC()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: instanceID, Status: domain.TaskStatusReady, FollowUpAt: &followUp}
	assignee := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: uuid.New()}
	outbox := &fakeOutbox{}
	deps := &outboundtemporal.Deps{
		Tasks: newFakeTaskRepo(task), Assignments: newFakeAssignmentRepo(assignee),
		Outbox: outbox, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.RecordSLAWarning(context.Background(), port.RecordSLAWarningInput{
		InstanceID: instanceID.String(), TenantID: uuid.New().String(), TaskID: taskID.String(), NodeKey: "sales/review",
	})
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusReady, task.Status, "audit-only: no status change")
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowTaskSLAWarning, outbox.enqueued[0].Type)

	var payload domain.WorkflowTaskSLAWarningPayload
	require.NoError(t, json.Unmarshal(outbox.enqueued[0].Payload, &payload))
	assert.Equal(t, []uuid.UUID{assignee.UserID}, payload.AssigneeUserIDs, "assignee_user_ids must not be omitted — the schema requires it non-null")
}

// TestRecordSLAWarning_RetriedCall_NoOp is the regression test for the
// duplicate-audit-event finding: RecordSLAWarning/Breach are audit-only, so
// nothing gates a retry on status — a retried activity (e.g. after a
// worker restart following a successful first attempt) must not
// double-emit workflow.task.sla-warning.
func TestRecordSLAWarning_RetriedCall_NoOp(t *testing.T) {
	taskID, instanceID := uuid.New(), uuid.New()
	followUp := time.Now().UTC()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: instanceID, Status: domain.TaskStatusReady, FollowUpAt: &followUp}
	outbox := &fakeOutbox{}
	deps := &outboundtemporal.Deps{
		Tasks: newFakeTaskRepo(task), Assignments: newFakeAssignmentRepo(),
		Outbox: outbox, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	in := port.RecordSLAWarningInput{InstanceID: instanceID.String(), TenantID: uuid.New().String(), TaskID: taskID.String(), NodeKey: "sales/review"}

	require.NoError(t, deps.RecordSLAWarning(context.Background(), in))
	require.NoError(t, deps.RecordSLAWarning(context.Background(), in), "a retried RecordSLAWarning must succeed idempotently, not error")
	assert.Len(t, outbox.enqueued, 1, "retry must not re-enqueue workflow.task.sla-warning")
}

// TestRecordSLAEvent_ExistsForTaskError forces the Outbox.ExistsForTask
// idempotency check itself to fail — distinct from the "already recorded"
// no-op path covered by TestRecordSLAWarning_RetriedCall_NoOp above.
func TestRecordSLAEvent_ExistsForTaskError(t *testing.T) {
	taskID, instanceID := uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: instanceID, Status: domain.TaskStatusReady}
	outbox := &fakeOutbox{existsErr: errBoom}
	deps := &outboundtemporal.Deps{
		Tasks: newFakeTaskRepo(task), Assignments: newFakeAssignmentRepo(),
		Outbox: outbox, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.RecordSLAWarning(context.Background(), port.RecordSLAWarningInput{
		InstanceID: instanceID.String(), TenantID: uuid.New().String(), TaskID: taskID.String(), NodeKey: "sales/review",
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "check existing sla event")
}

// TestRecordSLAEvent_ListActiveAssignmentsError forces
// Assignments.ListActiveByTask to fail after the idempotency check passes.
func TestRecordSLAEvent_ListActiveAssignmentsError(t *testing.T) {
	taskID, instanceID := uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: instanceID, Status: domain.TaskStatusReady}
	assignments := newFakeAssignmentRepo()
	assignments.listActiveErr = errBoom
	deps := &outboundtemporal.Deps{
		Tasks: newFakeTaskRepo(task), Assignments: assignments,
		Outbox: &fakeOutbox{}, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.RecordSLABreach(context.Background(), port.RecordSLABreachInput{
		InstanceID: instanceID.String(), TenantID: uuid.New().String(), TaskID: taskID.String(), NodeKey: "sales/review",
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "list active assignments")
}

func TestRecordSLABreach_EnqueuesAuditEventOnly(t *testing.T) {
	taskID, instanceID := uuid.New(), uuid.New()
	due := time.Now().UTC()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: instanceID, DueAt: &due}
	outbox := &fakeOutbox{}
	deps := &outboundtemporal.Deps{
		Tasks: newFakeTaskRepo(task), Assignments: newFakeAssignmentRepo(),
		Outbox: outbox, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.RecordSLABreach(context.Background(), port.RecordSLABreachInput{
		InstanceID: instanceID.String(), TenantID: uuid.New().String(), TaskID: taskID.String(), NodeKey: "sales/review",
	})
	require.NoError(t, err)
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowTaskSLABreached, outbox.enqueued[0].Type)
}
