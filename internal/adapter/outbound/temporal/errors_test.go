package temporal_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// erroringValidator rejects every payload — used to exercise
// enqueueInstanceEvent's BuildEnvelope-failure branch.
type erroringValidator struct{}

func (erroringValidator) Validate(context.Context, string, json.RawMessage) error { return errBoom }

func TestCreateTask_ParsesValidAndIgnoresInvalidDueDate(t *testing.T) {
	deps, tasks, _, _ := newTestDeps()

	stage := dsl.StageDef{Type: "review", DueDate: "2026-01-01T00:00:00Z", FollowUpDate: "not-a-date"}
	compiled, err := json.Marshal(stage)
	require.NoError(t, err)

	out, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), NodeKey: "sales/review", CompiledNode: compiled,
	})
	require.NoError(t, err)
	task := tasks.byID[uuid.MustParse(out.TaskID)]
	require.NotNil(t, task.DueAt)
	assert.Nil(t, task.FollowUpAt, "an unparseable FollowUpDate is treated as absent, not an error")
}

func TestCreateTask_InvalidInstanceID_IsNonRetryable(t *testing.T) {
	deps, _, _, _ := newTestDeps()
	compiled, _ := json.Marshal(dsl.StageDef{Type: "review"})
	_, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: "not-a-uuid", TenantID: uuid.New().String(), NodeKey: "sales/review", CompiledNode: compiled,
	})
	require.Error(t, err)
}

func TestCreateTask_MalformedCompiledNode_IsNonRetryable(t *testing.T) {
	deps, _, _, _ := newTestDeps()
	_, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), NodeKey: "sales/review", CompiledNode: json.RawMessage(`not-json`),
	})
	require.Error(t, err)
}

func TestCreateTask_InvalidDefaultAssigneeID_IsNonRetryable(t *testing.T) {
	deps, _, _, _ := newTestDeps()
	compiled, _ := json.Marshal(dsl.StageDef{Type: "review", DefaultAssignees: []string{"not-a-uuid"}})
	_, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), NodeKey: "sales/review", CompiledNode: compiled,
	})
	require.Error(t, err)
}

func TestCreateTask_InvalidContextJSON_ForConnector_IsNonRetryable(t *testing.T) {
	deps, _, _, _ := newTestDeps()
	stage := dsl.StageDef{Type: "connector", ConnectorType: "send-email", IOMapping: &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "a", Target: "b"}}}}
	compiled, _ := json.Marshal(stage)
	_, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), NodeKey: "sales/connector",
		CompiledNode: compiled, ContextJSON: "not-json",
	})
	require.Error(t, err)
}

func TestUpdateInstanceNodes_InvalidIDs(t *testing.T) {
	deps := &outboundtemporal.Deps{Instances: newFakeInstanceRepo()}
	err := deps.UpdateInstanceNodes(context.Background(), port.UpdateInstanceNodesInput{InstanceID: uuid.New().String(), TenantID: "bad"})
	require.Error(t, err)
	err = deps.UpdateInstanceNodes(context.Background(), port.UpdateInstanceNodesInput{InstanceID: "bad", TenantID: uuid.New().String()})
	require.Error(t, err)
}

func TestUpdateInstanceNodes_InstanceNotFound(t *testing.T) {
	deps := &outboundtemporal.Deps{Instances: newFakeInstanceRepo()}
	err := deps.UpdateInstanceNodes(context.Background(), port.UpdateInstanceNodesInput{InstanceID: uuid.New().String(), TenantID: uuid.New().String()})
	require.Error(t, err)
}

func TestClaimAssignment_InvalidIDs(t *testing.T) {
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(), newFakeAssignmentRepo())
	require.Error(t, deps.ClaimAssignment(context.Background(), port.ClaimAssignmentInput{TenantID: "bad", AssignmentID: uuid.New().String()}))
	require.Error(t, deps.ClaimAssignment(context.Background(), port.ClaimAssignmentInput{TenantID: uuid.New().String(), AssignmentID: "bad"}))
}

func TestClaimAssignment_AssignmentNotFound(t *testing.T) {
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(), newFakeAssignmentRepo())
	err := deps.ClaimAssignment(context.Background(), port.ClaimAssignmentInput{TenantID: uuid.New().String(), AssignmentID: uuid.New().String()})
	require.Error(t, err)
}

func TestCompleteAssignment_InvalidIDs(t *testing.T) {
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(), newFakeAssignmentRepo())
	_, err := deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{TenantID: "bad", AssignmentID: uuid.New().String()})
	require.Error(t, err)
	_, err = deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{TenantID: uuid.New().String(), AssignmentID: "bad"})
	require.Error(t, err)
}

func TestDeferTask_InvalidIDs(t *testing.T) {
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(), newFakeAssignmentRepo())
	base := port.DeferTaskInput{TaskID: uuid.New().String(), TenantID: uuid.New().String(), UserID: uuid.New().String(), AssignmentID: uuid.New().String()}

	bad := base
	bad.TenantID = "bad"
	_, err := deps.DeferTask(context.Background(), bad)
	require.Error(t, err)

	bad = base
	bad.TaskID = "bad"
	_, err = deps.DeferTask(context.Background(), bad)
	require.Error(t, err)

	bad = base
	bad.AssignmentID = "bad"
	_, err = deps.DeferTask(context.Background(), bad)
	require.Error(t, err)

	bad = base
	bad.UserID = "bad"
	_, err = deps.DeferTask(context.Background(), bad)
	require.Error(t, err)
}

func TestDeferTask_TaskNotFound(t *testing.T) {
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(), newFakeAssignmentRepo())
	_, err := deps.DeferTask(context.Background(), port.DeferTaskInput{
		TaskID: uuid.New().String(), TenantID: uuid.New().String(), UserID: uuid.New().String(), AssignmentID: uuid.New().String(),
	})
	require.Error(t, err)
}

func TestReassignAssignment_InvalidIDs(t *testing.T) {
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(), newFakeAssignmentRepo())
	base := port.ReassignAssignmentInput{
		TaskID: uuid.New().String(), TenantID: uuid.New().String(), OldUserID: uuid.New().String(),
		NewUserID: uuid.New().String(), AdminUserID: uuid.New().String(),
	}
	for _, mutate := range []func(*port.ReassignAssignmentInput){
		func(in *port.ReassignAssignmentInput) { in.TenantID = "bad" },
		func(in *port.ReassignAssignmentInput) { in.TaskID = "bad" },
		func(in *port.ReassignAssignmentInput) { in.OldUserID = "bad" },
		func(in *port.ReassignAssignmentInput) { in.NewUserID = "bad" },
		func(in *port.ReassignAssignmentInput) { in.AdminUserID = "bad" },
	} {
		bad := base
		mutate(&bad)
		require.Error(t, deps.ReassignAssignment(context.Background(), bad))
	}
}

func TestReassignAssignment_TaskNotFound(t *testing.T) {
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(), newFakeAssignmentRepo())
	err := deps.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TaskID: uuid.New().String(), TenantID: uuid.New().String(), OldUserID: uuid.New().String(),
		NewUserID: uuid.New().String(), AdminUserID: uuid.New().String(),
	})
	require.Error(t, err)
}

func TestUpdateTaskStatus_InvalidIDs(t *testing.T) {
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(), newFakeAssignmentRepo())
	require.Error(t, deps.UpdateTaskStatus(context.Background(), port.UpdateTaskStatusInput{TenantID: "bad", TaskID: uuid.New().String()}))
	require.Error(t, deps.UpdateTaskStatus(context.Background(), port.UpdateTaskStatusInput{TenantID: uuid.New().String(), TaskID: "bad"}))
}

func TestUpdateInstanceStatus_InvalidIDs(t *testing.T) {
	inst := &domain.Instance{ID: uuid.New(), RecordVersion: 1}
	deps, _ := newInstanceTestDeps(inst, nil, nil)
	require.Error(t, deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{TenantID: "bad", InstanceID: inst.ID.String()}))
	require.Error(t, deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{TenantID: uuid.New().String(), InstanceID: "bad"}))
}

func TestUpdateInstanceStatus_InstanceNotFound(t *testing.T) {
	deps, _ := newInstanceTestDeps(&domain.Instance{ID: uuid.New(), RecordVersion: 1}, nil, nil)
	err := deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), Status: domain.InstanceStatusCompleted,
	})
	require.Error(t, err)
}

func TestPauseInstance_InvalidIDs(t *testing.T) {
	deps, _ := newInstanceTestDeps(&domain.Instance{ID: uuid.New(), RecordVersion: 1}, nil, nil)
	require.Error(t, deps.PauseInstance(context.Background(), port.PauseInstanceInput{TenantID: "bad", InstanceID: uuid.New().String()}))
	require.Error(t, deps.PauseInstance(context.Background(), port.PauseInstanceInput{TenantID: uuid.New().String(), InstanceID: "bad"}))
}

func TestCancelInstance_InvalidIDs(t *testing.T) {
	deps, _ := newInstanceTestDeps(&domain.Instance{ID: uuid.New(), RecordVersion: 1}, nil, nil)
	base := port.CancelInstanceInput{InstanceID: uuid.New().String(), TenantID: uuid.New().String(), AdminUserID: uuid.New().String(), RecordVersion: 1}

	bad := base
	bad.TenantID = "bad"
	require.Error(t, deps.CancelInstance(context.Background(), bad))

	bad = base
	bad.InstanceID = "bad"
	require.Error(t, deps.CancelInstance(context.Background(), bad))

	bad = base
	bad.AdminUserID = "bad"
	require.Error(t, deps.CancelInstance(context.Background(), bad))
}

// TestCancelInstance_AlreadyTerminated_NoOp is the regression test for the
// retry-forever fix: CancelInstance no longer trusts in.RecordVersion (stale
// by the time an at-least-once Temporal retry replays it) — it refetches the
// instance and, finding it already Terminated, no-ops instead of attempting
// a version-guarded write that would now spuriously conflict.
func TestCancelInstance_AlreadyTerminated_NoOp(t *testing.T) {
	instanceID := uuid.New()
	inst := &domain.Instance{ID: instanceID, RecordVersion: 5, Status: domain.InstanceStatusTerminated}
	deps, outbox := newInstanceTestDeps(inst, nil, nil)

	err := deps.CancelInstance(context.Background(), port.CancelInstanceInput{
		InstanceID: instanceID.String(), TenantID: uuid.New().String(), AdminUserID: uuid.New().String(), RecordVersion: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, outbox.enqueued, "no-op retry must not re-enqueue workflow.instance.terminated")
}

func TestRecordForceRoute_InvalidIDs(t *testing.T) {
	deps := &outboundtemporal.Deps{Instances: newFakeInstanceRepo(), Tasks: newFakeTaskRepo(), Assignments: newFakeAssignmentRepo(), Transactor: fakeTransactor{}}
	base := port.RecordForceRouteInput{InstanceID: uuid.New().String(), TenantID: uuid.New().String(), AdminUserID: uuid.New().String()}

	bad := base
	bad.TenantID = "bad"
	require.Error(t, deps.RecordForceRoute(context.Background(), bad))

	bad = base
	bad.InstanceID = "bad"
	require.Error(t, deps.RecordForceRoute(context.Background(), bad))

	bad = base
	bad.AdminUserID = "bad"
	require.Error(t, deps.RecordForceRoute(context.Background(), bad))
}

func TestRecordSLAWarning_InvalidIDs(t *testing.T) {
	deps := &outboundtemporal.Deps{Tasks: newFakeTaskRepo(), Transactor: fakeTransactor{}}
	base := port.RecordSLAWarningInput{InstanceID: uuid.New().String(), TenantID: uuid.New().String(), TaskID: uuid.New().String()}

	bad := base
	bad.TenantID = "bad"
	require.Error(t, deps.RecordSLAWarning(context.Background(), bad))

	bad = base
	bad.InstanceID = "bad"
	require.Error(t, deps.RecordSLAWarning(context.Background(), bad))

	bad = base
	bad.TaskID = "bad"
	require.Error(t, deps.RecordSLAWarning(context.Background(), bad))
}

func TestRecordSLAWarning_TaskNotFound(t *testing.T) {
	deps := &outboundtemporal.Deps{Tasks: newFakeTaskRepo(), Transactor: fakeTransactor{}}
	err := deps.RecordSLAWarning(context.Background(), port.RecordSLAWarningInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), TaskID: uuid.New().String(),
	})
	require.Error(t, err)
}

func TestDeferTask_RegressionTaskCreateFails(t *testing.T) {
	taskID, deferrerID := uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	assignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: deferrerID}
	tasks := newFakeTaskRepo(task)
	tasks.createErr = errBoom
	deps, _ := newAssignmentTestDeps(tasks, newFakeAssignmentRepo(assignment))

	_, err := deps.DeferTask(context.Background(), port.DeferTaskInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(), UserID: deferrerID.String(), AssignmentID: assignment.ID.String(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestDeferTask_RegressionAssignmentCreateFails(t *testing.T) {
	taskID, deferrerID := uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	assignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: deferrerID}
	assignments := newFakeAssignmentRepo(assignment)
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(task), assignments)
	// Complete() on the deferring assignment succeeds normally; only the
	// regression assignment's Create should fail, so flip createErr after
	// the deferring-assignment Complete call would already have happened —
	// simplest correct trigger here is failing every Create, which this
	// path only calls once (for the regression assignment).
	assignments.createErr = errBoom

	_, err := deps.DeferTask(context.Background(), port.DeferTaskInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(), UserID: deferrerID.String(), AssignmentID: assignment.ID.String(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestCreateTask_TasksCreateFails(t *testing.T) {
	tasks := newFakeTaskRepo()
	tasks.createErr = errBoom
	deps := &outboundtemporal.Deps{Tasks: tasks, Assignments: newFakeAssignmentRepo(), Outbox: &fakeOutbox{}, Transactor: fakeTransactor{}, Validator: noopValidator{}}
	compiled, _ := json.Marshal(dsl.StageDef{Type: "review"})

	_, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), NodeKey: "sales/review", CompiledNode: compiled,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestCreateTask_AssignmentsCreateFails(t *testing.T) {
	assignments := newFakeAssignmentRepo()
	assignments.createErr = errBoom
	deps := &outboundtemporal.Deps{Tasks: newFakeTaskRepo(), Assignments: assignments, Outbox: &fakeOutbox{}, Transactor: fakeTransactor{}, Validator: noopValidator{}}
	compiled, _ := json.Marshal(dsl.StageDef{Type: "review", DefaultAssignees: []string{uuid.New().String()}})

	_, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), NodeKey: "sales/review", CompiledNode: compiled,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestPauseInstance_InvalidAdminUserID_TreatedAsNilActor(t *testing.T) {
	instanceID := uuid.New()
	inst := &domain.Instance{ID: instanceID, RecordVersion: 1}
	deps, outbox := newInstanceTestDeps(inst, nil, nil)

	err := deps.PauseInstance(context.Background(), port.PauseInstanceInput{
		InstanceID: instanceID.String(), TenantID: uuid.New().String(), AdminUserID: "not-a-uuid", RecordVersion: 1,
	})
	require.NoError(t, err, "adminUserIDPtr degrades to a nil actor rather than failing pause")
	require.Len(t, outbox.enqueued, 1)
}

func TestCompleteAssignment_AssignmentNotFound(t *testing.T) {
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(), newFakeAssignmentRepo())
	_, err := deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		AssignmentID: uuid.New().String(), TenantID: uuid.New().String(),
	})
	require.Error(t, err)
}

func TestCompleteAssignment_TaskNotFound(t *testing.T) {
	assignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: uuid.New(), UserID: uuid.New()}
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(), newFakeAssignmentRepo(assignment))
	_, err := deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		AssignmentID: assignment.ID.String(), TenantID: uuid.New().String(),
	})
	require.Error(t, err)
}

func TestCompleteAssignment_ListActiveByTaskFails_AlreadyCompletedNoOp(t *testing.T) {
	task := &domain.Task{ID: uuid.New(), RecordVersion: 1}
	now := time.Now()
	assignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: task.ID, UserID: uuid.New(), CompletedAt: &now}
	assignments := newFakeAssignmentRepo(assignment)
	assignments.listActiveErr = errBoom
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(task), assignments)

	_, err := deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		AssignmentID: assignment.ID.String(), TenantID: uuid.New().String(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestCompleteAssignment_ListActiveByTaskFails_AfterComplete(t *testing.T) {
	task := &domain.Task{ID: uuid.New(), RecordVersion: 1}
	assignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: task.ID, UserID: uuid.New()}
	assignments := newFakeAssignmentRepo(assignment)
	assignments.listActiveErr = errBoom
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(task), assignments)

	_, err := deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		AssignmentID: assignment.ID.String(), TenantID: uuid.New().String(), ResultJSON: `{}`,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestReassignAssignment_ListActiveByTaskFails(t *testing.T) {
	task := &domain.Task{ID: uuid.New(), RecordVersion: 1}
	assignments := newFakeAssignmentRepo()
	assignments.listActiveErr = errBoom
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(task), assignments)

	err := deps.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TaskID: task.ID.String(), TenantID: uuid.New().String(), OldUserID: uuid.New().String(),
		NewUserID: uuid.New().String(), AdminUserID: uuid.New().String(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestReassignAssignment_VacateFails(t *testing.T) {
	taskID, oldUser := uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, RecordVersion: 1}
	oldAssignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: oldUser}
	assignments := newFakeAssignmentRepo(oldAssignment)
	assignments.vacateErr = errBoom
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(task), assignments)

	err := deps.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(), OldUserID: oldUser.String(),
		NewUserID: uuid.New().String(), AdminUserID: uuid.New().String(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

// TestDeferTask_RegressionAssignmentAlreadyExists_NoOp is the coverage
// regression test for createRegressionTask's own idempotent-retry path: the
// regression task is created fresh, but its assignment insert already
// exists (a retry landing between the two inserts, or simply a second
// concurrent attempt) — must return the newly-created task, not error.
func TestDeferTask_RegressionAssignmentAlreadyExists_NoOp(t *testing.T) {
	tenantID, deferrerID := uuid.New(), uuid.New()
	taskID := uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), NodeKey: "sales/review", AssigneeMode: "single", RecordVersion: 1}
	assignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: deferrerID}
	tasks := newFakeTaskRepo(task)
	assignments := newFakeAssignmentRepo(assignment)
	assignments.createErr = domain.ErrAlreadyExists
	deps, _ := newAssignmentTestDeps(tasks, assignments)

	out, err := deps.DeferTask(context.Background(), port.DeferTaskInput{
		TaskID: taskID.String(), TenantID: tenantID.String(), UserID: deferrerID.String(),
		AssignmentID: assignment.ID.String(), Reason: "not ready yet",
	})
	require.NoError(t, err)
	_, parseErr := uuid.Parse(out.NewTaskID)
	require.NoError(t, parseErr)
}

func TestGetCompiledPlan_InvalidVersionID_IsNonRetryable(t *testing.T) {
	deps := &outboundtemporal.Deps{}
	_, err := deps.GetCompiledPlan(context.Background(), port.GetCompiledPlanInput{TenantID: uuid.New().String(), VersionID: "not-a-uuid"})
	require.Error(t, err)
}

func TestSupersedeBypassedTasks_UpdateStatusFails(t *testing.T) {
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
	assert.True(t, errors.Is(err, errBoom))
}

func TestFailActiveTasks_UpdateStatusFails(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	openTask := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, Status: domain.TaskStatusReady, RecordVersion: 1}
	tasks := newFakeTaskRepo(openTask)
	tasks.updateStatusErr = errBoom
	deps, _ := newInstanceTestDeps(inst, tasks, newFakeAssignmentRepo())

	err := deps.CancelInstance(context.Background(), port.CancelInstanceInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), AdminUserID: uuid.New().String(), RecordVersion: 1,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestClaimAssignment_SetLeadFails(t *testing.T) {
	task := &domain.Task{ID: uuid.New(), RecordVersion: 1}
	assignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: task.ID, UserID: uuid.New()}
	assignments := newFakeAssignmentRepo(assignment)
	assignments.setLeadErr = errBoom
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(task), assignments)

	err := deps.ClaimAssignment(context.Background(), port.ClaimAssignmentInput{
		AssignmentID: assignment.ID.String(), TenantID: uuid.New().String(), UserID: assignment.UserID.String(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestClaimAssignment_TaskNotFound(t *testing.T) {
	assignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: uuid.New(), UserID: uuid.New()}
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(), newFakeAssignmentRepo(assignment))

	err := deps.ClaimAssignment(context.Background(), port.ClaimAssignmentInput{
		AssignmentID: assignment.ID.String(), TenantID: uuid.New().String(), UserID: assignment.UserID.String(),
	})
	require.Error(t, err)
}

func TestReassignAssignment_TaskFoundButCreateFails(t *testing.T) {
	taskID, oldUser := uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	oldAssignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: oldUser}
	assignments := newFakeAssignmentRepo(oldAssignment)
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(task), assignments)
	assignments.createErr = errBoom

	err := deps.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(), OldUserID: oldUser.String(),
		NewUserID: uuid.New().String(), AdminUserID: uuid.New().String(),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestUpdateTaskStatus_UpdateStatusFails(t *testing.T) {
	taskID := uuid.New()
	tasks := newFakeTaskRepo(&domain.Task{ID: taskID, RecordVersion: 1})
	tasks.updateStatusErr = errBoom
	deps, _ := newAssignmentTestDeps(tasks, newFakeAssignmentRepo())

	err := deps.UpdateTaskStatus(context.Background(), port.UpdateTaskStatusInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(), Status: domain.TaskStatusFailed, RecordVersion: 1,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestUpdateInstanceStatus_UpdateStatusFails(t *testing.T) {
	instanceID := uuid.New()
	deps, _ := newInstanceTestDeps(&domain.Instance{ID: instanceID, RecordVersion: 1}, nil, nil)
	deps.Instances.(*fakeInstanceRepo).updateStatusErr = errBoom

	err := deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
		InstanceID: instanceID.String(), TenantID: uuid.New().String(), Status: domain.InstanceStatusCompleted,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestFailActiveTasks_VacateFails(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	openTask := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, Status: domain.TaskStatusReady, RecordVersion: 1}
	tasks := newFakeTaskRepo(openTask)
	activeAssignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: openTask.ID, UserID: uuid.New()}
	assignments := newFakeAssignmentRepo(activeAssignment)
	assignments.vacateErr = errBoom
	deps, _ := newInstanceTestDeps(inst, tasks, assignments)

	err := deps.CancelInstance(context.Background(), port.CancelInstanceInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), AdminUserID: uuid.New().String(), RecordVersion: 1,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestFailActiveTasks_ListByInstanceFails(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	tasks := newFakeTaskRepo()
	tasks.listErr = errBoom
	deps, _ := newInstanceTestDeps(inst, tasks, newFakeAssignmentRepo())

	err := deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), Status: domain.InstanceStatusFailed,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestEnqueueInstanceEvent_OutboxEnqueueFails(t *testing.T) {
	instanceID := uuid.New()
	inst := &domain.Instance{ID: instanceID, RecordVersion: 1}
	deps, outbox := newInstanceTestDeps(inst, nil, nil)
	outbox.enqueueErr = errBoom

	err := deps.PauseInstance(context.Background(), port.PauseInstanceInput{
		InstanceID: instanceID.String(), TenantID: uuid.New().String(), AdminUserID: uuid.New().String(), RecordVersion: 1,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}

func TestEnqueueInstanceEvent_ValidatorRejection_PropagatesFromPauseInstance(t *testing.T) {
	instanceID := uuid.New()
	inst := &domain.Instance{ID: instanceID, RecordVersion: 1}
	deps := &outboundtemporal.Deps{
		Instances: newFakeInstanceRepo(inst), Outbox: &fakeOutbox{}, Transactor: fakeTransactor{}, Validator: erroringValidator{},
	}
	err := deps.PauseInstance(context.Background(), port.PauseInstanceInput{
		InstanceID: instanceID.String(), TenantID: uuid.New().String(), AdminUserID: uuid.New().String(), RecordVersion: 1,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBoom))
}
