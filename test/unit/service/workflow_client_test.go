package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

func newWorkflowClientHarness() (*service.WorkflowClient, *fakeInstanceRepo, *fakeTaskRepo, *fakeAssignmentRepo, *fakeTemporalClient) {
	instances := newFakeInstanceRepo()
	tasks := newFakeTaskRepo()
	assignments := newFakeAssignmentRepo()
	temporal := &fakeTemporalClient{}
	return &service.WorkflowClient{Instances: instances, Tasks: tasks, Assignments: assignments, Temporal: temporal}, instances, tasks, assignments, temporal
}

func TestWorkflowClient_ReassignDelegate(t *testing.T) {
	t.Run("reassigns every active assignment, no delegation filter", func(t *testing.T) {
		svc, instances, tasks, assignments, temporal := newWorkflowClientHarness()
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.False(t, assignments.byID[a.ID].IsActive, "the old assignment must be vacated")
		require.Len(t, temporal.signals, 1)
		assert.Equal(t, port.SignalInstanceReassign, temporal.signals[0].SignalName)

		var foundNew bool
		for _, na := range assignments.byID {
			if na.UserID == newDelegate && na.TaskID == task.ID {
				foundNew = true
			}
		}
		assert.True(t, foundNew, "a new assignment for the new delegate must be created")
	})

	t.Run("delegation_id filters to only that delegation's tagged rows", func(t *testing.T) {
		svc, _, tasks, assignments, _ := newWorkflowClientHarness()
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		delegationID := uuid.New()

		taggedTask := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
		tasks.byID[taggedTask.ID] = taggedTask
		tagged := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: taggedTask.ID, UserID: oldDelegate, IsActive: true, Reason: "delegation:" + delegationID.String()}
		assignments.byID[tagged.ID] = tagged

		untaggedTask := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
		tasks.byID[untaggedTask.ID] = untaggedTask
		untagged := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: untaggedTask.ID, UserID: oldDelegate, IsActive: true, Reason: "manual-assign"}
		assignments.byID[untagged.ID] = untagged

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{
			TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate, DelegationID: &delegationID,
		})
		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.False(t, assignments.byID[tagged.ID].IsActive)
		assert.True(t, assignments.byID[untagged.ID].IsActive, "an untagged assignment must survive an explicit delegation_id filter")
	})

	t.Run("zero matching assignments is a valid, non-error outcome", func(t *testing.T) {
		svc, _, _, _, _ := newWorkflowClientHarness()
		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: uuid.New(), OldDelegateID: uuid.New(), NewDelegateID: uuid.New()})
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("ListActiveByUser error propagates", func(t *testing.T) {
		svc, _, _, assignments, _ := newWorkflowClientHarness()
		assignments.listActiveByUserErr = assert.AnError
		_, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: uuid.New(), OldDelegateID: uuid.New(), NewDelegateID: uuid.New()})
		assert.Error(t, err)
	})

	t.Run("an unreadable task is logged and skipped, not counted", func(t *testing.T) {
		svc, _, _, assignments, _ := newWorkflowClientHarness()
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: uuid.New(), UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err)
		assert.Zero(t, count)
	})

	t.Run("a vacate failure is logged and skipped, not counted", func(t *testing.T) {
		svc, instances, tasks, assignments, _ := newWorkflowClientHarness()
		log := &fakeLogger{}
		svc.Log = log
		assignments.vacateErr = assert.AnError
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err)
		assert.Zero(t, count)
		assert.NotEmpty(t, log.warnCalls)
	})

	t.Run("a create failure is logged and skipped, not counted", func(t *testing.T) {
		svc, instances, tasks, assignments, _ := newWorkflowClientHarness()
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a
		assignments.createErr = assert.AnError

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err)
		assert.Zero(t, count)
		assert.False(t, assignments.byID[a.ID].IsActive, "the old assignment is already vacated even though the replacement failed to create")
	})

	t.Run("a signal failure is logged but still counted, DB state already updated", func(t *testing.T) {
		svc, instances, tasks, assignments, temporal := newWorkflowClientHarness()
		log := &fakeLogger{}
		svc.Log = log
		temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return assert.AnError }
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.NotEmpty(t, log.warnCalls)
	})

	// Regression tests for the eligibility/liveness gap found reviewing this
	// path: unlike DelegationReconciler's Reroute/Reverse, this bulk
	// delegate-to-delegate reassignment never checked the new delegate at all.
	t.Run("eligible new delegate succeeds", func(t *testing.T) {
		svc, instances, tasks, assignments, temporal := newWorkflowClientHarness()
		definitions := &fakeDefinitionClient{}
		svc.Definitions = definitions
		svc.Eligibility = &fakeEligibilityChecker{check: func(context.Context, uuid.UUID, uuid.UUID, string, uuid.UUID) (bool, error) { return true, nil }}

		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		instanceID, versionID := uuid.New(), uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, dsl.StageDef{NodeID: "review", Role: "reviewer"}))
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, WorkflowVersionID: versionID, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/review", RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err)
		assert.Equal(t, 1, count)
		require.Len(t, temporal.signals, 1)
	})

	t.Run("ineligible new delegate's row is held, not counted", func(t *testing.T) {
		svc, instances, tasks, assignments, temporal := newWorkflowClientHarness()
		definitions := &fakeDefinitionClient{}
		svc.Definitions = definitions
		svc.Eligibility = &fakeEligibilityChecker{check: func(context.Context, uuid.UUID, uuid.UUID, string, uuid.UUID) (bool, error) { return false, nil }}

		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		instanceID, versionID := uuid.New(), uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, dsl.StageDef{NodeID: "review", Role: "reviewer"}))
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, WorkflowVersionID: versionID, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/review", RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err, "an ineligible row is held, not a whole-call error")
		assert.Zero(t, count)
		assert.True(t, assignments.byID[a.ID].IsActive, "an ineligible new delegate must not have the row reassigned to them")
		assert.Empty(t, temporal.signals)
	})

	t.Run("eligibility check error on one task is logged and skipped, not counted", func(t *testing.T) {
		svc, instances, tasks, assignments, temporal := newWorkflowClientHarness()
		definitions := &fakeDefinitionClient{}
		svc.Definitions = definitions
		svc.Eligibility = &fakeEligibilityChecker{check: func(context.Context, uuid.UUID, uuid.UUID, string, uuid.UUID) (bool, error) {
			return false, assert.AnError
		}}

		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		instanceID, versionID := uuid.New(), uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, dsl.StageDef{NodeID: "review", Role: "reviewer"}))
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, WorkflowVersionID: versionID, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/review", RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err)
		assert.Zero(t, count)
		assert.True(t, assignments.byID[a.ID].IsActive)
		assert.Empty(t, temporal.signals)
	})

	t.Run("Instances.GetByID error during eligibility check is logged and skipped", func(t *testing.T) {
		svc, _, tasks, assignments, _ := newWorkflowClientHarness()
		svc.Definitions = &fakeDefinitionClient{}
		svc.Eligibility = &fakeEligibilityChecker{}

		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		// task.WorkflowInstanceID has no matching entry in instances.byID.
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New(), NodeKey: "finance/review", RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err)
		assert.Zero(t, count)
		assert.True(t, assignments.byID[a.ID].IsActive)
	})

	t.Run("node not found in compiled plan is treated as ineligible, not a crash", func(t *testing.T) {
		svc, instances, tasks, assignments, _ := newWorkflowClientHarness()
		definitions := &fakeDefinitionClient{}
		svc.Definitions = definitions
		svc.Eligibility = &fakeEligibilityChecker{check: func(context.Context, uuid.UUID, uuid.UUID, string, uuid.UUID) (bool, error) {
			t.Fatal("CheckEligibility must not be called when the node has no matching compiled-plan stage")
			return false, nil
		}}

		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		instanceID, versionID := uuid.New(), uuid.New()
		// The compiled plan has no stage at all, so requiredLevelForTask can't
		// resolve a role for the task's "finance/review" node.
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t))
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, WorkflowVersionID: versionID, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/review", RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err)
		assert.Zero(t, count, "an unresolvable node is fail-closed, matching DelegationReconciler's own posture")
		assert.True(t, assignments.byID[a.ID].IsActive)
	})

	t.Run("liveness stub error fails open for the whole call, matching self-service semantics", func(t *testing.T) {
		svc, instances, tasks, assignments, temporal := newWorkflowClientHarness()
		svc.IAM = &fakeIAMClient{err: errors.New("iam client: user-status endpoint contract not yet confirmed")}

		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err, "an unresolved liveness check must not block the reassignment")
		assert.Equal(t, 1, count)
		require.Len(t, temporal.signals, 1)
	})

	t.Run("a confirmed-unavailable new delegate blocks the whole call", func(t *testing.T) {
		svc, _, _, assignments, _ := newWorkflowClientHarness()
		svc.IAM = &fakeIAMClient{status: port.UserStatus{IsDeleted: true}}
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		assert.ErrorIs(t, err, port.ErrAssigneeUnavailable)
		assert.Zero(t, count)
		assert.Empty(t, assignments.byID, "must not even list/attempt assignments once the new delegate is confirmed unavailable")
	})
}

func TestWorkflowClient_CancelByDelegate(t *testing.T) {
	svc, _, tasks, assignments, _ := newWorkflowClientHarness()
	tenantID, delegate := uuid.New(), uuid.New()
	task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New()}
	tasks.byID[task.ID] = task
	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: delegate, IsActive: true}
	assignments.byID[a.ID] = a

	count, err := svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate})
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.False(t, assignments.byID[a.ID].IsActive)
}

func TestWorkflowClient_CancelByDelegate_Errors(t *testing.T) {
	t.Run("ListActiveByUser error propagates", func(t *testing.T) {
		svc, _, _, assignments, _ := newWorkflowClientHarness()
		assignments.listActiveByUserErr = assert.AnError
		_, err := svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: uuid.New(), DelegateUserID: uuid.New()})
		assert.Error(t, err)
	})

	t.Run("a vacate failure is logged and skipped, not counted", func(t *testing.T) {
		svc, _, tasks, assignments, _ := newWorkflowClientHarness()
		log := &fakeLogger{}
		svc.Log = log
		assignments.vacateErr = assert.AnError
		tenantID, delegate := uuid.New(), uuid.New()
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New()}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: delegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate})
		require.NoError(t, err)
		assert.Zero(t, count)
		assert.NotEmpty(t, log.warnCalls)
	})
}

func TestWorkflowClient_DelegateImpact_Errors(t *testing.T) {
	t.Run("ListActiveByUser error propagates", func(t *testing.T) {
		svc, _, _, assignments, _ := newWorkflowClientHarness()
		assignments.listActiveByUserErr = assert.AnError
		_, err := svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: uuid.New(), DelegateUserID: uuid.New()})
		assert.Error(t, err)
	})

	t.Run("an unreadable task is logged and skipped from the preview", func(t *testing.T) {
		svc, _, _, assignments, _ := newWorkflowClientHarness()
		log := &fakeLogger{}
		svc.Log = log
		tenantID, delegate := uuid.New(), uuid.New()
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: uuid.New(), UserID: delegate, IsActive: true}
		assignments.byID[a.ID] = a

		result, err := svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: tenantID, DelegateUserID: delegate})
		require.NoError(t, err)
		assert.Empty(t, result.WorkflowIDs.Items)
		assert.NotEmpty(t, log.warnCalls)
	})

	t.Run("Page.Limit caps the previewed workflow IDs", func(t *testing.T) {
		svc, _, tasks, assignments, _ := newWorkflowClientHarness()
		tenantID, delegate := uuid.New(), uuid.New()
		for i := 0; i < 3; i++ {
			task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New()}
			tasks.byID[task.ID] = task
			assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: delegate, IsActive: true}
		}

		result, err := svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: tenantID, DelegateUserID: delegate, Page: port.Page{Limit: 2}})
		require.NoError(t, err)
		assert.Len(t, result.WorkflowIDs.Items, 2)
	})
}

func TestWorkflowClient_DelegateImpact(t *testing.T) {
	svc, _, tasks, assignments, _ := newWorkflowClientHarness()
	tenantID, delegate := uuid.New(), uuid.New()
	instanceID := uuid.New()

	// Two tasks on the same instance must dedupe to one workflow ID.
	taskA := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
	taskB := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
	tasks.byID[taskA.ID] = taskA
	tasks.byID[taskB.ID] = taskB
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: taskA.ID, UserID: delegate, IsActive: true}
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: taskB.ID, UserID: delegate, IsActive: true}

	result, err := svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: tenantID, DelegateUserID: delegate, Page: port.Page{Limit: 10}})
	require.NoError(t, err)
	assert.Equal(t, 2, result.ReassignedCount)
	require.Len(t, result.WorkflowIDs.Items, 1, "two tasks on the same instance must dedupe to one workflow id")
	assert.Equal(t, instanceID, result.WorkflowIDs.Items[0])
}

// TestWorkflowClient_DelegateImpact_ChainedDelegate proves the delegate-
// centric scoping documented on DelegateImpact itself: previewing impact for
// a delegate who has themselves delegated onward (A -> B -> C) only ever
// reflects C's own current holdings, never chasing back to A's original
// grant.
func TestWorkflowClient_DelegateImpact_ChainedDelegate(t *testing.T) {
	svc, _, tasks, assignments, _ := newWorkflowClientHarness()
	tenantID := uuid.New()
	grantorA, delegateB, delegateC := uuid.New(), uuid.New(), uuid.New()
	delegationID1, delegationID2 := uuid.New(), uuid.New()

	// A -> B: vacated, no longer an active assignment for B.
	taskAB := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New()}
	tasks.byID[taskAB.ID] = taskAB
	assignments.byID[uuid.New()] = &domain.TaskAssignment{
		ID: uuid.New(), TenantID: tenantID, TaskID: taskAB.ID, UserID: delegateB,
		IsActive: false, Reason: "delegation:" + delegationID1.String(),
	}

	// B -> C: the only currently active assignment in this chain.
	taskBC := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New()}
	tasks.byID[taskBC.ID] = taskBC
	assignments.byID[uuid.New()] = &domain.TaskAssignment{
		ID: uuid.New(), TenantID: tenantID, TaskID: taskBC.ID, UserID: delegateC,
		IsActive: true, Reason: "delegation:" + delegationID2.String(),
	}

	resultForC, err := svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: tenantID, DelegateUserID: delegateC, Page: port.Page{Limit: 10}})
	require.NoError(t, err)
	assert.Equal(t, 1, resultForC.ReassignedCount, "C's preview must only reflect C's own current holdings")
	require.Len(t, resultForC.WorkflowIDs.Items, 1)
	assert.Equal(t, taskBC.WorkflowInstanceID, resultForC.WorkflowIDs.Items[0])

	resultForA, err := svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: tenantID, DelegateUserID: grantorA, Page: port.Page{Limit: 10}})
	require.NoError(t, err)
	assert.Zero(t, resultForA.ReassignedCount, "the original grantor's preview must not chase the chain forward to C")
	assert.Empty(t, resultForA.WorkflowIDs.Items)
}
