package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// compiledPlanJSONWithDept is compiledPlanJSON's counterpart for tests that
// need a specific DepartmentDef (e.g. a real IAMDepartmentID) rather than
// the shared helper's fixed {ID: "finance", Label: "Finance"}.
func compiledPlanJSONWithDept(t *testing.T, dept dsl.DepartmentDef) string {
	t.Helper()
	collab := dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{dept},
		}},
	}
	b, err := json.Marshal(collab)
	require.NoError(t, err)
	return string(b)
}

func newDelegationReconcilerHarness() (*service.DelegationReconciler, *fakeInstanceRepo, *fakeTaskRepo, *fakeAssignmentRepo, *fakeOutbox, *fakeTemporalClient, *fakeDefinitionClient, *fakeEligibilityChecker) {
	instances := newFakeInstanceRepo()
	tasks := newFakeTaskRepo()
	assignments := newFakeAssignmentRepo()
	outbox := &fakeOutbox{}
	temporal := &fakeTemporalClient{}
	definitions := &fakeDefinitionClient{}
	eligibility := &fakeEligibilityChecker{}

	svc := &service.DelegationReconciler{
		Instances: instances, Tasks: tasks, Assignments: assignments,
		Outbox: outbox, Transactor: fakeTransactor{}, Temporal: temporal,
		Definitions: definitions, Eligibility: eligibility, Validator: noopValidator{},
	}
	return svc, instances, tasks, assignments, outbox, temporal, definitions, eligibility
}

// seedDelegationFixture wires one instance/task/assignment triple whose
// task NodeKey/DepartmentID resolve against the given compiled plan.
func seedDelegationFixture(instances *fakeInstanceRepo, tasks *fakeTaskRepo, assignments *fakeAssignmentRepo, definitions *fakeDefinitionClient, tenantID, delegatorID uuid.UUID, businessKey string, stage dsl.StageDef, t *testing.T) (*domain.Instance, *domain.Task, *domain.TaskAssignment) {
	versionID := uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, stage))

	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, WorkflowVersionID: versionID, BusinessKey: businessKey, TemporalWorkflowID: tenantID.String() + ":" + businessKey}
	instances.byID[inst.ID] = inst

	nodeKey := "finance/" + stage.NodeID
	task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: nodeKey, DepartmentID: uuid.Nil, RecordVersion: 1}
	tasks.byID[task.ID] = task

	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: delegatorID, IsActive: true}
	assignments.byID[a.ID] = a
	return inst, task, a
}

func TestDelegationReconciler_Reroute(t *testing.T) {
	t.Run("scope all reassigns an eligible row and tags it", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, temporal, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, task, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all",
		})
		require.NoError(t, err)
		assert.False(t, assignments.byID[a.ID].IsActive)
		require.Len(t, outbox.enqueued, 1)
		require.Len(t, temporal.signals, 1)
		assert.Equal(t, port.SignalInstanceReassign, temporal.signals[0].SignalName)

		var found *domain.TaskAssignment
		for _, na := range assignments.byID {
			if na.TaskID == task.ID && na.UserID == delegateID {
				found = na
			}
		}
		require.NotNil(t, found)
		assert.Equal(t, "delegation:"+delegationID.String(), found.Reason)
	})

	t.Run("ineligible delegate holds the row at the delegator", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, temporal, definitions, eligibility := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		eligibility.batchCheck = func(context.Context, []port.EligibilityCheckRequest, uuid.UUID) ([]bool, error) {
			return []bool{false}, nil
		}

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all",
		})
		require.NoError(t, err)
		assert.True(t, assignments.byID[a.ID].IsActive, "an ineligible delegate must not have the row rerouted to them")
		assert.Empty(t, outbox.enqueued)
		assert.Empty(t, temporal.signals)
	})

	t.Run("department scope excludes a row from a different department", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)

		otherDept := uuid.New().String()
		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "department", ScopeID: &otherDept,
		})
		require.NoError(t, err)
		assert.True(t, assignments.byID[a.ID].IsActive, "a row outside the given department scope must survive untouched")
		assert.Empty(t, outbox.enqueued)
	})

	t.Run("department scope matches a task by its real IAMDepartmentID", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		versionID := uuid.New()
		financeIAMDeptID := uuid.New().String()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSONWithDept(t,
			dsl.DepartmentDef{ID: "finance", IAMDepartmentID: financeIAMDeptID, Stages: []dsl.StageDef{{Type: "userTask", NodeID: "review", Role: "reviewer"}}}))

		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, WorkflowVersionID: versionID, BusinessKey: "biz-1", TemporalWorkflowID: tenantID.String() + ":biz-1"}
		instances.byID[inst.ID] = inst
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: "finance/review",
			DepartmentID: uuid.MustParse(financeIAMDeptID), RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: delegatorID, IsActive: true}
		assignments.byID[a.ID] = a

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "department", ScopeID: &financeIAMDeptID,
		})
		require.NoError(t, err)
		assert.False(t, assignments.byID[a.ID].IsActive)
		require.Len(t, outbox.enqueued, 1)
	})

	t.Run("default scope matches on business_key", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "tender-42", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)

		businessKey := "tender-42"
		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "tender", ScopeID: &businessKey,
		})
		require.NoError(t, err)
		assert.False(t, assignments.byID[a.ID].IsActive)
		require.Len(t, outbox.enqueued, 1)
	})

	t.Run("zero matching assignments is a valid, non-error outcome", func(t *testing.T) {
		svc, _, _, _, _, _, _, _ := newDelegationReconcilerHarness()
		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: uuid.New(), DelegationID: uuid.New(), DelegatorID: uuid.New(), DelegateID: uuid.New(), Scope: "all",
		})
		require.NoError(t, err)
	})
}

func TestDelegationReconciler_Reverse(t *testing.T) {
	t.Run("restores the eligible original assignee for a tagged row", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, temporal, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, task, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegateID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		a.Reason = "delegation:" + delegationID.String()

		err := svc.Reverse(context.Background(), port.DelegationReversalInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, EndedReason: "expired",
		})
		require.NoError(t, err)
		assert.False(t, assignments.byID[a.ID].IsActive)
		require.Len(t, outbox.enqueued, 1)
		require.Len(t, temporal.signals, 1)

		var restored *domain.TaskAssignment
		for _, na := range assignments.byID {
			if na.TaskID == task.ID && na.UserID == delegatorID {
				restored = na
			}
		}
		require.NotNil(t, restored)
	})

	t.Run("untagged assignment is left untouched", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegateID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		a.Reason = "manual-assign"

		err := svc.Reverse(context.Background(), port.DelegationReversalInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, EndedReason: "expired",
		})
		require.NoError(t, err)
		assert.True(t, assignments.byID[a.ID].IsActive)
		assert.Empty(t, outbox.enqueued)
	})

	t.Run("ineligible original assignee stays held with the delegate", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _, definitions, eligibility := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegateID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		a.Reason = "delegation:" + delegationID.String()
		eligibility.batchCheck = func(context.Context, []port.EligibilityCheckRequest, uuid.UUID) ([]bool, error) {
			return []bool{false}, nil
		}

		err := svc.Reverse(context.Background(), port.DelegationReversalInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, EndedReason: "expired",
		})
		require.NoError(t, err)
		assert.True(t, assignments.byID[a.ID].IsActive, "an ineligible original assignee must not have the row restored to them")
	})

	t.Run("ListActiveByUser error propagates", func(t *testing.T) {
		svc, _, _, assignments, _, _, _, _ := newDelegationReconcilerHarness()
		assignments.listActiveByUserErr = assert.AnError
		err := svc.Reverse(context.Background(), port.DelegationReversalInput{TenantID: uuid.New(), DelegationID: uuid.New(), DelegatorID: uuid.New(), DelegateID: uuid.New()})
		assert.Error(t, err)
	})
}

func TestDelegationReconciler_Reroute_ListActiveByUserError(t *testing.T) {
	svc, _, _, assignments, _, _, _, _ := newDelegationReconcilerHarness()
	assignments.listActiveByUserErr = assert.AnError
	err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: uuid.New(), DelegationID: uuid.New(), DelegatorID: uuid.New(), DelegateID: uuid.New(), Scope: "all"})
	assert.Error(t, err)
}

func TestDelegationReconciler_ResolveCandidates(t *testing.T) {
	t.Run("an unreadable instance is logged and skipped, not a batch failure", func(t *testing.T) {
		svc, _, tasks, assignments, _, _, _, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: delegatorID, IsActive: true}
		assignments.byID[a.ID] = a

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all"})
		require.NoError(t, err)
		assert.True(t, assignments.byID[a.ID].IsActive, "an unreadable instance leaves the row untouched")
	})
}

func TestDelegationReconciler_EligibleCandidates(t *testing.T) {
	t.Run("an unreadable compiled plan holds the row, doesn't fail the batch", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		definitions.err = assert.AnError

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all"})
		require.NoError(t, err)
		assert.True(t, assignments.byID[a.ID].IsActive)
		assert.Empty(t, outbox.enqueued)
	})

	t.Run("an unmarshalable compiled plan holds the row, doesn't fail the batch", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		definitions.resp.CompiledPlanJSON = "not json"

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all"})
		require.NoError(t, err)
		assert.True(t, assignments.byID[a.ID].IsActive)
	})

	t.Run("a main plan missing from the collaboration holds the row, doesn't fail the batch", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		definitions.resp.CompiledPlanJSON = `{"main_plan":"does-not-exist","plans":[]}`

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all"})
		require.NoError(t, err)
		assert.True(t, assignments.byID[a.ID].IsActive)
	})

	t.Run("a task whose department has no matching stage holds the row", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
		_, task, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		task.NodeKey = "finance/no-such-stage"

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all"})
		require.NoError(t, err)
		assert.True(t, assignments.byID[a.ID].IsActive)
	})

	t.Run("a task in a department absent from the compiled plan holds the row", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
		_, task, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		task.DepartmentID = uuid.New()

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all"})
		require.NoError(t, err)
		assert.True(t, assignments.byID[a.ID].IsActive)
	})

	t.Run("a CheckEligibilityBatch failure propagates", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _, definitions, eligibility := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
		seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		eligibility.batchCheck = func(context.Context, []port.EligibilityCheckRequest, uuid.UUID) ([]bool, error) {
			return nil, assert.AnError
		}

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all"})
		assert.Error(t, err)
	})
}

func TestDelegationReconciler_CommitReassignments(t *testing.T) {
	t.Run("a Vacate failure propagates and rolls back the batch", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
		seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		assignments.vacateErr = assert.AnError

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all"})
		assert.Error(t, err)
		assert.Empty(t, outbox.enqueued)
	})

	t.Run("a Create failure propagates and rolls back the batch", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
		seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		assignments.createErr = assert.AnError

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all"})
		assert.Error(t, err)
	})

	t.Run("a BuildEnvelope failure propagates and rolls back the batch", func(t *testing.T) {
		instances, tasks, assignments := newFakeInstanceRepo(), newFakeTaskRepo(), newFakeAssignmentRepo()
		outbox, temporal, definitions, eligibility := &fakeOutbox{}, &fakeTemporalClient{}, &fakeDefinitionClient{}, &fakeEligibilityChecker{}
		svc := &service.DelegationReconciler{
			Instances: instances, Tasks: tasks, Assignments: assignments,
			Outbox: outbox, Transactor: fakeTransactor{}, Temporal: temporal,
			Definitions: definitions, Eligibility: eligibility, Validator: failingValidator{},
		}
		tenantID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
		seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all"})
		assert.Error(t, err)
		assert.Empty(t, outbox.enqueued)
	})

	t.Run("an Outbox.Enqueue failure propagates and rolls back the batch", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
		seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		outbox.enqueueErr = assert.AnError

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all"})
		assert.Error(t, err)
	})

	t.Run("a post-commit signal failure is logged, not returned, DB state already updated", func(t *testing.T) {
		svc, instances, tasks, assignments, _, temporal, definitions, _ := newDelegationReconcilerHarness()
		log := &fakeLogger{}
		svc.Log = log
		temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return assert.AnError }
		tenantID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all"})
		require.NoError(t, err)
		assert.False(t, assignments.byID[a.ID].IsActive, "DB state already committed despite the signal failure")
		assert.NotEmpty(t, log.warnCalls)
	})
}
