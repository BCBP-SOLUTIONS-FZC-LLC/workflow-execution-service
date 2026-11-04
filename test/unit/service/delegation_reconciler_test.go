package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
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
		Definitions: definitions, Eligibility: eligibility, Validator: realValidator(),
		Availability: newFakeUserAvailability(), Locks: &fakeLocker{},
	}
	return svc, instances, tasks, assignments, outbox, temporal, definitions, eligibility
}

// seedDelegationFixture wires one instance/task/assignment triple whose
// task NodeKey/DepartmentID resolve against the given compiled plan.
func seedDelegationFixture(instances *fakeInstanceRepo, tasks *fakeTaskRepo, assignments *fakeAssignmentRepo, definitions *fakeDefinitionClient, tenantID, delegatorID uuid.UUID, businessKey string, stage dsl.StageDef, t *testing.T) (*domain.Instance, *domain.Task, *domain.TaskAssignment) {
	versionID := uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, stage))

	subjectID := "subject-of-" + businessKey
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: subjectID, BusinessKey: businessKey, TemporalWorkflowID: tenantID.String() + ":" + subjectID + ":" + businessKey}
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

	t.Run("a failed eligibility read fails the reroute so the event is retried, and asks for the delegate's tenant", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, temporal, definitions, eligibility := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		var askedTenant uuid.UUID
		eligibility.batchCheck = func(_ context.Context, tenant uuid.UUID, _ []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
			askedTenant = tenant
			return []port.EligibilityResult{{Err: assert.AnError}}, nil
		}

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all",
		})
		require.ErrorIs(t, err, assert.AnError)
		assert.Equal(t, tenantID, askedTenant)
		assert.True(t, assignments.byID[a.ID].IsActive, "nothing is rerouted while the read is unavailable")
		assert.Empty(t, outbox.enqueued)
		assert.Empty(t, temporal.signals)
	})

	t.Run("one read per distinct department and level, answered back to every row of the pair", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, temporal, definitions, eligibility := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		inst, _, _ := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-pairs", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		definitions.resp = publishedCompiledWorkflow(uuid.New(), inst.WorkflowVersionID, compiledPlanJSON(t,
			dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"},
			dsl.StageDef{Type: "userTask", NodeID: "approve", Role: "approver"}))
		rows := map[string]*domain.TaskAssignment{}
		for _, spec := range []struct{ name, node string }{{"review-2", "review"}, {"review-3", "review"}, {"approve-1", "approve"}, {"approve-2", "approve"}} {
			task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: "finance/" + spec.node, RecordVersion: 1}
			tasks.byID[task.ID] = task
			a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: delegatorID, IsActive: true}
			assignments.byID[a.ID] = a
			rows[spec.name] = a
		}
		var asked []port.EligibilityCheckRequest
		eligibility.batchCheck = func(_ context.Context, _ uuid.UUID, reqs []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
			asked = reqs
			out := make([]port.EligibilityResult, len(reqs))
			for i, r := range reqs {
				out[i] = port.EligibilityResult{Eligible: r.RequiredLevel == "reviewer"}
			}
			return out, nil
		}

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all",
		})

		require.NoError(t, err)
		require.Len(t, asked, 2, "five rows of two distinct pairs are two reads")
		assert.ElementsMatch(t, []string{"reviewer", "approver"}, []string{asked[0].RequiredLevel, asked[1].RequiredLevel})
		assert.False(t, assignments.byID[rows["review-2"].ID].IsActive, "a reviewer row follows its pair's answer")
		assert.False(t, assignments.byID[rows["review-3"].ID].IsActive)
		assert.True(t, assignments.byID[rows["approve-1"].ID].IsActive, "an approver row is held when its pair is ineligible")
		assert.True(t, assignments.byID[rows["approve-2"].ID].IsActive)
		assert.Len(t, outbox.enqueued, 3, "the seeded row and the two other reviewer rows are rerouted")
		assert.Len(t, temporal.signals, 3)
	})

	t.Run("ineligible delegate holds the row at the delegator", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, temporal, definitions, eligibility := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		eligibility.batchCheck = func(context.Context, uuid.UUID, []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
			return []port.EligibilityResult{{Eligible: false}}, nil
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

	t.Run("department scope with an unreadable compiled plan is logged and excludes the row", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		log := &fakeLogger{}
		svc.Log = log
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		definitions.err = assert.AnError

		deptID := uuid.New().String()
		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "department", ScopeID: &deptID,
		})
		require.NoError(t, err)
		assert.True(t, assignments.byID[a.ID].IsActive, "a row whose compiled plan can't be read must survive untouched")
		assert.Empty(t, outbox.enqueued)
		assert.Equal(t, 1, warnsWithCause(log, assert.AnError.Error()), "the cause is logged once")
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

	t.Run("a subject-scoped delegation matches on subject_id", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "tender-42", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)

		subjectID := "subject-of-tender-42"
		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "tender", ScopeID: &subjectID,
		})
		require.NoError(t, err)
		assert.False(t, assignments.byID[a.ID].IsActive)
		require.Len(t, outbox.enqueued, 1)
	})

	t.Run("a subject-scoped delegation does not match on the business key", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "tender-42", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)

		businessKey := "tender-42"
		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "tender", ScopeID: &businessKey,
		})
		require.NoError(t, err)
		assert.True(t, assignments.byID[a.ID].IsActive, "the key identifies an instance within a subject, not the subject")
		assert.Empty(t, outbox.enqueued)
	})

	t.Run("a scope this service has no case for matches nothing and is counted", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "tender-42", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)

		subjectID := "subject-of-tender-42"
		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID,
			Scope: "contract", ScopeID: &subjectID,
		})
		require.NoError(t, err)
		assert.True(t, assignments.byID[a.ID].IsActive, "a scope with no case here must not move the delegator's work")
		assert.Empty(t, outbox.enqueued)
	})

	t.Run("adopting the delegate's existing row surfaces a read failure", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, task, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "tender-42", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		a.IsLead = true
		existing := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: delegateID, IsActive: true}
		assignments.byID[existing.ID] = existing
		assignments.listActiveByTaskErr = errors.New("boom")

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all",
		})
		require.Error(t, err, "the claim cannot be moved onto a row the reconciler could not read")
		assert.Empty(t, outbox.enqueued)
	})

	t.Run("a delegated task keeps its lead", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, task, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "tender-42", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		a.IsLead = true

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all",
		})
		require.NoError(t, err)

		var delegateAssignment *domain.TaskAssignment
		for _, got := range assignments.byID {
			if got.TaskID == task.ID && got.UserID == delegateID && got.IsActive {
				delegateAssignment = got
			}
		}
		require.NotNil(t, delegateAssignment, "the delegate must hold the assignment")
		assert.True(t, delegateAssignment.IsLead,
			"an assignee_mode=all task with no lead row is actionable by nobody, the delegate included")
		require.Len(t, outbox.enqueued, 1)
	})

	t.Run("a delegate already assigned to the task keeps one row and inherits the claim", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, task, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "tender-42", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		a.IsLead = true
		// the delegate is already a co-assignee — the likeliest shape for the
		// multi-assignee task a lead applies to at all
		existing := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: delegateID, IsActive: true}
		assignments.byID[existing.ID] = existing

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all",
		})
		require.NoError(t, err, "a delegate who already holds the task must not fail the whole reroute")

		activeForDelegate := 0
		for _, got := range assignments.byID {
			if got.TaskID == task.ID && got.UserID == delegateID && got.IsActive {
				activeForDelegate++
			}
		}
		assert.Equal(t, 1, activeForDelegate, "the row they already hold stands; no second is inserted")
		assert.True(t, existing.IsLead, "the claim moves onto the row they already hold")
		require.Len(t, outbox.enqueued, 1)
	})

	t.Run("a delegated task the delegator had not claimed gains no lead", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, task, _ := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "tender-42", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all",
		})
		require.NoError(t, err)

		for _, got := range assignments.byID {
			if got.TaskID == task.ID && got.UserID == delegateID && got.IsActive {
				assert.False(t, got.IsLead, "delegation must not invent a claim the delegator never made")
			}
		}
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
		eligibility.batchCheck = func(context.Context, uuid.UUID, []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
			return []port.EligibilityResult{{Eligible: false}}, nil
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
		log := &fakeLogger{}
		svc.Log = log

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all"})
		require.NoError(t, err)
		assert.True(t, assignments.byID[a.ID].IsActive)
		assert.Equal(t, 1, warnsWithCause(log, "invalid character"), "the decode cause is logged once")
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
		eligibility.batchCheck = func(context.Context, uuid.UUID, []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
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
			Availability: newFakeUserAvailability(),
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

func TestDelegationReconciler_Reverse_HoldsWorkForAnOutOfOfficeDelegator(t *testing.T) {
	setup := func(t *testing.T) (*service.DelegationReconciler, *service.OOOAvailabilityReconciler, *fakeTemporalClient, port.DelegationReversalInput, uuid.UUID) {
		svc, instances, tasks, assignments, _, temporal, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		inst, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegateID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		inst.Status = domain.InstanceStatusRunning
		a.Reason = "delegation:" + delegationID.String()
		store := newFakeUserAvailability()
		svc.Availability = store
		ooo := &service.OOOAvailabilityReconciler{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal, Availability: store}
		return svc, ooo, temporal, port.DelegationReversalInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, EndedReason: "delegate_disabled",
		}, inst.ID
	}
	pauses := func(temporal *fakeTemporalClient) int {
		count := 0
		for _, s := range temporal.signals {
			if s.SignalName == port.SignalInstancePause {
				count++
			}
		}
		return count
	}

	t.Run("a delegator who is out of office gets the work back paused", func(t *testing.T) {
		svc, ooo, temporal, in, _ := setup(t)
		delegate := in.DelegateID
		require.NoError(t, ooo.Apply(context.Background(), port.UserAvailabilityInput{
			TenantID: in.TenantID, UserID: in.DelegatorID, Status: "ooo", DelegateUserID: &delegate,
		}))
		require.Zero(t, pauses(temporal), "an out-of-office user with a delegate is not paused while the delegation covers the work")

		require.NoError(t, svc.Reverse(context.Background(), in))

		require.Equal(t, 1, pauses(temporal), "the work handed back to an out-of-office delegator must be held")
		for _, s := range temporal.signals {
			if s.SignalName == port.SignalInstancePause {
				b, err := json.Marshal(s.Payload)
				require.NoError(t, err)
				assert.Contains(t, string(b), `"Initiator":"`+domain.InitiatorOOO+`"`)
			}
		}
	})

	t.Run("a delegator who is available gets the work back running", func(t *testing.T) {
		svc, _, temporal, in, _ := setup(t)
		require.NoError(t, svc.Reverse(context.Background(), in))
		assert.Zero(t, pauses(temporal))
	})

	t.Run("a delegator who came back before the delegation ended gets the work back running", func(t *testing.T) {
		svc, ooo, temporal, in, _ := setup(t)
		delegate := in.DelegateID
		require.NoError(t, ooo.Apply(context.Background(), port.UserAvailabilityInput{
			TenantID: in.TenantID, UserID: in.DelegatorID, Status: "ooo", DelegateUserID: &delegate,
		}))
		require.NoError(t, ooo.Apply(context.Background(), port.UserAvailabilityInput{TenantID: in.TenantID, UserID: in.DelegatorID, Status: "available"}))

		require.NoError(t, svc.Reverse(context.Background(), in))

		assert.Zero(t, pauses(temporal))
	})
}

func TestDelegationReconciler_Reverse_AFailedAvailabilityReadChangesNothing(t *testing.T) {
	svc, instances, tasks, assignments, outbox, temporal, definitions, _ := newDelegationReconcilerHarness()
	store := newFakeUserAvailability()
	store.getErr = assert.AnError
	svc.Availability = store
	tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegateID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
	a.Reason = "delegation:" + delegationID.String()

	err := svc.Reverse(context.Background(), port.DelegationReversalInput{
		TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, EndedReason: "expired",
	})

	require.ErrorIs(t, err, assert.AnError, "the event is retried, so nothing may be half applied")
	assert.True(t, assignments.byID[a.ID].IsActive)
	assert.Empty(t, outbox.enqueued)
	assert.Empty(t, temporal.signals)
}

func TestDelegationReconciler_Reverse_PausesAnInstanceOnceForTwoRestoredTasks(t *testing.T) {
	svc, instances, tasks, assignments, _, temporal, definitions, _ := newDelegationReconcilerHarness()
	store := newFakeUserAvailability()
	svc.Availability = store
	tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	inst, _, first := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegateID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
	inst.Status = domain.InstanceStatusRunning
	first.Reason = "delegation:" + delegationID.String()
	secondTask := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: tasks.byID[first.TaskID].NodeKey, RecordVersion: 1}
	tasks.byID[secondTask.ID] = secondTask
	second := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: secondTask.ID, UserID: delegateID, IsActive: true, Reason: "delegation:" + delegationID.String()}
	assignments.byID[second.ID] = second
	require.NoError(t, store.byRecord(tenantID, delegatorID, "ooo"))

	require.NoError(t, svc.Reverse(context.Background(), port.DelegationReversalInput{
		TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, EndedReason: "expired",
	}))

	pauses := 0
	for _, s := range temporal.signals {
		if s.SignalName == port.SignalInstancePause {
			pauses++
		}
	}
	assert.Equal(t, 1, pauses, "two restored tasks of one instance pause the instance once")
}

func TestDelegationReconciler_Reverse_AReassignedDelegationPausesNothing(t *testing.T) {
	svc, instances, tasks, assignments, _, temporal, definitions, _ := newDelegationReconcilerHarness()
	store := newFakeUserAvailability()
	svc.Availability = store
	tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	inst, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegateID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
	inst.Status = domain.InstanceStatusRunning
	a.Reason = "delegation:" + delegationID.String()
	require.NoError(t, store.byRecord(tenantID, delegatorID, "ooo"))

	require.NoError(t, svc.Reverse(context.Background(), port.DelegationReversalInput{
		TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, EndedReason: "reassigned",
	}))

	for _, s := range temporal.signals {
		assert.NotEqual(t, port.SignalInstancePause, s.SignalName, "a new delegation follows a reassignment at once, so nothing waits with the absent delegator")
	}
	assert.False(t, assignments.byID[a.ID].IsActive, "the reversal still restored the task")
	restored := false
	for _, na := range assignments.byID {
		if na.UserID == delegatorID && na.IsActive {
			restored = true
		}
	}
	assert.True(t, restored, "the delegator holds the task again")
}

func TestDelegationReconciler_Reverse_APauseThatCannotBeSentFailsTheEventBeforeAnythingChanges(t *testing.T) {
	svc, instances, tasks, assignments, outbox, temporal, definitions, _ := newDelegationReconcilerHarness()
	store := newFakeUserAvailability()
	svc.Availability = store
	temporal.signalFunc = func(_ context.Context, _ string, _ uuid.UUID, signalName string, _ any) error {
		if signalName == port.SignalInstancePause {
			return assert.AnError
		}
		return nil
	}
	tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	inst, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegateID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
	inst.Status = domain.InstanceStatusRunning
	a.Reason = "delegation:" + delegationID.String()
	require.NoError(t, store.byRecord(tenantID, delegatorID, "ooo"))

	err := svc.Reverse(context.Background(), port.DelegationReversalInput{
		TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, EndedReason: "delegate_disabled",
	})

	require.Error(t, err, "the event is redelivered, which repeats the pause")
	assert.True(t, assignments.byID[a.ID].IsActive, "the assignment is still with the delegate: the pause comes first")
	assert.Empty(t, outbox.enqueued)
}

func TestDelegationReconciler_Reverse_ReleasesAnOutOfOfficePauseLeftByAFailedAttempt(t *testing.T) {
	setup := func(t *testing.T, status domain.InstanceStatus) (*service.DelegationReconciler, *fakeTemporalClient, port.DelegationReversalInput) {
		svc, instances, tasks, assignments, _, temporal, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		inst, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegateID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		inst.Status = status
		a.Reason = "delegation:" + delegationID.String()
		return svc, temporal, port.DelegationReversalInput{
			TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, EndedReason: "expired",
		}
	}
	resumes := func(temporal *fakeTemporalClient) (n int, initiator string) {
		for _, s := range temporal.signals {
			if s.SignalName != port.SignalInstanceResume {
				continue
			}
			n++
			b, err := json.Marshal(s.Payload)
			require.NoError(t, err)
			if strings.Contains(string(b), `"Initiator":"`+domain.InitiatorOOO+`"`) {
				initiator = domain.InitiatorOOO
			}
		}
		return n, initiator
	}

	t.Run("a paused instance of an available delegator is released with the ooo initiator", func(t *testing.T) {
		svc, temporal, in := setup(t, domain.InstanceStatusPaused)

		require.NoError(t, svc.Reverse(context.Background(), in))

		n, initiator := resumes(temporal)
		assert.Equal(t, 1, n, "the pause an earlier attempt left must not outlive the redelivery")
		assert.Equal(t, domain.InitiatorOOO, initiator, "the workflow honours the resume only for an ooo pause")
	})

	t.Run("a running instance is left alone", func(t *testing.T) {
		svc, temporal, in := setup(t, domain.InstanceStatusRunning)

		require.NoError(t, svc.Reverse(context.Background(), in))

		n, _ := resumes(temporal)
		assert.Zero(t, n)
	})
}

func TestDelegationReconciler_Reverse_LocksOnlyTheShortPart(t *testing.T) {
	svc, instances, tasks, assignments, _, temporal, definitions, eligibility := newDelegationReconcilerHarness()
	locker := &fakeLocker{}
	svc.Locks = locker
	store := newFakeUserAvailability()
	svc.Availability = store
	tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	inst, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegateID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
	inst.Status = domain.InstanceStatusRunning
	a.Reason = "delegation:" + delegationID.String()
	require.NoError(t, store.byRecord(tenantID, delegatorID, "ooo"))

	var eligibilityHeld, availabilityHeld, pauseHeld bool
	eligibility.batchCheck = func(_ context.Context, _ uuid.UUID, reqs []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
		eligibilityHeld = locker.held
		out := make([]port.EligibilityResult, len(reqs))
		for i := range out {
			out[i].Eligible = true
		}
		return out, nil
	}
	store.onGet = func() { availabilityHeld = locker.held }
	temporal.signalFunc = func(_ context.Context, _ string, _ uuid.UUID, signalName string, _ any) error {
		if signalName == port.SignalInstancePause {
			pauseHeld = locker.held
		}
		return nil
	}

	require.NoError(t, svc.Reverse(context.Background(), port.DelegationReversalInput{
		TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, EndedReason: "delegate_disabled",
	}))

	assert.Equal(t, []string{port.UserAvailabilityScope(tenantID, delegatorID)}, locker.keys, "the reversal takes the delegator's availability lock")
	assert.False(t, eligibilityHeld, "the slow reads stay outside the lock, which pins one of four lock connections")
	assert.True(t, availabilityHeld, "the status is read under the lock")
	assert.True(t, pauseHeld, "the pause is sent under the lock")
}
