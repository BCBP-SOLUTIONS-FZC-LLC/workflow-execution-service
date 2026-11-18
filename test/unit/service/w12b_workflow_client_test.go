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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

var errW12bEligibilityDown = errors.New("eligibility service unreachable")

func TestWorkflowClient_DelegateImpact_ANamedDelegationCountsOnlyRowsCarryingItsTag(t *testing.T) {
	delegationID, other, delegator := uuid.New(), uuid.New(), uuid.New()
	tag := "delegation:" + delegationID.String()
	cases := []struct {
		name      string
		reason    string
		wantMatch bool
	}{
		{"a hop with the delegator after the tag matches", tag + ":" + delegator.String(), true},
		{"the named hop later in a chain matches", "delegation:" + other.String() + ":" + delegator.String() + ";" + tag + ":" + delegator.String(), true},
		{"another delegation's tag does not match", "delegation:" + other.String() + ":" + delegator.String(), false},
		{"a capitalised prefix does not match", "Delegation:" + delegationID.String(), false},
		{"an upper-case prefix does not match", "DELEGATION:" + delegationID.String(), false},
		{"an upper-case id does not match", "delegation:" + strings.ToUpper(delegationID.String()), false},
		{"an empty reason does not match", "", false},
		{"a manual assignment does not match", "manual-assign", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := csaNewClientHarness()
			tenantID, delegate := uuid.New(), uuid.New()
			control := h.seed(tenantID, delegate, tag, domain.TaskStatusReady, domain.InstanceStatusRunning)
			candidate := h.seed(tenantID, delegate, tc.reason, domain.TaskStatusReady, domain.InstanceStatusRunning)

			named, err := h.svc.DelegateImpact(context.Background(), port.DelegateImpactInput{
				TenantID: tenantID, DelegateUserID: delegate, DelegationID: &delegationID, Page: port.Page{Limit: 10},
			})

			require.NoError(t, err)
			wantRows, wantInstances := []csaIDCall{{tenantID, control.task.ID}}, []uuid.UUID{control.instance.ID}
			if tc.wantMatch {
				wantRows = append(wantRows, csaIDCall{tenantID, candidate.task.ID})
				wantInstances = append(wantInstances, candidate.instance.ID)
			}
			assert.Equal(t, len(wantRows), named.ActiveAssignments, "the tagged control row always counts")
			assert.Equal(t, len(wantRows), named.ActiveWorkflows)
			assert.ElementsMatch(t, wantInstances, named.WorkflowIDs.Items)
			assert.ElementsMatch(t, wantRows, h.tasks.reads, "only the rows the filter keeps have their task read")
			assert.Equal(t, []csaUserCall{{tenantID, delegate}}, h.assignments.listed)
		})
	}
}

type w12bEligibilityCall struct {
	tenantID, userID, departmentID uuid.UUID
	level                          string
}

type w12bEligibility struct {
	answer func(call w12bEligibilityCall) (bool, error)
	calls  []w12bEligibilityCall
}

func (e *w12bEligibility) checker() *fakeEligibilityChecker {
	return &fakeEligibilityChecker{check: func(_ context.Context, tenantID, userID, departmentID uuid.UUID, level string) (bool, error) {
		call := w12bEligibilityCall{tenantID, userID, departmentID, level}
		e.calls = append(e.calls, call)
		return e.answer(call)
	}}
}

type w12bDefinitionsCall struct{ tenantID, versionID uuid.UUID }

type w12bDefinitionsRecorder struct {
	inner *dlgfixDefinitions
	calls []w12bDefinitionsCall
}

func (d *w12bDefinitionsRecorder) GetCompiledWorkflow(ctx context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.calls = append(d.calls, w12bDefinitionsCall{tenantID, versionID})
	return d.inner.GetCompiledWorkflow(ctx, tenantID, versionID)
}

type w12bListCall struct{ tenantID, userID uuid.UUID }

// w12bTenantAssignments answers the listing of a user's active rows the way row-level security does, for the caller's tenant only,
// and records the tenant and user of each call.
type w12bTenantAssignments struct {
	port.TaskAssignmentRepository
	listed []w12bListCall
}

func (r *w12bTenantAssignments) ListActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.listed = append(r.listed, w12bListCall{tenantID, userID})
	all, err := r.TaskAssignmentRepository.ListActiveByUser(ctx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	var inTenant []*domain.TaskAssignment
	for _, a := range all {
		if a.TenantID == tenantID {
			inTenant = append(inTenant, a)
		}
	}
	return inTenant, nil
}

// w12bIsolation is a delegated row of the old delegate in another tenant, which no call of this tenant may move.
type w12bIsolation struct {
	listing *w12bTenantAssignments
	foreign *domain.TaskAssignment
}

func w12bIsolate(h *dlgfixClient) w12bIsolation {
	listing := &w12bTenantAssignments{TaskAssignmentRepository: h.assignments}
	h.svc.Assignments = listing
	foreign := &domain.TaskAssignment{
		ID: uuid.New(), TenantID: uuid.New(), TaskID: uuid.New(), UserID: h.oldDelegate, IsActive: true,
		Reason: "delegation:" + uuid.NewString(),
	}
	h.assignments.add(foreign)
	return w12bIsolation{listing: listing, foreign: foreign}
}

func (i w12bIsolation) require(t *testing.T, h *dlgfixClient) {
	t.Helper()
	require.NotEmpty(t, i.listing.listed)
	for _, call := range i.listing.listed {
		assert.Equal(t, w12bListCall{h.tenantID, h.oldDelegate}, call, "the old delegate's rows are listed in the caller's tenant")
	}
	assert.True(t, i.foreign.IsActive, "another tenant's row is not vacated")
	assert.False(t, h.holdsActive(i.foreign.TaskID, h.newDelegate), "and not handed over")
	assert.NotContains(t, h.assignments.vacated, i.foreign.ID)
}

func TestWorkflowClient_ReassignDelegate_EligibilityIsAskedOncePerDepartmentAndLevelAndAFalseAnswerIsReused(t *testing.T) {
	h := dlgfixNewClient(t)
	isolation := w12bIsolate(h)
	definitions := &w12bDefinitionsRecorder{inner: h.definitions}
	eligibility := &w12bEligibility{answer: func(call w12bEligibilityCall) (bool, error) { return call.level == "approver", nil }}
	h.svc.Definitions, h.svc.Eligibility = definitions, eligibility.checker()
	firstReviewer, secondReviewer, approver := h.seed(t, "reviewer"), h.seed(t, "reviewer"), h.seed(t, "approver")

	count, err := h.reassign()

	require.NoError(t, err)
	assert.Equal(t, 1, count, "only the task the new delegate is eligible for moves")
	assert.Equal(t, []w12bEligibilityCall{
		{h.tenantID, h.newDelegate, uuid.Nil, "reviewer"},
		{h.tenantID, h.newDelegate, uuid.Nil, "approver"},
	}, eligibility.calls, "the second reviewer task reuses the first one's false answer")
	assert.True(t, h.holdsActive(firstReviewer.task.ID, h.oldDelegate), "an ineligible task stays with the old delegate")
	assert.True(t, h.holdsActive(secondReviewer.task.ID, h.oldDelegate))
	assert.False(t, h.holdsActive(firstReviewer.task.ID, h.newDelegate))
	assert.False(t, h.holdsActive(secondReviewer.task.ID, h.newDelegate))
	assert.True(t, h.holdsActive(approver.task.ID, h.newDelegate), "the eligible task is the control: it moved")
	require.Len(t, h.outbox.signals, 1)
	assert.Equal(t, approver.instance.ID, h.outbox.signals[0].InstanceID)
	assert.Equal(t, []uuid.UUID{h.tenantID}, h.outbox.signalTenants)
	assert.Len(t, definitions.calls, 3, "each task's own plan is read in the caller's tenant")
	for _, call := range definitions.calls {
		assert.Equal(t, h.tenantID, call.tenantID)
	}
	isolation.require(t, h)
}

func TestWorkflowClient_ReassignDelegate_EligibilityIsAskedPerDepartmentEvenAtTheSameLevel(t *testing.T) {
	h := dlgfixNewClient(t)
	isolation := w12bIsolate(h)
	salesID, legalID := uuid.New(), uuid.New()
	stage := dsl.StageDef{NodeID: "review", Role: "reviewer"}
	planJSON, err := json.Marshal(dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{
				{ID: "sales", Label: "Sales", IAMDepartmentID: salesID.String(), Stages: []dsl.StageDef{stage}},
				{ID: "legal", Label: "Legal", IAMDepartmentID: legalID.String(), Stages: []dsl.StageDef{stage}},
			},
		}},
	})
	require.NoError(t, err)
	versionID := uuid.New()
	h.definitions.byVersion[versionID] = publishedCompiledWorkflow(uuid.New(), versionID, string(planJSON))
	seedIn := func(nodeKey string, departmentID uuid.UUID) dlgfixHeld {
		inst := &domain.Instance{
			ID: uuid.New(), TenantID: h.tenantID, WorkflowVersionID: versionID,
			TemporalWorkflowID: h.tenantID.String() + ":" + uuid.NewString(), RecordVersion: 1,
		}
		h.instances.byID[inst.ID] = inst
		task := &domain.Task{ID: uuid.New(), TenantID: h.tenantID, WorkflowInstanceID: inst.ID, NodeKey: nodeKey, DepartmentID: departmentID, RecordVersion: 1}
		h.tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: task.ID, UserID: h.oldDelegate, IsActive: true, Reason: "delegation:" + uuid.NewString()}
		h.assignments.add(a)
		return dlgfixHeld{instance: inst, task: task, assignment: a, versionID: versionID}
	}
	sales, legal, secondSales := seedIn("sales/review", salesID), seedIn("legal/review", legalID), seedIn("sales/review", salesID)
	eligibility := &w12bEligibility{answer: func(call w12bEligibilityCall) (bool, error) { return call.departmentID == legalID, nil }}
	h.svc.Definitions, h.svc.Eligibility = h.definitions, eligibility.checker()

	count, err := h.reassign()

	require.NoError(t, err)
	assert.Equal(t, 1, count, "only the legal task moves: the answer for one department is not reused for the other")
	assert.Equal(t, []w12bEligibilityCall{
		{h.tenantID, h.newDelegate, salesID, "reviewer"},
		{h.tenantID, h.newDelegate, legalID, "reviewer"},
	}, eligibility.calls, "one call per department, the second sales task reusing the first answer")
	assert.True(t, h.holdsActive(legal.task.ID, h.newDelegate))
	assert.True(t, h.holdsActive(sales.task.ID, h.oldDelegate))
	assert.True(t, h.holdsActive(secondSales.task.ID, h.oldDelegate))
	assert.False(t, h.holdsActive(sales.task.ID, h.newDelegate))
	assert.False(t, h.holdsActive(secondSales.task.ID, h.newDelegate))
	isolation.require(t, h)
}

func TestWorkflowClient_ReassignDelegate_AnEligibilityErrorFailsTheCallAndIsNotRemembered(t *testing.T) {
	h := dlgfixNewClient(t)
	isolation := w12bIsolate(h)
	healthy := false
	eligibility := &w12bEligibility{answer: func(w12bEligibilityCall) (bool, error) {
		if !healthy {
			return false, errW12bEligibilityDown
		}
		return true, nil
	}}
	h.svc.Definitions, h.svc.Eligibility = h.definitions, eligibility.checker()
	first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")

	count, err := h.reassign()

	require.ErrorIs(t, err, port.ErrEligibilityRead)
	require.ErrorIs(t, err, errW12bEligibilityDown)
	assert.Zero(t, count)
	assert.Len(t, eligibility.calls, 1, "the call stops at the first failure")
	h.requireUntouched(t, first, second)

	healthy = true
	count, err = h.reassign()

	require.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.Len(t, eligibility.calls, 2, "the failed answer was not cached, so the retry asked again, and its true answer then covered the second task")
	assert.Equal(t, w12bEligibilityCall{h.tenantID, h.newDelegate, uuid.Nil, "reviewer"}, eligibility.calls[1])
	assert.True(t, h.holdsActive(first.task.ID, h.newDelegate))
	assert.True(t, h.holdsActive(second.task.ID, h.newDelegate))
	isolation.require(t, h)
}

func TestWorkflowClient_ReassignDelegate_WithoutAnEligibilityOrDefinitionsClientEveryTaskMovesWithoutARead(t *testing.T) {
	cases := []struct {
		name  string
		build func(h *dlgfixClient, definitions *w12bDefinitionsRecorder, eligibility *w12bEligibility)
	}{
		{"no eligibility client", func(h *dlgfixClient, definitions *w12bDefinitionsRecorder, _ *w12bEligibility) {
			h.svc.Definitions, h.svc.Eligibility = definitions, nil
		}},
		{"no definitions client", func(h *dlgfixClient, _ *w12bDefinitionsRecorder, eligibility *w12bEligibility) {
			h.svc.Definitions, h.svc.Eligibility = nil, eligibility.checker()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := dlgfixNewClient(t)
			isolation := w12bIsolate(h)
			definitions := &w12bDefinitionsRecorder{inner: h.definitions}
			eligibility := &w12bEligibility{answer: func(w12bEligibilityCall) (bool, error) { return false, errW12bEligibilityDown }}
			tc.build(h, definitions, eligibility)
			first, second := h.seed(t, "reviewer"), h.seed(t, "approver")

			count, err := h.reassign()

			require.NoError(t, err)
			assert.Equal(t, 2, count)
			assert.Empty(t, eligibility.calls, "nothing asks eligibility")
			assert.Empty(t, definitions.calls, "and no plan is read")
			assert.True(t, h.holdsActive(first.task.ID, h.newDelegate))
			assert.True(t, h.holdsActive(second.task.ID, h.newDelegate))
			isolation.require(t, h)
		})
	}
}

func TestWorkflowClient_ReassignDelegate_TwoTasksOfOneInstanceAreEachMovedWithTheirReasonAndLead(t *testing.T) {
	h := dlgfixNewClient(t)
	isolation := w12bIsolate(h)
	instances := &csaInstances{fakeInstanceRepo: h.instances}
	h.svc.Instances = instances
	first := h.seed(t, "reviewer")
	first.assignment.Reason = "delegation:" + uuid.NewString() + ":" + uuid.NewString()
	first.assignment.IsLead = false
	secondTask := &domain.Task{ID: uuid.New(), TenantID: h.tenantID, WorkflowInstanceID: first.instance.ID, NodeKey: "finance/review", RecordVersion: 1}
	h.tasks.byID[secondTask.ID] = secondTask
	secondAssignment := &domain.TaskAssignment{
		ID: uuid.New(), TenantID: h.tenantID, TaskID: secondTask.ID, UserID: h.oldDelegate, IsActive: true, IsLead: true,
		Reason: "delegation:" + uuid.NewString() + ":" + uuid.NewString(),
	}
	h.assignments.add(secondAssignment)

	count, err := h.reassign()

	require.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.False(t, first.assignment.IsActive)
	assert.False(t, secondAssignment.IsActive)
	assert.ElementsMatch(t, []uuid.UUID{first.assignment.ID, secondAssignment.ID}, h.assignments.vacated, "each old row is vacated once")

	require.Len(t, h.assignments.created, 2)
	byTask := map[uuid.UUID]*domain.TaskAssignment{}
	for _, created := range h.assignments.created {
		byTask[created.TaskID] = created
		assert.Equal(t, h.tenantID, created.TenantID)
		assert.Equal(t, h.newDelegate, created.UserID)
		assert.NotEqual(t, uuid.Nil, created.ID)
		assert.NotEqual(t, first.assignment.ID, created.ID, "the new row has an id of its own")
		assert.NotEqual(t, secondAssignment.ID, created.ID)
	}
	require.Len(t, byTask, 2, "one new row per task")
	assert.NotEqual(t, byTask[first.task.ID].ID, byTask[secondTask.ID].ID)
	assert.Equal(t, first.assignment.Reason, byTask[first.task.ID].Reason, "the new row keeps the reason")
	assert.False(t, byTask[first.task.ID].IsLead)
	assert.Equal(t, secondAssignment.Reason, byTask[secondTask.ID].Reason)
	assert.True(t, byTask[secondTask.ID].IsLead, "and the lead flag")

	require.Len(t, h.outbox.signals, 2, "one reassign signal per task")
	var signalledTasks []string
	for _, signal := range h.outbox.signals {
		assert.Equal(t, port.SignalInstanceReassign, signal.SignalName)
		assert.Equal(t, first.instance.ID, signal.InstanceID)
		wire := csaDecodeReassign(t, signal.Payload)
		assert.Equal(t, h.oldDelegate.String(), wire.OldUserID)
		assert.Equal(t, h.newDelegate.String(), wire.NewUserID)
		signalledTasks = append(signalledTasks, wire.TaskID)
	}
	assert.ElementsMatch(t, []string{first.task.ID.String(), secondTask.ID.String()}, signalledTasks)
	assert.Equal(t, []uuid.UUID{h.tenantID, h.tenantID}, h.outbox.signalTenants)
	assert.Equal(t, []csaIDCall{{h.tenantID, first.instance.ID}, {h.tenantID, first.instance.ID}}, instances.reads, "the instance is read once per task")
	isolation.require(t, h)
}
