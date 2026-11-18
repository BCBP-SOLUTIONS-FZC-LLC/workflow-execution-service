package service_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var (
	errW8aDefinitionsDown = errors.New("w8a: definitions connection refused")
	errW8aCountFailed     = errors.New("w8a: assignee count query failed")
)

// w8aCountCall is one assignee-count read: the tenant and the task ids it asked about.
type w8aCountCall struct {
	TenantID uuid.UUID
	TaskIDs  []uuid.UUID
}

// w8aCountFailingAssignments serves every call as the harness's repository does,
// except the assignee count, which fails.
type w8aCountFailingAssignments struct {
	*csdAssignmentRepo
	countCalls []w8aCountCall
}

func (r *w8aCountFailingAssignments) CountActiveByTasks(_ context.Context, tenantID uuid.UUID, taskIDs []uuid.UUID) (map[uuid.UUID]int, error) {
	r.countCalls = append(r.countCalls, w8aCountCall{TenantID: tenantID, TaskIDs: taskIDs})
	return nil, errW8aCountFailed
}

// w8aVanishingTasks reads the harness's task like its repository does, until
// the task is gone from the goneFrom-th read on.
type w8aVanishingTasks struct {
	*csdTaskRepo
	goneFrom int
	reads    []csdIDCall
}

func (r *w8aVanishingTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads = append(r.reads, csdIDCall{TenantID: tenantID, ID: id})
	if len(r.reads) >= r.goneFrom {
		return nil, domain.ErrNotFound
	}
	return r.csdTaskRepo.GetByID(ctx, tenantID, id)
}

func w8aUnexpandablePlan() string {
	return fmt.Sprintf(`{"main_plan":"main","schema_version":%d,"plans":[{"name":"main","departments":[],"execution":{"steps":[{"call_plan":{"node_id":"CA_1","plan":"Nowhere"}}]}}]}`, dsl.CurrentSchemaVersion)
}

func TestTaskClaim_EligibilityInputsCannotBeRead_ReturnsTheCauseAndClaimsNothing(t *testing.T) {
	cases := []struct {
		name            string
		arrange         func(h *csdTaskHarness)
		wantErrs        []error
		notErrs         []error
		wantPlanReads   int
		wantEligibility int
	}{
		{
			name: "the Definitions service cannot serve the compiled plan",
			arrange: func(h *csdTaskHarness) {
				h.definitions.err = fmt.Errorf("%w: %w", port.ErrDefinitionUnavailable, errW8aDefinitionsDown)
			},
			wantErrs:      []error{port.ErrDefinitionUnavailable, errW8aDefinitionsDown},
			notErrs:       []error{port.ErrVersionInvalid, port.ErrAssigneeIneligible},
			wantPlanReads: 1,
		},
		{
			name: "the compiled plan does not expand",
			arrange: func(h *csdTaskHarness) {
				h.definitions.resp = publishedCompiledWorkflow(uuid.New(), h.versionID, w8aUnexpandablePlan())
			},
			wantErrs:      []error{port.ErrVersionInvalid},
			notErrs:       []error{port.ErrDefinitionUnavailable, port.ErrAssigneeIneligible},
			wantPlanReads: 1,
		},
		{
			name: "the eligibility read runs into its deadline",
			arrange: func(h *csdTaskHarness) {
				h.eligibility.err = fmt.Errorf("eligibility read: %w", context.DeadlineExceeded)
			},
			wantErrs:        []error{context.DeadlineExceeded},
			notErrs:         []error{port.ErrAssigneeIneligible, port.ErrDefinitionUnavailable},
			wantPlanReads:   1,
			wantEligibility: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := csdNewTaskHarness(t, "all", csdReviewStage())
			tc.arrange(h)

			got, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

			require.Error(t, err)
			assert.Nil(t, got)
			for _, want := range tc.wantErrs {
				assert.ErrorIs(t, err, want)
			}
			for _, not := range tc.notErrs {
				assert.NotErrorIs(t, err, not)
			}
			assert.Equal(t, []csdIDCall{{TenantID: h.tenantID, ID: h.task.ID}}, h.tasks.gets, "the task was read once, for the caller's tenant")
			assert.Equal(t, []csdIDCall{{TenantID: h.tenantID, ID: h.instanceID}, {TenantID: h.tenantID, ID: h.instanceID}}, h.instances.gets,
				"the instance is read for the state check and again for the eligibility check, in the caller's tenant")
			assert.Len(t, h.definitions.calls, tc.wantPlanReads)
			for _, call := range h.definitions.calls {
				assert.Equal(t, csdIDCall{TenantID: h.tenantID, ID: h.versionID}, call, "the plan is read for the caller's tenant and the instance's version")
			}
			require.Len(t, h.eligibility.calls, tc.wantEligibility)
			for _, call := range h.eligibility.calls {
				assert.Equal(t, csdEligibilityCall{TenantID: h.tenantID, UserID: h.callerID, DepartmentID: h.deptID, Level: "approver"}, call)
			}
			h.requireTaskUntouched(t)
			assert.Empty(t, h.temporal.signals)
		})
	}
}

func TestTaskClaim_AssigneeCountFailsAfterTheLeadIsSet_StillReturnsTheClaimedTask(t *testing.T) {
	h := csdNewTaskHarness(t, "all", csdReviewStage())
	counting := &w8aCountFailingAssignments{csdAssignmentRepo: h.assignments}
	h.svc.Assignments = counting
	logger := &fakeLogger{}
	h.svc.Log = logger

	got, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.NoError(t, err, "the claim has committed, so a failed count does not fail the answer")
	assert.Equal(t, h.task.ID, got.ID)
	assert.EqualValues(t, csdTaskVersion+1, got.RecordVersion, "the answer is the task after the claim")
	assert.Equal(t, h.callerID, h.assignments.leadOf(h.task.ID), "the claim itself stands")
	assert.Equal(t, []csdSetLeadCall{{h.tenantID, h.task.ID, h.caller.ID, csdTaskVersion}}, h.assignments.claims)
	assert.Equal(t, []w8aCountCall{{TenantID: h.tenantID, TaskIDs: []uuid.UUID{h.task.ID}}}, counting.countCalls, "one count was asked for, for this task and tenant")
	assert.Equal(t, 1, warnsWithCause(logger, errW8aCountFailed.Error()), "the failed count is logged with its cause")
}

func TestTaskClaim_TaskGoneAtTheReadBackAfterTheLeadIsSet_ReturnsTaskNotFoundWithoutASecondAttempt(t *testing.T) {
	h := csdNewTaskHarness(t, "all", csdReviewStage())
	vanishing := &w8aVanishingTasks{csdTaskRepo: h.tasks, goneFrom: 2}
	h.svc.Tasks = vanishing

	got, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.ErrorIs(t, err, port.ErrTaskNotFound)
	assert.Nil(t, got)
	assert.Equal(t, []csdIDCall{
		{TenantID: h.tenantID, ID: h.task.ID},
		{TenantID: h.tenantID, ID: h.task.ID},
	}, vanishing.reads, "the task is read once to claim and once to read back; a not-found read-back is not retried")
	assert.Equal(t, []csdSetLeadCall{{h.tenantID, h.task.ID, h.caller.ID, csdTaskVersion}}, h.assignments.claims, "the lead was set before the task went missing")
}
