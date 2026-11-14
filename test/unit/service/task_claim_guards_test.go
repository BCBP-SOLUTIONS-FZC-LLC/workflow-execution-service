package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestTaskClaim_TaskInAStatusThatIsNotActive_ReturnsInvalidTaskState(t *testing.T) {
	cases := []struct {
		name    string
		status  domain.TaskStatus
		wantErr error
	}{
		{"completed", domain.TaskStatusCompleted, port.ErrInvalidTaskState},
		{"failed", domain.TaskStatusFailed, port.ErrInvalidTaskState},
		{"deferred", domain.TaskStatusDeferred, port.ErrInvalidTaskState},
		{"superseded", domain.TaskStatusSuperseded, port.ErrInvalidTaskState},
		{"ready is claimable", domain.TaskStatusReady, nil},
		{"in progress is claimable", domain.TaskStatusInProgress, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := csdNewTaskHarness(t, "all", csdReviewStage())
			h.task.Status = tc.status

			_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

			if tc.wantErr == nil {
				require.NoError(t, err)
				assert.Equal(t, h.callerID, h.assignments.leadOf(h.task.ID))
				return
			}
			require.ErrorIs(t, err, tc.wantErr)
			assert.Equal(t, []csdIDCall{{h.tenantID, h.task.ID}}, h.tasks.gets)
			assert.Empty(t, h.assignments.lists, "a task that cannot be claimed is refused before its assignees are read")
			assert.Empty(t, h.eligibility.calls)
			h.requireTaskUntouched(t)
			assert.Equal(t, tc.status, h.task.Status)
		})
	}
}

func TestTaskClaim_CurrentLeadRepeatsTheClaimWithTheNewVersion_ReturnsAlreadyClaimed(t *testing.T) {
	h := csdNewTaskHarness(t, "all", csdReviewStage())
	first, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)
	require.NoError(t, err)
	require.EqualValues(t, csdTaskVersion+1, first.RecordVersion, "a claim moves the task to a new record version")
	require.Equal(t, h.callerID, h.assignments.leadOf(h.task.ID))

	_, err = h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, first.RecordVersion)

	require.ErrorIs(t, err, port.ErrTaskAlreadyClaimed)
	require.Len(t, h.assignments.setLeads, 1, "the repeat must not write a second lead")
	assert.Equal(t, csdSetLeadCall{h.tenantID, h.task.ID, h.caller.ID, csdTaskVersion}, h.assignments.setLeads[0])
	assert.Equal(t, h.callerID, h.assignments.leadOf(h.task.ID))
	assert.EqualValues(t, csdTaskVersion+1, h.task.RecordVersion)
	assert.True(t, h.other.IsActive)
	assert.False(t, h.other.IsLead)
}

func TestTaskClaim_RepeatedWithTheOriginalVersionAfterASuccessfulClaim_ReturnsRecordVersionConflict(t *testing.T) {
	h := csdNewTaskHarness(t, "all", csdReviewStage())
	_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)
	require.NoError(t, err)

	_, err = h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.NotErrorIs(t, err, port.ErrTaskAlreadyClaimed, "a stale version is a conflict, not an already-claimed answer")
	require.Len(t, h.assignments.setLeads, 1, "the stale retry must not write a second lead")
	assert.Equal(t, h.callerID, h.assignments.leadOf(h.task.ID))
	assert.EqualValues(t, csdTaskVersion+1, h.task.RecordVersion)
	assert.Len(t, h.assignments.lists, 1, "the stale version is refused before the assignees are read again")
}

func TestTaskClaim_TaskNodeHasNoStageInThePlan_ReturnsAssigneeIneligible(t *testing.T) {
	h := csdNewTaskHarness(t, "all", csdOtherStage())

	_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.ErrorIs(t, err, port.ErrAssigneeIneligible)
	assert.Contains(t, err.Error(), h.task.ID.String(), "the message names the task whose node is missing")
	assert.Equal(t, []csdIDCall{{h.tenantID, h.versionID}}, h.definitions.calls, "the plan is read for the caller's tenant and the instance's version")
	assert.Empty(t, h.eligibility.calls, "with no stage there is no level to ask Org & Membership about")
	h.requireTaskUntouched(t)
}

func TestTaskClaim_PlanAlreadyCachedAndDefinitionsDown_ClaimsWithoutCallingDefinitions(t *testing.T) {
	h := csdNewTaskHarness(t, "all", csdReviewStage())
	// Seed the shared cache the way an earlier read would have left it.
	cachedPlan, err := json.Marshal(h.definitions.resp)
	require.NoError(t, err)
	h.cache.values["compiled_plan:"+h.tenantID.String()+":"+h.versionID.String()] = string(cachedPlan)
	h.definitions.err = fmt.Errorf("%w: compiled plan returned 503", port.ErrDefinitionUnavailable)

	got, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.NoError(t, err)
	assert.Equal(t, h.task.ID, got.ID)
	assert.Empty(t, h.definitions.calls, "a cached plan is served without a Definitions call")
	assert.Equal(t, []string{"compiled_plan:" + h.tenantID.String() + ":" + h.versionID.String()}, h.cache.gets)
	require.Len(t, h.eligibility.calls, 1)
	assert.Equal(t, csdEligibilityCall{h.tenantID, h.callerID, h.deptID, "approver"}, h.eligibility.calls[0], "the level comes from the cached plan")
	assert.Equal(t, h.callerID, h.assignments.leadOf(h.task.ID))
	assert.Equal(t, []csdSetLeadCall{{h.tenantID, h.task.ID, h.caller.ID, csdTaskVersion}}, h.assignments.setLeads)
}

func TestTaskClaim_EligibilityReadFails_ReturnsTheUpstreamErrorAndClaimsNothing(t *testing.T) {
	h := csdNewTaskHarness(t, "all", csdReviewStage())
	cause := errors.New("csd: org and membership timed out")
	h.eligibility.err = fmt.Errorf("%w: eligibility endpoint returned 503: %w", adapterhttp.ErrUpstreamUnavailable, cause)

	_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.ErrorIs(t, err, adapterhttp.ErrUpstreamUnavailable)
	require.ErrorIs(t, err, cause)
	assert.NotErrorIs(t, err, port.ErrAssigneeIneligible, "an unreadable answer is not a 'not eligible' answer")
	require.Len(t, h.eligibility.calls, 1)
	assert.Equal(t, csdEligibilityCall{h.tenantID, h.callerID, h.deptID, "approver"}, h.eligibility.calls[0])
	h.requireTaskUntouched(t)
}

func TestTaskClaim_InstanceRowMissingDuringTheEligibilityCheck_ReturnsInstanceNotFound(t *testing.T) {
	h := csdNewTaskHarness(t, "all", csdReviewStage())
	delete(h.instances.byID, h.instanceID)

	_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.ErrorIs(t, err, port.ErrInstanceNotFound)
	assert.NotErrorIs(t, err, port.ErrTaskNotFound)
	assert.Equal(t, []csdIDCall{{h.tenantID, h.instanceID}}, h.instances.gets)
	assert.Empty(t, h.definitions.calls)
	assert.Empty(t, h.eligibility.calls)
	h.requireTaskUntouched(t)
}

func TestTaskClaim_TaskOfAnotherTenant_ReturnsTaskNotFound(t *testing.T) {
	h := csdNewTaskHarness(t, "all", csdReviewStage())
	otherTenant := uuid.New()

	_, err := h.svc.Claim(context.Background(), otherTenant, h.task.ID, h.callerID, csdTaskVersion)

	require.ErrorIs(t, err, port.ErrTaskNotFound)
	assert.Equal(t, []csdIDCall{{otherTenant, h.task.ID}}, h.tasks.gets, "the task is read for the caller's tenant")
	assert.Empty(t, h.assignments.lists, "nothing about the task is read for another tenant")
	assert.Empty(t, h.eligibility.calls)
	h.requireTaskUntouched(t)

	// The owning tenant claims the same task, so the refusal above came from
	// tenant scoping and not from the task's state or version.
	_, err = h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)
	require.NoError(t, err)
	assert.Equal(t, h.callerID, h.assignments.leadOf(h.task.ID))
}

func TestTaskClaim_UnknownTaskID_ReturnsTaskNotFound(t *testing.T) {
	h := csdNewTaskHarness(t, "all", csdReviewStage())
	unknown := uuid.New()

	_, err := h.svc.Claim(context.Background(), h.tenantID, unknown, h.callerID, csdTaskVersion)

	require.ErrorIs(t, err, port.ErrTaskNotFound)
	assert.Equal(t, []csdIDCall{{h.tenantID, unknown}}, h.tasks.gets)
	assert.Empty(t, h.assignments.lists)
	assert.Empty(t, h.definitions.calls)
	h.requireTaskUntouched(t)
}
