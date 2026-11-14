package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var csdCompleteResult = json.RawMessage(`{"decision":"approve"}`)

func TestTaskComplete_CallerWithoutAnAssignment_ReturnsNotAssigneeAndSignalsNothing(t *testing.T) {
	h := csdNewTaskHarness(t, "single", csdReviewStage())
	// The caller is an administrator of the tenant but holds no assignment on
	// this task: drop the caller's assignment row.
	h.caller.IsActive = false

	_, err := h.svc.Complete(context.Background(), h.tenantID, h.task.ID, h.callerID, csdCompleteResult, csdTaskVersion)

	require.ErrorIs(t, err, port.ErrNotAssignee)
	assert.Equal(t, []csdIDCall{{h.tenantID, h.task.ID}}, h.assignments.lists, "the assignees are read for the task in the caller's tenant")
	assert.Empty(t, h.temporal.signals, "a refused caller sends nothing to the workflow")
	assert.Empty(t, h.eligibility.calls, "the refusal comes before the eligibility read")
	assert.Empty(t, h.instances.gets)
	assert.EqualValues(t, csdTaskVersion, h.task.RecordVersion)
}

func TestTaskComplete_TaskNodeHasNoStageInThePlan_ReturnsAssigneeIneligibleAndSignalsNothing(t *testing.T) {
	h := csdNewTaskHarness(t, "single", csdOtherStage())

	_, err := h.svc.Complete(context.Background(), h.tenantID, h.task.ID, h.callerID, csdCompleteResult, csdTaskVersion)

	require.ErrorIs(t, err, port.ErrAssigneeIneligible)
	assert.Contains(t, err.Error(), h.task.ID.String(), "the message names the task whose node is missing")
	assert.Equal(t, []csdIDCall{{h.tenantID, h.instanceID}}, h.instances.gets)
	assert.Equal(t, []csdIDCall{{h.tenantID, h.versionID}}, h.definitions.calls)
	assert.Empty(t, h.eligibility.calls, "with no stage there is no level to ask Org & Membership about")
	assert.Empty(t, h.temporal.signals)
	assert.EqualValues(t, csdTaskVersion, h.task.RecordVersion)
}

func TestTaskComplete_PlanCannotBeFetched_ReturnsDefinitionUnavailableAndSignalsNothing(t *testing.T) {
	h := csdNewTaskHarness(t, "single", csdReviewStage())
	h.definitions.err = fmt.Errorf("%w: compiled plan returned 503", port.ErrDefinitionUnavailable)

	_, err := h.svc.Complete(context.Background(), h.tenantID, h.task.ID, h.callerID, csdCompleteResult, csdTaskVersion)

	require.ErrorIs(t, err, port.ErrDefinitionUnavailable)
	assert.NotErrorIs(t, err, port.ErrAssigneeIneligible, "an unreadable plan is not a 'not eligible' answer")
	assert.Equal(t, []csdIDCall{{h.tenantID, h.versionID}}, h.definitions.calls)
	assert.Empty(t, h.eligibility.calls)
	assert.Empty(t, h.temporal.signals)
	assert.EqualValues(t, csdTaskVersion, h.task.RecordVersion)
}

func TestTaskComplete_AssignmentReadFails_ReturnsTheRawErrorAndSignalsNothing(t *testing.T) {
	h := csdNewTaskHarness(t, "single", csdReviewStage())
	cause := errors.New("csd: assignments table unavailable")
	h.assignments.listErr = cause

	_, err := h.svc.Complete(context.Background(), h.tenantID, h.task.ID, h.callerID, csdCompleteResult, csdTaskVersion)

	require.ErrorIs(t, err, cause)
	for _, sentinel := range []error{port.ErrNotAssignee, port.ErrTaskNotFound, port.ErrRecordVersionConflict, port.ErrInvalidTaskState} {
		assert.NotErrorIs(t, err, sentinel, "a database failure must not look like a client error")
	}
	assert.Equal(t, []csdIDCall{{h.tenantID, h.task.ID}}, h.assignments.lists)
	assert.Empty(t, h.eligibility.calls)
	assert.Empty(t, h.temporal.signals)
	assert.EqualValues(t, csdTaskVersion, h.task.RecordVersion)
}
