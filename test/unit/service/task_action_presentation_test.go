package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func (h *taHarness) requireTypeAndCount(t *testing.T, got *port.Task) {
	t.Helper()
	require.NotNil(t, got)
	assert.Equal(t, h.task.ID, got.ID)
	assert.Equal(t, "userTask", got.TaskType)
	assert.Equal(t, 2, got.AssigneeCount, "the caller and the other user are both active")
	require.Len(t, h.counting.counts, 1, "one count call per action")
	assert.Equal(t, []uuid.UUID{h.task.ID}, h.counting.counts[0])
}

func TestTaskClaim_ResponseCarriesTypeAndAssigneeCount(t *testing.T) {
	h := newTaHarness(t, "all")

	got, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.NoError(t, err)
	h.requireTypeAndCount(t, got)
}

func TestTaskClaim_LeadRepeatingItsClaim_ResponseCarriesTypeAndAssigneeCount(t *testing.T) {
	h := newTaHarness(t, "all")
	h.caller.IsLead = true

	got, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.NoError(t, err)
	h.requireTypeAndCount(t, got)
}

func TestTaskClaim_ReadBackKeepsFailing_ResponseStillCarriesTypeAndAssigneeCount(t *testing.T) {
	h, _ := newTaFlakyHarness(t, "all", 10)

	got, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.NoError(t, err)
	h.requireTypeAndCount(t, got)
}

func TestTaskComplete_ResponseCarriesTypeAndAssigneeCount(t *testing.T) {
	h := newTaHarness(t, "single")

	got, err := h.svc.Complete(context.Background(), h.tenantID, h.task.ID, h.callerID, json.RawMessage(`{}`), csdTaskVersion)

	require.NoError(t, err)
	h.requireTypeAndCount(t, got)
}

func TestTaskDefer_ResponseCarriesTypeAndAssigneeCount(t *testing.T) {
	h, _ := newTaDeferHarness(t, 0)

	got, err := h.svc.Defer(context.Background(), h.tenantID, h.task.ID, h.callerID, "waiting for documents", csdTaskVersion)

	require.NoError(t, err)
	h.requireTypeAndCount(t, got)
}

func TestTaskReassign_ResponseCarriesTypeAndAssigneeCount(t *testing.T) {
	h := newTaHarness(t, "single")

	got, err := h.svc.Reassign(context.Background(), h.tenantID, h.task.ID, uuid.New(), uuid.New(), csdTaskVersion)

	require.NoError(t, err)
	h.requireTypeAndCount(t, got)
}

func TestTaskActions_CountFailsAfterTheActionTookEffect_StillAnswerSuccess(t *testing.T) {
	cases := map[string]func(h *taHarness) (*port.Task, error){
		"complete": func(h *taHarness) (*port.Task, error) {
			return h.svc.Complete(context.Background(), h.tenantID, h.task.ID, h.callerID, json.RawMessage(`{}`), csdTaskVersion)
		},
		"reassign": func(h *taHarness) (*port.Task, error) {
			return h.svc.Reassign(context.Background(), h.tenantID, h.task.ID, uuid.New(), uuid.New(), csdTaskVersion)
		},
		"claim": func(h *taHarness) (*port.Task, error) {
			return h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)
		},
	}
	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			mode := "single"
			if name == "claim" {
				mode = "all"
			}
			h := newTaHarness(t, mode)
			h.counting.err = assert.AnError

			got, err := call(h)

			require.NoError(t, err, "the change is already made, so a failed count does not undo the answer")
			assert.Equal(t, h.task.ID, got.ID)
			assert.Equal(t, "userTask", got.TaskType)
		})
	}
}
