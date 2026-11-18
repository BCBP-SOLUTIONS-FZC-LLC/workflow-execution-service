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

func TestTaskActions_ACountReadThatFailsAfterTheActionIsAnsweredWithoutACount(t *testing.T) {
	cases := map[string]struct {
		mode string
		call func(h *taHarness) (*port.Task, error)
	}{
		"claim": {"all", func(h *taHarness) (*port.Task, error) {
			return h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)
		}},
		"complete": {"single", func(h *taHarness) (*port.Task, error) {
			return h.svc.Complete(context.Background(), h.tenantID, h.task.ID, h.callerID, json.RawMessage(`{}`), csdTaskVersion)
		}},
		"reassign": {"single", func(h *taHarness) (*port.Task, error) {
			return h.svc.Reassign(context.Background(), h.tenantID, h.task.ID, uuid.New(), uuid.New(), csdTaskVersion)
		}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newTaHarness(t, tc.mode)
			h.counting.err = assert.AnError

			got, err := tc.call(h)

			require.NoError(t, err)
			assert.Equal(t, h.task.ID, got.ID)
			assert.True(t, got.AssigneeCountUnavailable, "the answer says the count is not known instead of reporting 0")
		})
	}

	t.Run("defer", func(t *testing.T) {
		h, _ := newTaDeferHarness(t, 0)
		h.counting.err = assert.AnError

		got, err := h.svc.Defer(context.Background(), h.tenantID, h.task.ID, h.callerID, "waiting for documents", csdTaskVersion)

		require.NoError(t, err)
		assert.Equal(t, h.task.ID, got.ID)
		assert.True(t, got.AssigneeCountUnavailable)
	})
}

func TestTaskActions_ASuccessfulCountReadLeavesTheCountAvailable(t *testing.T) {
	h := newTaHarness(t, "single")

	got, err := h.svc.Reassign(context.Background(), h.tenantID, h.task.ID, uuid.New(), uuid.New(), csdTaskVersion)

	require.NoError(t, err)
	assert.False(t, got.AssigneeCountUnavailable)
	assert.Equal(t, 2, got.AssigneeCount)
}
