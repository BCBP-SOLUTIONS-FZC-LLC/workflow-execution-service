package service_test

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type w16aReassignWire struct {
	TaskID        string
	OldUserID     string
	NewUserID     string
	AdminUserID   string
	RecordVersion int64
	RecordedAt    string
}

func TestDelegationReassign_EachMovedTaskGetsOneReassignSignalWithItsOwnFields(t *testing.T) {
	h := dlgfixNewClient(t)
	first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	first.task.RecordVersion = 5
	second.task.RecordVersion = 9
	held := map[uuid.UUID]dlgfixHeld{first.task.ID: first, second.task.ID: second}
	versions := map[uuid.UUID]int64{first.task.ID: 5, second.task.ID: 9}

	count, err := h.reassign()

	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Len(t, h.outbox.signals, 2, "one signal per moved task")
	assert.Equal(t, []uuid.UUID{h.tenantID, h.tenantID}, h.outbox.signalTenants, "each is recorded for the delegation's tenant")

	seen := map[uuid.UUID]bool{}
	for _, sig := range h.outbox.signals {
		assert.Equal(t, port.SignalInstanceReassign, sig.SignalName)
		raw, err := json.Marshal(sig.Payload)
		require.NoError(t, err)
		var wire w16aReassignWire
		require.NoError(t, json.Unmarshal(raw, &wire))

		taskID, err := uuid.Parse(wire.TaskID)
		require.NoError(t, err)
		moved, ok := held[taskID]
		require.True(t, ok, "the signal names one of the moved tasks")
		assert.False(t, seen[taskID], "no task is signalled twice")
		seen[taskID] = true

		assert.Equal(t, moved.instance.ID, sig.InstanceID)
		assert.Equal(t, moved.instance.TemporalWorkflowID, sig.TemporalWorkflowID, "the workflow of that task's own instance")
		assert.Equal(t, h.oldDelegate.String(), wire.OldUserID)
		assert.Equal(t, h.newDelegate.String(), wire.NewUserID)
		assert.Equal(t, versions[taskID], wire.RecordVersion, "the version of that task")
		assert.Empty(t, wire.AdminUserID, "a delegation move names no administrator")
		assert.NotEmpty(t, wire.RecordedAt, "the time it was recorded is carried")
	}
	assert.Len(t, seen, 2)
}
