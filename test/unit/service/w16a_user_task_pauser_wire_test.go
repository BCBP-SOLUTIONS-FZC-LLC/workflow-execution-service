package service_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestUserTaskPauser_PauseSignal_CarriesTheStoredWorkflowIdTheRowVersionAndNoAdminFields(t *testing.T) {
	for _, status := range []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusPaused, domain.InstanceStatusDegraded} {
		t.Run(string(status), func(t *testing.T) {
			h := newW11bPauseHarness()
			inst := h.hold(status)
			inst.RecordVersion = 11
			inst.TemporalWorkflowID = "stored-workflow-id-" + inst.ID.String()

			require.NoError(t, h.pause())

			require.Len(t, h.temporal.signals, 1)
			sig := h.temporal.signals[0]
			assert.Equal(t, port.SignalInstancePause, sig.SignalName)
			assert.Equal(t, inst.ID, sig.InstanceID, "the channel suffix is the instance id")
			assert.Equal(t, inst.TemporalWorkflowID, sig.TemporalWorkflowID, "the workflow id is the one stored on the instance row")
			raw, err := json.Marshal(sig.Payload)
			require.NoError(t, err)
			var wire struct {
				AdminUserID   string
				Reason        string
				Initiator     string
				TargetNodeKey string
				RecordVersion int64
			}
			require.NoError(t, json.Unmarshal(raw, &wire))
			assert.Equal(t, "safety_net", wire.Initiator)
			assert.Equal(t, int64(11), wire.RecordVersion, "the version of the instance row that was read")
			assert.Empty(t, wire.AdminUserID, "no administrator is named")
			assert.Empty(t, wire.Reason)
			assert.Empty(t, wire.TargetNodeKey)
			h.requireReadsInTenant(t, inst.ID)
		})
	}
}
