package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// The service gates a pause only on the instance's record version and state;
// the repeat that reaches a workflow already paused is the workflow's to drop.
func TestInstanceLifecycle_TwoPausesWithTheSameRecordVersion_BothPassTheGateAndSignal(t *testing.T) {
	tenantID := uuid.New()
	inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 4)
	h := newCscHarness(inst)
	firstAdmin, secondAdmin := uuid.New(), uuid.New()

	require.NoError(t, h.svc.Pause(context.Background(), tenantID, inst.ID, firstAdmin, "first", 4))
	require.NoError(t, h.svc.Pause(context.Background(), tenantID, inst.ID, secondAdmin, "second", 4),
		"the stored instance is still at version 4 and RUNNING, so the second call passes the same checks")

	require.Len(t, h.temporal.signals, 2, "both calls send their signal")
	for i, want := range []struct {
		admin  uuid.UUID
		reason string
	}{{firstAdmin, "first"}, {secondAdmin, "second"}} {
		sig := h.temporal.signals[i]
		assert.Equal(t, port.SignalInstancePause, sig.Name)
		assert.Equal(t, inst.TemporalWorkflowID, sig.WorkflowID)
		assert.Equal(t, inst.ID, sig.InstanceID)
		wire := cscDecodeWire(t, sig.Payload)
		assert.Equal(t, want.admin.String(), wire.AdminUserID)
		assert.Equal(t, want.reason, wire.Reason)
		assert.EqualValues(t, 4, wire.RecordVersion, "each signal carries the version it was checked against")
	}

	err := h.svc.Pause(context.Background(), tenantID, inst.ID, uuid.New(), "third", 3)
	require.ErrorIs(t, err, port.ErrRecordVersionConflict, "control: another version is refused")
	assert.Len(t, h.temporal.signals, 2, "and sends nothing")
}
