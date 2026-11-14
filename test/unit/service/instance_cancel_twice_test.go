package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

func TestInstanceCancel_TwoCancelsWithTheSameRecordVersion_BothAreAcceptedAndBothSignalTheWorkflow(t *testing.T) {
	const version = 4
	tenantID, instanceID := uuid.New(), uuid.New()
	instances := &csdInstanceRepo{byID: map[uuid.UUID]*domain.Instance{instanceID: {
		ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: version,
		TemporalWorkflowID: "wf-" + instanceID.String(),
	}}}
	temporal := &csdPayloadTemporal{}
	svc := &service.InstanceService{Instances: instances, Temporal: temporal}
	firstAdmin, secondAdmin := uuid.New(), uuid.New()

	require.NoError(t, svc.Cancel(context.Background(), tenantID, instanceID, firstAdmin, "first", version))
	require.NoError(t, svc.Cancel(context.Background(), tenantID, instanceID, secondAdmin, "second", version),
		"nothing at the service moves the instance's record version, so the second cancel passes the same check")

	assert.Equal(t, []csdIDCall{{tenantID, instanceID}, {tenantID, instanceID}}, instances.gets)
	require.Len(t, temporal.signals, 2, "the service sends a signal for each accepted cancel")
	for i, admin := range []uuid.UUID{firstAdmin, secondAdmin} {
		sig := temporal.signals[i]
		assert.Equal(t, csdSignalCall{"wf-" + instanceID.String(), instanceID, port.SignalInstanceCancel}, sig.csdSignalCall)
		raw, err := json.Marshal(sig.Payload)
		require.NoError(t, err)
		var wire struct {
			AdminUserID   string
			Reason        string
			RecordVersion int64
		}
		require.NoError(t, json.Unmarshal(raw, &wire))
		assert.Equal(t, admin.String(), wire.AdminUserID)
		assert.EqualValues(t, version, wire.RecordVersion)
	}
	assert.EqualValues(t, version, instances.byID[instanceID].RecordVersion)
	assert.Equal(t, domain.InstanceStatusRunning, instances.byID[instanceID].Status, "the service leaves the status to the workflow")
}
