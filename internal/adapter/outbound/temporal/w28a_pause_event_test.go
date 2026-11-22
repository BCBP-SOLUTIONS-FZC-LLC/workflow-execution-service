package temporal_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestPauseInstance_ThePausedEventNamesTheInitiatorAndTheAdminFromTheSignal(t *testing.T) {
	cases := []struct {
		name      string
		initiator string
		hasAdmin  bool
	}{
		{name: "an admin pause", initiator: domain.InitiatorAdmin, hasAdmin: true},
		{name: "a safety net pause that carries an admin id", initiator: domain.InitiatorSafetyNet, hasAdmin: true},
		{name: "a tenant state pause with no admin", initiator: domain.InitiatorTenantState, hasAdmin: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			instanceID, tenantID, adminID, startedBy := uuid.New(), uuid.New(), uuid.New(), uuid.New()
			inst := &domain.Instance{
				ID: instanceID, TenantID: tenantID, RecordVersion: 4, Status: domain.InstanceStatusRunning, StartedByUserID: startedBy,
			}
			deps, outbox := newInstanceTestDeps(inst, nil, nil)
			in := port.PauseInstanceInput{
				InstanceID: instanceID.String(), TenantID: tenantID.String(), Initiator: tc.initiator, RecordVersion: 4,
			}
			if tc.hasAdmin {
				in.AdminUserID = adminID.String()
			}

			require.NoError(t, deps.PauseInstance(context.Background(), in))

			assert.Equal(t, domain.InstanceStatusPaused, inst.Status)
			require.Len(t, outbox.enqueued, 1)
			assert.Equal(t, domain.EventWorkflowInstancePaused, outbox.enqueued[0].Type)
			assert.Equal(t, tenantID.String(), outbox.enqueued[0].TenantID)
			var payload domain.WorkflowInstancePausedPayload
			require.NoError(t, json.Unmarshal(outbox.enqueued[0].Payload, &payload))
			assert.Equal(t, instanceID, payload.WorkflowInstanceID)
			assert.Equal(t, startedBy, payload.StartedByUserID)
			assert.Equal(t, tc.initiator, payload.Initiator)
			if tc.hasAdmin {
				require.NotNil(t, payload.ActorUserID)
				assert.Equal(t, adminID, *payload.ActorUserID)
			} else {
				assert.Nil(t, payload.ActorUserID)
			}
		})
	}
}
