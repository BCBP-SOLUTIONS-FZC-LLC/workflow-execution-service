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

func TestResumeInstance_TheResumedEventNamesTheInitiatorAndTheAdminWhoResumed(t *testing.T) {
	cases := []struct {
		name      string
		initiator string
	}{
		{"an admin resume", domain.InitiatorAdmin},
		{"a tenant-state resume", domain.InitiatorTenantState},
		{"an out-of-office resume", domain.InitiatorOOO},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			instanceID, tenantID, adminID, startedBy := uuid.New(), uuid.New(), uuid.New(), uuid.New()
			inst := &domain.Instance{
				ID: instanceID, TenantID: tenantID, RecordVersion: 1, Status: domain.InstanceStatusPaused, StartedByUserID: startedBy,
			}
			deps, outbox := newInstanceTestDeps(inst, nil, nil)
			in := port.ResumeInstanceInput{
				InstanceID: instanceID.String(), TenantID: tenantID.String(), AdminUserID: adminID.String(), Initiator: tc.initiator, RecordVersion: 1,
			}

			require.NoError(t, deps.ResumeInstance(context.Background(), in))
			require.NoError(t, deps.ResumeInstance(context.Background(), in), "a retried resume succeeds without a second write")

			assert.Equal(t, domain.InstanceStatusRunning, inst.Status)
			require.Len(t, outbox.enqueued, 1, "the retry enqueues no second event")
			assert.Equal(t, domain.EventWorkflowInstanceResumed, outbox.enqueued[0].Type)
			assert.Equal(t, tenantID.String(), outbox.enqueued[0].TenantID)
			var payload domain.WorkflowInstanceResumedPayload
			require.NoError(t, json.Unmarshal(outbox.enqueued[0].Payload, &payload))
			assert.Equal(t, instanceID, payload.WorkflowInstanceID)
			assert.Equal(t, tc.initiator, payload.Initiator)
			assert.Equal(t, startedBy, payload.StartedByUserID)
			require.NotNil(t, payload.ActorUserID)
			assert.Equal(t, adminID, *payload.ActorUserID)
		})
	}
}
