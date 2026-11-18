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

type w16aAdminWire struct {
	AdminUserID   string
	Reason        string
	Initiator     string
	TargetNodeKey string
	RecordVersion int64
}

func TestInstanceService_LifecycleSignals_EachCarriesItsOwnPayloadFields(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name       string
		status     domain.InstanceStatus
		withDone   bool
		call       func(s *service.InstanceService, tenantID, id, actor uuid.UUID, version int64) error
		wantSignal string
		want       func(actor uuid.UUID) w16aAdminWire
	}{
		{
			name: "pause carries the reason and the admin initiator", status: domain.InstanceStatusRunning, wantSignal: port.SignalInstancePause,
			call: func(s *service.InstanceService, tenantID, id, actor uuid.UUID, v int64) error {
				return s.Pause(ctx, tenantID, id, actor, "holiday freeze", v)
			},
			want: func(actor uuid.UUID) w16aAdminWire {
				return w16aAdminWire{AdminUserID: actor.String(), Reason: "holiday freeze", Initiator: "admin", RecordVersion: 7}
			},
		},
		{
			name: "resume carries the admin initiator and no reason", status: domain.InstanceStatusPaused, wantSignal: port.SignalInstanceResume,
			call: func(s *service.InstanceService, tenantID, id, actor uuid.UUID, v int64) error {
				return s.Resume(ctx, tenantID, id, actor, v)
			},
			want: func(actor uuid.UUID) w16aAdminWire {
				return w16aAdminWire{AdminUserID: actor.String(), Initiator: "admin", RecordVersion: 7}
			},
		},
		{
			name: "cancel carries the reason and no initiator", status: domain.InstanceStatusRunning, wantSignal: port.SignalInstanceCancel,
			call: func(s *service.InstanceService, tenantID, id, actor uuid.UUID, v int64) error {
				return s.Cancel(ctx, tenantID, id, actor, "raised in error", v)
			},
			want: func(actor uuid.UUID) w16aAdminWire {
				return w16aAdminWire{AdminUserID: actor.String(), Reason: "raised in error", RecordVersion: 7}
			},
		},
		{
			name: "force-forward carries the target node key and no reason or initiator", status: domain.InstanceStatusRunning, wantSignal: port.SignalInstanceForceFwd,
			call: func(s *service.InstanceService, tenantID, id, actor uuid.UUID, v int64) error {
				return s.ForceForward(ctx, tenantID, id, actor, "finance/review", v)
			},
			want: func(actor uuid.UUID) w16aAdminWire {
				return w16aAdminWire{AdminUserID: actor.String(), TargetNodeKey: "finance/review", RecordVersion: 7}
			},
		},
		{
			name: "force-back of a degraded instance carries the target node key", status: domain.InstanceStatusDegraded, wantSignal: port.SignalInstanceForceBack,
			call: func(s *service.InstanceService, tenantID, id, actor uuid.UUID, v int64) error {
				return s.ForceBack(ctx, tenantID, id, actor, "finance/review", v)
			},
			want: func(actor uuid.UUID) w16aAdminWire {
				return w16aAdminWire{AdminUserID: actor.String(), TargetNodeKey: "finance/review", RecordVersion: 7}
			},
		},
		{
			name: "force-back of a running instance drops the target node key", status: domain.InstanceStatusRunning, withDone: true, wantSignal: port.SignalInstanceForceBack,
			call: func(s *service.InstanceService, tenantID, id, actor uuid.UUID, v int64) error {
				return s.ForceBack(ctx, tenantID, id, actor, "finance/review", v)
			},
			want: func(actor uuid.UUID) w16aAdminWire {
				return w16aAdminWire{AdminUserID: actor.String(), RecordVersion: 7}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w13aNewLifecycleRig(t, compiledPlanJSON(t, assignedStage("review")))
			inst := rig.seed(tc.status, 7)
			if tc.withDone {
				done := &domain.Task{ID: uuid.New(), TenantID: rig.tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusCompleted}
				rig.tasks.byID[done.ID] = done
			}
			actor := uuid.New()

			require.NoError(t, tc.call(rig.svc, rig.tenantID, inst.ID, actor, 7))

			require.Len(t, rig.temporal.signals, 1)
			sig := rig.temporal.signals[0]
			assert.Equal(t, tc.wantSignal, sig.SignalName)
			assert.Equal(t, inst.TemporalWorkflowID, sig.TemporalWorkflowID)
			assert.Equal(t, inst.ID, sig.InstanceID)
			raw, err := json.Marshal(sig.Payload)
			require.NoError(t, err)
			var got w16aAdminWire
			require.NoError(t, json.Unmarshal(raw, &got))
			assert.Equal(t, tc.want(actor), got)
			assert.Equal(t, []w13aInstanceCall{{rig.tenantID, inst.ID}}, rig.instances.reads, "the instance is read once, in the caller's tenant")
		})
	}
}
