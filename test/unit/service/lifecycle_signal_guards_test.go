package service_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

func TestInstanceLifecycle_SignalDeadline_ReturnsTheDeadlineErrorAfterOneAttempt(t *testing.T) {
	tenantID, actorID := uuid.New(), uuid.New()
	inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 4)
	h := newCscHarness(inst)
	h.temporal.signalErr = fmt.Errorf("signal workflow: %w", context.DeadlineExceeded)

	err := h.svc.Pause(context.Background(), tenantID, inst.ID, actorID, "maintenance", 4)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, h.temporal.signals, 1)
	sig := h.temporal.signals[0]
	assert.Equal(t, inst.TemporalWorkflowID, sig.WorkflowID)
	assert.Equal(t, inst.ID, sig.InstanceID)
	assert.Equal(t, port.SignalInstancePause, sig.Name)
	wire := cscDecodeWire(t, sig.Payload)
	assert.Equal(t, actorID.String(), wire.AdminUserID)
	assert.Equal(t, "maintenance", wire.Reason)
	assert.EqualValues(t, 4, wire.RecordVersion)
	assert.Zero(t, h.tx.runs, "a failed signal opens no transaction")
}

func TestInstanceLifecycle_OtherTenantsInstance_ReturnsNotFoundAndSignalsNothing(t *testing.T) {
	type call func(svc *service.InstanceService, tenantID, instanceID, actorID uuid.UUID, version int64) error

	cases := []struct {
		name       string
		status     domain.InstanceStatus
		call       call
		wantSignal string
	}{
		{"pause", domain.InstanceStatusRunning, func(s *service.InstanceService, tn, id, a uuid.UUID, v int64) error {
			return s.Pause(context.Background(), tn, id, a, "reason", v)
		}, port.SignalInstancePause},
		{"resume", domain.InstanceStatusPaused, func(s *service.InstanceService, tn, id, a uuid.UUID, v int64) error {
			return s.Resume(context.Background(), tn, id, a, v)
		}, port.SignalInstanceResume},
		{"cancel", domain.InstanceStatusRunning, func(s *service.InstanceService, tn, id, a uuid.UUID, v int64) error {
			return s.Cancel(context.Background(), tn, id, a, "reason", v)
		}, port.SignalInstanceCancel},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ownerTenant, callerTenant, actorID := uuid.New(), uuid.New(), uuid.New()
			inst := newCscInstance(ownerTenant, tc.status, 2)
			h := newCscHarness(inst)

			err := tc.call(h.svc, callerTenant, inst.ID, actorID, 2)

			require.ErrorIs(t, err, port.ErrInstanceNotFound)
			require.Len(t, h.instances.gets, 1)
			assert.Equal(t, callerTenant, h.instances.gets[0].TenantID, "the read must be scoped to the caller's tenant")
			assert.Equal(t, inst.ID, h.instances.gets[0].InstanceID)
			assert.Empty(t, h.temporal.signals)
			assert.Zero(t, h.tx.runs)
			assert.Equal(t, tc.status, h.instances.byID[inst.ID].Status)
			assert.EqualValues(t, 2, h.instances.byID[inst.ID].RecordVersion)

			// The same call by the owning tenant goes through, so the refusal above
			// came from tenant scoping and not from the instance's state or version.
			require.NoError(t, tc.call(h.svc, ownerTenant, inst.ID, actorID, 2))
			require.Len(t, h.temporal.signals, 1)
			assert.Equal(t, tc.wantSignal, h.temporal.signals[0].Name)
		})
	}
}

func TestInstanceLifecycle_InstanceReadDatabaseFailure_ReturnsTheRawErrorAndActsOnNothing(t *testing.T) {
	cases := []struct {
		name string
		call func(svc *service.InstanceService, tenantID, instanceID, actorID uuid.UUID) error
	}{
		{"pause", func(s *service.InstanceService, tn, id, a uuid.UUID) error {
			return s.Pause(context.Background(), tn, id, a, "reason", 1)
		}},
		{"cancel", func(s *service.InstanceService, tn, id, a uuid.UUID) error {
			return s.Cancel(context.Background(), tn, id, a, "reason", 1)
		}},
		{"force-back", func(s *service.InstanceService, tn, id, a uuid.UUID) error {
			return s.ForceBack(context.Background(), tn, id, a, "finance/prep", 1)
		}},
		{"terminate", func(s *service.InstanceService, tn, id, a uuid.UUID) error {
			return s.Terminate(context.Background(), tn, id, a, "reason")
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := uuid.New()
			inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 1)
			h := newCscHarness(inst)
			h.instances.getErr = errCscDatabase

			err := tc.call(h.svc, tenantID, inst.ID, uuid.New())

			require.ErrorIs(t, err, errCscDatabase)
			for _, sentinel := range []error{
				port.ErrInstanceNotFound, port.ErrRecordVersionConflict,
				port.ErrInstanceAlreadyTerminal, port.ErrInvalidInstanceState,
			} {
				assert.NotErrorIs(t, err, sentinel, "a database failure must not look like a client error")
			}
			require.Len(t, h.instances.gets, 1)
			assert.Equal(t, tenantID, h.instances.gets[0].TenantID)
			assert.Equal(t, inst.ID, h.instances.gets[0].InstanceID)
			assert.Empty(t, h.temporal.signals)
			assert.Empty(t, h.temporal.terminates)
			assert.Empty(t, h.outbox.attempts)
			assert.Zero(t, h.tx.runs)
		})
	}
}

func TestInstanceLifecycle_CancelWithStaleVersionOnFinishedInstance_IsRefusedWithoutSignal(t *testing.T) {
	for _, status := range []domain.InstanceStatus{
		domain.InstanceStatusCompleted, domain.InstanceStatusTerminated, domain.InstanceStatusFailed,
	} {
		t.Run(string(status), func(t *testing.T) {
			tenantID := uuid.New()
			inst := newCscInstance(tenantID, status, 6)
			h := newCscHarness(inst)

			err := h.svc.Cancel(context.Background(), tenantID, inst.ID, uuid.New(), "late", 5)

			require.Error(t, err)
			assert.True(t, errors.Is(err, port.ErrRecordVersionConflict) || errors.Is(err, port.ErrInstanceAlreadyTerminal),
				"a stale cancel of a finished instance is refused as a conflict or as already terminal, got %v", err)
			assert.Empty(t, h.temporal.signals)

			// With the current version the same instance is reported as finished.
			err = h.svc.Cancel(context.Background(), tenantID, inst.ID, uuid.New(), "late", 6)
			require.ErrorIs(t, err, port.ErrInstanceAlreadyTerminal)
			assert.Empty(t, h.temporal.signals)
		})
	}
}

func TestInstanceLifecycle_CancelInUnlistedStatus_ReturnsInvalidStateNotTerminal(t *testing.T) {
	cases := []struct {
		name   string
		status domain.InstanceStatus
	}{
		{"unknown status value", domain.InstanceStatus("SUSPENDED")},
		{"empty status", domain.InstanceStatus("")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := uuid.New()
			inst := newCscInstance(tenantID, tc.status, 3)
			h := newCscHarness(inst)

			err := h.svc.Cancel(context.Background(), tenantID, inst.ID, uuid.New(), "reason", 3)

			require.ErrorIs(t, err, port.ErrInvalidInstanceState)
			assert.NotErrorIs(t, err, port.ErrInstanceAlreadyTerminal)
			assert.Empty(t, h.temporal.signals)
			assert.Zero(t, h.tx.runs)
		})
	}
}

func TestInstanceService_ForceBack_VersionDiffersFromCurrent_ReturnsConflictAndSignalsNothing(t *testing.T) {
	cases := []struct {
		name    string
		version int64
	}{
		{"minus one", -1},
		{"older than current", 2},
		{"newer than current", 4},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID, actorID := uuid.New(), uuid.New()
			inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 3)
			h := newCscHarness(inst)
			done := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusCompleted}
			h.tasks.byID[done.ID] = done

			err := h.svc.ForceBack(context.Background(), tenantID, inst.ID, actorID, "", tc.version)

			require.ErrorIs(t, err, port.ErrRecordVersionConflict)
			assert.Empty(t, h.temporal.signals)
			require.Len(t, h.instances.gets, 1)
			assert.Equal(t, tenantID, h.instances.gets[0].TenantID)
			assert.Equal(t, inst.ID, h.instances.gets[0].InstanceID)

			// Control: the current version is accepted for the same instance and task
			// history, so the refusal above is the version check alone.
			require.NoError(t, h.svc.ForceBack(context.Background(), tenantID, inst.ID, actorID, "", 3))
			require.Len(t, h.temporal.signals, 1)
			assert.Equal(t, port.SignalInstanceForceBack, h.temporal.signals[0].Name)
		})
	}
}

func TestInstanceService_ForceForward_DegradedInstance_SignalsTheTargetNode(t *testing.T) {
	tenantID, actorID := uuid.New(), uuid.New()
	inst := newCscInstance(tenantID, domain.InstanceStatusDegraded, 4)
	h := newCscHarness(inst)
	h.definitions.resp = &port.CompiledWorkflow{
		VersionID: inst.WorkflowVersionID, Status: "PUBLISHED", IsValid: true,
		CompiledPlanJSON: cscPlanJSON(t, "finance", "prep", "settlement"),
	}

	err := h.svc.ForceForward(context.Background(), tenantID, inst.ID, actorID, "finance/settlement", 4)

	require.NoError(t, err)
	require.Len(t, h.temporal.signals, 1)
	sig := h.temporal.signals[0]
	assert.Equal(t, port.SignalInstanceForceFwd, sig.Name)
	assert.Equal(t, inst.TemporalWorkflowID, sig.WorkflowID)
	assert.Equal(t, inst.ID, sig.InstanceID)
	wire := cscDecodeWire(t, sig.Payload)
	assert.Equal(t, "finance/settlement", wire.TargetNodeKey)
	assert.Equal(t, actorID.String(), wire.AdminUserID)
	assert.EqualValues(t, 4, wire.RecordVersion)
	require.Len(t, h.definitions.reads, 1)
	assert.Equal(t, cscPlanRead{TenantID: tenantID, VersionID: inst.WorkflowVersionID}, h.definitions.reads[0])
}
