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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

const conflictCounter = "workflow_execution_service_record_version_conflict_total"

func conflictCount(t *testing.T, table string) float64 {
	t.Helper()
	return obstest.Sum(t, conflictCounter, map[string]string{"table": table})
}

func TestInstanceLifecycle_StaleRecordVersion_CountsAnInstanceConflict(t *testing.T) {
	obstest.Init(t)
	const staleVersion = 1
	cases := []struct {
		name   string
		status domain.InstanceStatus
		call   func(h *cscHarness, tenantID, instanceID uuid.UUID) error
	}{
		{"pause", domain.InstanceStatusRunning, func(h *cscHarness, tn, id uuid.UUID) error {
			return h.svc.Pause(context.Background(), tn, id, uuid.New(), "reason", staleVersion)
		}},
		{"resume", domain.InstanceStatusPaused, func(h *cscHarness, tn, id uuid.UUID) error {
			return h.svc.Resume(context.Background(), tn, id, uuid.New(), staleVersion)
		}},
		{"cancel", domain.InstanceStatusRunning, func(h *cscHarness, tn, id uuid.UUID) error {
			return h.svc.Cancel(context.Background(), tn, id, uuid.New(), "reason", staleVersion)
		}},
		{"force forward", domain.InstanceStatusRunning, func(h *cscHarness, tn, id uuid.UUID) error {
			return h.svc.ForceForward(context.Background(), tn, id, uuid.New(), "finance/review", staleVersion)
		}},
		{"force back", domain.InstanceStatusRunning, func(h *cscHarness, tn, id uuid.UUID) error {
			return h.svc.ForceBack(context.Background(), tn, id, uuid.New(), "finance/review", staleVersion)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := uuid.New()
			inst := newCscInstance(tenantID, tc.status, 5)
			h := newCscHarness(inst)
			beforeInstance, beforeTask := conflictCount(t, "workflow_instance"), conflictCount(t, "workflow_task")

			err := tc.call(h, tenantID, inst.ID)

			require.ErrorIs(t, err, port.ErrRecordVersionConflict)
			assert.Equal(t, beforeInstance+1, conflictCount(t, "workflow_instance"), "one conflict is counted for the instance")
			assert.Equal(t, beforeTask, conflictCount(t, "workflow_task"))
			assert.Empty(t, h.temporal.signals)
		})
	}
}

func TestInstanceLifecycle_CurrentRecordVersion_CountsNoConflict(t *testing.T) {
	obstest.Init(t)
	tenantID := uuid.New()
	inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 5)
	h := newCscHarness(inst)
	before := conflictCount(t, "workflow_instance")

	require.NoError(t, h.svc.Pause(context.Background(), tenantID, inst.ID, uuid.New(), "reason", 5))

	assert.Equal(t, before, conflictCount(t, "workflow_instance"))
}

func TestTaskActions_StaleRecordVersion_CountsATaskConflict(t *testing.T) {
	obstest.Init(t)
	const staleVersion = csdTaskVersion - 1
	cases := []struct {
		name string
		mode string
		call func(h *taHarness) error
	}{
		{"claim", "all", func(h *taHarness) error {
			_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, staleVersion)
			return err
		}},
		{"complete", "single", func(h *taHarness) error {
			_, err := h.svc.Complete(context.Background(), h.tenantID, h.task.ID, h.callerID, json.RawMessage(`{}`), staleVersion)
			return err
		}},
		{"defer", "single", func(h *taHarness) error {
			_, err := h.svc.Defer(context.Background(), h.tenantID, h.task.ID, h.callerID, "reason", staleVersion)
			return err
		}},
		{"reassign", "single", func(h *taHarness) error {
			_, err := h.svc.Reassign(context.Background(), h.tenantID, h.task.ID, uuid.New(), uuid.New(), staleVersion)
			return err
		}},
		{"override", "single", func(h *taHarness) error {
			in := h.overrideInput(uuid.New())
			in.RecordVersion = staleVersion
			_, err := h.svc.OverrideAssignee(context.Background(), in)
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTaHarness(t, tc.mode)
			beforeTask, beforeInstance := conflictCount(t, "workflow_task"), conflictCount(t, "workflow_instance")

			err := tc.call(h)

			require.ErrorIs(t, err, port.ErrRecordVersionConflict)
			assert.Equal(t, beforeTask+1, conflictCount(t, "workflow_task"), "one conflict is counted for the task")
			assert.Equal(t, beforeInstance, conflictCount(t, "workflow_instance"))
			h.requireNothingChanged(t)
		})
	}
}

func TestTaskComplete_CompletionAlreadyReserved_CountsATaskConflict(t *testing.T) {
	obstest.Init(t)
	h := newTaHarness(t, "single")
	cache := newFakeCacheStore()
	cache.values["task-complete:"+h.task.ID.String()] = "reserved"
	h.svc.Cache = cache
	before := conflictCount(t, "workflow_task")

	_, err := h.svc.Complete(context.Background(), h.tenantID, h.task.ID, h.callerID, json.RawMessage(`{}`), csdTaskVersion)

	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.Equal(t, before+1, conflictCount(t, "workflow_task"))
	assert.Empty(t, h.temporalLog.signals)
}

func TestTaskActions_CurrentRecordVersion_CountsNoConflict(t *testing.T) {
	obstest.Init(t)
	h := newTaHarness(t, "all")
	before := conflictCount(t, "workflow_task")

	_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.NoError(t, err)
	assert.Equal(t, before, conflictCount(t, "workflow_task"))
	require.NotNil(t, observability.RecordVersionConflictTotal)
}
