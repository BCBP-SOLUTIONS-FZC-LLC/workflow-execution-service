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

type w12bForceBackIDCall struct{ tenantID, id uuid.UUID }

type w12bForceBackInstances struct {
	port.InstanceRepository
	inst  *domain.Instance
	reads []w12bForceBackIDCall
}

func (r *w12bForceBackInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, w12bForceBackIDCall{tenantID, id})
	if tenantID != r.inst.TenantID || id != r.inst.ID {
		return nil, domain.ErrNotFound
	}
	return r.inst, nil
}

type w12bForceBackTasks struct {
	port.TaskRepository
	tasks []*domain.Task
	lists []w12bForceBackIDCall
}

func (r *w12bForceBackTasks) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, _ port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.lists = append(r.lists, w12bForceBackIDCall{tenantID, instanceID})
	return r.tasks, nil, nil
}

type w12bForceBackSignal struct {
	temporalWorkflowID string
	instanceID         uuid.UUID
	name               string
	payload            any
}

type w12bForceBackTemporal struct {
	port.TemporalClient
	signals []w12bForceBackSignal
}

func (c *w12bForceBackTemporal) SignalWorkflow(_ context.Context, temporalWorkflowID string, instanceID uuid.UUID, name string, payload any) error {
	c.signals = append(c.signals, w12bForceBackSignal{temporalWorkflowID, instanceID, name, payload})
	return nil
}

func TestInstanceService_ForceBack_TheTargetKeyDependsOnTheInstanceStatus(t *testing.T) {
	cases := []struct {
		name          string
		status        domain.InstanceStatus
		taskStatuses  []domain.TaskStatus
		callerKey     string
		wantRefused   bool
		wantListed    bool
		wantTargetKey string
	}{
		{"a running instance with a completed step discards the caller's key", domain.InstanceStatusRunning, []domain.TaskStatus{domain.TaskStatusReady, domain.TaskStatusCompleted}, "billing/prep", false, true, ""},
		{"a running instance with a completed step and no key sends none", domain.InstanceStatusRunning, []domain.TaskStatus{domain.TaskStatusCompleted}, "", false, true, ""},
		{"a running instance with no completed step is refused even with a key", domain.InstanceStatusRunning, []domain.TaskStatus{domain.TaskStatusReady, domain.TaskStatusInProgress}, "billing/prep", true, true, ""},
		{"a degraded instance keeps the caller's key and reads no tasks", domain.InstanceStatusDegraded, nil, "billing/prep", false, false, "billing/prep"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID, actorID := uuid.New(), uuid.New()
			inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: tc.status, RecordVersion: 3, TemporalWorkflowID: "tenant:subject:key"}
			instances := &w12bForceBackInstances{inst: inst}
			tasks := &w12bForceBackTasks{}
			for _, status := range tc.taskStatuses {
				tasks.tasks = append(tasks.tasks, &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: status})
			}
			temporal := &w12bForceBackTemporal{}
			svc := &service.InstanceService{Instances: instances, Tasks: tasks, Temporal: temporal}

			err := svc.ForceBack(context.Background(), tenantID, inst.ID, actorID, tc.callerKey, 3)

			assert.Equal(t, []w12bForceBackIDCall{{tenantID, inst.ID}}, instances.reads)
			if tc.wantListed {
				assert.Equal(t, []w12bForceBackIDCall{{tenantID, inst.ID}}, tasks.lists, "the tasks of this instance are listed in the caller's tenant")
			} else {
				assert.Empty(t, tasks.lists)
			}
			if tc.wantRefused {
				require.ErrorIs(t, err, port.ErrForceBackNoSavedBranch)
				assert.Empty(t, temporal.signals)
				return
			}
			require.NoError(t, err)
			require.Len(t, temporal.signals, 1)
			sent := temporal.signals[0]
			assert.Equal(t, "tenant:subject:key", sent.temporalWorkflowID)
			assert.Equal(t, inst.ID, sent.instanceID)
			assert.Equal(t, port.SignalInstanceForceBack, sent.name)
			raw, err := json.Marshal(sent.payload)
			require.NoError(t, err)
			var wire struct {
				AdminUserID   string
				TargetNodeKey string
				RecordVersion int64
			}
			require.NoError(t, json.Unmarshal(raw, &wire))
			assert.Equal(t, tc.wantTargetKey, wire.TargetNodeKey)
			assert.Equal(t, actorID.String(), wire.AdminUserID)
			assert.EqualValues(t, 3, wire.RecordVersion)
		})
	}
}
