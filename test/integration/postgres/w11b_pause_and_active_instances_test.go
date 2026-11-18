//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type w11bPauseSignal struct {
	workflowID string
	instanceID uuid.UUID
	name       string
}

type w11bPauseTemporal struct {
	port.TemporalClient
	signals []w11bPauseSignal
}

func (c *w11bPauseTemporal) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, signalName string, _ any) error {
	c.signals = append(c.signals, w11bPauseSignal{workflowID: workflowID, instanceID: instanceID, name: signalName})
	return nil
}

func TestArchiveGuard_CountsRunningPausedAndDegradedInstancesOfTheTenantsWorkflow(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	guard := &service.ArchiveGuard{Instances: instances}
	ctx := context.Background()

	seed := func(t *testing.T, tenantID, workflowID uuid.UUID, status domain.InstanceStatus) {
		t.Helper()
		inst := newInstance(tenantID, time.Now().UTC())
		inst.WorkflowID = workflowID
		require.NoError(t, instances.Create(ctx, inst))
		if status != domain.InstanceStatusRunning {
			_, err := instances.UpdateStatus(ctx, tenantID, inst.ID, status, inst.RecordVersion)
			require.NoError(t, err)
		}
	}

	cases := []struct {
		name       string
		statuses   []domain.InstanceStatus
		otherFlow  []domain.InstanceStatus
		otherTen   []domain.InstanceStatus
		wantActive bool
		wantCount  int32
	}{
		{
			name: "two running and one paused",
			statuses: []domain.InstanceStatus{
				domain.InstanceStatusRunning, domain.InstanceStatusRunning, domain.InstanceStatusPaused,
			},
			wantActive: true, wantCount: 3,
		},
		{
			name: "a degraded instance counts too",
			statuses: []domain.InstanceStatus{
				domain.InstanceStatusRunning, domain.InstanceStatusPaused, domain.InstanceStatusDegraded,
			},
			wantActive: true, wantCount: 3,
		},
		{
			name: "instances that ended, another workflow and another tenant do not count",
			statuses: []domain.InstanceStatus{
				domain.InstanceStatusRunning, domain.InstanceStatusRunning, domain.InstanceStatusPaused,
				domain.InstanceStatusCompleted, domain.InstanceStatusTerminated, domain.InstanceStatusFailed,
			},
			otherFlow:  []domain.InstanceStatus{domain.InstanceStatusRunning},
			otherTen:   []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusRunning},
			wantActive: true, wantCount: 3,
		},
		{
			name: "only ended instances",
			statuses: []domain.InstanceStatus{
				domain.InstanceStatusCompleted, domain.InstanceStatusTerminated,
			},
			wantActive: false, wantCount: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID, otherTenantID, workflowID := uuid.New(), uuid.New(), uuid.New()
			for _, status := range tc.statuses {
				seed(t, tenantID, workflowID, status)
			}
			for _, status := range tc.otherFlow {
				seed(t, tenantID, uuid.New(), status)
			}
			for _, status := range tc.otherTen {
				seed(t, otherTenantID, workflowID, status)
			}

			hasActive, count, err := guard.CheckActiveInstances(ctx, tenantID, workflowID)

			require.NoError(t, err)
			assert.Equal(t, tc.wantActive, hasActive)
			assert.Equal(t, tc.wantCount, count)
		})
	}
}

func TestPauseUserTasks_AUserHoldingTasksInTwoTenants_IsOnlyPausedInTheTenantOfTheCall(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	seed := func(t *testing.T, tenantID, userID uuid.UUID) *domain.Instance {
		t.Helper()
		inst := newInstance(tenantID, time.Now().UTC())
		require.NoError(t, instanceRepo.Create(ctx, inst))
		task := newTask(tenantID, inst.ID)
		require.NoError(t, taskRepo.Create(ctx, task))
		require.NoError(t, assignmentRepo.Create(ctx, newTaskAssignment(tenantID, task.ID, userID)))
		return inst
	}

	cases := []struct {
		name string
		call func(owning, other uuid.UUID) uuid.UUID
		want func(owning, other *domain.Instance) []w11bPauseSignal
	}{
		{
			name: "the owning tenant pauses only its own instance",
			call: func(owning, _ uuid.UUID) uuid.UUID { return owning },
			want: func(owning, _ *domain.Instance) []w11bPauseSignal {
				return []w11bPauseSignal{{owning.TemporalWorkflowID, owning.ID, port.SignalInstancePause}}
			},
		},
		{
			name: "the other tenant pauses only its own instance",
			call: func(_, other uuid.UUID) uuid.UUID { return other },
			want: func(_, other *domain.Instance) []w11bPauseSignal {
				return []w11bPauseSignal{{other.TemporalWorkflowID, other.ID, port.SignalInstancePause}}
			},
		},
		{
			name: "a tenant that owns none of the user's tasks pauses nothing",
			call: func(_, _ uuid.UUID) uuid.UUID { return uuid.New() },
			want: func(_, _ *domain.Instance) []w11bPauseSignal { return nil },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			owningTenant, otherTenant, userID := uuid.New(), uuid.New(), uuid.New()
			owning := seed(t, owningTenant, userID)
			other := seed(t, otherTenant, userID)
			temporal := &w11bPauseTemporal{}
			pauser := &service.UserTaskPauser{Instances: instanceRepo, Assignments: assignmentRepo, Tasks: taskRepo, Temporal: temporal}

			require.NoError(t, pauser.PauseUserTasks(ctx, tc.call(owningTenant, otherTenant), userID, nil), "a tenant without tasks is a plain success")

			assert.Equal(t, tc.want(owning, other), temporal.signals)
		})
	}
}
