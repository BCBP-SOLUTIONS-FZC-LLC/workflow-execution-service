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

func TestCancelByDelegate_ActiveAssignmentOnATerminalInstance_IsVacatedAndCounted(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	client := &service.WorkflowClient{Assignments: assignmentRepo}
	ctx := context.Background()

	for _, status := range []domain.InstanceStatus{
		domain.InstanceStatusCompleted, domain.InstanceStatusTerminated, domain.InstanceStatusFailed,
	} {
		t.Run(string(status), func(t *testing.T) {
			tenantID, otherTenantID, delegate := uuid.New(), uuid.New(), uuid.New()
			inst := newInstance(tenantID, time.Now().UTC())
			require.NoError(t, instanceRepo.Create(ctx, inst))
			task := newTask(tenantID, inst.ID)
			require.NoError(t, taskRepo.Create(ctx, task))
			assignment := newTaskAssignment(tenantID, task.ID, delegate)
			require.NoError(t, assignmentRepo.Create(ctx, assignment))
			ended, err := instanceRepo.UpdateStatus(ctx, tenantID, inst.ID, status, inst.RecordVersion)
			require.NoError(t, err)
			require.Equal(t, status, ended.Status)

			otherTenantCount, err := client.CancelByDelegate(ctx, port.CancelByDelegateInput{TenantID: otherTenantID, DelegateUserID: delegate})
			require.NoError(t, err)
			assert.Zero(t, otherTenantCount, "another tenant's call must not reach this tenant's assignment")
			stillActive, err := assignmentRepo.GetByID(ctx, tenantID, assignment.ID)
			require.NoError(t, err)
			require.True(t, stillActive.IsActive)

			count, err := client.CancelByDelegate(ctx, port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate})

			require.NoError(t, err)
			assert.Equal(t, 1, count)
			vacated, err := assignmentRepo.GetByID(ctx, tenantID, assignment.ID)
			require.NoError(t, err)
			assert.False(t, vacated.IsActive)
			assert.NotNil(t, vacated.VacatedAt)
			active, err := assignmentRepo.ListActiveByUser(ctx, tenantID, delegate)
			require.NoError(t, err)
			assert.Empty(t, active)

			again, err := client.CancelByDelegate(ctx, port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate})
			require.NoError(t, err)
			assert.Zero(t, again, "a vacated assignment is not counted twice")
		})
	}
}
