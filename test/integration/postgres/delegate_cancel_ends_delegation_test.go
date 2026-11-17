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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestCancelByDelegate_RecordsTheNamedDelegationAsEndedUnderRowLevelSecurity(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ended := postgres.NewEndedDelegationRepo(appPool)
	client := &service.WorkflowClient{
		Instances: instanceRepo, Tasks: taskRepo, Assignments: assignmentRepo,
		Transactor: postgres.NewTransactor(appPool), Signals: postgres.NewOutboxRepo(appPool), Ended: ended,
	}
	ctx := context.Background()
	tenantID, delegate, delegationID := uuid.New(), uuid.New(), uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	task := newTask(tenantID, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, task))
	held := newTaskAssignment(tenantID, task.ID, delegate)
	held.Reason = "delegation:" + delegationID.String()
	require.NoError(t, assignmentRepo.Create(ctx, held))

	count, err := client.CancelByDelegate(ctx, port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate, DelegationID: &delegationID})

	require.NoError(t, err)
	assert.Equal(t, 1, count)
	isEnded, err := ended.IsEnded(ctx, tenantID, delegationID)
	require.NoError(t, err)
	assert.True(t, isEnded, "the cancel ended the delegation")

	noneNamed, err := client.CancelByDelegate(ctx, port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate})
	require.NoError(t, err)
	assert.Zero(t, noneNamed)
}
