//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestReassignAssignment_ToACoAssigneeWhoIsNotLead_MovesTheLeadInPostgres(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	deps := &temporal.Deps{
		Instances: instanceRepo, Tasks: taskRepo, Assignments: assignmentRepo, Outbox: outboxRepo,
		Transactor: postgres.NewTransactor(appPool), Validator: validator,
	}
	ctx := context.Background()

	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	task := newTask(tenantID, inst.ID)
	task.AssigneeMode = "all"
	require.NoError(t, taskRepo.Create(withGUC(ctx, tenantID), task))
	leadUser, helperUser := uuid.New(), uuid.New()
	lead, helper := newTaskAssignment(tenantID, task.ID, leadUser), newTaskAssignment(tenantID, task.ID, helperUser)
	require.NoError(t, assignmentRepo.Create(withGUC(ctx, tenantID), lead))
	require.NoError(t, assignmentRepo.Create(withGUC(ctx, tenantID), helper))
	_, err = assignmentRepo.SetLead(ctx, tenantID, task.ID, lead.ID, task.RecordVersion)
	require.NoError(t, err)
	version := task.RecordVersion + 1
	in := port.ReassignAssignmentInput{
		TenantID: tenantID.String(), TaskID: task.ID.String(), OldUserID: leadUser.String(),
		NewUserID: helperUser.String(), AdminUserID: uuid.NewString(), RecordVersion: version,
	}

	require.NoError(t, deps.ReassignAssignment(ctx, in))
	require.NoError(t, deps.ReassignAssignment(ctx, in), "a retry after the commit changes nothing")

	active, err := assignmentRepo.ListActiveByTask(ctx, tenantID, task.ID)
	require.NoError(t, err)
	require.Len(t, active, 1)
	assert.Equal(t, helperUser, active[0].UserID)
	assert.True(t, active[0].IsLead)
	stored, err := taskRepo.GetByID(ctx, tenantID, task.ID)
	require.NoError(t, err)
	assert.Equal(t, version+1, stored.RecordVersion, "the move bumps the task version once")
	records, _, err := outboxRepo.ListByInstance(ctx, tenantID, inst.ID, port.PageRequest{Limit: 100})
	require.NoError(t, err)
	reassigned := 0
	for _, rec := range records {
		if rec.EventType == domain.EventWorkflowTaskReassigned {
			reassigned++
		}
	}
	assert.Equal(t, 1, reassigned)
}
