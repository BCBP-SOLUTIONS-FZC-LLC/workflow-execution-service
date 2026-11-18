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

func TestReassignAssignment_NoPreviousUserAndNoLeadRetryAfterTheCommitChangesNothingInPostgres(t *testing.T) {
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
	require.NoError(t, assignmentRepo.Create(withGUC(ctx, tenantID), newTaskAssignment(tenantID, task.ID, uuid.New())))
	newUser := uuid.New()
	in := port.ReassignAssignmentInput{
		TenantID: tenantID.String(), TaskID: task.ID.String(), NewUserID: newUser.String(),
		AdminUserID: uuid.NewString(), RecordVersion: task.RecordVersion,
	}

	require.NoError(t, deps.ReassignAssignment(ctx, in))
	require.NoError(t, deps.ReassignAssignment(ctx, in), "a retry after the commit is done, not a version conflict")

	active, err := assignmentRepo.ListActiveByTask(ctx, tenantID, task.ID)
	require.NoError(t, err)
	forNewUser := 0
	for _, a := range active {
		if a.UserID == newUser {
			forNewUser++
		}
	}
	assert.Equal(t, 1, forNewUser, "one assignment for the new user")
	stored, err := taskRepo.GetByID(ctx, tenantID, task.ID)
	require.NoError(t, err)
	assert.Equal(t, task.RecordVersion+1, stored.RecordVersion, "the task version moved once")
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
