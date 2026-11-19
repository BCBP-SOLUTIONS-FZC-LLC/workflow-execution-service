//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktemporal "go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func w29aTaskCount(t *testing.T, repo port.TaskRepository, tenantID, instanceID uuid.UUID) int {
	t.Helper()
	count := 0
	var cursor *port.Cursor
	for {
		page, next, err := repo.ListByInstance(context.Background(), tenantID, instanceID, port.PageRequest{After: cursor, Limit: 100})
		require.NoError(t, err)
		count += len(page)
		if next == nil {
			return count
		}
		cursor = next
	}
}

func TestCreateTask_UnderRowLevelSecurity_OnlyTheOwningTenantCanCreateAndSeeTheTask(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	deps := &temporal.Deps{
		Instances:   instanceRepo,
		Tasks:       taskRepo,
		Assignments: postgres.NewTaskAssignmentRepo(appPool),
		Outbox:      postgres.NewOutboxRepo(appPool),
		Transactor:  postgres.NewTransactor(appPool),
		Validator:   validator,
	}
	ctx := context.Background()
	assignee := uuid.New()
	inputFor := func(callerTenant uuid.UUID, inst *domain.Instance) port.CreateTaskInput {
		return port.CreateTaskInput{
			InstanceID:      inst.ID.String(),
			TenantID:        callerTenant.String(),
			NodeKey:         "finance/review",
			CompiledNode:    json.RawMessage(`{"type":"userTask","node_id":"review","default_assignees":["` + assignee.String() + `"]}`),
			VisitCount:      1,
			IAMDepartmentID: uuid.New().String(),
		}
	}

	t.Run("the owning tenant creates the task and only it can read the task back", func(t *testing.T) {
		owner, other := uuid.New(), uuid.New()
		inst := newInstance(owner, time.Now().UTC())
		require.NoError(t, instanceRepo.Create(ctx, inst))

		out, err := deps.CreateTask(ctx, inputFor(owner, inst))

		require.NoError(t, err)
		taskID := uuid.MustParse(out.TaskID)
		stored, err := taskRepo.GetByID(ctx, owner, taskID)
		require.NoError(t, err)
		assert.Equal(t, owner, stored.TenantID)
		assert.Equal(t, inst.ID, stored.WorkflowInstanceID)
		assert.Equal(t, 1, w29aTaskCount(t, taskRepo, owner, inst.ID))
		_, err = taskRepo.GetByID(ctx, other, taskID)
		require.ErrorIs(t, err, domain.ErrNotFound, "another tenant cannot read the task")
		assert.Equal(t, 0, w29aTaskCount(t, taskRepo, other, inst.ID))
	})

	t.Run("another tenant naming the instance gets a non-retryable not found and no task exists", func(t *testing.T) {
		owner, other := uuid.New(), uuid.New()
		inst := newInstance(owner, time.Now().UTC())
		require.NoError(t, instanceRepo.Create(ctx, inst))

		out, err := deps.CreateTask(ctx, inputFor(other, inst))

		require.Error(t, err)
		var appErr *sdktemporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		assert.Equal(t, "NotFoundError", appErr.Type())
		assert.True(t, appErr.NonRetryable())
		require.ErrorIs(t, err, domain.ErrNotFound)
		assert.Empty(t, out.TaskID)
		assert.Equal(t, 0, w29aTaskCount(t, taskRepo, owner, inst.ID), "the owner's instance gained no task")
		assert.Equal(t, 0, w29aTaskCount(t, taskRepo, other, inst.ID))
	})
}
