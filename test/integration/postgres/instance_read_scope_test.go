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

func TestInstanceRead_ScopeAndTasksCoverEveryTaskOfALargeInstance(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	svc := &service.InstanceService{
		Instances:   instanceRepo,
		Tasks:       taskRepo,
		Assignments: assignmentRepo,
		Outbox:      postgres.NewOutboxRepo(appPool),
	}
	ctx := context.Background()

	const taskCount = 150
	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))

	oldestDept := uuid.New()
	oldestAssignee := uuid.New()
	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < taskCount; i++ {
		task := newTask(tenantID, inst.ID)
		if i == 0 {
			task.DepartmentID = oldestDept
		}
		require.NoError(t, taskRepo.Create(ctx, task))
		seedTaskAt(t, superPool, ctx, task.ID, base.Add(time.Duration(i)*time.Second))
		if i == 1 {
			require.NoError(t, assignmentRepo.Create(ctx, newTaskAssignment(tenantID, task.ID, oldestAssignee)))
		}
	}

	byDepartment := port.ReadScope{CallerUserID: uuid.New(), Departments: []port.DepartmentRole{{DepartmentID: oldestDept, Role: "member"}}}
	byAssignment := port.ReadScope{CallerUserID: oldestAssignee}
	outsider := port.ReadScope{CallerUserID: uuid.New(), Departments: []port.DepartmentRole{{DepartmentID: uuid.New(), Role: "member"}}}

	t.Run("a department match on the oldest task past the first page", func(t *testing.T) {
		_, tasks, err := svc.Get(ctx, tenantID, inst.ID, byDepartment)
		require.NoError(t, err)
		assert.Len(t, tasks, taskCount, "the detail carries every task of the instance")
		_, err = svc.ListEvents(ctx, tenantID, inst.ID, byDepartment, port.Page{})
		assert.NoError(t, err)
	})

	t.Run("an assignment on a task past the first page", func(t *testing.T) {
		_, _, err := svc.Get(ctx, tenantID, inst.ID, byAssignment)
		assert.NoError(t, err)
	})

	t.Run("a caller with no task in scope is refused", func(t *testing.T) {
		_, _, err := svc.Get(ctx, tenantID, inst.ID, outsider)
		assert.ErrorIs(t, err, port.ErrNotAuthorizedForRead)
		_, err = svc.ListEvents(ctx, tenantID, inst.ID, outsider, port.Page{})
		assert.ErrorIs(t, err, port.ErrNotAuthorizedForRead)
	})
}
