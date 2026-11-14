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

type dlgfixSignals struct {
	port.TemporalClient
	signals []string
}

func (c *dlgfixSignals) SignalWorkflow(_ context.Context, _ string, _ uuid.UUID, name string, _ any) error {
	c.signals = append(c.signals, name)
	return nil
}

// A delegate who already holds the task must not make the move fail: a second
// active row is a unique violation, which aborts a Postgres transaction.
func TestReassignDelegate_ADelegateWhoAlreadyHoldsTheTaskKeepsOneActiveRow(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	temporal := &dlgfixSignals{}
	client := &service.WorkflowClient{
		Instances: instanceRepo, Tasks: taskRepo, Assignments: assignmentRepo,
		Transactor: postgres.NewTransactor(appPool), Temporal: temporal,
	}
	ctx := context.Background()

	tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	task := newTask(tenantID, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, task))
	old := newTaskAssignment(tenantID, task.ID, oldDelegate)
	require.NoError(t, assignmentRepo.Create(ctx, old))
	require.NoError(t, assignmentRepo.Create(ctx, newTaskAssignment(tenantID, task.ID, newDelegate)))

	count, err := client.ReassignDelegate(ctx, port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})

	require.NoError(t, err)
	assert.Equal(t, 1, count)
	vacated, err := assignmentRepo.GetByID(ctx, tenantID, old.ID)
	require.NoError(t, err)
	assert.False(t, vacated.IsActive)
	active, err := assignmentRepo.ListActiveByTask(ctx, tenantID, task.ID)
	require.NoError(t, err)
	require.Len(t, active, 1)
	assert.Equal(t, newDelegate, active[0].UserID)
	assert.Equal(t, []string{port.SignalInstanceReassign}, temporal.signals)
}
