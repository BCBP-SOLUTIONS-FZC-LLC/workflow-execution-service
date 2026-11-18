//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// recordedSignalNames lists the names of the signals waiting in the outbox for
// the relay, oldest first.
func recordedSignalNames(t *testing.T, superPool *pgcommon.Pool) []string {
	t.Helper()
	var names []string
	require.NoError(t, superPool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		rows, err := conn.Query(ctx, "SELECT payload -> 'data' ->> 'signal_name' FROM outbox_events WHERE event_type = $1 ORDER BY created_at, id", port.WorkflowSignalEventType)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			names = append(names, name)
		}
		return rows.Err()
	}))
	return names
}

// A delegate who already holds the task must not make the move fail: a second
// active row is a unique violation, which aborts a Postgres transaction.
func TestReassignDelegate_ADelegateWhoAlreadyHoldsTheTaskKeepsOneActiveRow(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	client := &service.WorkflowClient{
		Instances: instanceRepo, Tasks: taskRepo, Assignments: assignmentRepo,
		Transactor: postgres.NewTransactor(appPool), Outbox: postgres.NewOutboxRepo(appPool),
		Signals: postgres.NewOutboxRepo(appPool), Validator: validator, Locks: postgres.NewRecencyGuardRepo(appPool, appPool),
	}
	ctx := context.Background()

	tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	task := newTask(tenantID, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, task))
	old := newTaskAssignment(tenantID, task.ID, oldDelegate)
	old.Reason = "delegation:" + uuid.NewString()
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
	assert.Equal(t, []string{port.SignalInstanceReassign}, recordedSignalNames(t, superPool), "the signal is recorded with the move, for the relay to deliver")
}
