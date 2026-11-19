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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type fx11aGuardWorld struct {
	assignments *postgres.TaskAssignmentRepo
	tasks       *postgres.TaskRepo
	tenantID    uuid.UUID
	task        *domain.Task
	otherTask   *domain.Task
}

func fx11aNewGuardWorld(t *testing.T) *fx11aGuardWorld {
	t.Helper()
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ctx := context.Background()
	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, postgres.NewInstanceRepo(appPool).Create(ctx, inst))
	tasks := postgres.NewTaskRepo(appPool)
	task, other := newTask(tenantID, inst.ID), newTask(tenantID, inst.ID)
	other.NodeKey = "other/node"
	require.NoError(t, tasks.Create(ctx, task))
	require.NoError(t, tasks.Create(ctx, other))
	return &fx11aGuardWorld{
		assignments: postgres.NewTaskAssignmentRepo(appPool), tasks: tasks,
		tenantID: tenantID, task: task, otherTask: other,
	}
}

func (w *fx11aGuardWorld) seed(t *testing.T, task *domain.Task) *domain.TaskAssignment {
	t.Helper()
	a := newTaskAssignment(w.tenantID, task.ID, uuid.New())
	require.NoError(t, w.assignments.Create(context.Background(), a))
	return a
}

func TestTaskAssignmentRepo_Vacate_AnEndedRowIsNotFoundAndStaysAsItWas(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		end  func(t *testing.T, w *fx11aGuardWorld, a *domain.TaskAssignment)
	}{
		{"completed", func(t *testing.T, w *fx11aGuardWorld, a *domain.TaskAssignment) {
			_, err := w.assignments.Complete(ctx, w.tenantID, a.ID, json.RawMessage(`{"outcome":"approved"}`), w.task.RecordVersion)
			require.NoError(t, err)
		}},
		{"vacated", func(t *testing.T, w *fx11aGuardWorld, a *domain.TaskAssignment) {
			_, err := w.assignments.Vacate(ctx, w.tenantID, a.ID)
			require.NoError(t, err)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := fx11aNewGuardWorld(t)
			a := w.seed(t, w.task)
			tc.end(t, w, a)
			before, err := w.assignments.GetByID(ctx, w.tenantID, a.ID)
			require.NoError(t, err)

			got, err := w.assignments.Vacate(ctx, w.tenantID, a.ID)

			require.ErrorIs(t, err, domain.ErrNotFound)
			assert.Nil(t, got)
			after, err := w.assignments.GetByID(ctx, w.tenantID, a.ID)
			require.NoError(t, err)
			assert.Equal(t, before, after)
		})
	}
}

func TestTaskAssignmentRepo_SetLeadAndClaimLead_AForeignAssignmentIsNotFoundAndNothingChanges(t *testing.T) {
	ctx := context.Background()
	moves := map[string]func(w *fx11aGuardWorld, id uuid.UUID) (*domain.TaskAssignment, error){
		"set lead": func(w *fx11aGuardWorld, id uuid.UUID) (*domain.TaskAssignment, error) {
			return w.assignments.SetLead(ctx, w.tenantID, w.task.ID, id, w.task.RecordVersion)
		},
		"claim lead": func(w *fx11aGuardWorld, id uuid.UUID) (*domain.TaskAssignment, error) {
			return w.assignments.ClaimLead(ctx, w.tenantID, w.task.ID, id, w.task.RecordVersion)
		},
	}
	for name, move := range moves {
		t.Run(name, func(t *testing.T) {
			w := fx11aNewGuardWorld(t)
			foreign := w.seed(t, w.otherTask)

			got, err := move(w, foreign.ID)

			require.ErrorIs(t, err, domain.ErrNotFound)
			assert.Nil(t, got)
			stored, err := w.assignments.GetByID(ctx, w.tenantID, foreign.ID)
			require.NoError(t, err)
			assert.False(t, stored.IsLead, "an assignment of another task is not made lead")
			task, err := w.tasks.GetByID(ctx, w.tenantID, w.task.ID)
			require.NoError(t, err)
			assert.Equal(t, w.task.RecordVersion, task.RecordVersion)
		})
	}
}

func TestTaskAssignmentRepo_ListActiveByUserPaginated_OnlyReadyAndInProgressTasks(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ctx := context.Background()
	tenantID, userID := uuid.New(), uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, postgres.NewInstanceRepo(appPool).Create(ctx, inst))
	tasks := postgres.NewTaskRepo(appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	statuses := []domain.TaskStatus{
		domain.TaskStatusReady, domain.TaskStatusInProgress, domain.TaskStatusDeferred,
		domain.TaskStatusCompleted, domain.TaskStatusFailed, domain.TaskStatusSuperseded,
	}
	for _, status := range statuses {
		task := newTask(tenantID, inst.ID)
		task.NodeKey = "dept/" + string(status)
		task.Status = status
		require.NoError(t, tasks.Create(ctx, task))
		require.NoError(t, assignments.Create(ctx, newTaskAssignment(tenantID, task.ID, userID)))
	}

	rows, _, err := assignments.ListActiveByUserPaginated(ctx, tenantID, userID, port.PageRequest{Limit: 50})

	require.NoError(t, err)
	var got []domain.TaskStatus
	for _, row := range rows {
		got = append(got, row.Status)
	}
	assert.ElementsMatch(t, []domain.TaskStatus{domain.TaskStatusReady, domain.TaskStatusInProgress}, got)
}
