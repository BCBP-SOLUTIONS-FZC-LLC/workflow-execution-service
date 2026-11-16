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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type ivfixVacateWorld struct {
	assignments *postgres.TaskAssignmentRepo
	tenantID    uuid.UUID
	taskID      uuid.UUID
	taskVersion int64
}

func ivfixNewVacateWorld(t *testing.T) *ivfixVacateWorld {
	t.Helper()
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ctx := context.Background()
	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, postgres.NewInstanceRepo(appPool).Create(ctx, inst))
	task := newTask(tenantID, inst.ID)
	require.NoError(t, postgres.NewTaskRepo(appPool).Create(ctx, task))
	return &ivfixVacateWorld{assignments: postgres.NewTaskAssignmentRepo(appPool), tenantID: tenantID, taskID: task.ID, taskVersion: task.RecordVersion}
}

func (w *ivfixVacateWorld) seed(t *testing.T) *domain.TaskAssignment {
	t.Helper()
	a := newTaskAssignment(w.tenantID, w.taskID, uuid.New())
	require.NoError(t, w.assignments.Create(context.Background(), a))
	return a
}

func TestTaskAssignmentRepo_VacateActive_VacatesAnActiveRowAndReturnsIt(t *testing.T) {
	w := ivfixNewVacateWorld(t)
	ctx := context.Background()
	a := w.seed(t)

	got, err := w.assignments.VacateActive(ctx, w.tenantID, a.ID)

	require.NoError(t, err)
	assert.Equal(t, a.ID, got.ID)
	assert.False(t, got.IsActive)
	assert.NotNil(t, got.VacatedAt)
	stored, err := w.assignments.GetByID(ctx, w.tenantID, a.ID)
	require.NoError(t, err)
	assert.False(t, stored.IsActive)
	active, err := w.assignments.ListActiveByTask(ctx, w.tenantID, w.taskID)
	require.NoError(t, err)
	assert.Empty(t, active)
}

func TestTaskAssignmentRepo_VacateActive_AnEndedRowIsNotFoundAndStaysAsItWas(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		end  func(t *testing.T, w *ivfixVacateWorld, id uuid.UUID)
	}{
		{"completed", func(t *testing.T, w *ivfixVacateWorld, id uuid.UUID) {
			_, err := w.assignments.Complete(ctx, w.tenantID, id, json.RawMessage(`{"outcome":"approved"}`), w.taskVersion)
			require.NoError(t, err)
		}},
		{"vacated", func(t *testing.T, w *ivfixVacateWorld, id uuid.UUID) {
			_, err := w.assignments.Vacate(ctx, w.tenantID, id)
			require.NoError(t, err)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := ivfixNewVacateWorld(t)
			a := w.seed(t)
			tc.end(t, w, a.ID)
			before, err := w.assignments.GetByID(ctx, w.tenantID, a.ID)
			require.NoError(t, err)

			got, err := w.assignments.VacateActive(ctx, w.tenantID, a.ID)

			require.ErrorIs(t, err, domain.ErrNotFound)
			assert.Nil(t, got)
			after, err := w.assignments.GetByID(ctx, w.tenantID, a.ID)
			require.NoError(t, err)
			assert.Equal(t, before, after, "the ended row keeps its vacated_at, completed_at, result and updated_at")
		})
	}
}

func TestTaskAssignmentRepo_VacateActive_AnotherTenantsRowIsNotTouched(t *testing.T) {
	w := ivfixNewVacateWorld(t)
	ctx := context.Background()
	a := w.seed(t)

	got, err := w.assignments.VacateActive(ctx, uuid.New(), a.ID)

	require.ErrorIs(t, err, domain.ErrNotFound)
	assert.Nil(t, got)
	stored, err := w.assignments.GetByID(ctx, w.tenantID, a.ID)
	require.NoError(t, err)
	assert.True(t, stored.IsActive)
	assert.Nil(t, stored.VacatedAt)
}
