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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// A lead move is stamped with the task version the caller read: a stale version, or a
// task that is missing, fails the move and leaves the lead flags and the version as they were.
func TestTaskAssignmentRepo_SetLead_ChecksAndBumpsTheTaskVersion(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	type world struct {
		tenantID uuid.UUID
		task     *domain.Task
		lead     *domain.TaskAssignment
		holder   *domain.TaskAssignment
	}
	// seed gives a task a lead and a co-assignee, in a tenant of its own.
	seed := func(t *testing.T) world {
		t.Helper()
		tenantID := uuid.New()
		inst := newInstance(tenantID, time.Now().UTC())
		require.NoError(t, instanceRepo.Create(ctx, inst))
		task := newTask(tenantID, inst.ID)
		require.NoError(t, taskRepo.Create(ctx, task))
		lead := newTaskAssignment(tenantID, task.ID, uuid.New())
		lead.IsLead = true
		require.NoError(t, assignmentRepo.Create(ctx, lead))
		holder := newTaskAssignment(tenantID, task.ID, uuid.New())
		require.NoError(t, assignmentRepo.Create(ctx, holder))
		return world{tenantID: tenantID, task: task, lead: lead, holder: holder}
	}
	versionOf := func(t *testing.T, w world) int64 {
		t.Helper()
		stored, err := taskRepo.GetByID(ctx, w.tenantID, w.task.ID)
		require.NoError(t, err)
		return stored.RecordVersion
	}
	leadOf := func(t *testing.T, w world, a *domain.TaskAssignment) bool {
		t.Helper()
		stored, err := assignmentRepo.GetByID(ctx, w.tenantID, a.ID)
		require.NoError(t, err)
		return stored.IsLead
	}

	t.Run("the version read is current: the lead moves to the delegate's own row and the version is bumped once", func(t *testing.T) {
		w := seed(t)
		before := versionOf(t, w)

		moved, err := assignmentRepo.SetLead(ctx, w.tenantID, w.task.ID, w.holder.ID, before)

		require.NoError(t, err)
		assert.Equal(t, w.holder.ID, moved.ID)
		assert.True(t, leadOf(t, w, w.holder))
		assert.False(t, leadOf(t, w, w.lead), "the previous lead is demoted")
		assert.Equal(t, before+1, versionOf(t, w))
	})

	t.Run("a stale version fails the move and changes nothing", func(t *testing.T) {
		w := seed(t)
		before := versionOf(t, w)

		_, err := assignmentRepo.SetLead(ctx, w.tenantID, w.task.ID, w.holder.ID, before-1)

		require.ErrorIs(t, err, domain.ErrRecordVersionConflict)
		assert.False(t, leadOf(t, w, w.holder), "the lead flag did not move")
		assert.True(t, leadOf(t, w, w.lead), "the lead keeps it")
		assert.Equal(t, before, versionOf(t, w), "the version was not bumped")
	})

	t.Run("a version that moved after it was read fails the move once and the retry with a fresh read succeeds", func(t *testing.T) {
		w := seed(t)
		read := versionOf(t, w)
		_, err := taskRepo.BumpRecordVersion(ctx, w.tenantID, w.task.ID, read)
		require.NoError(t, err, "another writer touches the task after the read")

		_, err = assignmentRepo.SetLead(ctx, w.tenantID, w.task.ID, w.holder.ID, read)
		require.ErrorIs(t, err, domain.ErrRecordVersionConflict)
		assert.False(t, leadOf(t, w, w.holder))

		_, err = assignmentRepo.SetLead(ctx, w.tenantID, w.task.ID, w.holder.ID, versionOf(t, w))
		require.NoError(t, err)
		assert.True(t, leadOf(t, w, w.holder))
	})

	t.Run("a task that does not exist fails the move as not found", func(t *testing.T) {
		w := seed(t)
		before := versionOf(t, w)

		_, err := assignmentRepo.SetLead(ctx, w.tenantID, uuid.New(), w.holder.ID, before)

		require.ErrorIs(t, err, domain.ErrNotFound)
		assert.False(t, leadOf(t, w, w.holder))
		assert.True(t, leadOf(t, w, w.lead))
		assert.Equal(t, before, versionOf(t, w))
	})

	t.Run("another tenant cannot see the task, so its move is not found and changes nothing", func(t *testing.T) {
		w := seed(t)
		before := versionOf(t, w)

		_, err := assignmentRepo.SetLead(ctx, uuid.New(), w.task.ID, w.holder.ID, before)

		require.ErrorIs(t, err, domain.ErrNotFound)
		assert.False(t, leadOf(t, w, w.holder))
		assert.True(t, leadOf(t, w, w.lead))
		assert.Equal(t, before, versionOf(t, w))
	})
}
