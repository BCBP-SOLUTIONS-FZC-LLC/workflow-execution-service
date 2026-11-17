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

type claimedAtWorld struct {
	assignments *postgres.TaskAssignmentRepo
	tenantID    uuid.UUID
	task        *domain.Task
}

func newClaimedAtWorld(t *testing.T) *claimedAtWorld {
	t.Helper()
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	ctx := context.Background()
	w := &claimedAtWorld{assignments: postgres.NewTaskAssignmentRepo(appPool), tenantID: uuid.New()}
	inst := newInstance(w.tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	w.task = newTask(w.tenantID, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, w.task))
	return w
}

func (w *claimedAtWorld) assignee(t *testing.T, lead bool) *domain.TaskAssignment {
	t.Helper()
	a := newTaskAssignment(w.tenantID, w.task.ID, uuid.New())
	a.IsLead = lead
	require.NoError(t, w.assignments.Create(context.Background(), a))
	return a
}

func (w *claimedAtWorld) claimedAt(t *testing.T, a *domain.TaskAssignment) *time.Time {
	t.Helper()
	stored, err := w.assignments.GetByID(context.Background(), w.tenantID, a.ID)
	require.NoError(t, err)
	return stored.ClaimedAt
}

func TestTaskAssignmentRepo_MovingTheLeadDoesNotRecordAClaim(t *testing.T) {
	w := newClaimedAtWorld(t)
	first := w.assignee(t, true)
	second := w.assignee(t, false)

	moved, err := w.assignments.SetLead(context.Background(), w.tenantID, w.task.ID, second.ID, w.task.RecordVersion)

	require.NoError(t, err)
	assert.True(t, moved.IsLead)
	assert.Nil(t, w.claimedAt(t, second), "a lead handed over or moved by an admin did not claim the task")
	assert.Nil(t, w.claimedAt(t, first))
}

func TestTaskAssignmentRepo_ClaimLeadRecordsTheFirstClaimTimeOfTheClaimerOnly(t *testing.T) {
	w := newClaimedAtWorld(t)
	claimer := w.assignee(t, false)
	bystander := w.assignee(t, false)
	assert.Nil(t, w.claimedAt(t, claimer), "an assignee who has not claimed has no claim time")

	claimed, err := w.assignments.ClaimLead(context.Background(), w.tenantID, w.task.ID, claimer.ID, w.task.RecordVersion)

	require.NoError(t, err)
	assert.True(t, claimed.IsLead)
	require.NotNil(t, claimed.ClaimedAt, "a claim records when it happened")
	first := w.claimedAt(t, claimer)
	require.NotNil(t, first)
	assert.Nil(t, w.claimedAt(t, bystander), "a co-assignee who did not claim has no claim time")

	_, err = w.assignments.SetLead(context.Background(), w.tenantID, w.task.ID, bystander.ID, w.task.RecordVersion+1)
	require.NoError(t, err)
	_, err = w.assignments.ClaimLead(context.Background(), w.tenantID, w.task.ID, claimer.ID, w.task.RecordVersion+2)
	require.NoError(t, err)
	again := w.claimedAt(t, claimer)
	require.NotNil(t, again)
	assert.True(t, first.Equal(*again), "claiming again keeps the first claim time")
}
