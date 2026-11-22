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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type fx7bAssignmentWorld struct {
	tenant   uuid.UUID
	task     *domain.Task
	lead     *domain.TaskAssignment
	claimant *domain.TaskAssignment
}

func fx7bSeedAssignments(t *testing.T, appPool *pgcommon.Pool) fx7bAssignmentWorld {
	t.Helper()
	ctx := context.Background()
	tenant := uuid.New()
	inst := newInstance(tenant, time.Now().UTC())
	require.NoError(t, postgres.NewInstanceRepo(appPool).Create(ctx, inst))
	task := newTask(tenant, inst.ID)
	require.NoError(t, postgres.NewTaskRepo(appPool).Create(ctx, task))
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	lead := newTaskAssignment(tenant, task.ID, uuid.New())
	lead.IsLead = true
	require.NoError(t, assignments.Create(ctx, lead))
	claimant := newTaskAssignment(tenant, task.ID, uuid.New())
	require.NoError(t, assignments.Create(ctx, claimant))
	return fx7bAssignmentWorld{tenant: tenant, task: task, lead: lead, claimant: claimant}
}

func TestTaskAssignmentRepo_LeadMove_WithoutACallerTransactionWhenALaterStatementFails_LeavesTheVersionAndTheLeadUnchanged(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	tasks := postgres.NewTaskRepo(appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	w8bSuperExec(t, superPool, `CREATE FUNCTION fx7b_refuse_promotion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'refused'; END $$`)
	w8bSuperExec(t, superPool, `CREATE TRIGGER fx7b_refuse_promotion BEFORE UPDATE ON workflow_task_assignment FOR EACH ROW WHEN (NOT OLD.is_lead AND NEW.is_lead) EXECUTE FUNCTION fx7b_refuse_promotion()`)

	moves := map[string]func(w fx7bAssignmentWorld) (*domain.TaskAssignment, error){
		"claim": func(w fx7bAssignmentWorld) (*domain.TaskAssignment, error) {
			return assignments.ClaimLead(ctx, w.tenant, w.task.ID, w.claimant.ID, w.task.RecordVersion)
		},
		"set lead": func(w fx7bAssignmentWorld) (*domain.TaskAssignment, error) {
			return assignments.SetLead(ctx, w.tenant, w.task.ID, w.claimant.ID, w.task.RecordVersion)
		},
	}
	for name, move := range moves {
		t.Run(name, func(t *testing.T) {
			w := fx7bSeedAssignments(t, appPool)

			moved, err := move(w)

			w8bRequireNoClientAnswer(t, err)
			assert.Nil(t, moved)
			stored, err := tasks.GetByID(ctx, w.tenant, w.task.ID)
			require.NoError(t, err)
			assert.Equal(t, w.task.RecordVersion, stored.RecordVersion, "the version bump is rolled back")
			lead, err := assignments.GetByID(ctx, w.tenant, w.lead.ID)
			require.NoError(t, err)
			assert.True(t, lead.IsLead, "the lead keeps the lead")
		})
	}
}

func TestTaskAssignmentRepo_Complete_WithoutACallerTransactionWhenTheVersionIsStale_LeavesTheAssignmentOpen(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()
	w := fx7bSeedAssignments(t, appPool)

	done, err := assignments.Complete(ctx, w.tenant, w.claimant.ID, json.RawMessage(`{}`), w.task.RecordVersion+7)

	require.ErrorIs(t, err, domain.ErrRecordVersionConflict)
	assert.Nil(t, done)
	stored, err := assignments.GetByID(ctx, w.tenant, w.claimant.ID)
	require.NoError(t, err)
	assert.True(t, stored.IsActive, "the stale completion is rolled back, so the assignment is still open")
	assert.Nil(t, stored.CompletedAt)
}
