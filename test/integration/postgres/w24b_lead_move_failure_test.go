//go:build integration

package postgres_test

import (
	"context"
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

func TestTaskAssignmentRepo_LeadMove_InsideATransactionWhenALaterStatementFails_ReturnsTheDatabaseErrorAndRollsBackTheVersionBump(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	transactor := postgres.NewTransactor(appPool)
	ctx := context.Background()

	type world struct {
		tenant   uuid.UUID
		task     *domain.Task
		lead     *domain.TaskAssignment
		claimant *domain.TaskAssignment
	}
	seed := func(t *testing.T) world {
		t.Helper()
		tenant := uuid.New()
		inst := newInstance(tenant, time.Now().UTC())
		require.NoError(t, instances.Create(ctx, inst))
		task := newTask(tenant, inst.ID)
		require.NoError(t, tasks.Create(ctx, task))
		lead := newTaskAssignment(tenant, task.ID, uuid.New())
		lead.IsLead = true
		require.NoError(t, assignments.Create(ctx, lead))
		claimant := newTaskAssignment(tenant, task.ID, uuid.New())
		require.NoError(t, assignments.Create(ctx, claimant))
		return world{tenant: tenant, task: task, lead: lead, claimant: claimant}
	}
	unchanged := func(t *testing.T, w world) {
		t.Helper()
		stored, err := tasks.GetByID(ctx, w.tenant, w.task.ID)
		require.NoError(t, err)
		assert.Equal(t, w.task.RecordVersion, stored.RecordVersion, "the version bump was rolled back")
		lead, err := assignments.GetByID(ctx, w.tenant, w.lead.ID)
		require.NoError(t, err)
		assert.True(t, lead.IsLead, "the lead keeps the lead")
		claimant, err := assignments.GetByID(ctx, w.tenant, w.claimant.ID)
		require.NoError(t, err)
		assert.False(t, claimant.IsLead)
	}

	moves := []struct {
		name string
		move func(ctx context.Context, w world) (*domain.TaskAssignment, error)
	}{
		{"claim", func(ctx context.Context, w world) (*domain.TaskAssignment, error) {
			return assignments.ClaimLead(ctx, w.tenant, w.task.ID, w.claimant.ID, w.task.RecordVersion)
		}},
		{"set lead", func(ctx context.Context, w world) (*domain.TaskAssignment, error) {
			return assignments.SetLead(ctx, w.tenant, w.task.ID, w.claimant.ID, w.task.RecordVersion)
		}},
	}
	failures := []struct {
		name  string
		setup []string
	}{
		{
			name: "the statement that clears the other leads fails",
			setup: []string{
				`CREATE FUNCTION w24b_refuse_demotion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'w24b: demotion refused'; END $$`,
				`CREATE TRIGGER w24b_refuse_demotion BEFORE UPDATE ON workflow_task_assignment FOR EACH ROW WHEN (OLD.is_lead AND NOT NEW.is_lead) EXECUTE FUNCTION w24b_refuse_demotion()`,
			},
		},
		{
			name: "the statement that sets the new lead fails",
			setup: []string{
				`CREATE FUNCTION w24b_refuse_promotion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'w24b: promotion refused'; END $$`,
				`CREATE TRIGGER w24b_refuse_promotion BEFORE UPDATE ON workflow_task_assignment FOR EACH ROW WHEN (NOT OLD.is_lead AND NEW.is_lead) EXECUTE FUNCTION w24b_refuse_promotion()`,
			},
		},
	}
	for _, failure := range failures {
		for _, mv := range moves {
			t.Run(failure.name+" during a "+mv.name+" inside a transaction", func(t *testing.T) {
				w := seed(t)
				// Each case gets its own trigger on a fresh function name, removed afterwards.
				for _, stmt := range failure.setup {
					w8bSuperExec(t, superPool, stmt)
				}
				t.Cleanup(func() {
					w8bSuperExec(t, superPool, `DROP TRIGGER IF EXISTS w24b_refuse_demotion ON workflow_task_assignment`)
					w8bSuperExec(t, superPool, `DROP TRIGGER IF EXISTS w24b_refuse_promotion ON workflow_task_assignment`)
					w8bSuperExec(t, superPool, `DROP FUNCTION IF EXISTS w24b_refuse_demotion()`)
					w8bSuperExec(t, superPool, `DROP FUNCTION IF EXISTS w24b_refuse_promotion()`)
				})

				var moved *domain.TaskAssignment
				err := transactor.RunInTx(withGUC(ctx, w.tenant), func(txCtx context.Context) error {
					var moveErr error
					moved, moveErr = mv.move(txCtx, w)
					return moveErr
				})

				require.Error(t, err)
				assert.True(t, pgcommon.IsPgError(err), "the database's own error reaches the caller: %v", err)
				w8bRequireNoClientAnswer(t, err)
				assert.Nil(t, moved)
				unchanged(t, w)
			})
		}
	}
}
