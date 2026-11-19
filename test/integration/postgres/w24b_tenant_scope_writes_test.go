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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestRepos_ByIDCallsAsAnotherTenant_ReturnNotFoundAndLeaveTheRowUntouched(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	owner, intruder := uuid.New(), uuid.New()
	inst := newInstance(owner, time.Now().UTC())
	require.NoError(t, instances.Create(ctx, inst))
	task := newTask(owner, inst.ID)
	require.NoError(t, tasks.Create(ctx, task))
	assignment := newTaskAssignment(owner, task.ID, uuid.New())
	require.NoError(t, assignments.Create(ctx, assignment))
	require.Equal(t, int64(1), inst.RecordVersion)
	require.Equal(t, int64(1), task.RecordVersion)

	cases := []struct {
		name string
		call func() error
	}{
		{"instance status update", func() error {
			_, err := instances.UpdateStatus(ctx, intruder, inst.ID, domain.InstanceStatusTerminated, inst.RecordVersion)
			return err
		}},
		{"instance current node keys update", func() error {
			_, err := instances.UpdateCurrentNodeKeys(ctx, intruder, inst.ID, []string{"hijacked"}, inst.RecordVersion)
			return err
		}},
		{"instance run id record", func() error {
			return instances.RecordRunID(ctx, intruder, inst.ID, "intruder-run")
		}},
		{"task status update", func() error {
			_, err := tasks.UpdateStatus(ctx, intruder, task.ID, domain.TaskStatusCompleted, task.RecordVersion)
			return err
		}},
		{"task record version bump", func() error {
			_, err := tasks.BumpRecordVersion(ctx, intruder, task.ID, task.RecordVersion)
			return err
		}},
		{"task read by id", func() error {
			_, err := tasks.GetByID(ctx, intruder, task.ID)
			return err
		}},
		{"assignment vacate", func() error {
			_, err := assignments.Vacate(ctx, intruder, assignment.ID)
			return err
		}},
		{"assignment vacate of an active one", func() error {
			_, err := assignments.VacateActive(ctx, intruder, assignment.ID)
			return err
		}},
		{"assignment complete", func() error {
			_, err := assignments.Complete(ctx, intruder, assignment.ID, json.RawMessage(`{"outcome":"stolen"}`), task.RecordVersion)
			return err
		}},
		{"assignment set lead", func() error {
			_, err := assignments.SetLead(ctx, intruder, task.ID, assignment.ID, task.RecordVersion)
			return err
		}},
		{"assignment claim lead", func() error {
			_, err := assignments.ClaimLead(ctx, intruder, task.ID, assignment.ID, task.RecordVersion)
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, tc.call(), domain.ErrNotFound)
		})
	}

	t.Run("the owner still sees every row exactly as seeded", func(t *testing.T) {
		gotInst, err := instances.GetByID(ctx, owner, inst.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusRunning, gotInst.Status)
		assert.Equal(t, []string{"review"}, gotInst.CurrentNodeKeys)
		assert.Equal(t, int64(1), gotInst.RecordVersion)
		assert.Empty(t, gotInst.TemporalRunID)

		gotTask, err := tasks.GetByID(ctx, owner, task.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.TaskStatusReady, gotTask.Status)
		assert.Equal(t, int64(1), gotTask.RecordVersion)

		gotAssignment, err := assignments.GetByID(ctx, owner, assignment.ID)
		require.NoError(t, err)
		assert.True(t, gotAssignment.IsActive)
		assert.False(t, gotAssignment.IsLead)
		assert.Nil(t, gotAssignment.CompletedAt)
		assert.Nil(t, gotAssignment.VacatedAt)
	})
}

func TestTaskRepo_GetByInstanceAndNode_ForAnotherTenantsInstance_IsNotFound(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	ctx := context.Background()

	owner, intruder := uuid.New(), uuid.New()
	inst := newInstance(owner, time.Now().UTC())
	require.NoError(t, instances.Create(ctx, inst))
	task := newTask(owner, inst.ID)
	require.NoError(t, tasks.Create(ctx, task))

	cases := []struct {
		name    string
		tenant  uuid.UUID
		node    string
		wantErr error
		wantID  uuid.UUID
	}{
		{name: "the owning tenant finds the task", tenant: owner, node: task.NodeKey, wantID: task.ID},
		{name: "another tenant naming the same instance and node finds nothing", tenant: intruder, node: task.NodeKey, wantErr: domain.ErrNotFound},
		{name: "the owning tenant with an unknown node finds nothing", tenant: owner, node: "no/such/node", wantErr: domain.ErrNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tasks.GetByInstanceAndNode(ctx, tc.tenant, inst.ID, tc.node)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantID, got.ID)
		})
	}
}

// w24bAssignmentRig seeds assignments for a user on separate tasks, each task
// back-dated one minute apart so a page order is deterministic.
type w24bAssignmentRig struct {
	t           *testing.T
	superPool   *pgcommon.Pool
	instances   *postgres.InstanceRepo
	tasks       *postgres.TaskRepo
	assignments *postgres.TaskAssignmentRepo
	base        time.Time
	next        int
}

func w24bNewAssignmentRig(t *testing.T) *w24bAssignmentRig {
	t.Helper()
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	return &w24bAssignmentRig{
		t:           t,
		superPool:   superPool,
		instances:   postgres.NewInstanceRepo(appPool),
		tasks:       postgres.NewTaskRepo(appPool),
		assignments: postgres.NewTaskAssignmentRepo(appPool),
		base:        time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond),
	}
}

// assign creates a task (with its own instance) for the tenant and one active
// assignment of the user on it, and returns the task and the assignment.
func (r *w24bAssignmentRig) assign(tenant, user uuid.UUID) (*domain.Task, *domain.TaskAssignment) {
	r.t.Helper()
	ctx := context.Background()
	inst := newInstance(tenant, r.base)
	require.NoError(r.t, r.instances.Create(ctx, inst))
	task := newTask(tenant, inst.ID)
	require.NoError(r.t, r.tasks.Create(ctx, task))
	seedTaskAt(r.t, r.superPool, ctx, task.ID, r.base.Add(time.Duration(r.next)*time.Minute))
	r.next++
	assignment := newTaskAssignment(tenant, task.ID, user)
	require.NoError(r.t, r.assignments.Create(ctx, assignment))
	return task, assignment
}

func w24bTaskIDs(rows []port.ActiveUserTaskRow) []uuid.UUID {
	var ids []uuid.UUID
	for _, row := range rows {
		ids = append(ids, row.TaskID)
	}
	return ids
}

func TestTaskAssignmentRepo_ListActiveByUserPaginated_AnotherTenantsAssignments_AreNotListed(t *testing.T) {
	rig := w24bNewAssignmentRig(t)
	ctx := context.Background()
	tenantA, tenantB := uuid.New(), uuid.New()
	user := uuid.New()

	taskOfA, _ := rig.assign(tenantA, user)
	taskOfB, _ := rig.assign(tenantB, user)

	cases := []struct {
		name   string
		tenant uuid.UUID
		user   uuid.UUID
		want   []uuid.UUID
	}{
		{name: "tenant A sees only its own task for the user", tenant: tenantA, user: user, want: []uuid.UUID{taskOfA.ID}},
		{name: "tenant B sees only its own task for the same user id", tenant: tenantB, user: user, want: []uuid.UUID{taskOfB.ID}},
		{name: "a tenant with no assignments for the user gets an empty page", tenant: uuid.New(), user: user, want: nil},
		{name: "the owning tenant with another user gets an empty page", tenant: tenantA, user: uuid.New(), want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, next, err := rig.assignments.ListActiveByUserPaginated(ctx, tc.tenant, tc.user, port.PageRequest{Limit: 10})
			require.NoError(t, err)
			assert.Nil(t, next)
			assert.Equal(t, tc.want, w24bTaskIDs(rows))
		})
	}
}

func TestTaskAssignmentRepo_ListActiveByUserPaginated_ExactlyLimitAssignments_GiveNoCursor(t *testing.T) {
	rig := w24bNewAssignmentRig(t)
	ctx := context.Background()
	tenant, user := uuid.New(), uuid.New()

	var seeded []uuid.UUID
	for i := 0; i < 3; i++ {
		task, _ := rig.assign(tenant, user)
		seeded = append(seeded, task.ID)
	}
	newestFirst := []uuid.UUID{seeded[2], seeded[1], seeded[0]}

	cases := []struct {
		name       string
		limit      int
		want       []uuid.UUID
		wantCursor bool
	}{
		{name: "limit equal to the rows gives all rows and no cursor", limit: 3, want: newestFirst},
		{name: "limit above the rows gives all rows and no cursor", limit: 4, want: newestFirst},
		{name: "limit one below the rows gives a cursor on the last returned task", limit: 2, want: newestFirst[:2], wantCursor: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, next, err := rig.assignments.ListActiveByUserPaginated(ctx, tenant, user, port.PageRequest{Limit: tc.limit})
			require.NoError(t, err)
			assert.Equal(t, tc.want, w24bTaskIDs(rows))
			if !tc.wantCursor {
				assert.Nil(t, next)
				return
			}
			require.NotNil(t, next)
			assert.Equal(t, tc.want[len(tc.want)-1], next.ID)
		})
	}
}

func TestTaskAssignmentRepo_VacateAllActiveByUser_VacatesOnlyTheUsersActiveRowsInTheTenant(t *testing.T) {
	rig := w24bNewAssignmentRig(t)
	ctx := context.Background()
	tenantA, tenantB := uuid.New(), uuid.New()
	user, otherUser := uuid.New(), uuid.New()

	_, activeOne := rig.assign(tenantA, user)
	_, activeTwo := rig.assign(tenantA, user)
	completedTask, completed := rig.assign(tenantA, user)
	_, vacatedEarlier := rig.assign(tenantA, user)
	_, otherUsers := rig.assign(tenantA, otherUser)
	_, otherTenants := rig.assign(tenantB, user)

	_, err := rig.assignments.Complete(ctx, tenantA, completed.ID, json.RawMessage(`{"outcome":"done"}`), completedTask.RecordVersion)
	require.NoError(t, err)
	_, err = rig.assignments.Vacate(ctx, tenantA, vacatedEarlier.ID)
	require.NoError(t, err)

	taskVersions := map[uuid.UUID]int64{}
	for _, id := range []uuid.UUID{activeOne.TaskID, activeTwo.TaskID, completedTask.ID, otherUsers.TaskID} {
		got, getErr := rig.tasks.GetByID(ctx, tenantA, id)
		require.NoError(t, getErr)
		taskVersions[id] = got.RecordVersion
	}

	t.Run("the first call returns exactly the two active rows, now inactive", func(t *testing.T) {
		vacated, err := rig.assignments.VacateAllActiveByUser(ctx, tenantA, user)
		require.NoError(t, err)
		var ids []uuid.UUID
		for _, a := range vacated {
			ids = append(ids, a.ID)
			assert.False(t, a.IsActive)
			assert.NotNil(t, a.VacatedAt)
			assert.Equal(t, tenantA, a.TenantID)
			assert.Equal(t, user, a.UserID)
		}
		assert.ElementsMatch(t, []uuid.UUID{activeOne.ID, activeTwo.ID}, ids)
	})

	t.Run("rows that were not active or not the user's are left as they were", func(t *testing.T) {
		got, err := rig.assignments.GetByID(ctx, tenantA, completed.ID)
		require.NoError(t, err)
		assert.NotNil(t, got.CompletedAt)
		assert.Nil(t, got.VacatedAt, "a completed assignment is not vacated afterwards")

		got, err = rig.assignments.GetByID(ctx, tenantA, otherUsers.ID)
		require.NoError(t, err)
		assert.True(t, got.IsActive, "another user's assignment stays active")

		got, err = rig.assignments.GetByID(ctx, tenantB, otherTenants.ID)
		require.NoError(t, err)
		assert.True(t, got.IsActive, "the same user's assignment in another tenant stays active")
	})

	t.Run("no task record version moves", func(t *testing.T) {
		for id, want := range taskVersions {
			got, err := rig.tasks.GetByID(ctx, tenantA, id)
			require.NoError(t, err)
			assert.Equal(t, want, got.RecordVersion)
		}
	})

	t.Run("a second call finds nothing and returns an empty slice", func(t *testing.T) {
		vacated, err := rig.assignments.VacateAllActiveByUser(ctx, tenantA, user)
		require.NoError(t, err)
		assert.NotNil(t, vacated)
		assert.Empty(t, vacated)
	})

	t.Run("a tenant that holds nothing for the user gets an empty slice", func(t *testing.T) {
		vacated, err := rig.assignments.VacateAllActiveByUser(ctx, uuid.New(), user)
		require.NoError(t, err)
		assert.Empty(t, vacated)
	})
}
