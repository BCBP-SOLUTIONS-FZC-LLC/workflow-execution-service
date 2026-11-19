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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestTaskRepo_ListByTenant_FiltersCombineWithAndAndWithScope(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	tenant, otherTenant := uuid.New(), uuid.New()
	deptOne, deptTwo := uuid.New(), uuid.New()
	cutoff := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Microsecond)
	at := func(d time.Duration) *time.Time { v := cutoff.Add(d); return &v }

	inst := newInstance(tenant, time.Now().UTC())
	require.NoError(t, instances.Create(ctx, inst))

	seed := func(dept uuid.UUID, due *time.Time, inProgress bool) *domain.Task {
		task := newTask(tenant, inst.ID)
		task.DepartmentID = dept
		task.DueAt = due
		require.NoError(t, tasks.Create(ctx, task))
		if inProgress {
			updated, err := tasks.UpdateStatus(ctx, tenant, task.ID, domain.TaskStatusInProgress, task.RecordVersion)
			require.NoError(t, err)
			return updated
		}
		return task
	}
	dueEarlier := seed(deptOne, at(-time.Hour), false)
	dueExactly := seed(deptOne, at(0), true)
	dueLater := seed(deptTwo, at(time.Hour), false)
	noDue := seed(deptOne, nil, false)
	assigned := seed(deptTwo, at(-2*time.Hour), true)

	assignee, formerAssignee := uuid.New(), uuid.New()
	require.NoError(t, assignments.Create(ctx, newTaskAssignment(tenant, assigned.ID, assignee)))
	former := newTaskAssignment(tenant, dueEarlier.ID, formerAssignee)
	require.NoError(t, assignments.Create(ctx, former))
	_, err := assignments.Vacate(ctx, tenant, former.ID)
	require.NoError(t, err)

	// Another tenant's task matches every filter value below and must never show.
	otherInst := newInstance(otherTenant, time.Now().UTC())
	require.NoError(t, instances.Create(ctx, otherInst))
	foreign := newTask(otherTenant, otherInst.ID)
	foreign.DepartmentID = deptOne
	foreign.DueAt = at(-time.Hour)
	require.NoError(t, tasks.Create(ctx, foreign))

	ready, inProgress := domain.TaskStatusReady, domain.TaskStatusInProgress
	ids := func(ts ...*domain.Task) []uuid.UUID {
		var out []uuid.UUID
		for _, task := range ts {
			out = append(out, task.ID)
		}
		return out
	}

	cases := []struct {
		name   string
		filter port.TaskListFilter
		want   []uuid.UUID
	}{
		{name: "department narrows to that department's tasks", filter: port.TaskListFilter{DepartmentID: &deptOne}, want: ids(dueEarlier, dueExactly, noDue)},
		{name: "the other department gets the rest", filter: port.TaskListFilter{DepartmentID: &deptTwo}, want: ids(dueLater, assigned)},
		{name: "due_before is strict and leaves out tasks with no due time", filter: port.TaskListFilter{DueBefore: &cutoff}, want: ids(dueEarlier, assigned)},
		{name: "department and due_before both apply", filter: port.TaskListFilter{DepartmentID: &deptOne, DueBefore: &cutoff}, want: ids(dueEarlier)},
		{name: "status, department and due_before all apply", filter: port.TaskListFilter{Status: &ready, DepartmentID: &deptTwo, DueBefore: at(2 * time.Hour)}, want: ids(dueLater)},
		{name: "status and department apply together", filter: port.TaskListFilter{Status: &inProgress, DepartmentID: &deptTwo}, want: ids(assigned)},
		{name: "assignee matches an active assignment", filter: port.TaskListFilter{AssigneeUserID: &assignee}, want: ids(assigned)},
		{name: "assignee does not match a vacated assignment", filter: port.TaskListFilter{AssigneeUserID: &formerAssignee}, want: nil},
		{name: "assignee and department apply together", filter: port.TaskListFilter{AssigneeUserID: &assignee, DepartmentID: &deptOne}, want: nil},
		{
			name: "scope outside the filtered department leaves nothing",
			filter: port.TaskListFilter{
				DepartmentID: &deptOne,
				Scope:        &port.ScopeFilter{DepartmentIDs: []uuid.UUID{deptTwo}, CallerUserID: uuid.New()},
			},
			want: nil,
		},
		{
			name: "scope covering the filtered department keeps its tasks",
			filter: port.TaskListFilter{
				DepartmentID: &deptOne,
				Scope:        &port.ScopeFilter{DepartmentIDs: []uuid.UUID{deptOne}, CallerUserID: uuid.New()},
			},
			want: ids(dueEarlier, dueExactly, noDue),
		},
		{
			name: "an assignment widens scope only inside the filtered department",
			filter: port.TaskListFilter{
				DepartmentID: &deptTwo,
				Scope:        &port.ScopeFilter{DepartmentIDs: []uuid.UUID{deptOne}, CallerUserID: assignee},
			},
			want: ids(assigned),
		},
		{
			name: "a vacated assignment does not widen scope",
			filter: port.TaskListFilter{
				Scope: &port.ScopeFilter{DepartmentIDs: []uuid.UUID{uuid.New()}, CallerUserID: formerAssignee},
			},
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, next, err := tasks.ListByTenant(ctx, tenant, tc.filter, port.PageRequest{Limit: 10})
			require.NoError(t, err)
			assert.Nil(t, next)
			var gotIDs []uuid.UUID
			for _, task := range got {
				gotIDs = append(gotIDs, task.ID)
			}
			assert.ElementsMatch(t, tc.want, gotIDs)
			assert.NotContains(t, gotIDs, foreign.ID)
		})
	}

	t.Run("a status the database does not know is refused with no rows", func(t *testing.T) {
		unknown := domain.TaskStatus("NOT_A_STATUS")
		got, next, err := tasks.ListByTenant(ctx, tenant, port.TaskListFilter{Status: &unknown}, port.PageRequest{Limit: 10})
		require.Error(t, err)
		assert.True(t, pgcommon.IsPgError(err), "the database's own error reaches the caller: %v", err)
		assert.NotErrorIs(t, err, domain.ErrNotFound)
		assert.Empty(t, got)
		assert.Nil(t, next)

		known, _, err := tasks.ListByTenant(ctx, tenant, port.TaskListFilter{Status: &ready}, port.PageRequest{Limit: 10})
		require.NoError(t, err, "the same repository still answers a valid status afterwards")
		assert.Len(t, known, 3)
	})
}
