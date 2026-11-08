package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// TestUserTaskPauser_DepartmentScope covers the optional department scope of the
// membership-revocation pause: with a department only the instances holding the
// user's task in that department are paused, without one every instance is.
func TestUserTaskPauser_DepartmentScope(t *testing.T) {
	tenantID, userID := uuid.New(), uuid.New()
	deptA, deptB := uuid.New(), uuid.New()
	other := uuid.New()

	type taskSpec struct {
		dept     uuid.UUID
		instance string
	}

	cases := []struct {
		name  string
		scope *uuid.UUID
		tasks []taskSpec
		want  []string
	}{
		{
			name:  "a department pauses only the instances with the user's task in it",
			scope: &deptA,
			tasks: []taskSpec{{deptA, "a"}, {deptB, "b"}},
			want:  []string{"a"},
		},
		{
			name:  "no department pauses the user's tasks in every department",
			scope: nil,
			tasks: []taskSpec{{deptA, "a"}, {deptB, "b"}},
			want:  []string{"a", "b"},
		},
		{
			name:  "a department with no task of the user pauses nothing",
			scope: &other,
			tasks: []taskSpec{{deptA, "a"}, {deptB, "b"}},
			want:  nil,
		},
		{
			name:  "an instance holding tasks in both departments is paused once for either scope",
			scope: &deptB,
			tasks: []taskSpec{{deptA, "shared"}, {deptB, "shared"}},
			want:  []string{"shared"},
		},
		{
			name:  "two tasks of the user in the scoped department on one instance signal once",
			scope: &deptA,
			tasks: []taskSpec{{deptA, "a"}, {deptA, "a"}},
			want:  []string{"a"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			instances := newFakeInstanceRepo()
			tasks := newFakeTaskRepo()
			assignments := newFakeAssignmentRepo()
			temporal := &fakeTemporalClient{}

			byKey := map[string]uuid.UUID{}
			for _, ts := range tc.tasks {
				instanceID, ok := byKey[ts.instance]
				if !ok {
					instanceID = uuid.New()
					byKey[ts.instance] = instanceID
					instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:" + ts.instance}
				}
				task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, DepartmentID: ts.dept}
				tasks.byID[task.ID] = task
				assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
			}

			pauser := &service.UserTaskPauser{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal}
			require.NoError(t, pauser.PauseUserTasks(context.Background(), tenantID, userID, tc.scope))

			var got []uuid.UUID
			for _, s := range temporal.signals {
				got = append(got, s.InstanceID)
			}
			var want []uuid.UUID
			for _, key := range tc.want {
				want = append(want, byKey[key])
			}
			assert.ElementsMatch(t, want, got)
		})
	}

	t.Run("a task in another department of another user does not widen the scope", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		tasks := newFakeTaskRepo()
		assignments := newFakeAssignmentRepo()
		temporal := &fakeTemporalClient{}
		otherUser := uuid.New()

		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:x"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, DepartmentID: deptA}
		tasks.byID[task.ID] = task
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: otherUser, IsActive: true}

		pauser := &service.UserTaskPauser{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal}
		require.NoError(t, pauser.PauseUserTasks(context.Background(), tenantID, userID, &deptA))
		assert.Empty(t, temporal.signals)
	})
}
