package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func fxhRowsFor(world *wkactWorld, tenantID, taskID, userID uuid.UUID) int {
	n := 0
	for _, a := range world.assignments {
		if a.TenantID == tenantID && a.TaskID == taskID && a.UserID == userID {
			n++
		}
	}
	return n
}

func TestReassignAssignment_ARetryAfterACommittedAttemptChangesNothingInEveryShape(t *testing.T) {
	t.Parallel()
	shapes := []struct {
		name     string
		leadHeld bool
		oldLead  bool
	}{
		{name: "the previous lead is named", leadHeld: true, oldLead: true},
		{name: "nobody is named and the task has no lead"},
		{name: "nobody is named and the task has a lead", leadHeld: true},
	}
	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			world := wkactNewWorld()
			tenant := uuid.New()
			inst := world.addInstance(tenant, domain.InstanceStatusRunning)
			task := world.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
			task.RecordVersion = 6
			lead := world.addAssignment(tenant, task, true)
			lead.IsLead = s.leadHeld
			world.addAssignment(tenant, task, true)
			newUser := uuid.New()
			in := port.ReassignAssignmentInput{
				TenantID: tenant.String(), TaskID: task.ID.String(),
				NewUserID: newUser.String(), AdminUserID: uuid.NewString(), RecordVersion: 6,
			}
			if s.oldLead {
				in.OldUserID = lead.UserID.String()
			}
			deps, _ := wkactNewDeps(t, world)

			require.NoError(t, deps.ReassignAssignment(context.Background(), in))
			require.NoError(t, deps.ReassignAssignment(context.Background(), in), "a retry after the attempt committed is done, not a version conflict")

			assert.Equal(t, 1, fxhRowsFor(world, tenant, task.ID, newUser), "one assignment row for the new user")
			assert.Len(t, world.callsTo("Assignments.Create"), 1)
			assert.Len(t, world.eventsOfType(domain.EventWorkflowTaskReassigned), 1)
			assert.Equal(t, int64(7), task.RecordVersion, "the task version moved once")
		})
	}
}

func TestReassignAssignment_ReassigningBackToAUserWhoHeldTheTaskBeforeCreatesAFreshAssignment(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	task := world.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
	task.RecordVersion = 6
	first := world.addAssignment(tenant, task, true)
	second := uuid.New()
	deps, _ := wkactNewDeps(t, world)
	admin := uuid.NewString()

	require.NoError(t, deps.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TenantID: tenant.String(), TaskID: task.ID.String(), OldUserID: first.UserID.String(), NewUserID: second.String(), AdminUserID: admin, RecordVersion: 6,
	}))
	require.NoError(t, deps.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TenantID: tenant.String(), TaskID: task.ID.String(), OldUserID: second.String(), NewUserID: first.UserID.String(), AdminUserID: admin, RecordVersion: 7,
	}))
	require.NoError(t, deps.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TenantID: tenant.String(), TaskID: task.ID.String(), OldUserID: first.UserID.String(), NewUserID: second.String(), AdminUserID: admin, RecordVersion: 8,
	}))

	assert.Equal(t, 2, fxhRowsFor(world, tenant, task.ID, second), "the third move is a new move, not a replay of the first")
	assert.Len(t, world.eventsOfType(domain.EventWorkflowTaskReassigned), 3)
}
