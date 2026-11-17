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

// wkruSetup is an all-assignee task nobody has claimed: two holders, no lead.
func wkruSetup(t *testing.T) (world *wkactWorld, tenant uuid.UUID, task *domain.Task, holder, other *domain.TaskAssignment, in port.ReassignAssignmentInput) {
	t.Helper()
	world = wkactNewWorld()
	tenant = uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	task = world.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
	task.AssigneeMode = "all"
	task.RecordVersion = 6
	holder = world.addAssignment(tenant, task, true)
	other = world.addAssignment(tenant, task, true)
	in = port.ReassignAssignmentInput{
		TenantID: tenant.String(), TaskID: task.ID.String(),
		NewUserID: holder.UserID.String(), AdminUserID: uuid.NewString(), RecordVersion: 6,
	}
	return world, tenant, task, holder, other, in
}

func TestReassignAssignment_UnclaimedTaskToAnExistingHolder_MakesThemLeadAndVacatesNobody(t *testing.T) {
	t.Parallel()
	world, tenant, task, holder, other, in := wkruSetup(t)
	deps, tx := wkactNewDeps(t, world)

	require.NoError(t, deps.ReassignAssignment(context.Background(), in))

	assert.True(t, world.assignments[holder.ID].IsLead, "the holder now leads")
	assert.True(t, world.assignments[holder.ID].IsActive)
	assert.True(t, world.assignments[other.ID].IsActive, "nobody was replaced")
	assert.False(t, world.assignments[other.ID].IsLead)
	assert.Empty(t, world.callsTo("Assignments.Vacate"))
	assert.Empty(t, world.callsTo("Assignments.Create"))
	assert.Empty(t, world.callsTo("Assignments.ClaimLead"), "an admin's lead move is not a claim")
	setLead := world.callsTo("Assignments.SetLead")
	require.Len(t, setLead, 1)
	assert.Equal(t, task.ID, setLead[0].ID)
	assert.Equal(t, int64(6), setLead[0].Version)
	events := world.eventsOfType(domain.EventWorkflowTaskReassigned)
	require.Len(t, events, 1, "the record of the move is matched by a move")
	fields := wkactPayloadFields(t, events[0])
	assert.NotContains(t, fields, "old_user_id", "no previous user is recorded")
	assert.JSONEq(t, `"`+holder.UserID.String()+`"`, string(fields["new_user_id"]))
	assert.Equal(t, 1, tx.committed)
	world.requireAllCallsForTenant(t, tenant)
}

func TestReassignAssignment_UnclaimedTaskToAnExistingHolder_RetryChangesNothingMore(t *testing.T) {
	t.Parallel()
	world, _, _, _, _, in := wkruSetup(t)
	deps, _ := wkactNewDeps(t, world)

	require.NoError(t, deps.ReassignAssignment(context.Background(), in))
	require.NoError(t, deps.ReassignAssignment(context.Background(), in))

	assert.Len(t, world.callsTo("Assignments.SetLead"), 1)
	assert.Len(t, world.eventsOfType(domain.EventWorkflowTaskReassigned), 1)
}

func TestReassignAssignment_LedTaskToANonLeadHolderWithoutNamingTheLead_ChangesNothing(t *testing.T) {
	t.Parallel()
	world, _, _, holder, other, in := wkruSetup(t)
	world.assignments[other.ID].IsLead = true
	deps, _ := wkactNewDeps(t, world)

	require.NoError(t, deps.ReassignAssignment(context.Background(), in))

	assert.Empty(t, world.callsTo("Assignments.SetLead"))
	assert.False(t, world.assignments[holder.ID].IsLead)
	assert.True(t, world.assignments[other.ID].IsLead, "a lead nobody replaced keeps the task")
	assert.Empty(t, world.eventsOfType(domain.EventWorkflowTaskReassigned))
}
