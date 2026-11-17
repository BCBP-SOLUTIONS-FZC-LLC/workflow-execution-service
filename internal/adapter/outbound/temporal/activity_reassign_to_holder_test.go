package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// wkrhSetup is an all-assignee task led by lead and also held, without the lead
// flag, by helper.
func wkrhSetup(t *testing.T) (world *wkactWorld, tenant uuid.UUID, task *domain.Task, lead, helper *domain.TaskAssignment, in port.ReassignAssignmentInput) {
	t.Helper()
	world = wkactNewWorld()
	tenant = uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	task = world.addTask(tenant, inst, "sales/review", domain.TaskStatusInProgress)
	task.AssigneeMode = "all"
	task.RecordVersion = 4
	lead = world.addAssignment(tenant, task, true)
	lead.IsLead = true
	helper = world.addAssignment(tenant, task, true)
	in = port.ReassignAssignmentInput{
		TenantID: tenant.String(), TaskID: task.ID.String(), OldUserID: lead.UserID.String(),
		NewUserID: helper.UserID.String(), AdminUserID: uuid.NewString(), RecordVersion: 4,
	}
	return world, tenant, task, lead, helper, in
}

func TestReassignAssignment_ToACoAssigneeWhoIsNotLead_MakesThemLeadAndVacatesTheReplacedLead(t *testing.T) {
	t.Parallel()
	world, tenant, task, lead, helper, in := wkrhSetup(t)
	deps, tx := wkactNewDeps(t, world)

	require.NoError(t, deps.ReassignAssignment(context.Background(), in))

	assert.True(t, world.assignments[helper.ID].IsLead, "the co-assignee leads the task")
	assert.True(t, world.assignments[helper.ID].IsActive)
	assert.False(t, world.assignments[lead.ID].IsActive, "the replaced lead is vacated")
	assert.Len(t, world.callsTo("Assignments.Create"), 0, "no second row for a user who already holds the task")
	assert.Empty(t, world.callsTo("Assignments.ClaimLead"), "an admin's lead move is not a claim")
	setLead := world.callsTo("Assignments.SetLead")
	require.Len(t, setLead, 1)
	assert.Equal(t, int64(4), setLead[0].Version, "the lead change is made at the version the request was read at")
	events := world.eventsOfType(domain.EventWorkflowTaskReassigned)
	require.Len(t, events, 1, "the record of the move is matched by a move")
	assert.Equal(t, 1, tx.committed)
	world.requireAllCallsForTenant(t, tenant)
	assert.Equal(t, task.ID, setLead[0].ID, "the lead is set on this task")
}

func TestReassignAssignment_ToACoAssigneeWhoIsNotLead_RetryChangesNothingMore(t *testing.T) {
	t.Parallel()
	world, _, _, _, _, in := wkrhSetup(t)
	deps, _ := wkactNewDeps(t, world)

	require.NoError(t, deps.ReassignAssignment(context.Background(), in))
	require.NoError(t, deps.ReassignAssignment(context.Background(), in))

	assert.Len(t, world.callsTo("Assignments.SetLead"), 1)
	assert.Len(t, world.eventsOfType(domain.EventWorkflowTaskReassigned), 1)
}

func TestReassignAssignment_ToTheUserWhoIsAlreadyLead_ChangesNothing(t *testing.T) {
	t.Parallel()
	world, _, _, lead, helper, in := wkrhSetup(t)
	in.NewUserID, in.OldUserID = lead.UserID.String(), helper.UserID.String()
	deps, _ := wkactNewDeps(t, world)

	require.NoError(t, deps.ReassignAssignment(context.Background(), in))

	assert.Empty(t, world.callsTo("Assignments.SetLead"))
	assert.Empty(t, world.callsTo("Assignments.Vacate"))
	assert.Empty(t, world.eventsOfType(domain.EventWorkflowTaskReassigned))
	assert.True(t, world.assignments[helper.ID].IsActive)
}

func TestReassignAssignment_ToACoAssigneeWithAStaleVersion_IsANonRetryableConflict(t *testing.T) {
	t.Parallel()
	world, _, _, lead, _, in := wkrhSetup(t)
	in.RecordVersion = 3
	deps, _ := wkactNewDeps(t, world)
	deps.Assignments = staleLeadAssignments{wkactAssignments{w: world}}

	err := deps.ReassignAssignment(context.Background(), in)

	require.ErrorIs(t, err, domain.ErrRecordVersionConflict)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	assert.Equal(t, "VersionConflict", appErr.Type())
	assert.True(t, world.assignments[lead.ID].IsActive, "the whole change is rolled back")
	assert.Empty(t, world.eventsOfType(domain.EventWorkflowTaskReassigned))
}

// staleLeadAssignments refuses a lead change made at an older task version, as
// the real repository does.
type staleLeadAssignments struct {
	wkactAssignments
}

func (r staleLeadAssignments) SetLead(ctx context.Context, tenant, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	if task := r.w.tasks[taskID]; task.RecordVersion != version {
		return nil, domain.ErrRecordVersionConflict
	}
	return r.wkactAssignments.SetLead(ctx, tenant, taskID, id, version)
}
