package temporal_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// fx9HolderScene is an all-assignee task led by lead and also held, without the lead flag, by helper and
// by other.
type fx9HolderScene struct {
	world  *wkactWorld
	tenant uuid.UUID
	task   *domain.Task
	lead   *domain.TaskAssignment
	helper *domain.TaskAssignment
	other  *domain.TaskAssignment
}

func fx9NewHolderScene() *fx9HolderScene {
	world := wkactNewWorld()
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	task := world.addTask(tenant, inst, "sales/review", domain.TaskStatusInProgress)
	task.AssigneeMode = "all"
	task.RecordVersion = 4
	lead := world.addAssignment(tenant, task, true)
	lead.IsLead = true
	return &fx9HolderScene{
		world: world, tenant: tenant, task: task, lead: lead,
		helper: world.addAssignment(tenant, task, true), other: world.addAssignment(tenant, task, true),
	}
}

func (s *fx9HolderScene) input(oldUser, newUser uuid.UUID) port.ReassignAssignmentInput {
	return port.ReassignAssignmentInput{
		TenantID: s.tenant.String(), TaskID: s.task.ID.String(), OldUserID: oldUser.String(), NewUserID: newUser.String(),
		AdminUserID: uuid.NewString(), RecordVersion: s.task.RecordVersion,
	}
}

func fx9ReassignedUsers(t *testing.T, s *fx9HolderScene) (oldUser, newUser string) {
	t.Helper()
	events := s.world.eventsOfType(domain.EventWorkflowTaskReassigned)
	require.Len(t, events, 1)
	fields := wkactPayloadFields(t, events[0])
	require.NoError(t, json.Unmarshal(fields["old_user_id"], &oldUser))
	require.NoError(t, json.Unmarshal(fields["new_user_id"], &newUser))
	return oldUser, newUser
}

func TestReassignAssignment_ToAHolderWhenTheNamedOldUserIsACoAssigneeNotTheLead_VacatesThatCoAssigneeAndRecordsTheMove(t *testing.T) {
	t.Parallel()
	s := fx9NewHolderScene()
	deps, tx := wkactNewDeps(t, s.world)

	require.NoError(t, deps.ReassignAssignment(context.Background(), s.input(s.other.UserID, s.helper.UserID)))

	assert.False(t, s.world.assignments[s.other.ID].IsActive, "the named old co-assignee no longer holds the task")
	assert.True(t, s.world.assignments[s.lead.ID].IsActive, "the lead is not the one replaced")
	assert.True(t, s.world.assignments[s.lead.ID].IsLead)
	assert.True(t, s.world.assignments[s.helper.ID].IsActive)
	assert.False(t, s.world.assignments[s.helper.ID].IsLead, "nobody asked to change the lead")
	assert.Empty(t, s.world.callsTo("Assignments.Create"), "no second row for a user who already holds the task")
	assert.Empty(t, s.world.callsTo("Assignments.SetLead"))
	oldUser, newUser := fx9ReassignedUsers(t, s)
	assert.Equal(t, s.other.UserID.String(), oldUser)
	assert.Equal(t, s.helper.UserID.String(), newUser)
	assert.Equal(t, int64(5), s.task.RecordVersion, "the change moves the task version once")
	assert.Equal(t, 1, tx.committed, "the vacate and the event commit together")
	s.world.requireAllCallsForTenant(t, s.tenant)
}

func TestReassignAssignment_ToTheUserWhoAlreadyLeads_VacatesTheNamedOldUserAndRecordsTheMove(t *testing.T) {
	t.Parallel()
	s := fx9NewHolderScene()
	deps, tx := wkactNewDeps(t, s.world)

	require.NoError(t, deps.ReassignAssignment(context.Background(), s.input(s.helper.UserID, s.lead.UserID)))

	assert.False(t, s.world.assignments[s.helper.ID].IsActive, "the named old user no longer holds the task")
	assert.True(t, s.world.assignments[s.lead.ID].IsActive)
	assert.True(t, s.world.assignments[s.lead.ID].IsLead)
	assert.True(t, s.world.assignments[s.other.ID].IsActive, "an assignee who was not named stays")
	oldUser, newUser := fx9ReassignedUsers(t, s)
	assert.Equal(t, s.helper.UserID.String(), oldUser)
	assert.Equal(t, s.lead.UserID.String(), newUser)
	assert.Equal(t, int64(5), s.task.RecordVersion)
	assert.Equal(t, 1, tx.committed)
}

func TestReassignAssignment_ToAHolderWhoseCoAssigneeWasVacated_ARetryChangesNothingMore(t *testing.T) {
	t.Parallel()
	s := fx9NewHolderScene()
	deps, _ := wkactNewDeps(t, s.world)
	in := s.input(s.other.UserID, s.helper.UserID)

	require.NoError(t, deps.ReassignAssignment(context.Background(), in))
	require.NoError(t, deps.ReassignAssignment(context.Background(), in), "a retry after the attempt committed is done, not a conflict")

	assert.Len(t, s.world.callsTo("Assignments.Vacate"), 1)
	assert.Len(t, s.world.eventsOfType(domain.EventWorkflowTaskReassigned), 1)
	assert.Equal(t, int64(5), s.task.RecordVersion, "the task version moved once")
}

func TestReassignAssignment_ToAHolderWhenTheNamedOldUserIsTheSameUser_ChangesNothing(t *testing.T) {
	t.Parallel()
	s := fx9NewHolderScene()
	deps, tx := wkactNewDeps(t, s.world)

	require.NoError(t, deps.ReassignAssignment(context.Background(), s.input(s.helper.UserID, s.helper.UserID)))

	assert.True(t, s.world.assignments[s.helper.ID].IsActive, "a user is never vacated by being named as their own replacement")
	assert.Empty(t, s.world.callsTo("Assignments.Vacate"))
	assert.Empty(t, s.world.eventsOfType(domain.EventWorkflowTaskReassigned))
	assert.Equal(t, int64(4), s.task.RecordVersion)
	assert.Equal(t, 1, tx.committed)
}

func TestReassignAssignment_ToAHolderWhenTheNamedOldUserNoLongerHoldsTheTask_ChangesNothing(t *testing.T) {
	t.Parallel()
	s := fx9NewHolderScene()
	deps, _ := wkactNewDeps(t, s.world)

	require.NoError(t, deps.ReassignAssignment(context.Background(), s.input(uuid.New(), s.helper.UserID)))

	assert.Empty(t, s.world.callsTo("Assignments.Vacate"))
	assert.Empty(t, s.world.eventsOfType(domain.EventWorkflowTaskReassigned))
	assert.Equal(t, int64(4), s.task.RecordVersion)
	for _, a := range []*domain.TaskAssignment{s.lead, s.helper, s.other} {
		assert.True(t, s.world.assignments[a.ID].IsActive)
	}
}
