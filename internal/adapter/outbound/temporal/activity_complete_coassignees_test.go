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

func TestCompleteAssignment_TheLeadsCompletionClosesTheCoAssigneesAssignments(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps, _ := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	task := world.addTask(tenant, inst, "sales/review", domain.TaskStatusInProgress)
	lead := world.addAssignment(tenant, task, true)
	lead.IsLead = true
	helperOne := world.addAssignment(tenant, task, true)
	helperTwo := world.addAssignment(tenant, task, true)

	out, err := deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		TenantID: tenant.String(), TaskID: task.ID.String(), UserID: lead.UserID.String(), ResultJSON: `{"ok":true}`,
	})

	require.NoError(t, err)
	assert.True(t, out.AllDone, "nothing is left open, so the task can be marked completed as the instance moves on")
	assert.False(t, world.assignments[helperOne.ID].IsActive)
	assert.False(t, world.assignments[helperTwo.ID].IsActive)
	assert.NotNil(t, world.assignments[lead.ID].CompletedAt)
	assert.Nil(t, world.assignments[helperOne.ID].CompletedAt, "a helper who did not answer has no result recorded")
	assert.Len(t, world.callsTo("Assignments.Vacate"), 2)
	completed := world.eventsOfType(domain.EventWorkflowTaskCompleted)
	assert.Len(t, completed, 1, "one completion event, for the person who answered")
	world.requireAllCallsForTenant(t, tenant)
}

func TestCompleteAssignment_ARetryAfterTheLeadCompleted_ChangesNothing(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps, _ := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	task := world.addTask(tenant, inst, "sales/review", domain.TaskStatusInProgress)
	lead := world.addAssignment(tenant, task, true)
	world.addAssignment(tenant, task, true)
	in := port.CompleteAssignmentInput{TenantID: tenant.String(), TaskID: task.ID.String(), UserID: lead.UserID.String(), ResultJSON: `{}`}

	_, err := deps.CompleteAssignment(context.Background(), in)
	require.NoError(t, err)
	vacatesAfterFirst := len(world.callsTo("Assignments.Vacate"))
	out, err := deps.CompleteAssignment(context.Background(), in)

	require.NoError(t, err)
	assert.True(t, out.AllDone)
	assert.Len(t, world.callsTo("Assignments.Vacate"), vacatesAfterFirst)
	assert.Len(t, world.eventsOfType(domain.EventWorkflowTaskCompleted), 1)
}

func TestCompleteAssignment_ASingleAssigneeTask_IsAllDoneWithoutVacating(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps, _ := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	task := world.addTask(tenant, inst, "sales/review", domain.TaskStatusInProgress)
	only := world.addAssignment(tenant, task, true)

	out, err := deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		TenantID: tenant.String(), TaskID: task.ID.String(), UserID: only.UserID.String(),
	})

	require.NoError(t, err)
	assert.True(t, out.AllDone)
	assert.Empty(t, world.callsTo("Assignments.Vacate"))
}
