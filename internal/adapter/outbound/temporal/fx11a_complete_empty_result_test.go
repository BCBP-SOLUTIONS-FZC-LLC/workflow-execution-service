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

func TestCompleteAssignment_NoResultJSON_StoresNullNotAnEmptyDocument(t *testing.T) {
	t.Parallel()

	world := wkactNewWorld()
	deps, _ := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	task := world.addTask(tenant, inst, "sales/review", domain.TaskStatusInProgress)
	assignment := world.addAssignment(tenant, task, true)

	_, err := deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		TaskID: task.ID.String(), TenantID: tenant.String(), UserID: assignment.UserID.String(),
	})

	require.NoError(t, err)
	assert.Nil(t, world.assignments[assignment.ID].ResultJSON)
}
