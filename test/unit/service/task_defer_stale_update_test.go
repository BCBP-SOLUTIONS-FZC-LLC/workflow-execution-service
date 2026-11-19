package service_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func TestTaskService_Defer_AVersionConflictFromTheWorkflowIsCountedAndReturnedAsOne(t *testing.T) {
	obstest.Init(t)
	svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
	tenantID, instanceID := uuid.New(), uuid.New()
	instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz"}
	done := newReadyTask(tenantID, instanceID)
	done.Status = domain.TaskStatusCompleted
	tasks.byID[done.ID] = done
	task := newReadyTask(tenantID, instanceID)
	tasks.byID[task.ID] = task
	userID := uuid.New()
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
	temporal.updateFunc = func(context.Context, string, uuid.UUID, string, any) error {
		return fmt.Errorf("update workflow: %w", port.ErrRecordVersionConflict)
	}
	before := conflictCount(t, "workflow_task")

	_, err := svc.Defer(context.Background(), tenantID, task.ID, userID, "needs rework", task.RecordVersion)

	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.Equal(t, before+1, conflictCount(t, "workflow_task"))
}
