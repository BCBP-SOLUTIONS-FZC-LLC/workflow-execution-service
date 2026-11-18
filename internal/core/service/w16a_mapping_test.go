package service

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestToPortTask_EveryFieldIsCopiedAndTheStatusAndTypeAreDerived(t *testing.T) {
	tenantID, instanceID, deptID, deferredFrom := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	id := uuid.New()
	created := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	updated := created.Add(time.Hour)
	due := created.Add(48 * time.Hour)
	followUp := created.Add(24 * time.Hour)
	completed := created.Add(2 * time.Hour)
	connector := "send-email"
	extras := json.RawMessage(`{"resolved_inputs":{"to":"a@b.c"}}`)

	cases := []struct {
		name string
		task *domain.Task
		want *port.Task
	}{
		{
			name: "a connector task with every optional field set",
			task: &domain.Task{
				ID: id, TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/review", DepartmentID: deptID,
				Status: domain.TaskStatusCompleted, RecordVersion: 9, VisitCount: 4, AssigneeMode: "all", ConnectorType: &connector,
				ExtrasJSON: extras, DeferredFromTaskID: &deferredFrom, DueAt: &due, FollowUpAt: &followUp,
				CreatedAt: created, UpdatedAt: updated, CompletedAt: &completed,
			},
			want: &port.Task{
				ID: id, TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/review", DepartmentID: deptID,
				Status: port.TaskStatus("COMPLETED"), RecordVersion: 9, TaskType: "serviceTask", AssigneeMode: "all", ConnectorType: &connector,
				ExtrasJSON: extras, DeferredFromTaskID: &deferredFrom, DueAt: &due, FollowUpAt: &followUp,
				CreatedAt: created, UpdatedAt: updated, CompletedAt: &completed,
			},
		},
		{
			name: "a user task with no optional field set",
			task: &domain.Task{
				ID: id, TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "legal/sign", DepartmentID: deptID,
				Status: domain.TaskStatusInProgress, RecordVersion: 2, AssigneeMode: "single", CreatedAt: created, UpdatedAt: updated,
			},
			want: &port.Task{
				ID: id, TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "legal/sign", DepartmentID: deptID,
				Status: port.TaskStatus("IN_PROGRESS"), RecordVersion: 2, TaskType: "userTask", AssigneeMode: "single",
				CreatedAt: created, UpdatedAt: updated,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := toPortTask(tc.task)

			assert.Equal(t, tc.want, got)
			assert.Zero(t, got.AssigneeCount, "the mapper does not know the assignee count")
			assert.False(t, got.AssigneeCountUnavailable)
		})
	}
}
