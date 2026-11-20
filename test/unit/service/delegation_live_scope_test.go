package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestWorkflowClient_DelegateWork_CoversOnlyOpenTasksOfLiveInstances(t *testing.T) {
	cases := []struct {
		name     string
		task     domain.TaskStatus
		instance domain.InstanceStatus
		inScope  bool
	}{
		{"a READY task of a running instance", domain.TaskStatusReady, domain.InstanceStatusRunning, true},
		{"an IN_PROGRESS task of a running instance", domain.TaskStatusInProgress, domain.InstanceStatusRunning, true},
		{"a READY task of a paused instance", domain.TaskStatusReady, domain.InstanceStatusPaused, true},
		{"a READY task of a degraded instance", domain.TaskStatusReady, domain.InstanceStatusDegraded, true},
		{"a COMPLETED task", domain.TaskStatusCompleted, domain.InstanceStatusRunning, false},
		{"a FAILED task", domain.TaskStatusFailed, domain.InstanceStatusRunning, false},
		{"a SUPERSEDED task", domain.TaskStatusSuperseded, domain.InstanceStatusRunning, false},
		{"a DEFERRED task", domain.TaskStatusDeferred, domain.InstanceStatusRunning, false},
		{"a READY task of a completed instance", domain.TaskStatusReady, domain.InstanceStatusCompleted, false},
		{"a READY task of a terminated instance", domain.TaskStatusReady, domain.InstanceStatusTerminated, false},
		{"a READY task of a failed instance", domain.TaskStatusReady, domain.InstanceStatusFailed, false},
	}
	for _, tc := range cases {
		reason := "delegation:" + uuid.NewString()
		want := 0
		if tc.inScope {
			want = 1
		}

		t.Run("reassign moves "+tc.name+" only when in scope", func(t *testing.T) {
			h := csaNewClientHarness()
			tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
			held := h.seed(tenantID, oldDelegate, reason, tc.task, tc.instance)

			count, err := h.svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{
				TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate,
			})

			require.NoError(t, err)
			assert.Equal(t, want, count)
			assert.Equal(t, !tc.inScope, held.assignment.IsActive)
		})

		t.Run("impact counts "+tc.name+" only when in scope", func(t *testing.T) {
			h := csaNewClientHarness()
			tenantID, delegate := uuid.New(), uuid.New()
			held := h.seed(tenantID, delegate, reason, tc.task, tc.instance)

			got, err := h.svc.DelegateImpact(context.Background(), port.DelegateImpactInput{
				TenantID: tenantID, DelegateUserID: delegate, Page: port.Page{Limit: 10},
			})

			require.NoError(t, err)
			assert.Equal(t, want, got.ActiveAssignments)
			assert.Equal(t, want, got.ActiveWorkflows)
			if tc.inScope {
				assert.Equal(t, []uuid.UUID{held.instance.ID}, got.WorkflowIDs.Items)
				return
			}
			assert.Empty(t, got.WorkflowIDs.Items)
		})
	}
}
