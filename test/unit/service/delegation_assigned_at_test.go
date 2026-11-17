package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func requireAssignedBetween(t *testing.T, a *domain.TaskAssignment, before, after time.Time) {
	t.Helper()
	require.NotNil(t, a.AssignedAt, "the replacement row carries the time it was assigned")
	assert.False(t, a.AssignedAt.Before(before), "assigned_at is the time of the move")
	assert.False(t, a.AssignedAt.After(after), "assigned_at is the time of the move")
	assert.Nil(t, a.AssignedBy, "a move made by a delegation has no assigning admin")
}

func assignmentOf(assignments *fakeAssignmentRepo, taskID, userID uuid.UUID) *domain.TaskAssignment {
	for _, a := range assignments.byID {
		if a.TaskID == taskID && a.UserID == userID && a.IsActive {
			return a
		}
	}
	return nil
}

func TestDelegationMoves_ReplacementAssignmentCarriesTheTimeOfTheMove(t *testing.T) {
	t.Run("reassign to a new delegate", func(t *testing.T) {
		h := csaNewClientHarness()
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		held := h.seed(tenantID, oldDelegate, "delegation:"+uuid.NewString(), domain.TaskStatusReady, domain.InstanceStatusRunning)

		before := time.Now()
		_, err := h.svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		after := time.Now()

		require.NoError(t, err)
		require.Len(t, h.assignments.creation, 1)
		assert.Equal(t, newDelegate, h.assignments.creation[0].UserID)
		assert.Equal(t, held.task.ID, h.assignments.creation[0].TaskID)
		requireAssignedBetween(t, h.assignments.creation[0], before, after)
	})

	t.Run("reroute to a delegate", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
		_, task, _ := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)

		before := time.Now()
		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all"})
		after := time.Now()

		require.NoError(t, err)
		created := assignmentOf(assignments, task.ID, delegateID)
		require.NotNil(t, created)
		requireAssignedBetween(t, created, before, after)
	})

	t.Run("reversal back to the delegator", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		_, task, held := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegateID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
		held.Reason = "delegation:" + delegationID.String()

		before := time.Now()
		err := svc.Reverse(context.Background(), port.DelegationReversalInput{TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, EndedReason: "expired"})
		after := time.Now()

		require.NoError(t, err)
		restored := assignmentOf(assignments, task.ID, delegatorID)
		require.NotNil(t, restored)
		requireAssignedBetween(t, restored, before, after)
	})
}
