package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

func TestUserSafetyNetReconciler_VacateAssignments(t *testing.T) {
	t.Run("vacates every active assignment for the user tenant-wide", func(t *testing.T) {
		assignments := newFakeAssignmentRepo()
		tenantID, userID := uuid.New(), uuid.New()
		a1 := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: uuid.New(), UserID: userID, IsActive: true}
		a2 := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: uuid.New(), UserID: userID, IsActive: true}
		other := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: uuid.New(), UserID: uuid.New(), IsActive: true}
		assignments.byID[a1.ID] = a1
		assignments.byID[a2.ID] = a2
		assignments.byID[other.ID] = other

		svc := &service.UserSafetyNetReconciler{Assignments: assignments}
		err := svc.VacateAssignments(context.Background(), port.UserDeletedInput{TenantID: tenantID, UserID: userID})
		require.NoError(t, err)
		assert.False(t, assignments.byID[a1.ID].IsActive)
		assert.False(t, assignments.byID[a2.ID].IsActive)
		assert.True(t, assignments.byID[other.ID].IsActive, "a different user's assignment must survive untouched")
	})

	t.Run("no active assignments is a valid, non-error outcome", func(t *testing.T) {
		svc := &service.UserSafetyNetReconciler{Assignments: newFakeAssignmentRepo()}
		err := svc.VacateAssignments(context.Background(), port.UserDeletedInput{TenantID: uuid.New(), UserID: uuid.New()})
		require.NoError(t, err)
	})

	t.Run("repo error passes through", func(t *testing.T) {
		assignments := newFakeAssignmentRepo()
		assignments.vacateAllErr = errors.New("boom")
		svc := &service.UserSafetyNetReconciler{Assignments: assignments}
		err := svc.VacateAssignments(context.Background(), port.UserDeletedInput{TenantID: uuid.New(), UserID: uuid.New()})
		assert.Error(t, err)
	})
}
