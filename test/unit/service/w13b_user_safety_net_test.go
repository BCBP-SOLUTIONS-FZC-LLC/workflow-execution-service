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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// w13bVacateRepo records the tenant and user of each tenant-wide vacate and answers with a chosen error.
type w13bVacateRepo struct {
	port.TaskAssignmentRepository
	err   error
	calls []csdIDCall
}

func (r *w13bVacateRepo) VacateAllActiveByUser(_ context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.calls = append(r.calls, csdIDCall{TenantID: tenantID, ID: userID})
	return nil, r.err
}

func TestUserSafetyNetReconciler_VacateFails_ReturnsTheCause(t *testing.T) {
	cases := []struct {
		name  string
		cause error
	}{
		{"a plain failure", errW13bRepo},
		{"a cancelled request", context.Canceled},
		{"a failure that already wraps another", fmt.Errorf("w13b: wrapped: %w", errW13bRepo)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &w13bVacateRepo{err: tc.cause}
			svc := &service.UserSafetyNetReconciler{Assignments: repo}
			tenantID, userID := uuid.New(), uuid.New()

			err := svc.VacateAssignments(context.Background(), port.UserDeletedInput{TenantID: tenantID, UserID: userID})

			require.ErrorIs(t, err, tc.cause)
			assert.Equal(t, []csdIDCall{{TenantID: tenantID, ID: userID}}, repo.calls, "one tenant-wide vacate, for the deleted user's tenant and id")
		})
	}

	t.Run("control: a repository that succeeds gives no error", func(t *testing.T) {
		repo := &w13bVacateRepo{}
		svc := &service.UserSafetyNetReconciler{Assignments: repo}
		tenantID, userID := uuid.New(), uuid.New()

		require.NoError(t, svc.VacateAssignments(context.Background(), port.UserDeletedInput{TenantID: tenantID, UserID: userID}))
		assert.Equal(t, []csdIDCall{{TenantID: tenantID, ID: userID}}, repo.calls)
	})
}
