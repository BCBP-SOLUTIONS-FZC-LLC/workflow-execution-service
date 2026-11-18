//go:build integration

package postgres_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type w17aInbox struct{ marks []uuid.UUID }

func (f *w17aInbox) IsProcessed(context.Context, uuid.UUID, string) (bool, error) { return false, nil }

func (f *w17aInbox) MarkProcessed(_ context.Context, eventID uuid.UUID, _ string) error {
	f.marks = append(f.marks, eventID)
	return nil
}

func (f *w17aInbox) Prune(context.Context, time.Duration) (int64, error) { return 0, nil }

var _ port.ProcessedEventRepository = (*w17aInbox)(nil)

// w17aSeedAssignment creates an instance, a task and one active assignment for userID in tenantID.
func w17aSeedAssignment(t *testing.T, instances *postgres.InstanceRepo, tasks *postgres.TaskRepo, assignments *postgres.TaskAssignmentRepo, tenantID, userID uuid.UUID) *domain.TaskAssignment {
	t.Helper()
	ctx := context.Background()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instances.Create(ctx, inst))
	task := newTask(tenantID, inst.ID)
	require.NoError(t, tasks.Create(ctx, task))
	a := newTaskAssignment(tenantID, task.ID, userID)
	require.NoError(t, assignments.Create(ctx, a))
	return a
}

func TestUserDeletedEvent_UserAlsoHoldsAssignmentsInAnotherTenant_OnlyTheEventTenantsAreVacated(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	tenantA, tenantB := uuid.New(), uuid.New()
	userID, otherUserID := uuid.New(), uuid.New()
	aFirst := w17aSeedAssignment(t, instances, tasks, assignments, tenantA, userID)
	aSecond := w17aSeedAssignment(t, instances, tasks, assignments, tenantA, userID)
	aOther := w17aSeedAssignment(t, instances, tasks, assignments, tenantA, otherUserID)
	bFirst := w17aSeedAssignment(t, instances, tasks, assignments, tenantB, userID)
	bSecond := w17aSeedAssignment(t, instances, tasks, assignments, tenantB, userID)

	inbox := &w17aInbox{}
	h := handler.New(handler.Services{
		ProcessedEvents: inbox,
		UserSafetyNet:   &service.UserSafetyNetReconciler{Assignments: assignments},
	})
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/events/user-profile", h.HandleUserProfileEvents)
	eventID := uuid.New()
	body := `{"id":"` + eventID.String() + `","type":"UserDeleted","tenant_id":"` + tenantA.String() +
		`","time":"2030-01-02T03:04:05Z","data":{"user_id":"` + userID.String() + `","deleted_at":"2030-01-02T03:04:05Z"}}`
	r := httptest.NewRequest(http.MethodPost, "/events/user-profile", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, []uuid.UUID{eventID}, inbox.marks)
	for _, vacated := range []*domain.TaskAssignment{aFirst, aSecond} {
		stored, err := assignments.GetByID(ctx, tenantA, vacated.ID)
		require.NoError(t, err)
		assert.False(t, stored.IsActive, "the event tenant's assignment of the deleted user is vacated")
	}
	stillActiveA, err := assignments.ListActiveByUser(ctx, tenantA, userID)
	require.NoError(t, err)
	assert.Empty(t, stillActiveA)
	other, err := assignments.GetByID(ctx, tenantA, aOther.ID)
	require.NoError(t, err)
	assert.True(t, other.IsActive, "another user's assignment in the same tenant is untouched")

	_, err = assignments.GetByID(ctx, tenantA, bFirst.ID)
	require.ErrorIs(t, err, domain.ErrNotFound, "tenant A cannot read tenant B's assignment")
	_, err = assignments.Vacate(ctx, tenantA, bFirst.ID)
	require.ErrorIs(t, err, domain.ErrNotFound, "tenant A cannot vacate tenant B's assignment")

	stillActiveB, err := assignments.ListActiveByUser(ctx, tenantB, userID)
	require.NoError(t, err)
	gotB := make([]uuid.UUID, 0, len(stillActiveB))
	for _, a := range stillActiveB {
		gotB = append(gotB, a.ID)
	}
	assert.ElementsMatch(t, []uuid.UUID{bFirst.ID, bSecond.ID}, gotB, "the same user's assignments in the other tenant stay active")
}
