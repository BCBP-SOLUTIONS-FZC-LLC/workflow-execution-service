package handler_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type csfImpactAssignments struct {
	port.TaskAssignmentRepository
	active     []*domain.TaskAssignment
	gotTenants []uuid.UUID
	gotUsers   []uuid.UUID
}

func (r *csfImpactAssignments) ListActiveByUser(_ context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.gotTenants = append(r.gotTenants, tenantID)
	r.gotUsers = append(r.gotUsers, userID)
	return r.active, nil
}

type csfImpactTasks struct {
	port.TaskRepository
	byID       map[uuid.UUID]*domain.Task
	gotTenants []uuid.UUID
	gotTaskIDs []uuid.UUID
}

func (r *csfImpactTasks) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.gotTenants = append(r.gotTenants, tenantID)
	r.gotTaskIDs = append(r.gotTaskIDs, id)
	task, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return task, nil
}

type csfImpactBody struct {
	ActiveWorkflows   int         `json:"active_workflows"`
	ActiveAssignments int         `json:"active_assignments"`
	WorkflowIDs       []uuid.UUID `json:"workflow_ids"`
}

func TestDelegateImpact_UppercaseDelegationID_MatchesTheLowercaseReasonTag(t *testing.T) {
	delegationID := uuid.MustParse("abcdef01-2345-6789-abcd-ef0123456789")
	otherDelegationID := uuid.MustParse("fedcba98-7654-3210-fedc-ba9876543210")
	taggedInstance, otherInstance, untaggedInstance := uuid.New(), uuid.New(), uuid.New()

	newTasks := func() (*csfImpactTasks, []*domain.TaskAssignment) {
		tasks := &csfImpactTasks{byID: map[uuid.UUID]*domain.Task{}}
		var active []*domain.TaskAssignment
		for _, tc := range []struct {
			instance uuid.UUID
			reason   string
		}{
			{taggedInstance, "delegation:" + delegationID.String()},
			{otherInstance, "delegation:" + otherDelegationID.String()},
			{untaggedInstance, "manual-assign"},
		} {
			task := &domain.Task{ID: uuid.New(), TenantID: testTenantID, WorkflowInstanceID: tc.instance}
			tasks.byID[task.ID] = task
			active = append(active, &domain.TaskAssignment{
				ID: uuid.New(), TenantID: testTenantID, TaskID: task.ID, UserID: testOldDelegateID, IsActive: true, Reason: tc.reason,
			})
		}
		return tasks, active
	}

	query := func(t *testing.T, rawDelegationID string) (csfImpactBody, *csfImpactAssignments, *csfImpactTasks) {
		t.Helper()
		tasks, active := newTasks()
		assignments := &csfImpactAssignments{active: active}
		client := &service.WorkflowClient{Tasks: tasks, Assignments: assignments}
		router := newInternalRouter(handler.New(handler.Services{WorkflowClient: client}))

		url := "/api/v1/internal/workflows/delegate-impact?tenant_id=" + testTenantID.String() +
			"&delegate_user_id=" + testOldDelegateID.String() + "&delegation_id=" + rawDelegationID
		w := do(router, internalReq(http.MethodGet, url, nil))
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		var body csfImpactBody
		decodeJSON(t, w.Body, &body)
		return body, assignments, tasks
	}

	lower, _, _ := query(t, delegationID.String())
	upper, assignments, tasks := query(t, strings.ToUpper(delegationID.String()))

	require.NotEqual(t, delegationID.String(), strings.ToUpper(delegationID.String()), "the test id must have letters to change case")
	assert.Equal(t, 1, upper.ActiveAssignments)
	assert.Equal(t, 1, upper.ActiveWorkflows)
	assert.Equal(t, []uuid.UUID{taggedInstance}, upper.WorkflowIDs)
	assert.Equal(t, lower, upper, "an uppercase delegation_id selects the same work as the lowercase one")

	assert.Equal(t, []uuid.UUID{testTenantID}, assignments.gotTenants)
	assert.Equal(t, []uuid.UUID{testOldDelegateID}, assignments.gotUsers)
	assert.Equal(t, []uuid.UUID{testTenantID}, tasks.gotTenants, "only the matching assignment's task is read")
	require.Len(t, tasks.gotTaskIDs, 1)
	assert.Equal(t, assignments.active[0].TaskID, tasks.gotTaskIDs[0])
}
