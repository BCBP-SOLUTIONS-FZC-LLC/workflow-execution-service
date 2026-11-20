package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var errW11bListAssignments = errors.New("w11b: listing active assignments failed")

type w11bUserListCall struct {
	tenantID uuid.UUID
	userID   uuid.UUID
}

// w11bListingAssignments records every listing by user and can fail it.
type w11bListingAssignments struct {
	*fakeAssignmentRepo
	err   error
	calls []w11bUserListCall
}

func (r *w11bListingAssignments) ListActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.calls = append(r.calls, w11bUserListCall{tenantID: tenantID, userID: userID})
	if r.err != nil {
		return nil, r.err
	}
	return r.fakeAssignmentRepo.ListActiveByUser(ctx, tenantID, userID)
}

type w11bImpactHarness struct {
	svc         *service.WorkflowClient
	instances   *fakeInstanceRepo
	assignments *w11bListingAssignments
	tasks       *w11bRecordingTasks
	tenantID    uuid.UUID
	delegate    uuid.UUID
	held        []*domain.Task
}

func newW11bImpactHarness() *w11bImpactHarness {
	svc, instances, tasks, assignments, _ := newWorkflowClientHarness()
	h := &w11bImpactHarness{
		instances:   instances,
		assignments: &w11bListingAssignments{fakeAssignmentRepo: assignments},
		tasks:       &w11bRecordingTasks{fakeTaskRepo: tasks},
		tenantID:    uuid.New(),
		delegate:    uuid.New(),
	}
	svc.Assignments, svc.Tasks = h.assignments, h.tasks
	h.svc = svc
	return h
}

// holdTaskOf gives the delegate an active assignment on a new task of the instance.
func (h *w11bImpactHarness) holdTaskOf(instanceID uuid.UUID) *domain.Task {
	task := &domain.Task{
		ID: uuid.New(), TenantID: h.tenantID, WorkflowInstanceID: instanceID,
		CreatedAt: time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC).Add(time.Duration(len(h.held)) * time.Minute),
	}
	h.held = append(h.held, task)
	h.tasks.byID[task.ID] = task
	if _, ok := h.instances.byID[instanceID]; !ok {
		h.instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: h.tenantID}
	}
	h.assignments.add(&domain.TaskAssignment{
		ID: uuid.New(), TenantID: h.tenantID, TaskID: task.ID, UserID: h.delegate, IsActive: true, Reason: "delegation:" + uuid.NewString(),
	})
	return task
}

func (h *w11bImpactHarness) impact(page port.Page) (port.DelegateImpactResult, error) {
	return h.svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: h.tenantID, DelegateUserID: h.delegate, Page: page})
}

func TestWorkflowClient_DelegateImpact_LimitReachingTheLastWorkflow_HasNoNextCursor(t *testing.T) {
	t.Parallel()

	ids := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		uuid.MustParse("00000000-0000-0000-0000-000000000003"),
	}
	cases := []struct {
		name       string
		after      *uuid.UUID
		limit      int
		wantIDs    []uuid.UUID
		wantCursor bool
	}{
		{name: "limit one below the remaining count leaves a cursor", limit: 2, wantIDs: ids[:2], wantCursor: true},
		{name: "limit equal to the remaining count", limit: 3, wantIDs: ids},
		{name: "limit above the remaining count", limit: 4, wantIDs: ids},
		{name: "limit far above the remaining count", limit: 100, wantIDs: ids},
		{name: "after a cursor, limit equal to what remains", after: &ids[0], limit: 2, wantIDs: ids[1:]},
		{name: "after a cursor, limit above what remains", after: &ids[0], limit: 5, wantIDs: ids[1:]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newW11bImpactHarness()
			for _, id := range ids {
				h.holdTaskOf(id)
			}
			page := port.Page{Limit: tc.limit}
			if tc.after != nil {
				page.Cursor = &port.CursorPosition{ID: *tc.after}
			}

			result, err := h.impact(page)

			require.NoError(t, err)
			assert.Equal(t, tc.wantIDs, result.WorkflowIDs.Items)
			assert.Equal(t, tc.wantCursor, result.WorkflowIDs.NextCursor != "")
			if tc.wantCursor {
				pos, decodeErr := port.DecodeCursor(result.WorkflowIDs.NextCursor)
				require.NoError(t, decodeErr, "a cursor handed out can be read back")
				last := tc.wantIDs[len(tc.wantIDs)-1]
				assert.Equal(t, last, pos.ID, "the cursor names the last id of the page")
				var lastCreated time.Time
				for _, task := range h.held {
					if task.WorkflowInstanceID == last {
						lastCreated = task.CreatedAt
					}
				}
				assert.True(t, pos.CreatedAt.Equal(lastCreated), "the cursor carries that workflow's creation time")
			}
			assert.Equal(t, 3, result.ActiveWorkflows, "the totals do not depend on the page")
			assert.Equal(t, 3, result.ActiveAssignments)
			assert.Equal(t, []w11bUserListCall{{tenantID: h.tenantID, userID: h.delegate}}, h.assignments.calls)
			var wantReads []w11bIDCall
			for _, task := range h.held {
				wantReads = append(wantReads, w11bIDCall{tenantID: h.tenantID, id: task.ID})
			}
			assert.Equal(t, wantReads, h.tasks.gets, "every task is read in the caller's tenant")
		})
	}
}

func TestWorkflowClient_DelegateImpact_AnUnreadableTask_FailsTheCallWithNoCount(t *testing.T) {
	t.Parallel()

	h := newW11bImpactHarness()
	first := h.holdTaskOf(uuid.New())
	missing := h.holdTaskOf(uuid.New())
	delete(h.tasks.byID, missing.ID)

	result, err := h.impact(port.Page{Limit: 10})

	require.ErrorIs(t, err, domain.ErrNotFound, "the read error is returned, not a short count")
	assert.Equal(t, port.DelegateImpactResult{}, result, "nothing of the preview is shown")
	assert.Equal(t, []w11bIDCall{
		{tenantID: h.tenantID, id: first.ID}, {tenantID: h.tenantID, id: missing.ID},
	}, h.tasks.gets, "the tasks were asked for in the caller's tenant, up to the missing one")
}

func TestWorkflowClient_DelegateImpact_ListingTheAssignmentsFails_ReadsNoTaskAndShowsNothing(t *testing.T) {
	t.Parallel()

	h := newW11bImpactHarness()
	h.holdTaskOf(uuid.New())
	h.assignments.err = errW11bListAssignments

	result, err := h.impact(port.Page{Limit: 10})

	require.ErrorIs(t, err, errW11bListAssignments)
	assert.Equal(t, port.DelegateImpactResult{}, result)
	assert.Equal(t, []w11bUserListCall{{tenantID: h.tenantID, userID: h.delegate}}, h.assignments.calls)
	assert.Empty(t, h.tasks.gets, "no task is read once the listing failed")
}
