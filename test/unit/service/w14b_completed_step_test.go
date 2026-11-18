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
)

var errW14bListTasks = errors.New("w14b task listing down")

type w14bListCall struct {
	tenantID   uuid.UUID
	instanceID uuid.UUID
	after      *port.Cursor
	limit      int
}

// w14bTaskPager serves scripted pages of an instance's tasks and records every
// listing request it receives; every other repository call is the shared fake's.
type w14bTaskPager struct {
	*fakeTaskRepo
	pages  [][]*domain.Task
	failAt int
	calls  []w14bListCall
}

func w14bPageCursor(idx int) port.Cursor {
	return port.Cursor{
		CreatedAt: time.Unix(1_700_000_000+int64(idx), 0).UTC(),
		ID:        uuid.NewSHA1(uuid.NameSpaceOID, []byte{byte(idx)}),
	}
}

func (p *w14bTaskPager) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, page port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	idx := len(p.calls)
	call := w14bListCall{tenantID: tenantID, instanceID: instanceID, limit: page.Limit}
	if page.After != nil {
		after := *page.After
		call.after = &after
	}
	p.calls = append(p.calls, call)
	if idx == p.failAt {
		return nil, nil, errW14bListTasks
	}
	if idx >= len(p.pages) {
		return nil, nil, errors.New("w14b: listing read past the last scripted page")
	}
	if idx == len(p.pages)-1 {
		return p.pages[idx], nil, nil
	}
	next := w14bPageCursor(idx)
	return p.pages[idx], &next, nil
}

func w14bTasksWithStatus(tenantID, instanceID uuid.UUID, status domain.TaskStatus, n int) []*domain.Task {
	out := make([]*domain.Task, 0, n)
	for range n {
		task := newReadyTask(tenantID, instanceID)
		task.Status = status
		out = append(out, task)
	}
	return out
}

type w14bDeferRig struct {
	svcDefer func() (*port.Task, error)
	pager    *w14bTaskPager
	temporal *fakeTemporalClient
	tenantID uuid.UUID
	instance uuid.UUID
}

func w14bNewDeferRig(pages [][]*domain.Task, failAt int, tenantID, instanceID uuid.UUID) *w14bDeferRig {
	svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
	instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz"}
	task := newReadyTask(tenantID, instanceID)
	tasks.byID[task.ID] = task
	userID := uuid.New()
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
	pager := &w14bTaskPager{fakeTaskRepo: tasks, pages: pages, failAt: failAt}
	svc.Tasks = pager
	return &w14bDeferRig{
		svcDefer: func() (*port.Task, error) {
			return svc.Defer(context.Background(), tenantID, task.ID, userID, "not my department", task.RecordVersion)
		},
		pager: pager, temporal: temporal, tenantID: tenantID, instance: instanceID,
	}
}

// w14bAssertListing checks the first n listing calls: the caller's tenant and
// instance, 50 rows a page, the first without a cursor and each next one with
// the cursor the previous page returned.
func w14bAssertListing(t *testing.T, rig *w14bDeferRig, n int) {
	t.Helper()
	require.Len(t, rig.pager.calls, n)
	for i, call := range rig.pager.calls {
		assert.Equal(t, rig.tenantID, call.tenantID, "call %d", i)
		assert.Equal(t, rig.instance, call.instanceID, "call %d", i)
		assert.Equal(t, 50, call.limit, "call %d", i)
		if i == 0 {
			assert.Nil(t, call.after)
			continue
		}
		require.NotNil(t, call.after, "call %d", i)
		assert.Equal(t, w14bPageCursor(i-1), *call.after, "call %d", i)
	}
}

func TestTaskServiceDefer_CompletedStepSearchPagesFiftyAtATime(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		completedPage int
		wantListings  int
		wantRefused   bool
	}{
		{name: "found on the first page, no second page is read", completedPage: 0, wantListings: 1},
		{name: "found on the second page, the third page is never read", completedPage: 1, wantListings: 2},
		{name: "found on the third page, all three pages are read", completedPage: 2, wantListings: 3},
		{name: "found nowhere, every page is read and the defer is refused", completedPage: -1, wantListings: 3, wantRefused: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tenantID, instanceID := uuid.New(), uuid.New()
			pages := make([][]*domain.Task, 3)
			for i := range pages {
				pages[i] = w14bTasksWithStatus(tenantID, instanceID, domain.TaskStatusReady, 49)
				if i == tc.completedPage {
					pages[i] = append(pages[i], w14bTasksWithStatus(tenantID, instanceID, domain.TaskStatusCompleted, 1)...)
				} else {
					pages[i] = append(pages[i], w14bTasksWithStatus(tenantID, instanceID, domain.TaskStatusFailed, 1)...)
				}
			}
			rig := w14bNewDeferRig(pages, -1, tenantID, instanceID)

			_, err := rig.svcDefer()

			w14bAssertListing(t, rig, tc.wantListings)
			if tc.wantRefused {
				require.ErrorIs(t, err, port.ErrInvalidTaskState)
				assert.Empty(t, rig.temporal.updates)
				return
			}
			require.NoError(t, err)
			require.Len(t, rig.temporal.updates, 1)
			assert.Equal(t, "stage-defer", rig.temporal.updates[0].SignalName)
			assert.Equal(t, instanceID, rig.temporal.updates[0].InstanceID)
		})
	}
}

func TestTaskServiceDefer_OnlyACompletedTaskCountsAsACompletedStep(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		statuses    []domain.TaskStatus
		wantRefused bool
	}{
		{name: "no tasks at all", statuses: nil, wantRefused: true},
		{name: "a failed task", statuses: []domain.TaskStatus{domain.TaskStatusFailed}, wantRefused: true},
		{name: "a superseded task", statuses: []domain.TaskStatus{domain.TaskStatusSuperseded}, wantRefused: true},
		{name: "a deferred task", statuses: []domain.TaskStatus{domain.TaskStatusDeferred}, wantRefused: true},
		{name: "a ready task", statuses: []domain.TaskStatus{domain.TaskStatusReady}, wantRefused: true},
		{name: "an in-progress task", statuses: []domain.TaskStatus{domain.TaskStatusInProgress}, wantRefused: true},
		{
			name: "every other status together",
			statuses: []domain.TaskStatus{
				domain.TaskStatusFailed, domain.TaskStatusSuperseded, domain.TaskStatusDeferred,
				domain.TaskStatusReady, domain.TaskStatusInProgress,
			},
			wantRefused: true,
		},
		{
			name: "a completed task among the others",
			statuses: []domain.TaskStatus{
				domain.TaskStatusFailed, domain.TaskStatusSuperseded, domain.TaskStatusCompleted,
				domain.TaskStatusDeferred, domain.TaskStatusReady, domain.TaskStatusInProgress,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tenantID, instanceID := uuid.New(), uuid.New()
			page := make([]*domain.Task, 0, len(tc.statuses))
			for _, status := range tc.statuses {
				page = append(page, w14bTasksWithStatus(tenantID, instanceID, status, 1)...)
			}
			rig := w14bNewDeferRig([][]*domain.Task{page}, -1, tenantID, instanceID)

			_, err := rig.svcDefer()

			w14bAssertListing(t, rig, 1)
			if tc.wantRefused {
				require.ErrorIs(t, err, port.ErrInvalidTaskState)
				assert.Empty(t, rig.temporal.updates)
				assert.Empty(t, rig.temporal.signals)
				return
			}
			require.NoError(t, err)
			require.Len(t, rig.temporal.updates, 1)
			assert.Equal(t, "stage-defer", rig.temporal.updates[0].SignalName)
		})
	}
}

func TestTaskServiceDefer_TaskListingFailureIsReturnedWrapped(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		failAt       int
		wantListings int
	}{
		{name: "the first page fails", failAt: 0, wantListings: 1},
		{name: "a later page fails after an earlier one had no match", failAt: 1, wantListings: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tenantID, instanceID := uuid.New(), uuid.New()
			pages := [][]*domain.Task{
				w14bTasksWithStatus(tenantID, instanceID, domain.TaskStatusReady, 50),
				w14bTasksWithStatus(tenantID, instanceID, domain.TaskStatusCompleted, 1),
			}
			rig := w14bNewDeferRig(pages, tc.failAt, tenantID, instanceID)

			_, err := rig.svcDefer()

			require.ErrorIs(t, err, errW14bListTasks)
			assert.NotErrorIs(t, err, port.ErrInvalidTaskState, "a failed read is not 'no completed step'")
			assert.Contains(t, err.Error(), "list tasks by instance")
			w14bAssertListing(t, rig, tc.wantListings)
			assert.Empty(t, rig.temporal.updates)
			assert.Empty(t, rig.temporal.signals)
		})
	}
}
